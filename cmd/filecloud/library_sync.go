package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/mingming-cn/filecloud/internal/object"
	"golang.org/x/sys/unix"
)

const (
	syncRecoveryPrefix        = ".filecloud-internal-sync-"
	syncTombstonePrefix       = ".filecloud-internal-sync-trash-"
	maxSyncParentWalk         = 1024
	deleteCandidatePrefixLen  = 12
	_maxCandidateCommitBytes  = 64 << 10
	_maxCandidateHistoryBytes = 8 + maxSyncParentWalk*(_maxCandidateCommitBytes+4)
)

var (
	_candidateHistoryMagic = [4]byte{'F', 'C', 'H', '1'}
	_emptyCandidateHistory = []byte{'F', 'C', 'H', '1', 0, 0, 0, 0}
)

type pendingPublication struct {
	BaseCommit, BaseRoot, ExpectedHead, ExpectedETag string
	CandidateCommit, CandidateRoot                   string
	CandidateData                                    []byte
	CapturedCommit, CapturedRoot                     string
	CapturedData, CandidateHistory                   []byte
	DeletionCount, TrackedCount                      int64
	RequiresDeleteConfirmation                       bool
	DeleteConfirmed                                  bool
	LegacyRevalidationRequired                       bool
}

const _pendingPublicationWhere = `worktree = ? AND base_commit = ? AND base_root = ? AND expected_head = ?
	AND expected_etag = ? AND candidate_commit = ? AND candidate_root = ? AND candidate_data = ?
	AND captured_commit = ? AND captured_root = ? AND captured_data = ? AND candidate_history = ?
	AND deletion_count = ? AND tracked_count = ? AND requires_delete_confirmation = ? AND delete_confirmed = ?
	AND legacy_revalidation_required = ?`

type _pendingPublicationExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func _pendingPublicationArgs(worktree string, value pendingPublication) []any {
	return []any{worktree, value.BaseCommit, value.BaseRoot, value.ExpectedHead, value.ExpectedETag,
		value.CandidateCommit, value.CandidateRoot, value.CandidateData, value.CapturedCommit, value.CapturedRoot,
		value.CapturedData, value.CandidateHistory, value.DeletionCount, value.TrackedCount,
		value.RequiresDeleteConfirmation, value.DeleteConfirmed, value.LegacyRevalidationRequired}
}

func _execPendingPublicationCAS(ctx context.Context, execer _pendingPublicationExecer, statement string, statementArgs []any,
	worktree string, old pendingPublication, changedMessage string) error {
	result, err := execer.ExecContext(ctx, statement+" WHERE "+_pendingPublicationWhere,
		append(statementArgs, _pendingPublicationArgs(worktree, old)...)...)
	if err != nil {
		return err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return errors.Join(errors.New(changedMessage), err)
	}
	return nil
}

func _deletePendingPublication(ctx context.Context, execer _pendingPublicationExecer, worktree string,
	old pendingPublication, changedMessage string) error {
	return _execPendingPublicationCAS(ctx, execer, "DELETE FROM pending_publications", nil, worktree, old, changedMessage)
}

func _assertPendingPublication(ctx context.Context, execer _pendingPublicationExecer, worktree string,
	old pendingPublication) error {
	return _execPendingPublicationCAS(ctx, execer, "UPDATE pending_publications SET worktree = worktree", nil,
		worktree, old, "pending publication changed before remote publication")
}

func _encodeCandidateHistory(commits [][]byte) ([]byte, error) {
	if len(commits) > maxSyncParentWalk {
		return nil, errors.New("candidate history exceeds synchronization budget")
	}
	size := 8
	for _, data := range commits {
		if len(data) == 0 || len(data) > _maxCandidateCommitBytes || size > _maxCandidateHistoryBytes-4-len(data) {
			return nil, errors.New("candidate history metadata exceeds synchronization budget")
		}
		size += 4 + len(data)
	}
	result := make([]byte, size)
	copy(result, _candidateHistoryMagic[:])
	binary.BigEndian.PutUint32(result[4:8], uint32(len(commits)))
	offset := 8
	for _, data := range commits {
		binary.BigEndian.PutUint32(result[offset:offset+4], uint32(len(data)))
		offset += 4
		copy(result[offset:], data)
		offset += len(data)
	}
	return result, nil
}

func _decodeCandidateHistory(data []byte) ([][]byte, error) {
	if len(data) < 8 || len(data) > _maxCandidateHistoryBytes || !bytes.Equal(data[:4], _candidateHistoryMagic[:]) {
		return nil, errors.New("candidate history has an invalid encoding")
	}
	count := binary.BigEndian.Uint32(data[4:8])
	if count > maxSyncParentWalk {
		return nil, errors.New("candidate history exceeds synchronization budget")
	}
	result := make([][]byte, 0, int(count))
	offset := 8
	for range count {
		if len(data)-offset < 4 {
			return nil, errors.New("candidate history is truncated")
		}
		size := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		if size == 0 || size > _maxCandidateCommitBytes || uint64(size) > uint64(len(data)-offset) {
			return nil, errors.New("candidate history contains an invalid metadata length")
		}
		result = append(result, data[offset:offset+int(size)])
		offset += int(size)
	}
	if offset != len(data) {
		return nil, errors.New("candidate history has trailing data")
	}
	canonical, err := _encodeCandidateHistory(result)
	if err != nil || !bytes.Equal(canonical, data) {
		return nil, errors.New("candidate history is not canonical")
	}
	return result, nil
}

type deleteConfirmationRequiredError struct {
	candidate        string
	deleted, tracked int64
}

func (value *deleteConfirmationRequiredError) Error() string {
	percentage := 0.0
	if value.tracked > 0 {
		percentage = float64(value.deleted) * 100 / float64(value.tracked)
	}
	return fmt.Sprintf("candidate %s deletes %d of %d tracked paths (%.1f%%; protection threshold: more than 100 or at least 10%%); rerun sync --confirm-delete %s",
		value.candidate[:deleteCandidatePrefixLen], value.deleted, value.tracked, percentage, value.candidate[:deleteCandidatePrefixLen])
}

type syncRecovery struct {
	path, name, tombstone, kind, id, mtime string
	size                                   int64
	device, inode                          uint64
	completed                              bool
}

func syncLibrary(ctx context.Context, db *sql.DB, options bindOptions, binding clientBinding, stdout io.Writer, config libraryClientConfig) error {
	budget := _newReplayBudget()
	pending, err := loadPendingPublication(ctx, db, binding.Worktree)
	if err != nil {
		return err
	}
	if options.confirmDeleteSet && (pending == nil || (!pending.RequiresDeleteConfirmation && !pending.LegacyRevalidationRequired)) {
		return errors.New("--confirm-delete requires a protected pending deletion candidate for this worktree")
	}
	checkout, err := loadPendingCheckout(ctx, db, binding.ServerURL, binding.LibraryID, binding.Worktree)
	if err != nil {
		return err
	}
	if checkout != nil {
		return continueSyncCheckout(ctx, db, options, binding, *checkout, stdout, config)
	}

	snapshot, err := scanWorktreeWithConfig(options.worktreeRoot, options.scanConfig)
	if err != nil {
		return fmt.Errorf("scan worktree: %w", err)
	}
	head, err := getRemoteHead(ctx, options.base, binding.LibraryID, options.token)
	if err != nil {
		return err
	}
	if head.CommitID == nil {
		return errors.New("bound library Head is empty")
	}
	if pending != nil {
		return resumePublication(ctx, db, options, binding, snapshot, head, *pending, stdout, config, budget)
	}

	localChanged := snapshot.root != binding.SyncBaseRoot
	remoteChanged := *head.CommitID != binding.SyncBase
	switch {
	case !localChanged && !remoteChanged:
		if err := replacePathIndex(ctx, db, binding.Worktree, snapshot.paths); err != nil {
			return err
		}
		_, err = fmt.Fprintln(stdout, "library already synchronized")
		return err
	case localChanged && remoteChanged:
		return _startRecursiveMerge(ctx, db, options, binding, snapshot, head, stdout, config, budget)
	case remoteChanged:
		pendingCheckout := pendingCheckout{ServerURL: binding.ServerURL, LibraryID: binding.LibraryID, Worktree: binding.Worktree,
			UserID: binding.UserID, DeviceID: binding.DeviceID, TargetCommit: *head.CommitID, HeadETag: head.ETag, ApplyState: "pending"}
		if err := savePendingCheckout(ctx, db, pendingCheckout); err != nil {
			return err
		}
		return continueSyncCheckout(ctx, db, options, binding, pendingCheckout, stdout, config)
	default:
		data, id, err := canonicalCommit(binding.UserID, binding.DeviceID, snapshot.root, []string{binding.SyncBase}, config.now)
		if err != nil {
			return err
		}
		deleted, tracked := deletionStats(options.scanConfig.trackedPaths, snapshot.paths)
		protected := protectedDeletion(deleted, tracked)
		pending = &pendingPublication{BaseCommit: binding.SyncBase, BaseRoot: binding.SyncBaseRoot,
			ExpectedHead: *head.CommitID, ExpectedETag: head.ETag, CandidateCommit: id, CandidateRoot: snapshot.root,
			CandidateData: data, CapturedCommit: id, CapturedRoot: snapshot.root, CapturedData: data,
			CandidateHistory: _emptyCandidateHistory, DeletionCount: deleted, TrackedCount: tracked,
			RequiresDeleteConfirmation: protected}
		if err := savePendingPublication(ctx, db, binding.Worktree, *pending); err != nil {
			return err
		}
		if protected {
			return deletionConfirmationError(*pending)
		}
		return uploadAndPublishPending(ctx, db, options, binding, snapshot, *pending, stdout, config, budget)
	}
}

func loadPendingPublication(ctx context.Context, db *sql.DB, worktree string) (*pendingPublication, error) {
	var value pendingPublication
	var candidateLength, capturedLength, historyLength int
	err := db.QueryRowContext(ctx, `SELECT base_commit, base_root, expected_head, expected_etag, candidate_commit, candidate_root,
		length(candidate_data), CASE WHEN length(candidate_data) BETWEEN 1 AND 65536 THEN candidate_data END,
		captured_commit, captured_root, length(captured_data),
		CASE WHEN length(captured_data) BETWEEN 1 AND 65536 THEN captured_data END,
		length(candidate_history), CASE WHEN length(candidate_history) BETWEEN 8 AND 67112968 THEN candidate_history END,
		deletion_count, tracked_count, requires_delete_confirmation, delete_confirmed, legacy_revalidation_required
		FROM pending_publications WHERE worktree = ?`, worktree).Scan(
		&value.BaseCommit, &value.BaseRoot, &value.ExpectedHead, &value.ExpectedETag, &value.CandidateCommit,
		&value.CandidateRoot, &candidateLength, &value.CandidateData, &value.CapturedCommit, &value.CapturedRoot,
		&capturedLength, &value.CapturedData, &historyLength, &value.CandidateHistory, &value.DeletionCount,
		&value.TrackedCount, &value.RequiresDeleteConfirmation, &value.DeleteConfirmed, &value.LegacyRevalidationRequired)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read pending publication: %w", err)
	}
	if candidateLength != len(value.CandidateData) || capturedLength != len(value.CapturedData) ||
		historyLength != len(value.CandidateHistory) {
		return nil, errors.New("pending publication metadata exceeds synchronization budget")
	}
	return &value, nil
}

func savePendingPublication(ctx context.Context, db *sql.DB, worktree string, value pendingPublication) error {
	_, err := db.ExecContext(ctx, `INSERT INTO pending_publications(worktree, base_commit, base_root, expected_head,
		expected_etag, candidate_commit, candidate_root, candidate_data, captured_commit, captured_root, captured_data,
		candidate_history, deletion_count, tracked_count, requires_delete_confirmation, delete_confirmed,
		legacy_revalidation_required) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, worktree,
		value.BaseCommit, value.BaseRoot, value.ExpectedHead, value.ExpectedETag, value.CandidateCommit,
		value.CandidateRoot, value.CandidateData, value.CapturedCommit, value.CapturedRoot, value.CapturedData,
		value.CandidateHistory, value.DeletionCount, value.TrackedCount, value.RequiresDeleteConfirmation,
		value.DeleteConfirmed, value.LegacyRevalidationRequired)
	if err != nil {
		return fmt.Errorf("save pending publication: %w", err)
	}
	return nil
}

type _pendingMergeCommit struct {
	id     string
	data   []byte
	commit object.Commit
}

func _pendingMergeSequence(value pendingPublication, binding clientBinding) ([]_pendingMergeCommit, error) {
	if len(value.CapturedData) == 0 || len(value.CapturedData) > _maxCandidateCommitBytes ||
		len(value.CandidateData) == 0 || len(value.CandidateData) > _maxCandidateCommitBytes {
		return nil, errors.New("pending publication commit metadata exceeds synchronization budget")
	}
	captured, err := object.VerifyCommit(value.CapturedData, value.CapturedCommit)
	capturedLocal := len(captured.Parents) == 1 && captured.Parents[0] == binding.SyncBase
	capturedMerge := len(captured.Parents) == 2 && captured.Parents[0] != captured.Parents[1] &&
		captured.Parents[0] != value.CapturedCommit && captured.Parents[1] != value.CapturedCommit
	if err != nil || captured.AuthorUserID != binding.UserID || captured.DeviceID != binding.DeviceID ||
		captured.Root != value.CapturedRoot || (!capturedLocal && !capturedMerge) {
		return nil, errors.New("pending publication captured commit is invalid")
	}
	history, err := _decodeCandidateHistory(value.CandidateHistory)
	if err != nil {
		return nil, err
	}
	candidate, err := object.VerifyCommit(value.CandidateData, value.CandidateCommit)
	if err != nil || candidate.AuthorUserID != binding.UserID || candidate.DeviceID != binding.DeviceID ||
		candidate.Root != value.CandidateRoot {
		return nil, errors.New("pending publication candidate commit is invalid")
	}
	sequence := make([]_pendingMergeCommit, 0, len(history)+2)
	if capturedMerge {
		sequence = append(sequence, _pendingMergeCommit{id: value.CapturedCommit, data: value.CapturedData, commit: captured})
	}
	if value.CandidateCommit == value.CapturedCommit {
		if len(history) != 0 || !bytes.Equal(value.CandidateData, value.CapturedData) ||
			(capturedLocal && (value.BaseCommit != binding.SyncBase || value.BaseRoot != binding.SyncBaseRoot ||
				value.ExpectedHead != binding.SyncBase)) || (capturedMerge && captured.Parents[0] != value.ExpectedHead) {
			return nil, errors.New("pending local publication has an invalid history")
		}
		return sequence, nil
	}
	if len(history)+len(sequence)+1 > maxSyncParentWalk {
		return nil, errors.New("pending publication merge history exceeds synchronization budget")
	}
	seen := map[string]bool{value.CapturedCommit: true}
	if capturedMerge {
		seen[captured.Parents[0]] = true
	}
	previous := value.CapturedCommit
	for index, data := range append(history, value.CandidateData) {
		id := object.ID(data)
		commit, verifyErr := object.VerifyCommit(data, id)
		if verifyErr != nil || commit.AuthorUserID != binding.UserID || commit.DeviceID != binding.DeviceID ||
			len(commit.Parents) != 2 || commit.Parents[1] != previous || seen[id] || seen[commit.Parents[0]] {
			return nil, fmt.Errorf("pending publication candidate chain is not linked to the binding Sync Base: merge history entry %d is invalid", index)
		}
		if index == len(history) && id != value.CandidateCommit {
			return nil, errors.New("pending publication current candidate does not match its history")
		}
		seen[id] = true
		seen[commit.Parents[0]] = true
		sequence = append(sequence, _pendingMergeCommit{id: id, data: data, commit: commit})
		previous = id
	}
	if sequence[len(sequence)-1].commit.Parents[0] != value.ExpectedHead {
		return nil, errors.New("pending publication candidate does not match expected Head")
	}
	if len(sequence) > 1 && value.BaseCommit != sequence[len(sequence)-2].commit.Parents[0] ||
		len(sequence) == 1 && !capturedMerge && value.BaseCommit != binding.SyncBase {
		return nil, errors.New("pending publication Base does not match merge history")
	}
	return sequence, nil
}

func verifyPendingPublication(value pendingPublication, binding clientBinding) error {
	if _, err := _pendingMergeSequence(value, binding); err != nil || value.DeletionCount < 0 ||
		value.TrackedCount < value.DeletionCount || value.DeleteConfirmed && !value.RequiresDeleteConfirmation ||
		(value.LegacyRevalidationRequired && (value.DeletionCount != 0 || value.TrackedCount != 0 ||
			value.RequiresDeleteConfirmation || value.DeleteConfirmed)) ||
		(!value.LegacyRevalidationRequired && value.RequiresDeleteConfirmation != protectedDeletion(value.DeletionCount, value.TrackedCount)) {
		return errors.Join(errors.New("pending publication is corrupt or does not match the binding"), err)
	}
	return nil
}

func _verifyPendingPublicationChain(ctx context.Context, options bindOptions, value pendingPublication, binding clientBinding,
	budget *_replayBudget) error {
	sequence, err := _pendingMergeSequence(value, binding)
	if err != nil {
		return err
	}
	if len(sequence) == 0 {
		return nil
	}
	if sequence[0].id == value.CapturedCommit {
		localID := sequence[0].commit.Parents[1]
		seen := map[string]bool{value.CapturedCommit: true}
		for localID != binding.SyncBase {
			if seen[localID] {
				return errors.New("pending publication captured second-parent chain contains a cycle")
			}
			seen[localID] = true
			local, localErr := _budgetedRemoteCommit(ctx, options, localID, budget)
			if localErr != nil || local.AuthorUserID != binding.UserID || local.DeviceID != binding.DeviceID ||
				(len(local.Parents) != 1 && len(local.Parents) != 2) {
				return errors.Join(errors.New("pending publication captured second-parent chain is invalid"), localErr)
			}
			localID = local.Parents[len(local.Parents)-1]
		}
	}
	base, err := _budgetedRemoteCommit(ctx, options, value.BaseCommit, budget)
	if err != nil || base.AuthorUserID != binding.UserID || base.Root != value.BaseRoot {
		return errors.Join(errors.New("pending publication Base is invalid"), err)
	}
	baseLinked, err := _remoteCommitDescendsFrom(ctx, options, value.BaseCommit, binding.SyncBase, binding.UserID, budget)
	if err != nil || !baseLinked {
		return errors.Join(errors.New("pending publication Base is not anchored to the binding Sync Base"), err)
	}
	previousRemote := binding.SyncBase
	if sequence[0].id == value.CapturedCommit {
		previousRemote = value.BaseCommit
	}
	for index, entry := range sequence {
		remoteID := entry.commit.Parents[0]
		remote, remoteErr := _budgetedRemoteCommit(ctx, options, remoteID, budget)
		if remoteErr != nil || remote.AuthorUserID != binding.UserID {
			return errors.Join(fmt.Errorf("pending publication merge Remote %d is invalid", index), remoteErr)
		}
		linked, linkErr := _remoteCommitDescendsFrom(ctx, options, remoteID, previousRemote, binding.UserID, budget)
		if linkErr != nil || !linked {
			return errors.Join(fmt.Errorf("pending publication merge Remote %d is not anchored to its predecessor", index), linkErr)
		}
		if remoteID == value.BaseCommit && remote.Root != value.BaseRoot {
			return errors.New("pending publication Base root does not match merge history")
		}
		previousRemote = remoteID
	}
	return nil
}

func resumePublication(ctx context.Context, db *sql.DB, options bindOptions, binding clientBinding, snapshot worktreeSnapshot,
	head remoteHead, pending pendingPublication, stdout io.Writer, config libraryClientConfig, budget *_replayBudget) error {
	if err := verifyPendingPublication(pending, binding); err != nil {
		return err
	}
	if err := _verifyPendingPublicationChain(ctx, options, pending, binding, budget); err != nil {
		return err
	}
	if *head.CommitID == pending.CandidateCommit {
		if snapshot.root != pending.CapturedRoot {
			return recoverPublishedCandidate(ctx, db, options, binding, head, pending)
		}
		if pending.CandidateRoot == pending.CapturedRoot {
			return finalizePublished(ctx, db, binding, snapshot, head, pending, stdout)
		}
		return transitionPublishedSuccessor(ctx, db, options, binding, snapshot, head, pending, stdout, config)
	}
	ancestor, err := _remoteCommitDescendsFrom(ctx, options, *head.CommitID, pending.CandidateCommit, binding.UserID, budget)
	if err != nil {
		return err
	}
	if ancestor {
		if snapshot.root == pending.CapturedRoot {
			return transitionPublishedSuccessor(ctx, db, options, binding, snapshot, head, pending, stdout, config)
		}
		return recoverPublishedCandidate(ctx, db, options, binding, head, pending)
	}
	if *head.CommitID != pending.ExpectedHead {
		return replacePendingForTrivialMerge(ctx, db, options, binding, snapshot, head, pending, stdout, config, budget)
	}
	if head.ETag != pending.ExpectedETag {
		if err := refreshPendingETag(ctx, db, binding.Worktree, &pending, head.ETag); err != nil {
			return err
		}
	}
	if pending.LegacyRevalidationRequired {
		if snapshot.root != pending.CapturedRoot {
			if err := discardPendingPublication(ctx, db, binding.Worktree, pending); err != nil {
				return err
			}
			return errors.New("worktree changed after legacy pending publication was created; stale candidate discarded, rerun sync")
		}
		deleted, tracked := deletionStats(options.scanConfig.trackedPaths, snapshot.paths)
		protected := protectedDeletion(deleted, tracked)
		if err := _execPendingPublicationCAS(ctx, db, `UPDATE pending_publications SET deletion_count = ?, tracked_count = ?,
			requires_delete_confirmation = ?, delete_confirmed = 0, legacy_revalidation_required = 0`,
			[]any{deleted, tracked, protected}, binding.Worktree, pending,
			"legacy pending publication changed during revalidation"); err != nil {
			return fmt.Errorf("revalidate legacy pending publication: %w", err)
		}
		pending.DeletionCount, pending.TrackedCount = deleted, tracked
		pending.RequiresDeleteConfirmation, pending.DeleteConfirmed, pending.LegacyRevalidationRequired = protected, false, false
	}
	if snapshot.root != pending.CapturedRoot {
		if err := discardPendingPublication(ctx, db, binding.Worktree, pending); err != nil {
			return err
		}
		return errors.New("worktree changed after pending publication was created; stale candidate discarded, rerun sync")
	}
	if pending.CandidateCommit != pending.CapturedCommit {
		if _, err := _rebuildPendingMerge(ctx, options, binding, snapshot, pending, budget); err != nil {
			return fmt.Errorf("revalidate pending recursive merge: %w", err)
		}
	}
	if pending.RequiresDeleteConfirmation && !pending.DeleteConfirmed {
		if !options.confirmDeleteSet {
			return deletionConfirmationError(pending)
		}
		if len(options.confirmDelete) != deleteCandidatePrefixLen || options.confirmDelete != pending.CandidateCommit[:deleteCandidatePrefixLen] {
			return errors.New("--confirm-delete must exactly match the 12-character prefix of this worktree's protected candidate")
		}
		if err := _execPendingPublicationCAS(ctx, db, "UPDATE pending_publications SET delete_confirmed = 1", nil,
			binding.Worktree, pending, "protected deletion candidate changed before confirmation"); err != nil {
			return fmt.Errorf("confirm pending deletion: %w", err)
		}
		pending.DeleteConfirmed = true
	} else if options.confirmDeleteSet && (len(options.confirmDelete) != deleteCandidatePrefixLen || options.confirmDelete != pending.CandidateCommit[:deleteCandidatePrefixLen]) {
		return errors.New("--confirm-delete must exactly match the 12-character prefix of this worktree's protected candidate")
	}
	return uploadAndPublishPending(ctx, db, options, binding, snapshot, pending, stdout, config, budget)
}

func refreshPendingETag(ctx context.Context, db *sql.DB, worktree string, pending *pendingPublication, etag string) error {
	if err := _execPendingPublicationCAS(ctx, db, "UPDATE pending_publications SET expected_etag = ?", []any{etag},
		worktree, *pending, "pending publication changed before ETag refresh"); err != nil {
		return fmt.Errorf("refresh pending publication ETag: %w", err)
	}
	pending.ExpectedETag = etag
	return nil
}

func replacePendingForTrivialMerge(ctx context.Context, db *sql.DB, options bindOptions, binding clientBinding,
	snapshot worktreeSnapshot, head remoteHead, old pendingPublication, stdout io.Writer, config libraryClientConfig,
	budget *_replayBudget) error {
	if head.CommitID == nil {
		return errors.New("remote merge Head is empty")
	}
	verified, err := scanWorktreeWithConfig(options.worktreeRoot, options.scanConfig)
	if err != nil || verified.root != old.CapturedRoot {
		return errors.New("worktree changed before pending merge replacement")
	}
	rebuilt := _rebuiltPendingMerge{snapshot: verified, paths: verified.paths}
	if old.CandidateCommit != old.CapturedCommit {
		rebuilt, err = _rebuildPendingMerge(ctx, options, binding, verified, old, budget)
		if err != nil {
			return fmt.Errorf("rebuild pending recursive merge before replacement: %w", err)
		}
	}
	base, err := _budgetedRemoteCommit(ctx, options, old.ExpectedHead, budget)
	if err != nil || base.AuthorUserID != binding.UserID {
		return errors.Join(errors.New("verify previous expected Head for merge"), err)
	}
	remote, err := _budgetedRemoteCommit(ctx, options, *head.CommitID, budget)
	if err != nil || remote.AuthorUserID != binding.UserID {
		return errors.Join(errors.New("verify latest Head for merge"), err)
	}
	prepared, err := _prepareRecursiveMerge(ctx, options, rebuilt.snapshot, base.Root, old.CandidateRoot, remote.Root,
		*head.CommitID, old.CandidateCommit, binding, config, budget)
	if err != nil {
		return err
	}
	verified, err = scanWorktreeWithConfig(options.worktreeRoot, options.scanConfig)
	if err != nil || verified.root != old.CapturedRoot {
		return errors.New("worktree changed while preparing pending merge replacement")
	}
	basePaths, err := _verifiedRemotePaths(ctx, options, base.Root, budget)
	if err != nil {
		return fmt.Errorf("verify replacement Base snapshot: %w", err)
	}
	deleted, tracked := deletionStats(_trackedPathSet(basePaths), prepared.paths)
	history, err := _decodeCandidateHistory(old.CandidateHistory)
	if err != nil {
		return err
	}
	if old.CandidateCommit != old.CapturedCommit {
		history = append(history, old.CandidateData)
	}
	encodedHistory, err := _encodeCandidateHistory(history)
	if err != nil {
		return err
	}
	next := pendingPublication{BaseCommit: old.ExpectedHead, BaseRoot: base.Root, ExpectedHead: *head.CommitID,
		ExpectedETag: head.ETag, CandidateCommit: prepared.candidateID, CandidateRoot: prepared.root,
		CandidateData: prepared.candidateData, CapturedCommit: old.CapturedCommit, CapturedRoot: old.CapturedRoot,
		CapturedData: old.CapturedData, CandidateHistory: encodedHistory, DeletionCount: deleted, TrackedCount: tracked,
		RequiresDeleteConfirmation: protectedDeletion(deleted, tracked)}
	if err := verifyPendingPublication(next, binding); err != nil {
		return fmt.Errorf("verify replacement pending publication: %w", err)
	}
	if err := _execPendingPublicationCAS(ctx, db, `UPDATE pending_publications SET base_commit = ?, base_root = ?, expected_head = ?,
		expected_etag = ?, candidate_commit = ?, candidate_root = ?, candidate_data = ?, captured_commit = ?,
		captured_root = ?, captured_data = ?, candidate_history = ?, deletion_count = ?, tracked_count = ?,
		requires_delete_confirmation = ?, delete_confirmed = 0, legacy_revalidation_required = 0`,
		[]any{next.BaseCommit, next.BaseRoot, next.ExpectedHead, next.ExpectedETag, next.CandidateCommit,
			next.CandidateRoot, next.CandidateData, next.CapturedCommit, next.CapturedRoot, next.CapturedData,
			next.CandidateHistory, next.DeletionCount, next.TrackedCount, next.RequiresDeleteConfirmation},
		binding.Worktree, old, "pending publication changed before conflict replacement"); err != nil {
		return fmt.Errorf("replace pending publication after Head conflict: %w", err)
	}
	if config.afterPendingReplacement != nil {
		if err := config.afterPendingReplacement(); err != nil {
			return fmt.Errorf("after pending publication replacement: %w", err)
		}
	}
	if next.RequiresDeleteConfirmation {
		return deletionConfirmationError(next)
	}
	directories := append(rebuilt.directories, prepared.directories...)
	return _uploadAndPublishPrepared(ctx, db, options, binding, snapshot, next, directories, stdout, config, budget)
}

func discardPendingPublication(ctx context.Context, db *sql.DB, worktree string, pending pendingPublication) error {
	if err := _deletePendingPublication(ctx, db, worktree, pending, "pending publication changed before discard"); err != nil {
		return fmt.Errorf("discard changed pending publication: %w", err)
	}
	return nil
}

func recoverPublishedCandidate(ctx context.Context, db *sql.DB, options bindOptions, binding clientBinding,
	head remoteHead, pending pendingPublication) error {
	cacheRoot, err := openVerifiedCacheRoot(options.clientDir)
	if err != nil {
		return err
	}
	defer cacheRoot.Close()
	options.cacheRoot = cacheRoot
	commit, err := downloadTargetCommit(ctx, options, pending.CapturedCommit, binding.UserID)
	if err != nil {
		return fmt.Errorf("recover captured publication metadata: %w", err)
	}
	if commit.Root != pending.CapturedRoot {
		return errors.New("captured publication commit has a different root")
	}
	paths, err := deriveRemotePaths(ctx, options, commit.Root, false)
	if err != nil {
		return fmt.Errorf("recover published candidate paths: %w", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE bindings SET sync_base_commit = ?, sync_base_root = ?, head_etag = ?
		WHERE server_url = ? AND library_id = ? AND worktree = ? AND user_id = ? AND device_id = ?
		AND sync_base_commit = ? AND sync_base_root = ? AND head_etag = ?`, pending.CapturedCommit,
		pending.CapturedRoot, head.ETag, binding.ServerURL, binding.LibraryID, binding.Worktree, binding.UserID,
		binding.DeviceID, binding.SyncBase, binding.SyncBaseRoot, binding.HeadETag)
	if err != nil {
		return errors.Join(err, tx.Rollback())
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return errors.Join(errors.New("published candidate recovery did not advance the expected binding"), err, tx.Rollback())
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM path_index WHERE worktree = ?", binding.Worktree); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	for _, path := range paths {
		if _, err := tx.ExecContext(ctx, `INSERT INTO path_index(worktree, path, type, object_id, canonical_mtime, actual_mtime, size)
			VALUES (?, ?, ?, ?, ?, ?, ?)`, binding.Worktree, path.path, path.kind, path.id, path.mtime, path.mtime, path.size); err != nil {
			return errors.Join(err, tx.Rollback())
		}
	}
	if err := _deletePendingPublication(ctx, tx, binding.Worktree, pending,
		"published candidate recovery did not clear the expected pending publication"); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("recover published candidate: %w", err)
	}
	return errors.New("published candidate recovered as Sync Base while preserving local changes; rerun sync")
}

func uploadAndPublishPending(ctx context.Context, db *sql.DB, options bindOptions, binding clientBinding, snapshot worktreeSnapshot,
	pending pendingPublication, stdout io.Writer, config libraryClientConfig, budget *_replayBudget) error {
	if pending.LegacyRevalidationRequired {
		return errors.New("legacy pending publication requires deletion revalidation before publication")
	}
	var directories []scannedObject
	if pending.CandidateCommit != pending.CapturedCommit {
		rebuilt, err := _rebuildPendingMerge(ctx, options, binding, snapshot, pending, budget)
		if err != nil {
			return fmt.Errorf("rebuild pending recursive merge for upload: %w", err)
		}
		directories = rebuilt.directories
	}
	return _uploadAndPublishPrepared(ctx, db, options, binding, snapshot, pending, directories, stdout, config, budget)
}

func _uploadAndPublishPrepared(ctx context.Context, db *sql.DB, options bindOptions, binding clientBinding,
	snapshot worktreeSnapshot, pending pendingPublication, directories []scannedObject, stdout io.Writer,
	config libraryClientConfig, budget *_replayBudget) error {
	if err := uploadSnapshot(ctx, options, snapshot); err != nil {
		return err
	}
	if err := putMetadata(ctx, options.base, options.libraryID, options.token, "commits", pending.CapturedCommit,
		pending.CapturedData); err != nil {
		return err
	}
	if err := _uploadMergedDirectories(ctx, options, directories); err != nil {
		return err
	}
	if err := _uploadCandidateHistory(ctx, options, pending.CandidateHistory); err != nil {
		return err
	}
	if pending.CandidateCommit != pending.CapturedCommit {
		if err := putMetadata(ctx, options.base, options.libraryID, options.token, "commits", pending.CandidateCommit,
			pending.CandidateData); err != nil {
			return err
		}
	}
	return publishPending(ctx, db, options, binding, snapshot, pending, stdout, config, budget)
}

func _uploadCandidateHistory(ctx context.Context, options bindOptions, encoded []byte) error {
	history, err := _decodeCandidateHistory(encoded)
	if err != nil {
		return err
	}
	if len(history) == 0 {
		return nil
	}
	references := make([]clientObjectReference, 0, len(history))
	for _, data := range history {
		references = append(references, clientObjectReference{ObjectID: object.ID(data), ObjectType: "Commit"})
	}
	missing, err := checkRemoteObjects(ctx, options, references)
	if err != nil {
		return err
	}
	for _, data := range history {
		id := object.ID(data)
		if missing["commits\x00"+id] {
			if err := putMetadata(ctx, options.base, options.libraryID, options.token, "commits", id, data); err != nil {
				return err
			}
		}
	}
	return nil
}

func deletionStats(trackedPaths map[string]bool, paths []checkoutPath) (deleted, tracked int64) {
	present := make(map[string]bool, len(paths))
	for _, path := range paths {
		present[path.path] = true
	}
	for path := range trackedPaths {
		tracked++
		if !present[path] {
			deleted++
		}
	}
	return deleted, tracked
}

func protectedDeletion(deleted, tracked int64) bool {
	if deleted > 100 {
		return true
	}
	if tracked == 0 {
		return false
	}
	threshold := tracked / 10
	if tracked%10 != 0 {
		threshold++
	}
	return deleted >= threshold
}

func deletionConfirmationError(pending pendingPublication) error {
	return &deleteConfirmationRequiredError{candidate: pending.CandidateCommit, deleted: pending.DeletionCount, tracked: pending.TrackedCount}
}

func _budgetedRemoteCommit(ctx context.Context, options bindOptions, id string, budget *_replayBudget) (object.Commit, error) {
	if commit, ok := budget.commits[id]; ok {
		return commit, nil
	}
	if budget.commitFetches >= budget.commitLimit {
		return object.Commit{}, errors.New("remote commit history exceeds cumulative synchronization budget")
	}
	commit, err := getRemoteCommit(ctx, options.base, options.libraryID, options.token, id)
	if err != nil {
		return object.Commit{}, err
	}
	budget.commitFetches++
	budget.commits[id] = commit
	return commit, nil
}

func _remoteCommitDescendsFrom(ctx context.Context, options bindOptions, head, ancestor, owner string,
	budget *_replayBudget) (bool, error) {
	pending := []string{head}
	seen := make(map[string]bool)
	for len(pending) != 0 {
		id := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if id == ancestor {
			return true, nil
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		if !budget.walked[id] {
			budget.walked[id] = true
			budget.commitWalks++
			if budget.commitWalks > budget.commitLimit {
				return false, errors.New("remote commit history exceeds cumulative synchronization budget")
			}
		}
		commit, err := _budgetedRemoteCommit(ctx, options, id, budget)
		if err != nil {
			return false, fmt.Errorf("verify remote publication history: %w", err)
		}
		if commit.AuthorUserID != owner {
			return false, errors.New("remote publication history has a different owner")
		}
		pending = append(pending, commit.Parents...)
	}
	return false, nil
}

func transitionPublishedSuccessor(ctx context.Context, db *sql.DB, options bindOptions, binding clientBinding, snapshot worktreeSnapshot,
	head remoteHead, pending pendingPublication, stdout io.Writer, config libraryClientConfig) error {
	if head.CommitID == nil {
		return errors.New("remote successor Head is empty")
	}
	checkout := pendingCheckout{ServerURL: binding.ServerURL, LibraryID: binding.LibraryID, Worktree: binding.Worktree,
		UserID: binding.UserID, DeviceID: binding.DeviceID, TargetCommit: *head.CommitID, HeadETag: head.ETag, ApplyState: "pending"}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := updateBindingAndIndex(ctx, tx, binding, pending.CapturedCommit, pending.CapturedRoot, head.ETag, snapshot.paths); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO pending_checkouts(server_url, library_id, worktree, user_id, device_id,
		target_commit, target_root, head_etag, apply_state) VALUES (?, ?, ?, ?, ?, ?, '', ?, 'pending')`, checkout.ServerURL,
		checkout.LibraryID, checkout.Worktree, checkout.UserID, checkout.DeviceID, checkout.TargetCommit, checkout.HeadETag); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	if err := _deletePendingPublication(ctx, tx, binding.Worktree, pending,
		"publication changed during checkout transition"); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("transition superseded publication: %w", err)
	}
	binding.SyncBase, binding.SyncBaseRoot, binding.HeadETag = pending.CapturedCommit, pending.CapturedRoot, head.ETag
	return continueSyncCheckout(ctx, db, options, binding, checkout, stdout, config)
}

func publishPending(ctx context.Context, db *sql.DB, options bindOptions, binding clientBinding, snapshot worktreeSnapshot,
	pending pendingPublication, stdout io.Writer, config libraryClientConfig, budget *_replayBudget) error {
	if config.beforeHeadCAS != nil {
		if err := config.beforeHeadCAS(); err != nil {
			return fmt.Errorf("prepare Head publication: %w", err)
		}
	}
	verified, err := scanWorktreeWithConfig(options.worktreeRoot, options.scanConfig)
	if err != nil || verified.root != pending.CapturedRoot {
		return errors.New("worktree changed before Head publication")
	}
	current, err := getRemoteHead(ctx, options.base, options.libraryID, options.token)
	if err != nil {
		return err
	}
	if current.CommitID == nil {
		return errors.New("library Head is empty before publication")
	}
	if *current.CommitID != pending.ExpectedHead {
		return resumePublication(ctx, db, options, binding, snapshot, current, pending, stdout, config, budget)
	}
	if current.ETag != pending.ExpectedETag {
		if err := refreshPendingETag(ctx, db, binding.Worktree, &pending, current.ETag); err != nil {
			return err
		}
	}
	if err := _assertPendingPublication(ctx, db, binding.Worktree, pending); err != nil {
		return err
	}
	_, _, publishErr := updateRemoteHead(ctx, options.base, options.libraryID, options.token, pending.ExpectedETag, pending.CandidateCommit)
	published, getErr := getRemoteHead(ctx, options.base, options.libraryID, options.token)
	if getErr != nil {
		return errors.Join(publishErr, fmt.Errorf("resolve library Head after publish: %w", getErr))
	}
	observed, scanErr := scanWorktreeWithConfig(options.worktreeRoot, options.scanConfig)
	if scanErr != nil {
		return errors.Join(publishErr, fmt.Errorf("rescan worktree after publish: %w", scanErr))
	}
	if published.CommitID != nil && *published.CommitID == pending.CandidateCommit {
		return resumePublication(ctx, db, options, binding, observed, published, pending, stdout, config, budget)
	}
	if published.CommitID != nil && *published.CommitID == pending.ExpectedHead {
		if published.ETag != pending.ExpectedETag {
			if err := refreshPendingETag(ctx, db, binding.Worktree, &pending, published.ETag); err != nil {
				return errors.Join(publishErr, err)
			}
		}
		return errors.Join(publishErr, errors.New("library Head publication did not complete; rerun sync"))
	}
	if published.CommitID == nil {
		return errors.Join(publishErr, errors.New("library Head became empty during publication"))
	}
	resumeErr := resumePublication(ctx, db, options, binding, observed, published, pending, stdout, config, budget)
	if resumeErr != nil {
		return errors.Join(publishErr, resumeErr)
	}
	return nil
}

func finalizePublished(ctx context.Context, db *sql.DB, binding clientBinding, snapshot worktreeSnapshot, head remoteHead,
	pending pendingPublication, stdout io.Writer) error {
	if head.CommitID == nil || *head.CommitID != pending.CandidateCommit {
		return errors.New("published Head does not match pending publication")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := updateBindingAndIndex(ctx, tx, binding, pending.CandidateCommit, pending.CandidateRoot, head.ETag, snapshot.paths); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	if err := _deletePendingPublication(ctx, tx, binding.Worktree, pending,
		"publication changed during finalization"); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("finalize publication: %w", err)
	}
	_, err = fmt.Fprintln(stdout, "library synchronized")
	return err
}

func continueSyncCheckout(ctx context.Context, db *sql.DB, options bindOptions, binding clientBinding, pending pendingCheckout,
	stdout io.Writer, config libraryClientConfig) error {
	if pending.UserID != binding.UserID || pending.DeviceID != binding.DeviceID {
		return errors.New("pending checkout does not match the binding")
	}
	if pending.ApplyState == "finalized" {
		return finishSyncCleanup(ctx, db, options, pending, stdout, config)
	}
	if pending.ApplyState == "rolling_back" {
		if err := rollbackSyncApply(ctx, db, options.worktreeRoot, options.clientDir, binding.Worktree, config); err != nil {
			return err
		}
		if err := clearApplyingCheckout(ctx, db, binding.Worktree); err != nil {
			return err
		}
		return syncLibrary(ctx, db, options, binding, stdout, config)
	}
	if pending.ApplyState != "pending" && pending.ApplyState != "applying" {
		return errors.New("pending checkout has invalid apply state")
	}
	cacheRoot, err := openVerifiedCacheRoot(options.clientDir)
	if err != nil {
		return err
	}
	defer cacheRoot.Close()
	options.cacheRoot = cacheRoot
	commit, err := downloadTargetCommit(ctx, options, pending.TargetCommit, binding.UserID)
	if err != nil {
		return err
	}
	if pending.TargetRoot == "" {
		pending.TargetRoot = commit.Root
		if _, err := db.ExecContext(ctx, "UPDATE pending_checkouts SET target_root = ? WHERE worktree = ?", pending.TargetRoot, binding.Worktree); err != nil {
			return err
		}
	} else if pending.TargetRoot != commit.Root {
		return errors.New("pending checkout target commit changed root")
	}
	paths, err := downloadCheckoutTree(ctx, options, pending.TargetRoot)
	if err != nil {
		return err
	}
	if err := saveCheckoutPaths(ctx, db, binding.Worktree, paths); err != nil {
		return err
	}
	if pending.ApplyState == "pending" {
		if config.beforeSyncRecoveryPrepare != nil {
			if err := config.beforeSyncRecoveryPrepare(); err != nil {
				return fmt.Errorf("prepare sync recovery: %w", err)
			}
		}
		stable, err := scanWorktreeWithConfig(options.worktreeRoot, options.scanConfig)
		if err != nil || stable.root != binding.SyncBaseRoot {
			if clearErr := clearUnstartedCheckout(ctx, db, binding.Worktree); clearErr != nil {
				return errors.Join(errors.New("local worktree changed before remote apply"), err, clearErr)
			}
			return errors.Join(errors.New("local and remote libraries both changed; merge is not supported yet"), err)
		}
		if err := registerSyncRecoveryPlan(ctx, db, binding.Worktree, stable.paths, config); err != nil {
			return err
		}
		pending.ApplyState = "applying"
	}
	if err := prepareSyncRecoveries(ctx, db, options.worktreeRoot, binding.Worktree, config); err != nil {
		return err
	}
	if config.beforeCheckoutMaterialize != nil {
		if err := config.beforeCheckoutMaterialize(); err != nil {
			return fmt.Errorf("prepare checkout materialization: %w", err)
		}
	}
	if err := materializeCheckout(ctx, db, options, paths, config); err != nil {
		return err
	}
	if err := verifyAllSyncRecoveries(ctx, db, options.worktreeRoot, binding.Worktree); err != nil {
		rollbackErr := beginSyncRollback(ctx, db, binding.Worktree)
		if rollbackErr == nil {
			rollbackErr = rollbackSyncApply(ctx, db, options.worktreeRoot, options.clientDir, binding.Worktree, config)
		}
		if rollbackErr == nil {
			rollbackErr = clearApplyingCheckout(ctx, db, binding.Worktree)
		}
		return errors.Join(fmt.Errorf("captured local content changed during remote apply: %w", err), rollbackErr)
	}
	ignored, err := registeredSyncRecoveryNames(ctx, db, binding.Worktree)
	if err != nil {
		return err
	}
	scanConfig := options.scanConfig
	scanConfig.ignoredRootNames = ignored
	verified, err := scanWorktreeWithConfig(options.worktreeRoot, scanConfig)
	if err != nil || verified.root != pending.TargetRoot {
		return errors.Join(errors.New("applied remote snapshot did not match its fixed target"), err)
	}
	if config.beforeFinalize != nil {
		if err := config.beforeFinalize(); err != nil {
			return fmt.Errorf("finalize checkout: %w", err)
		}
	}
	if err := verifyAllSyncRecoveries(ctx, db, options.worktreeRoot, binding.Worktree); err != nil {
		return fmt.Errorf("captured local content changed before finalization: %w", err)
	}
	ignored, err = registeredSyncRecoveryNames(ctx, db, binding.Worktree)
	if err != nil {
		return err
	}
	scanConfig = options.scanConfig
	scanConfig.ignoredRootNames = ignored
	verified, err = scanWorktreeWithConfig(options.worktreeRoot, scanConfig)
	if err != nil || verified.root != pending.TargetRoot {
		return errors.Join(errors.New("remote target changed before finalization"), err)
	}
	if err := finalizeSyncApply(ctx, db, binding, pending, paths, config); err != nil {
		return err
	}
	pending.ApplyState = "finalized"
	return finishSyncCleanup(ctx, db, options, pending, stdout, config)
}

func clearUnstartedCheckout(ctx context.Context, db *sql.DB, worktree string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for _, query := range []string{"DELETE FROM checkout_paths WHERE worktree = ?", "DELETE FROM pending_checkouts WHERE worktree = ?"} {
		if _, err := tx.ExecContext(ctx, query, worktree); err != nil {
			return errors.Join(err, tx.Rollback())
		}
	}
	return tx.Commit()
}

func beginSyncRollback(ctx context.Context, db *sql.DB, worktree string) error {
	result, err := db.ExecContext(ctx, `UPDATE pending_checkouts SET apply_state = 'rolling_back'
		WHERE worktree = ? AND apply_state IN ('applying', 'rolling_back')`, worktree)
	if err != nil {
		return err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return errors.Join(errors.New("begin sync rollback did not update pending state"), err)
	}
	return nil
}

func clearApplyingCheckout(ctx context.Context, db *sql.DB, worktree string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for _, query := range []string{"DELETE FROM checkout_paths WHERE worktree = ?", "DELETE FROM sync_recoveries WHERE worktree = ?", "DELETE FROM pending_checkouts WHERE worktree = ?"} {
		if _, err := tx.ExecContext(ctx, query, worktree); err != nil {
			return errors.Join(err, tx.Rollback())
		}
	}
	return tx.Commit()
}

func registerSyncRecoveryPlan(ctx context.Context, db *sql.DB, worktree string, paths []checkoutPath, config libraryClientConfig) error {
	top := make([]checkoutPath, 0)
	for _, path := range paths {
		if !strings.Contains(path.path, "/") {
			top = append(top, path)
		}
	}
	sort.Slice(top, func(i, j int) bool { return top[i].path < top[j].path })
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	var existing int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM sync_recoveries WHERE worktree = ?", worktree).Scan(&existing); err != nil || existing != 0 {
		return errors.Join(errors.New("sync recovery plan already exists"), err, tx.Rollback())
	}
	for _, path := range top {
		name, err := newReservedName(syncRecoveryPrefix)
		if err != nil {
			return errors.Join(err, tx.Rollback())
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO sync_recoveries(worktree, path, recovery_name, type, object_id,
			canonical_mtime, size, device, inode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, worktree, path.path, name,
			path.kind, path.id, path.mtime, path.size, path.device, path.inode); err != nil {
			return errors.Join(err, tx.Rollback())
		}
	}
	result, err := tx.ExecContext(ctx, "UPDATE pending_checkouts SET apply_state = 'applying' WHERE worktree = ? AND apply_state = 'pending'", worktree)
	if err != nil {
		return errors.Join(err, tx.Rollback())
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return errors.Join(errors.New("register sync recovery plan did not advance pending state"), err, tx.Rollback())
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("register sync recovery plan: %w", err)
	}
	if config.afterSyncRecoveryRegistered != nil {
		for _, path := range top {
			if err := config.afterSyncRecoveryRegistered(path.path); err != nil {
				return fmt.Errorf("after sync recovery registration: %w", err)
			}
		}
	}
	return nil
}

func registeredSyncRecoveryNames(ctx context.Context, db *sql.DB, worktree string) (map[string]bool, error) {
	values, err := loadSyncRecoveries(ctx, db, worktree)
	if err != nil {
		return nil, err
	}
	names := make(map[string]bool, len(values)*2)
	for _, value := range values {
		names[value.name] = true
		if value.tombstone != "" {
			names[value.tombstone] = true
		}
	}
	return names, nil
}

func loadSyncRecoveries(ctx context.Context, db *sql.DB, worktree string) ([]syncRecovery, error) {
	rows, err := db.QueryContext(ctx, `SELECT path, recovery_name, tombstone_name, type, object_id, canonical_mtime,
		size, device, inode, completed FROM sync_recoveries WHERE worktree = ? ORDER BY path`, worktree)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var values []syncRecovery
	for rows.Next() {
		var value syncRecovery
		if err := rows.Scan(&value.path, &value.name, &value.tombstone, &value.kind, &value.id, &value.mtime,
			&value.size, &value.device, &value.inode, &value.completed); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func prepareSyncRecoveries(ctx context.Context, db *sql.DB, root *openedWorktree, worktree string, config libraryClientConfig) error {
	values, err := loadSyncRecoveries(ctx, db, worktree)
	if err != nil {
		return err
	}
	for _, value := range values {
		visible, visibleErr := statAt(root.directory, value.path)
		hidden, hiddenErr := statAt(root.directory, value.name)
		visibleExists, hiddenExists := visibleErr == nil, hiddenErr == nil
		if visibleErr != nil && !errors.Is(visibleErr, syscall.ENOENT) {
			return visibleErr
		}
		if hiddenErr != nil && !errors.Is(hiddenErr, syscall.ENOENT) {
			return hiddenErr
		}
		if value.completed {
			if !hiddenExists || !statMatchesRecovery(hidden, value) {
				return fmt.Errorf("completed sync recovery %q changed identity", value.path)
			}
			continue
		}
		switch {
		case visibleExists && !hiddenExists:
			if !statMatchesRecovery(visible, value) {
				return fmt.Errorf("worktree path %q changed before recovery rename", value.path)
			}
			if err := verifyNamedRecovery(root.directory, value.path, value); err != nil {
				return fmt.Errorf("worktree path %q changed before recovery rename: %w", value.path, err)
			}
			if config.beforeSyncRecoveryRename != nil {
				if err := config.beforeSyncRecoveryRename(value.path, value.name); err != nil {
					return fmt.Errorf("before sync recovery rename: %w", err)
				}
			}
			current, err := statAt(root.directory, value.path)
			if err != nil || !statMatchesRecovery(current, value) {
				return fmt.Errorf("worktree path %q changed at recovery rename", value.path)
			}
			if err := journalRename(ctx, db, root, worktree, fsPhasePreBase, "", value.path, value.name,
				value.kind, "", value.name, value.device, value.inode, config.fsActionFault); err != nil {
				return fmt.Errorf("move existing path %q to registered recovery: %w", value.path, err)
			}
			if config.afterSyncRecoveryRename != nil {
				if err := config.afterSyncRecoveryRename(value.path, value.name); err != nil {
					return fmt.Errorf("after sync recovery rename: %w", err)
				}
			}
		case !visibleExists && hiddenExists:
			if !statMatchesRecovery(hidden, value) {
				return fmt.Errorf("renamed sync recovery %q changed identity", value.path)
			}
		default:
			return fmt.Errorf("sync recovery %q has ambiguous visible and hidden state", value.path)
		}
		if config.beforeSyncRecoveryCompleted != nil {
			if err := config.beforeSyncRecoveryCompleted(value.path); err != nil {
				return fmt.Errorf("before sync recovery completion: %w", err)
			}
		}
		result, err := db.ExecContext(ctx, `UPDATE sync_recoveries SET completed = 1 WHERE worktree = ? AND path = ? AND completed = 0`, worktree, value.path)
		if err != nil {
			return err
		}
		if changed, err := result.RowsAffected(); err != nil || changed != 1 {
			return errors.New("record sync recovery completion failed")
		}
	}
	return nil
}

func verifyAllSyncRecoveries(ctx context.Context, db *sql.DB, root *openedWorktree, worktree string) error {
	values, err := loadSyncRecoveries(ctx, db, worktree)
	if err != nil {
		return err
	}
	for _, value := range values {
		if !value.completed {
			return fmt.Errorf("sync recovery %q is incomplete", value.path)
		}
		name := value.name
		if value.tombstone != "" {
			name = value.tombstone
		}
		if err := verifyNamedRecovery(root.directory, name, value); err != nil {
			return fmt.Errorf("verify captured path %q: %w", value.path, err)
		}
	}
	return nil
}

func verifyNamedRecovery(parent *os.File, name string, expected syncRecovery) error {
	file, info, err := openScannableAt(parent, name, expected.path)
	if err != nil {
		return err
	}
	defer file.Close()
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || (expected.device != 0 && (uint64(stat.Dev) != expected.device || stat.Ino != expected.inode)) {
		return errors.New("captured path identity changed")
	}
	mtime := info.ModTime().UTC().Truncate(time.Second).Format("2006-01-02T15:04:05Z")
	snapshot := worktreeSnapshot{blocks: make(map[string]blockSource)}
	var id string
	if expected.kind == "File" && info.Mode().IsRegular() {
		id, err = scanRegularFile(file, expected.path, info, &snapshot)
	} else if expected.kind == "Directory" && info.IsDir() {
		id, err = scanDirectory(file, expected.path, &snapshot)
	} else {
		return errors.New("captured path type changed")
	}
	if err != nil || id != expected.id || mtime != expected.mtime || (expected.kind == "File" && info.Size() != expected.size) {
		return errors.Join(errors.New("captured path content or mtime changed"), err)
	}
	return nil
}

func statAt(parent *os.File, name string) (unix.Stat_t, error) {
	var stat unix.Stat_t
	err := unix.Fstatat(int(parent.Fd()), name, &stat, unix.AT_SYMLINK_NOFOLLOW)
	return stat, err
}

func statMatchesRecovery(stat unix.Stat_t, value syncRecovery) bool {
	mode := uint32(syscall.S_IFREG)
	if value.kind == "Directory" {
		mode = syscall.S_IFDIR
	}
	return uint64(stat.Dev) == value.device && stat.Ino == value.inode && stat.Mode&syscall.S_IFMT == mode
}

func finalizeSyncApply(ctx context.Context, db *sql.DB, binding clientBinding, pending pendingCheckout, paths []checkoutPath, config libraryClientConfig) error {
	worktree := binding.Worktree
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := assertNoIncompletePreBase(ctx, tx, worktree); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	if err := updateBindingAndIndex(ctx, tx, binding, pending.TargetCommit, pending.TargetRoot, pending.HeadETag, paths); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	result, err := tx.ExecContext(ctx, `UPDATE pending_checkouts SET apply_state = 'finalized' WHERE worktree = ? AND apply_state = 'applying'`, worktree)
	if err != nil {
		return errors.Join(err, tx.Rollback())
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return errors.Join(errors.New("finalize sync apply did not advance pending state"), err, tx.Rollback())
	}
	if config.fsTransactionFault != nil {
		if err := config.fsTransactionFault("before_base_commit"); err != nil {
			return errors.Join(err, tx.Rollback())
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("finalize synchronized checkout: %w", err)
	}
	if config.fsTransactionFault != nil {
		if err := config.fsTransactionFault("after_base_commit"); err != nil {
			return err
		}
	}
	return nil
}

func finishSyncCleanup(ctx context.Context, db *sql.DB, options bindOptions, pending pendingCheckout, stdout io.Writer, config libraryClientConfig) error {
	if err := cleanupSyncRecoveries(ctx, db, options.worktreeRoot, pending.Worktree, config); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for _, query := range []string{"DELETE FROM checkout_paths WHERE worktree = ?", "DELETE FROM pending_checkouts WHERE worktree = ? AND apply_state = 'finalized'", "DELETE FROM fs_actions WHERE worktree = ? AND state = 'completed' AND origin_action_id IS NOT NULL", "DELETE FROM fs_actions WHERE worktree = ? AND state = 'completed'"} {
		if _, err := tx.ExecContext(ctx, query, pending.Worktree); err != nil {
			return errors.Join(err, tx.Rollback())
		}
	}
	if config.fsTransactionFault != nil {
		if err := config.fsTransactionFault("before_cleanup_commit"); err != nil {
			return errors.Join(err, tx.Rollback())
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("complete synchronized checkout cleanup: %w", err)
	}
	if config.fsTransactionFault != nil {
		if err := config.fsTransactionFault("after_cleanup_commit"); err != nil {
			return err
		}
	}
	latest, err := getRemoteHead(ctx, options.base, options.libraryID, options.token)
	if err != nil {
		return fmt.Errorf("verify library Head after checkout: %w", err)
	}
	if latest.CommitID == nil || *latest.CommitID != pending.TargetCommit {
		return errors.New("library Head advanced during checkout; rerun sync")
	}
	_, err = fmt.Fprintln(stdout, "library synchronized")
	return err
}

func snapshotRecoveryRemoval(parent *os.File, name string, expected syncRecovery) ([]checkoutPath, error) {
	file, info, err := openScannableAt(parent, name, expected.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() || uint64(stat.Dev) != expected.device || stat.Ino != expected.inode ||
		info.ModTime().UTC().Truncate(time.Second).Format("2006-01-02T15:04:05Z") != expected.mtime {
		return nil, errors.New("captured directory identity, type, or mtime changed")
	}
	snapshot := worktreeSnapshot{blocks: make(map[string]blockSource)}
	id, err := scanDirectory(file, expected.path, &snapshot)
	if err != nil || id != expected.id {
		return nil, errors.Join(errors.New("captured directory content changed"), err)
	}
	return snapshot.paths, nil
}

func persistRecoveryRemovalPlan(ctx context.Context, db *sql.DB, root *openedWorktree, worktree string,
	value syncRecovery, paths []checkoutPath, fault fsActionFault) ([]fsAction, error) {
	if err := bindFSJournalRoot(ctx, db, worktree, root); err != nil {
		return nil, err
	}
	identities := map[string][2]uint64{value.tombstone: {value.device, value.inode}}
	for _, path := range paths {
		relative := strings.TrimPrefix(path.path, value.path+"/")
		identities[value.tombstone+"/"+relative] = [2]uint64{path.device, path.inode}
	}
	sort.Slice(paths, func(i, j int) bool {
		left, right := strings.Count(paths[i].path, "/"), strings.Count(paths[j].path, "/")
		if left != right {
			return left > right
		}
		return paths[i].path < paths[j].path
	})
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	fail := func(err error) ([]fsAction, error) { return nil, errors.Join(err, tx.Rollback()) }
	var order int64
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(action_order), -1) + 1 FROM fs_actions WHERE worktree = ?", worktree).Scan(&order); err != nil {
		return fail(err)
	}
	actions := make([]fsAction, 0, len(paths)+1)
	for _, path := range paths {
		relative := strings.TrimPrefix(path.path, value.path+"/")
		actual := value.tombstone + "/" + relative
		parent, leaf := splitFSActionPath(actual)
		identity, ok := identities[parent]
		if !ok {
			return fail(errors.New("captured removal plan has no parent identity"))
		}
		id, err := newFSActionID()
		if err != nil {
			return fail(err)
		}
		op := fsOpUnlink
		if path.kind == "Directory" {
			op = fsOpRmdir
		}
		actions = append(actions, fsAction{Worktree: worktree, ActionID: id, Order: order, Phase: fsPhasePostBase,
			Op: op, Parent: parent, ParentDevice: identity[0], ParentInode: identity[1], Source: leaf,
			ExpectedKind: path.kind, ExpectedDevice: path.device, ExpectedInode: path.inode,
			ExpectedObject: path.id, ExpectedSize: path.size, ExpectedMtime: path.mtime, State: fsStateIntent})
		order++
	}
	id, err := newFSActionID()
	if err != nil {
		return fail(err)
	}
	actions = append(actions, fsAction{Worktree: worktree, ActionID: id, Order: order, Phase: fsPhasePostBase,
		Op: fsOpRmdir, Parent: "", ParentDevice: root.device, ParentInode: root.inode, Source: value.tombstone,
		ExpectedKind: "Directory", ExpectedDevice: value.device, ExpectedInode: value.inode,
		ExpectedObject: value.id, ExpectedMtime: value.mtime, InternalSource: value.tombstone, State: fsStateIntent})
	for _, action := range actions {
		if err := validateFSAction(action); err != nil {
			return fail(err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO fs_actions(worktree, action_id, action_order, phase, op, parent_path,
			parent_device, parent_inode, source_name, target_name, expected_kind, expected_device, expected_inode,
			expected_object, expected_size, expected_mtime, internal_source, internal_target, state)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, action.Worktree, action.ActionID,
			action.Order, action.Phase, action.Op, action.Parent, action.ParentDevice, action.ParentInode, action.Source,
			action.Target, action.ExpectedKind, action.ExpectedDevice, action.ExpectedInode, action.ExpectedObject,
			action.ExpectedSize, action.ExpectedMtime, action.InternalSource, action.InternalTarget, action.State); err != nil {
			return fail(err)
		}
	}
	if fault != nil {
		for _, action := range actions {
			if err := fault("before_intent_commit", action); err != nil {
				return fail(err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	if fault != nil {
		for _, action := range actions {
			if err := fault("after_intent_commit", action); err != nil {
				return nil, err
			}
		}
	}
	return actions, nil
}

func removeRecoveryDirectory(ctx context.Context, db *sql.DB, root *openedWorktree, worktree string,
	value syncRecovery, fault fsActionFault) error {
	paths, err := snapshotRecoveryRemoval(root.directory, value.tombstone, value)
	if err != nil {
		return err
	}
	actions, err := persistRecoveryRemovalPlan(ctx, db, root, worktree, value, paths, fault)
	if err != nil {
		return err
	}
	for _, action := range actions {
		if err := completeFSAction(ctx, db, root, action, fault); err != nil {
			return err
		}
	}
	return nil
}

func cleanupSyncRecoveries(ctx context.Context, db *sql.DB, root *openedWorktree, worktree string, config libraryClientConfig) error {
	values, err := loadSyncRecoveries(ctx, db, worktree)
	if err != nil {
		return err
	}
	for _, value := range values {
		if value.tombstone == "" {
			value.tombstone, err = newReservedName(syncTombstonePrefix)
			if err != nil {
				return err
			}
			if _, err := db.ExecContext(ctx, `UPDATE sync_recoveries SET tombstone_name = ? WHERE worktree = ? AND path = ? AND tombstone_name = ''`,
				value.tombstone, worktree, value.path); err != nil {
				return err
			}
		}
		if config.beforeSyncRecoveryCleanup != nil {
			if err := config.beforeSyncRecoveryCleanup(value.path, value.name); err != nil {
				return fmt.Errorf("before sync recovery cleanup: %w", err)
			}
		}
		recoveryStat, recoveryErr := statAt(root.directory, value.name)
		tombstoneStat, tombstoneErr := statAt(root.directory, value.tombstone)
		recoveryExists, tombstoneExists := recoveryErr == nil, tombstoneErr == nil
		if recoveryErr != nil && !errors.Is(recoveryErr, syscall.ENOENT) {
			return recoveryErr
		}
		if tombstoneErr != nil && !errors.Is(tombstoneErr, syscall.ENOENT) {
			return tombstoneErr
		}
		switch {
		case recoveryExists && !tombstoneExists:
			if !statMatchesRecovery(recoveryStat, value) {
				return fmt.Errorf("registered sync recovery %q changed identity", value.path)
			}
			if err := journalRename(ctx, db, root, worktree, fsPhasePostBase, "", value.name, value.tombstone,
				value.kind, value.name, value.tombstone, value.device, value.inode, config.fsActionFault); err != nil {
				return fmt.Errorf("isolate sync recovery %q for cleanup: %w", value.path, err)
			}
		case !recoveryExists && tombstoneExists:
			if !statMatchesRecovery(tombstoneStat, value) {
				return fmt.Errorf("sync recovery tombstone %q changed identity", value.path)
			}
		case !recoveryExists && !tombstoneExists:
			if _, err := db.ExecContext(ctx, "DELETE FROM sync_recoveries WHERE worktree = ? AND path = ?", worktree, value.path); err != nil {
				return err
			}
			continue
		default:
			return fmt.Errorf("sync recovery %q has ambiguous cleanup state", value.path)
		}
		if config.afterSyncRecoveryCleanupRename != nil {
			if err := config.afterSyncRecoveryCleanupRename(value.path, value.tombstone); err != nil {
				return fmt.Errorf("after sync recovery cleanup rename: %w", err)
			}
		}
		if err := verifyNamedRecovery(root.directory, value.tombstone, value); err != nil {
			return fmt.Errorf("verify sync recovery %q before cleanup: %w", value.path, err)
		}
		if value.kind == "File" {
			if err := journalRemove(ctx, db, root, worktree, fsPhasePostBase, "", value.tombstone,
				"File", value.tombstone, value.id, value.mtime, value.size, value.device, value.inode, config.fsActionFault); err != nil {
				return fmt.Errorf("remove sync recovery %q: %w", value.path, err)
			}
		} else {
			if err := removeRecoveryDirectory(ctx, db, root, worktree, value, config.fsActionFault); err != nil {
				return fmt.Errorf("remove sync recovery %q: %w", value.path, err)
			}
		}
		if _, err := db.ExecContext(ctx, "DELETE FROM sync_recoveries WHERE worktree = ? AND path = ?", worktree, value.path); err != nil {
			return err
		}
	}
	return nil
}

type rollbackCheckoutPath struct {
	path, kind, id, mtime, rollbackName string
	size                                int64
	tempDevice, tempInode               uint64
	targetDevice, targetInode           uint64
	completed                           bool
}

func rollbackSyncApply(ctx context.Context, db *sql.DB, root *openedWorktree, clientDir, worktree string, config libraryClientConfig) error {
	cacheRoot, err := openVerifiedCacheRoot(clientDir)
	if err != nil {
		return err
	}
	defer cacheRoot.Close()
	temps, err := checkoutTempNames(ctx, db, worktree)
	if err != nil {
		return err
	}
	if err := cleanupCheckoutTemps(ctx, db, root, worktree, fsPhaseRollback, temps, config.fsActionFault); err != nil {
		return err
	}
	if err := rollbackInstalledCheckoutPaths(ctx, db, root, cacheRoot, worktree, config); err != nil {
		return err
	}
	return rollbackSyncRecoveries(ctx, db, root, worktree, config)
}

func loadRollbackCheckoutPaths(ctx context.Context, db *sql.DB, worktree string) ([]rollbackCheckoutPath, error) {
	rows, err := db.QueryContext(ctx, `SELECT path, type, object_id, canonical_mtime, size, rollback_name,
		temp_device, temp_inode, target_device, target_inode, completed FROM checkout_paths WHERE worktree = ?`, worktree)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var values []rollbackCheckoutPath
	for rows.Next() {
		var value rollbackCheckoutPath
		if err := rows.Scan(&value.path, &value.kind, &value.id, &value.mtime, &value.size, &value.rollbackName,
			&value.tempDevice, &value.tempInode, &value.targetDevice, &value.targetInode, &value.completed); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].kind != values[j].kind {
			return values[i].kind == "File"
		}
		leftDepth, rightDepth := strings.Count(values[i].path, "/"), strings.Count(values[j].path, "/")
		if leftDepth != rightDepth {
			return leftDepth > rightDepth
		}
		return values[i].path > values[j].path
	})
	return values, nil
}

func deleteRolledBackCheckoutPath(ctx context.Context, db *sql.DB, worktree, path string) error {
	_, err := db.ExecContext(ctx, `DELETE FROM checkout_paths WHERE worktree=? AND path=? AND NOT EXISTS (
		SELECT 1 FROM fs_actions origin JOIN fs_actions preserve
			ON preserve.worktree=origin.worktree AND preserve.origin_action_id=origin.action_id
		WHERE origin.worktree=checkout_paths.worktree AND origin.source_name=checkout_paths.temp_name
			AND origin.action_outcome='rolled_back')`, worktree, path)
	return err
}

func rollbackInstalledCheckoutPaths(ctx context.Context, db *sql.DB, root *openedWorktree, cacheRoot *os.File, worktree string, config libraryClientConfig) error {
	values, err := loadRollbackCheckoutPaths(ctx, db, worktree)
	if err != nil {
		return err
	}
	for _, value := range values {
		parent, name, found, err := openRollbackParent(root, value.path)
		if err != nil {
			return err
		}
		if !found {
			if err := deleteRolledBackCheckoutPath(ctx, db, worktree, value.path); err != nil {
				return err
			}
			continue
		}
		err = rollbackCheckoutTarget(ctx, db, root, parent, cacheRoot, name, worktree, value, config)
		closeErr := parent.Close()
		if err != nil || closeErr != nil {
			return errors.Join(err, closeErr)
		}
	}
	return nil
}

func rollbackCheckoutTarget(ctx context.Context, db *sql.DB, root *openedWorktree, parent, cacheRoot *os.File, name, worktree string, value rollbackCheckoutPath, config libraryClientConfig) error {
	device, inode := value.tempDevice, value.tempInode
	if value.kind == "Directory" && value.targetDevice != 0 && value.targetInode != 0 {
		device, inode = value.targetDevice, value.targetInode
	}
	_, visibleErr := statAt(parent, name)
	visibleExists := visibleErr == nil
	if visibleErr != nil && !errors.Is(visibleErr, syscall.ENOENT) {
		return visibleErr
	}
	if value.rollbackName == "" {
		if !visibleExists {
			return deleteRolledBackCheckoutPath(ctx, db, worktree, value.path)
		}
		if device == 0 || inode == 0 {
			return fmt.Errorf("checkout target %q has no matching registered identity", value.path)
		}
		if err := verifyRollbackCheckoutTarget(parent, cacheRoot, name, value, device, inode); err != nil {
			return fmt.Errorf("checkout target %q changed after installation: %w", value.path, err)
		}
		var err error
		value.rollbackName, err = newReservedName(syncTombstonePrefix)
		if err != nil {
			return err
		}
		result, err := db.ExecContext(ctx, `UPDATE checkout_paths SET rollback_name = ?
			WHERE worktree = ? AND path = ? AND rollback_name = ''`, value.rollbackName, worktree, value.path)
		if err != nil {
			return err
		}
		if changed, err := result.RowsAffected(); err != nil || changed != 1 {
			return errors.Join(errors.New("register checkout rollback name failed"), err)
		}
	}
	tombstone, tombstoneErr := statAt(parent, value.rollbackName)
	tombstoneExists := tombstoneErr == nil
	if tombstoneErr != nil && !errors.Is(tombstoneErr, syscall.ENOENT) {
		return tombstoneErr
	}
	switch {
	case visibleExists && !tombstoneExists:
		if device == 0 || inode == 0 {
			return fmt.Errorf("checkout target %q changed before rollback", value.path)
		}
		if err := verifyRollbackCheckoutTarget(parent, cacheRoot, name, value, device, inode); err != nil {
			return fmt.Errorf("checkout target %q changed before rollback: %w", value.path, err)
		}
		parentPath, _ := splitFSActionPath(value.path)
		if err := journalRename(ctx, db, root, worktree, fsPhaseRollback, parentPath, name, value.rollbackName,
			value.kind, "", value.rollbackName, device, inode, config.fsActionFault); err != nil {
			return fmt.Errorf("isolate checkout target %q for rollback: %w", value.path, err)
		}
		if err := syncRollbackParent(parent, config); err != nil {
			return err
		}
	case !visibleExists && tombstoneExists:
		if !statMatchesCheckoutTarget(tombstone, value.kind, device, inode) {
			return fmt.Errorf("checkout rollback tombstone %q changed identity", value.path)
		}
	case !visibleExists && !tombstoneExists:
		return deleteRolledBackCheckoutPath(ctx, db, worktree, value.path)
	default:
		return fmt.Errorf("checkout target %q has ambiguous rollback state", value.path)
	}
	if err := verifyRollbackCheckoutTarget(parent, cacheRoot, value.rollbackName, value, device, inode); err != nil {
		return fmt.Errorf("checkout rollback tombstone %q changed before deletion: %w", value.path, err)
	}
	parentPath, _ := splitFSActionPath(value.path)
	if err := journalRemove(ctx, db, root, worktree, fsPhaseRollback, parentPath, value.rollbackName,
		value.kind, value.rollbackName, value.id, value.mtime, value.size, device, inode, config.fsActionFault); err != nil {
		return fmt.Errorf("remove checkout target %q during rollback: %w", value.path, err)
	}
	if err := syncRollbackParent(parent, config); err != nil {
		return err
	}
	return deleteRolledBackCheckoutPath(ctx, db, worktree, value.path)
}

func verifyRollbackCheckoutTarget(parent, cacheRoot *os.File, name string, value rollbackCheckoutPath, device, inode uint64) error {
	if value.kind != "File" {
		stat, err := statAt(parent, name)
		if err != nil || !statMatchesCheckoutTarget(stat, value.kind, device, inode) {
			return errors.Join(errors.New("checkout target identity or type changed"), err)
		}
		return nil
	}
	metadata, err := readCachedRollbackFile(cacheRoot, value.id)
	if err != nil || metadata.Size != value.size {
		return errors.Join(errors.New("cached checkout File metadata changed"), err)
	}
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), value.path)
	defer file.Close()
	var before unix.Stat_t
	if err := unix.Fstat(fd, &before); err != nil || !statMatchesCheckoutTarget(before, "File", device, inode) ||
		before.Size != value.size || canonicalStatMtime(before) != value.mtime {
		return errors.Join(errors.New("checkout file identity, size, or canonical mtime changed"), err)
	}
	buffer := make([]byte, object.MaxBlockSize)
	for index, blockID := range metadata.Blocks {
		size := object.MaxBlockSize
		if index == len(metadata.Blocks)-1 {
			size = int(metadata.Size - int64(index*object.MaxBlockSize))
		}
		count, readErr := io.ReadFull(file, buffer[:size])
		if readErr != nil || count != size || object.ID(buffer[:count]) != blockID {
			return errors.Join(errors.New("checkout file content changed"), readErr)
		}
	}
	var extra [1]byte
	if count, readErr := file.Read(extra[:]); count != 0 || !errors.Is(readErr, io.EOF) {
		return errors.New("checkout file size changed")
	}
	var after unix.Stat_t
	if err := unix.Fstat(fd, &after); err != nil || !sameRollbackFileState(before, after) || !statMatchesCheckoutTarget(after, "File", device, inode) {
		return errors.Join(errors.New("checkout file changed during rollback verification"), err)
	}
	return nil
}

func sameRollbackFileState(left, right unix.Stat_t) bool {
	return left.Dev == right.Dev && left.Ino == right.Ino && left.Mode == right.Mode && left.Nlink == right.Nlink &&
		left.Size == right.Size && left.Mtim == right.Mtim && left.Ctim == right.Ctim
}

func canonicalStatMtime(stat unix.Stat_t) string {
	return time.Unix(stat.Mtim.Sec, stat.Mtim.Nsec).UTC().Truncate(time.Second).Format("2006-01-02T15:04:05Z")
}

func readCachedRollbackFile(cacheRoot *os.File, id string) (object.File, error) {
	if cacheRoot == nil || !object.ValidID(id) {
		return object.File{}, errors.New("invalid cached checkout File")
	}
	fd, err := unix.Dup(int(cacheRoot.Fd()))
	if err != nil {
		return object.File{}, err
	}
	current := os.NewFile(uintptr(fd), cacheRoot.Name())
	for _, name := range []string{"objects", "files", id[:2]} {
		next, openErr := unix.Openat(int(current.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		current.Close()
		if openErr != nil {
			return object.File{}, fmt.Errorf("open cached checkout File metadata: %w", openErr)
		}
		current = os.NewFile(uintptr(next), name)
	}
	defer current.Close()
	data, found, err := readCacheFile(current, id[2:])
	if err != nil || !found {
		return object.File{}, errors.Join(errors.New("cached checkout File metadata is missing"), err)
	}
	file, err := object.VerifyFile(data, id)
	if err != nil {
		return object.File{}, errors.New("cached checkout File metadata failed verification")
	}
	return file, nil
}

func statMatchesCheckoutTarget(stat unix.Stat_t, kind string, device, inode uint64) bool {
	mode := uint32(syscall.S_IFREG)
	if kind == "Directory" {
		mode = syscall.S_IFDIR
	}
	return uint64(stat.Dev) == device && stat.Ino == inode && stat.Mode&syscall.S_IFMT == mode && (kind != "File" || stat.Nlink == 1)
}

func openRollbackParent(root *openedWorktree, path string) (*os.File, string, bool, error) {
	components := strings.Split(path, "/")
	current, err := unix.Dup(int(root.directory.Fd()))
	if err != nil {
		return nil, "", false, err
	}
	currentPath := root.path
	for _, component := range components[:len(components)-1] {
		next, openErr := unix.Openat(current, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		unix.Close(current)
		if errors.Is(openErr, syscall.ENOENT) {
			return nil, "", false, nil
		}
		if openErr != nil {
			return nil, "", false, openErr
		}
		current, currentPath = next, filepath.Join(currentPath, component)
	}
	return os.NewFile(uintptr(current), currentPath), components[len(components)-1], true, nil
}

func syncRollbackParent(parent *os.File, config libraryClientConfig) error {
	if err := parent.Sync(); err != nil {
		return err
	}
	return config.syncDirectory(parent.Name())
}

func rollbackSyncRecoveries(ctx context.Context, db *sql.DB, root *openedWorktree, worktree string, config libraryClientConfig) error {
	values, err := loadSyncRecoveries(ctx, db, worktree)
	if err != nil {
		return err
	}
	for _, value := range values {
		visible, visibleErr := statAt(root.directory, value.path)
		hidden, hiddenErr := statAt(root.directory, value.name)
		visibleExists, hiddenExists := visibleErr == nil, hiddenErr == nil
		if visibleErr != nil && !errors.Is(visibleErr, syscall.ENOENT) {
			return visibleErr
		}
		if hiddenErr != nil && !errors.Is(hiddenErr, syscall.ENOENT) {
			return hiddenErr
		}
		switch {
		case !hiddenExists && visibleExists:
			if !statMatchesRecovery(visible, value) || verifyNamedRecovery(root.directory, value.path, value) != nil {
				return fmt.Errorf("restored sync recovery %q changed identity or content", value.path)
			}
			if err := syncRollbackParent(root.directory, config); err != nil {
				return err
			}
		case hiddenExists && !visibleExists:
			if !statMatchesRecovery(hidden, value) {
				return fmt.Errorf("sync recovery %q changed identity", value.path)
			}
			if err := verifyNamedRecovery(root.directory, value.name, value); err != nil {
				return fmt.Errorf("verify sync recovery %q for rollback: %w", value.path, err)
			}
			if err := journalRename(ctx, db, root, worktree, fsPhaseRollback, "", value.name, value.path,
				value.kind, value.name, "", value.device, value.inode, config.fsActionFault); err != nil {
				return fmt.Errorf("restore sync recovery %q: %w", value.path, err)
			}
			if err := syncRollbackParent(root.directory, config); err != nil {
				return err
			}
		default:
			return fmt.Errorf("sync recovery %q has ambiguous rollback state", value.path)
		}
		if config.afterSyncRecoveryRestore != nil {
			if err := config.afterSyncRecoveryRestore(value.path); err != nil {
				return fmt.Errorf("after sync recovery restore: %w", err)
			}
		}
		if _, err := db.ExecContext(ctx, "DELETE FROM sync_recoveries WHERE worktree = ? AND path = ?", worktree, value.path); err != nil {
			return err
		}
	}
	return nil
}

func newReservedName(prefix string) (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(value[:]), nil
}

func updateBindingAndIndex(ctx context.Context, tx *sql.Tx, binding clientBinding, commit, root, etag string, paths []checkoutPath) error {
	worktree := binding.Worktree
	result, err := tx.ExecContext(ctx, `UPDATE bindings SET sync_base_commit = ?, sync_base_root = ?, head_etag = ?
		WHERE server_url = ? AND library_id = ? AND worktree = ? AND user_id = ? AND device_id = ?
		AND sync_base_commit = ? AND sync_base_root = ? AND head_etag = ?`, commit, root, etag,
		binding.ServerURL, binding.LibraryID, binding.Worktree, binding.UserID, binding.DeviceID,
		binding.SyncBase, binding.SyncBaseRoot, binding.HeadETag)
	if err != nil {
		return err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return errors.New("synchronization did not update one binding")
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM path_index WHERE worktree = ?", worktree); err != nil {
		return err
	}
	for _, path := range paths {
		if _, err := tx.ExecContext(ctx, `INSERT INTO path_index(worktree, path, type, object_id, canonical_mtime, actual_mtime, size)
			VALUES (?, ?, ?, ?, ?, ?, ?)`, worktree, path.path, path.kind, path.id, path.mtime, path.mtime, path.size); err != nil {
			return err
		}
	}
	return nil
}

func replacePathIndex(ctx context.Context, db *sql.DB, worktree string, paths []checkoutPath) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM path_index WHERE worktree = ?", worktree); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	for _, path := range paths {
		if _, err := tx.ExecContext(ctx, `INSERT INTO path_index(worktree, path, type, object_id, canonical_mtime, actual_mtime, size)
			VALUES (?, ?, ?, ?, ?, ?, ?)`, worktree, path.path, path.kind, path.id, path.mtime, path.mtime, path.size); err != nil {
			return errors.Join(err, tx.Rollback())
		}
	}
	return tx.Commit()
}
