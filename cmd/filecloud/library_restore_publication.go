package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/mingming-cn/filecloud/internal/object"
)

const (
	_restorePreviewHeader   = 8
	_restorePreviewMaxBytes = _restorePreviewHeader + _restorePreviewPathLimit*(4+_mergeMaxPath)
)

var _emptyRestorePreview = []byte{'F', 'R', 'P', '1', 0, 0, 0, 0}

func _encodeRestorePreview(paths []string) ([]byte, error) {
	if len(paths) > _restorePreviewPathLimit {
		return nil, errors.New("restore preview exceeds path limit")
	}
	ordered := append([]string(nil), paths...)
	sort.Strings(ordered)
	for index, path := range ordered {
		if !object.ValidPath(path) {
			return nil, fmt.Errorf("restore preview path %q is invalid", path)
		}
		if index > 0 && ordered[index-1] == path {
			return nil, errors.New("restore preview contains a duplicate path")
		}
	}
	result := make([]byte, _restorePreviewHeader)
	copy(result, _emptyRestorePreview[:4])
	binary.BigEndian.PutUint32(result[4:8], uint32(len(ordered)))
	for _, path := range ordered {
		if len(path) > _mergeMaxPath || len(result) > _restorePreviewMaxBytes-4-len(path) {
			return nil, errors.New("restore preview exceeds metadata budget")
		}
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(path)))
		result = append(result, length[:]...)
		result = append(result, path...)
	}
	return result, nil
}

func _decodeRestorePreview(data []byte) ([]string, error) {
	if len(data) < _restorePreviewHeader || len(data) > _restorePreviewMaxBytes || !bytes.Equal(data[:4], _emptyRestorePreview[:4]) {
		return nil, errors.New("restore preview has an invalid encoding")
	}
	count := binary.BigEndian.Uint32(data[4:8])
	if count > _restorePreviewPathLimit {
		return nil, errors.New("restore preview exceeds path limit")
	}
	paths := make([]string, 0, int(count))
	offset := _restorePreviewHeader
	for range count {
		if len(data)-offset < 4 {
			return nil, errors.New("restore preview is truncated")
		}
		length := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		if length == 0 || length > _mergeMaxPath || uint64(length) > uint64(len(data)-offset) {
			return nil, errors.New("restore preview contains an invalid path length")
		}
		path := string(data[offset : offset+int(length)])
		offset += int(length)
		if !object.ValidPath(path) || len(paths) != 0 && paths[len(paths)-1] >= path {
			return nil, errors.New("restore preview paths are not canonical")
		}
		paths = append(paths, path)
	}
	if offset != len(data) {
		return nil, errors.New("restore preview has trailing data")
	}
	canonical, err := _encodeRestorePreview(paths)
	if err != nil || !bytes.Equal(canonical, data) {
		return nil, errors.New("restore preview is not canonical")
	}
	return paths, nil
}

func validateRestorePublicationFields(value pendingPublication) error {
	if value.Kind != PublicationKindRestore {
		return fmt.Errorf("unsupported pending publication kind %q", value.Kind)
	}
	if !object.ValidID(value.SourceCommit) || !object.ValidID(value.SourceRoot) || !object.ValidPath(value.SourcePath) {
		return errors.New("restore publication source fields are invalid")
	}
	if len(value.CandidateData) == 0 || len(value.CandidateData) > _maxCandidateCommitBytes ||
		len(value.CapturedData) == 0 || len(value.CapturedData) > _maxCandidateCommitBytes {
		return errors.New("restore publication commit metadata exceeds synchronization budget")
	}
	if len(value.CandidateHistory) != 0 || value.DeletionCount != 0 || value.TrackedCount != 0 ||
		value.RequiresDeleteConfirmation || value.DeleteConfirmed || value.LegacyRevalidationRequired {
		return errors.New("restore publication contains synchronization-only state")
	}
	if value.CandidateRoot == value.CapturedRoot {
		return errors.New("restore publication candidate root must differ from captured root")
	}
	if value.ChangedPathCount <= 0 {
		return errors.New("restore publication must contain changed paths")
	}
	if value.ChangedPathCount != value.CreatedCount+value.UpdatedCount+value.TypeReplacementCount {
		return errors.New("restore publication statistics do not match changed path count")
	}
	for _, count := range []int64{value.CreatedCount, value.UpdatedCount, value.TypeReplacementCount,
		value.RemovedDescendantCount, value.PreservedCurrentOnlyCount, value.ChangedPathCount} {
		if count < 0 || count > _mergeMaxObjects {
			return errors.New("restore publication statistics are invalid")
		}
	}
	paths, err := _decodeRestorePreview(value.ChangedPathPreview)
	if err != nil {
		return err
	}
	if value.ChangedPathCount < int64(len(paths)) {
		return errors.New("restore publication changed path count is smaller than its preview")
	}
	if value.PreviewTruncated {
		if len(paths) != _restorePreviewPathLimit || value.ChangedPathCount <= int64(len(paths)) {
			return errors.New("restore publication preview truncation is inconsistent")
		}
	} else if value.ChangedPathCount != int64(len(paths)) {
		return errors.New("restore publication changed path count does not match its preview")
	}
	return nil
}

func verifyRestorePublication(value pendingPublication, binding clientBinding) error {
	if err := validateRestorePublicationFields(value); err != nil {
		return err
	}
	if !validClientUUID(binding.UserID) || !validClientUUID(binding.DeviceID) ||
		!object.ValidID(binding.SyncBase) || !object.ValidID(binding.SyncBaseRoot) || binding.HeadETag == "" {
		return errors.New("restore publication binding is invalid")
	}
	if value.BaseCommit != binding.SyncBase || value.BaseRoot != binding.SyncBaseRoot ||
		value.ExpectedETag != binding.HeadETag || value.CapturedCommit != value.ExpectedHead {
		return errors.New("restore publication does not match the binding")
	}
	for _, id := range []string{value.BaseCommit, value.BaseRoot, value.ExpectedHead, value.CandidateCommit,
		value.CandidateRoot, value.CapturedCommit, value.CapturedRoot} {
		if !object.ValidID(id) {
			return errors.New("restore publication object identity is invalid")
		}
	}
	captured, err := object.VerifyCommit(value.CapturedData, value.CapturedCommit)
	if err != nil || captured.AuthorUserID != binding.UserID || captured.Root != value.CapturedRoot {
		return errors.New("restore publication captured commit is invalid")
	}
	candidate, err := object.VerifyCommit(value.CandidateData, value.CandidateCommit)
	if err != nil || candidate.AuthorUserID != binding.UserID || candidate.DeviceID != binding.DeviceID ||
		candidate.Root != value.CandidateRoot || len(candidate.Parents) != 1 || candidate.Parents[0] != value.ExpectedHead ||
		candidate.Message != "restore "+value.SourceCommit+" "+value.SourcePath {
		return errors.New("restore publication candidate commit is invalid")
	}
	if _, err := parseCanonicalProtocolMtime(candidate.CreatedAt); err != nil {
		return errors.New("restore publication candidate timestamp is invalid")
	}
	return nil
}

func isEmptyRestoreFields(value pendingPublication) bool {
	return value.SourceCommit == "" && value.SourcePath == "" && value.SourceRoot == "" &&
		value.CreatedCount == 0 && value.UpdatedCount == 0 && value.TypeReplacementCount == 0 &&
		value.RemovedDescendantCount == 0 && value.PreservedCurrentOnlyCount == 0 &&
		bytes.Equal(value.ChangedPathPreview, _emptyRestorePreview) && value.ChangedPathCount == 0 &&
		!value.PreviewTruncated && !value.RestoreConfirmed
}

func dispatchRestorePublication(ctx context.Context, db *sql.DB, options bindOptions, binding clientBinding,
	snapshot worktreeSnapshot, head remoteHead, pending pendingPublication, stdout io.Writer,
	config libraryClientConfig, budget *_replayBudget, mode _publicationDispatchMode) error {
	if err := verifyRestorePublication(pending, binding); err != nil {
		return fmt.Errorf("verify restore publication: %w", err)
	}
	if !pending.RestoreConfirmed {
		prefix := pending.CandidateCommit[:deleteCandidatePrefixLen]
		return fmt.Errorf("restore publication requires exact confirmation; rerun restore --confirm %s", prefix)
	}
	switch mode {
	case _publicationDispatchStart, _publicationDispatchResume:
		return resumeRestorePublication(ctx, db, options, binding, snapshot, head, pending, stdout, config, budget)
	default:
		return errors.New("restore publication dispatcher has an invalid mode")
	}
}

func resumeRestorePublication(ctx context.Context, db *sql.DB, options bindOptions, binding clientBinding,
	snapshot worktreeSnapshot, head remoteHead, pending pendingPublication, stdout io.Writer,
	config libraryClientConfig, budget *_replayBudget) error {
	if head.CommitID == nil {
		return discardRestorePublication(ctx, db, binding.Worktree, pending, "library Head is empty during restore")
	}
	if *head.CommitID == pending.CandidateCommit {
		if snapshot.root != pending.CapturedRoot {
			return discardRestorePublication(ctx, db, binding.Worktree, pending,
				"restore candidate is published but the worktree changed; rerun sync")
		}
		return transitionPublishedRestore(ctx, db, options, binding, snapshot, head, pending, stdout, config)
	}
	ancestor, err := _remoteCommitDescendsFrom(ctx, options, *head.CommitID, pending.CandidateCommit, binding.UserID, budget)
	if err != nil {
		return err
	}
	if ancestor {
		if snapshot.root != pending.CapturedRoot {
			return discardRestorePublication(ctx, db, binding.Worktree, pending,
				"restore candidate is published but the worktree changed; rerun sync")
		}
		return transitionPublishedRestore(ctx, db, options, binding, snapshot, head, pending, stdout, config)
	}
	if *head.CommitID != pending.ExpectedHead || head.ETag != pending.ExpectedETag || snapshot.root != pending.CapturedRoot {
		return discardRestorePublication(ctx, db, binding.Worktree, pending,
			"restore candidate became stale; rerun restore preview")
	}
	return uploadAndPublishRestore(ctx, db, options, binding, snapshot, pending, stdout, config, budget)
}

func discardRestorePublication(ctx context.Context, db *sql.DB, worktree string, pending pendingPublication, message string) error {
	if err := _deletePendingPublication(ctx, db, worktree, pending, "restore publication changed before discard"); err != nil {
		return err
	}
	return errors.New(message)
}

func uploadAndPublishRestore(ctx context.Context, db *sql.DB, options bindOptions, binding clientBinding,
	snapshot worktreeSnapshot, pending pendingPublication, stdout io.Writer, config libraryClientConfig,
	budget *_replayBudget) error {
	source, err := fetchRestoreSourceWithOptions(ctx, options, binding, pending.SourceCommit)
	if err != nil {
		return err
	}
	plan, err := planRestoreSnapshotWithOptions(ctx, options, snapshot, source, pending.SourcePath)
	if err != nil {
		return err
	}
	preview, err := _encodeRestorePreview(plan.changedPaths)
	if err != nil {
		return err
	}
	if plan.resultRoot != pending.CandidateRoot || plan.createdCount != pending.CreatedCount || plan.updatedCount != pending.UpdatedCount ||
		plan.typeReplacementCount != pending.TypeReplacementCount || plan.removedDescendantCount != pending.RemovedDescendantCount ||
		plan.preservedCurrentOnlyCount != pending.PreservedCurrentOnlyCount || plan.changedPathCount != pending.ChangedPathCount ||
		plan.previewTruncated != pending.PreviewTruncated || !bytes.Equal(preview, pending.ChangedPathPreview) {
		return discardRestorePublication(ctx, db, binding.Worktree, pending, "restore candidate became stale; rerun restore preview")
	}
	references := make([]clientObjectReference, 0, len(plan.directories)+1)
	for _, directory := range plan.directories {
		references = append(references, clientObjectReference{ObjectID: directory.id, ObjectType: "Directory"})
	}
	references = append(references, clientObjectReference{ObjectID: pending.CandidateCommit, ObjectType: "Commit"})
	missing, err := checkRemoteObjects(ctx, options, references)
	if err != nil {
		return err
	}
	for _, directory := range plan.directories {
		if !missing["directories\x00"+directory.id] {
			continue
		}
		if err := putMetadata(ctx, options.base, options.libraryID, options.token, "directories", directory.id, directory.data); err != nil {
			return err
		}
	}
	if missing["commits\x00"+pending.CandidateCommit] {
		if err := putMetadata(ctx, options.base, options.libraryID, options.token, "commits", pending.CandidateCommit, pending.CandidateData); err != nil {
			return err
		}
	}
	return publishPending(ctx, db, options, binding, snapshot, pending, stdout, config, budget)
}

func transitionPublishedRestore(ctx context.Context, db *sql.DB, options bindOptions, binding clientBinding,
	snapshot worktreeSnapshot, head remoteHead, pending pendingPublication, stdout io.Writer,
	config libraryClientConfig) error {
	if head.CommitID == nil {
		return errors.New("published restore Head is empty")
	}
	checkout := pendingCheckout{ServerURL: binding.ServerURL, LibraryID: binding.LibraryID, Worktree: binding.Worktree,
		UserID: binding.UserID, DeviceID: binding.DeviceID, TargetCommit: *head.CommitID, HeadETag: head.ETag,
		ApplyState: "pending", ConflictPromotions: _emptyConflictPromotions}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := updateBindingAndIndex(ctx, tx, binding, pending.CapturedCommit, pending.CapturedRoot, head.ETag, snapshot.paths); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO pending_checkouts(server_url, library_id, worktree, user_id, device_id,
		target_commit, target_root, head_etag, apply_state, conflict_promotions)
		VALUES (?, ?, ?, ?, ?, ?, '', ?, 'pending', ?)`, checkout.ServerURL, checkout.LibraryID, checkout.Worktree,
		checkout.UserID, checkout.DeviceID, checkout.TargetCommit, checkout.HeadETag, checkout.ConflictPromotions); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	if err := _deletePendingPublication(ctx, tx, binding.Worktree, pending,
		"restore publication changed during checkout transition"); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("transition restore publication to checkout: %w", err)
	}
	binding.SyncBase, binding.SyncBaseRoot, binding.HeadETag = pending.CapturedCommit, pending.CapturedRoot, head.ETag
	if config.afterSyncCheckoutTransition != nil {
		if err := config.afterSyncCheckoutTransition(); err != nil {
			return fmt.Errorf("after restore checkout transition: %w", err)
		}
	}
	return continueSyncCheckout(ctx, db, options, binding, checkout, stdout, config)
}
