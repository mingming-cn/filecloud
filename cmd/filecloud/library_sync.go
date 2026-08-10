package main

import (
	"context"
	"crypto/rand"
	"database/sql"
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
	syncRecoveryPrefix  = ".filecloud-internal-sync-"
	syncTombstonePrefix = ".filecloud-internal-sync-trash-"
	maxSyncParentWalk   = 1024
)

type pendingPublication struct {
	BaseCommit, BaseRoot, ExpectedETag string
	CandidateCommit, CandidateRoot     string
	CandidateData                      []byte
}

type syncRecovery struct {
	path, name, tombstone, kind, id, mtime string
	size                                   int64
	device, inode                          uint64
	completed                              bool
}

func syncLibrary(ctx context.Context, db *sql.DB, options bindOptions, binding clientBinding, stdout io.Writer, config libraryClientConfig) error {
	pending, err := loadPendingPublication(ctx, db, binding.Worktree)
	if err != nil {
		return err
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
		return resumePublication(ctx, db, options, binding, snapshot, head, *pending, stdout, config)
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
		return errors.New("local and remote libraries both changed; merge is not supported yet")
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
		pending = &pendingPublication{BaseCommit: binding.SyncBase, BaseRoot: binding.SyncBaseRoot, ExpectedETag: head.ETag,
			CandidateCommit: id, CandidateRoot: snapshot.root, CandidateData: data}
		if err := uploadSnapshot(ctx, options, snapshot); err != nil {
			return err
		}
		if err := putMetadata(ctx, options.base, options.libraryID, options.token, "commits", id, data); err != nil {
			return err
		}
		if err := savePendingPublication(ctx, db, binding.Worktree, *pending); err != nil {
			return err
		}
		return publishPending(ctx, db, options, binding, snapshot, *pending, stdout, config)
	}
}

func loadPendingPublication(ctx context.Context, db *sql.DB, worktree string) (*pendingPublication, error) {
	var value pendingPublication
	err := db.QueryRowContext(ctx, `SELECT base_commit, base_root, expected_etag, candidate_commit, candidate_root, candidate_data
		FROM pending_publications WHERE worktree = ?`, worktree).Scan(&value.BaseCommit, &value.BaseRoot, &value.ExpectedETag,
		&value.CandidateCommit, &value.CandidateRoot, &value.CandidateData)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read pending publication: %w", err)
	}
	return &value, nil
}

func savePendingPublication(ctx context.Context, db *sql.DB, worktree string, value pendingPublication) error {
	_, err := db.ExecContext(ctx, `INSERT INTO pending_publications(worktree, base_commit, base_root, expected_etag,
		candidate_commit, candidate_root, candidate_data) VALUES (?, ?, ?, ?, ?, ?, ?)`, worktree, value.BaseCommit,
		value.BaseRoot, value.ExpectedETag, value.CandidateCommit, value.CandidateRoot, value.CandidateData)
	if err != nil {
		return fmt.Errorf("save pending publication: %w", err)
	}
	return nil
}

func verifyPendingPublication(value pendingPublication, binding clientBinding) error {
	commit, err := object.VerifyCommit(value.CandidateData, value.CandidateCommit)
	if err != nil || value.BaseCommit != binding.SyncBase || value.BaseRoot != binding.SyncBaseRoot ||
		commit.Root != value.CandidateRoot || commit.AuthorUserID != binding.UserID || commit.DeviceID != binding.DeviceID ||
		len(commit.Parents) != 1 || commit.Parents[0] != value.BaseCommit {
		return errors.New("pending publication is corrupt or does not match the binding")
	}
	return nil
}

func resumePublication(ctx context.Context, db *sql.DB, options bindOptions, binding clientBinding, snapshot worktreeSnapshot,
	head remoteHead, pending pendingPublication, stdout io.Writer, config libraryClientConfig) error {
	if err := verifyPendingPublication(pending, binding); err != nil {
		return err
	}
	if snapshot.root != pending.CandidateRoot {
		return errors.New("worktree changed after pending publication was created")
	}
	if *head.CommitID == pending.CandidateCommit {
		return finalizePublished(ctx, db, binding, snapshot, head, pending, stdout)
	}
	if *head.CommitID != pending.BaseCommit || head.ETag != pending.ExpectedETag {
		ancestor, err := remoteCommitDescendsFrom(ctx, options, *head.CommitID, pending.CandidateCommit, binding.UserID)
		if err != nil {
			return err
		}
		if !ancestor {
			return errors.New("local and remote libraries both changed; merge is not supported yet")
		}
		return transitionPublishedSuccessor(ctx, db, options, binding, snapshot, head, pending, stdout, config)
	}
	if err := uploadSnapshot(ctx, options, snapshot); err != nil {
		return err
	}
	if err := putMetadata(ctx, options.base, options.libraryID, options.token, "commits", pending.CandidateCommit, pending.CandidateData); err != nil {
		return err
	}
	return publishPending(ctx, db, options, binding, snapshot, pending, stdout, config)
}

func remoteCommitDescendsFrom(ctx context.Context, options bindOptions, head, ancestor, owner string) (bool, error) {
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
		if len(seen) > maxSyncParentWalk {
			return false, errors.New("remote commit history exceeds synchronization budget")
		}
		commit, err := getRemoteCommit(ctx, options.base, options.libraryID, options.token, id)
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
	if err := updateBindingAndIndex(ctx, tx, binding.Worktree, pending.CandidateCommit, pending.CandidateRoot, head.ETag, snapshot.paths); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO pending_checkouts(server_url, library_id, worktree, user_id, device_id,
		target_commit, target_root, head_etag, apply_state) VALUES (?, ?, ?, ?, ?, ?, '', ?, 'pending')`, checkout.ServerURL,
		checkout.LibraryID, checkout.Worktree, checkout.UserID, checkout.DeviceID, checkout.TargetCommit, checkout.HeadETag); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM pending_publications WHERE worktree = ?", binding.Worktree); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("transition superseded publication: %w", err)
	}
	binding.SyncBase, binding.SyncBaseRoot, binding.HeadETag = pending.CandidateCommit, pending.CandidateRoot, head.ETag
	return continueSyncCheckout(ctx, db, options, binding, checkout, stdout, config)
}

func publishPending(ctx context.Context, db *sql.DB, options bindOptions, binding clientBinding, snapshot worktreeSnapshot,
	pending pendingPublication, stdout io.Writer, config libraryClientConfig) error {
	if config.beforeHeadCAS != nil {
		if err := config.beforeHeadCAS(); err != nil {
			return fmt.Errorf("prepare Head publication: %w", err)
		}
	}
	verified, err := scanWorktreeWithConfig(options.worktreeRoot, options.scanConfig)
	if err != nil || verified.root != pending.CandidateRoot {
		return errors.New("worktree changed before Head publication")
	}
	_, _, publishErr := updateRemoteHead(ctx, options.base, options.libraryID, options.token, pending.ExpectedETag, pending.CandidateCommit)
	published, getErr := getRemoteHead(ctx, options.base, options.libraryID, options.token)
	if getErr != nil {
		return errors.Join(publishErr, fmt.Errorf("resolve library Head after publish: %w", getErr))
	}
	if published.CommitID == nil || *published.CommitID != pending.CandidateCommit {
		if published.CommitID != nil && *published.CommitID != pending.BaseCommit {
			ancestor, err := remoteCommitDescendsFrom(ctx, options, *published.CommitID, pending.CandidateCommit, binding.UserID)
			if err != nil {
				return errors.Join(publishErr, err)
			}
			if ancestor {
				return transitionPublishedSuccessor(ctx, db, options, binding, snapshot, published, pending, stdout, config)
			}
			return errors.Join(publishErr, errors.New("local and remote libraries both changed; merge is not supported yet"))
		}
		return errors.Join(publishErr, errors.New("library Head publication did not complete; rerun sync"))
	}
	return finalizePublished(ctx, db, binding, snapshot, published, pending, stdout)
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
	if err := updateBindingAndIndex(ctx, tx, binding.Worktree, pending.CandidateCommit, pending.CandidateRoot, head.ETag, snapshot.paths); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM pending_publications WHERE worktree = ?", binding.Worktree); err != nil {
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
	if err := finalizeSyncApply(ctx, db, binding.Worktree, pending, paths); err != nil {
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
			if err := unix.Renameat2(int(root.directory.Fd()), value.path, int(root.directory.Fd()), value.name, unix.RENAME_NOREPLACE); err != nil {
				return fmt.Errorf("move existing path %q to registered recovery: %w", value.path, err)
			}
			if err := root.directory.Sync(); err != nil {
				return err
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

func finalizeSyncApply(ctx context.Context, db *sql.DB, worktree string, pending pendingCheckout, paths []checkoutPath) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := updateBindingAndIndex(ctx, tx, worktree, pending.TargetCommit, pending.TargetRoot, pending.HeadETag, paths); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	result, err := tx.ExecContext(ctx, `UPDATE pending_checkouts SET apply_state = 'finalized' WHERE worktree = ? AND apply_state = 'applying'`, worktree)
	if err != nil {
		return errors.Join(err, tx.Rollback())
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return errors.Join(errors.New("finalize sync apply did not advance pending state"), err, tx.Rollback())
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("finalize synchronized checkout: %w", err)
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
	for _, query := range []string{"DELETE FROM checkout_paths WHERE worktree = ?", "DELETE FROM pending_checkouts WHERE worktree = ? AND apply_state = 'finalized'"} {
		if _, err := tx.ExecContext(ctx, query, pending.Worktree); err != nil {
			return errors.Join(err, tx.Rollback())
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("complete synchronized checkout cleanup: %w", err)
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
			if err := unix.Renameat2(int(root.directory.Fd()), value.name, int(root.directory.Fd()), value.tombstone, unix.RENAME_NOREPLACE); err != nil {
				return fmt.Errorf("isolate sync recovery %q for cleanup: %w", value.path, err)
			}
			if err := root.directory.Sync(); err != nil {
				return err
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
		if err := removeReservedTree(root.directory, value.tombstone, value.device, value.inode, value.kind); err != nil {
			return fmt.Errorf("remove sync recovery %q: %w", value.path, err)
		}
		if err := root.directory.Sync(); err != nil {
			return err
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
	if err := cleanupCheckoutTemps(root, temps); err != nil {
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
			if _, err := db.ExecContext(ctx, "DELETE FROM checkout_paths WHERE worktree = ? AND path = ?", worktree, value.path); err != nil {
				return err
			}
			continue
		}
		err = rollbackCheckoutTarget(ctx, db, parent, cacheRoot, name, worktree, value, config)
		closeErr := parent.Close()
		if err != nil || closeErr != nil {
			return errors.Join(err, closeErr)
		}
	}
	return nil
}

func rollbackCheckoutTarget(ctx context.Context, db *sql.DB, parent, cacheRoot *os.File, name, worktree string, value rollbackCheckoutPath, config libraryClientConfig) error {
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
			_, err := db.ExecContext(ctx, "DELETE FROM checkout_paths WHERE worktree = ? AND path = ?", worktree, value.path)
			return err
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
		if err := unix.Renameat2(int(parent.Fd()), name, int(parent.Fd()), value.rollbackName, unix.RENAME_NOREPLACE); err != nil {
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
		_, err := db.ExecContext(ctx, "DELETE FROM checkout_paths WHERE worktree = ? AND path = ?", worktree, value.path)
		return err
	default:
		return fmt.Errorf("checkout target %q has ambiguous rollback state", value.path)
	}
	if err := verifyRollbackCheckoutTarget(parent, cacheRoot, value.rollbackName, value, device, inode); err != nil {
		return fmt.Errorf("checkout rollback tombstone %q changed before deletion: %w", value.path, err)
	}
	if err := removeRollbackTarget(parent, value.rollbackName, device, inode, value.kind); err != nil {
		return fmt.Errorf("remove checkout target %q during rollback: %w", value.path, err)
	}
	if err := syncRollbackParent(parent, config); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx, "DELETE FROM checkout_paths WHERE worktree = ? AND path = ?", worktree, value.path)
	return err
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

func removeRollbackTarget(parent *os.File, name string, device, inode uint64, kind string) error {
	flags := unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC
	if kind == "Directory" {
		flags |= unix.O_DIRECTORY
	}
	fd, err := unix.Openat(int(parent.Fd()), name, flags, 0)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), name)
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil || !statMatchesCheckoutTarget(stat, kind, device, inode) {
		return errors.Join(errors.New("checkout rollback target identity changed"), err, file.Close())
	}
	if kind == "Directory" {
		if _, err := file.Readdirnames(1); err == nil {
			return errors.Join(errors.New("checkout rollback directory contains unexpected user content"), file.Close())
		} else if !errors.Is(err, io.EOF) {
			return errors.Join(err, file.Close())
		}
	}
	if err := file.Close(); err != nil {
		return err
	}
	unlinkFlags := 0
	if kind == "Directory" {
		unlinkFlags = unix.AT_REMOVEDIR
	}
	return unix.Unlinkat(int(parent.Fd()), name, unlinkFlags)
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
			if err := unix.Renameat2(int(root.directory.Fd()), value.name, int(root.directory.Fd()), value.path, unix.RENAME_NOREPLACE); err != nil {
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

func removeReservedTree(parent *os.File, name string, device, inode uint64, kind string) error {
	flags := unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC
	if kind == "Directory" {
		flags |= unix.O_DIRECTORY
	}
	fd, err := unix.Openat(int(parent.Fd()), name, flags, 0)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), name)
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return errors.Join(err, file.Close())
	}
	expectedMode := uint32(syscall.S_IFREG)
	if kind == "Directory" {
		expectedMode = syscall.S_IFDIR
	}
	if uint64(stat.Dev) != device || stat.Ino != inode || stat.Mode&syscall.S_IFMT != expectedMode || (kind == "File" && stat.Nlink != 1) {
		return errors.Join(errors.New("reserved cleanup path identity or type changed"), file.Close())
	}
	if kind == "File" {
		if err := file.Close(); err != nil {
			return err
		}
		return unix.Unlinkat(int(parent.Fd()), name, 0)
	}
	entries, err := file.ReadDir(-1)
	if err != nil {
		return errors.Join(err, file.Close())
	}
	for _, entry := range entries {
		childTombstone, err := newReservedName(syncTombstonePrefix)
		if err != nil {
			return errors.Join(err, file.Close())
		}
		if err := unix.Renameat2(fd, entry.Name(), fd, childTombstone, unix.RENAME_NOREPLACE); err != nil {
			return errors.Join(err, file.Close())
		}
		child, err := statAt(file, childTombstone)
		if err != nil {
			return errors.Join(err, file.Close())
		}
		childKind := "File"
		if child.Mode&syscall.S_IFMT == syscall.S_IFDIR {
			childKind = "Directory"
		} else if child.Mode&syscall.S_IFMT != syscall.S_IFREG {
			return errors.Join(errors.New("reserved cleanup tree contains unsupported type"), file.Close())
		}
		if err := removeReservedTree(file, childTombstone, uint64(child.Dev), child.Ino, childKind); err != nil {
			return errors.Join(err, file.Close())
		}
	}
	if err := file.Sync(); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Close(); err != nil {
		return err
	}
	return unix.Unlinkat(int(parent.Fd()), name, unix.AT_REMOVEDIR)
}

func newReservedName(prefix string) (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(value[:]), nil
}

func updateBindingAndIndex(ctx context.Context, tx *sql.Tx, worktree, commit, root, etag string, paths []checkoutPath) error {
	result, err := tx.ExecContext(ctx, `UPDATE bindings SET sync_base_commit = ?, sync_base_root = ?, head_etag = ? WHERE worktree = ?`, commit, root, etag, worktree)
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
