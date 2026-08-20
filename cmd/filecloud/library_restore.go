package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/mingming-cn/filecloud/internal/object"
)

func runLibraryRestore(ctx context.Context, args []string, stdout, stderr io.Writer, config libraryClientConfig) error {
	flags := newFlagSet("library restore", stderr)
	clientDir := flags.String("client-dir", "", "Filecloud client state directory")
	worktree := flags.String("worktree", "", "Bound worktree directory")
	commitID := flags.String("commit", "", "Complete historical CommitId")
	sourcePath := flags.String("path", "", "Protocol-relative snapshot path or .")
	confirm := flags.String("confirm", "", "Confirm a pending restore candidate")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *clientDir == "" || *worktree == "" || flags.NArg() != 0 {
		return errors.New("usage: filecloud library restore --client-dir path --worktree path --commit 64-hex --path relative-path-or-dot | --confirm 12-hex-prefix")
	}
	var commitSet, pathSet, confirmSet bool
	flags.Visit(func(value *flag.Flag) {
		switch value.Name {
		case "commit":
			commitSet = true
		case "path":
			pathSet = true
		case "confirm":
			confirmSet = true
		}
	})
	if confirmSet {
		if commitSet || pathSet {
			return errors.New("restore --confirm cannot combine with --commit or --path")
		}
		if len(*confirm) != deleteCandidatePrefixLen || strings.ToLower(*confirm) != *confirm || !isLowerHex(*confirm) {
			return errors.New("restore --confirm must be a 12-character lowercase hexadecimal prefix")
		}
		return runLibraryRestoreConfirm(ctx, *clientDir, *worktree, *confirm, stdout, stderr, config)
	}
	if !commitSet {
		return errors.New("restore preview requires --commit")
	}
	if !object.ValidID(*commitID) {
		return errors.New("restore --commit must be a complete 64-character lowercase object ID")
	}
	if !pathSet || !object.ValidPath(*sourcePath) {
		return errors.New("restore preview requires a canonical --path")
	}
	return runLibraryRestorePreview(ctx, *clientDir, *worktree, *commitID, *sourcePath, stdout, stderr, config)
}

func isLowerHex(value string) bool {
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

type restoreClientState struct {
	locks   *clientLocks
	db      *sql.DB
	root    *openedWorktree
	binding clientBinding
	options bindOptions
	config  libraryClientConfig
}

func openRestoreClientState(ctx context.Context, clientDir, worktree string, stderr io.Writer, config libraryClientConfig) (state *restoreClientState, retErr error) {
	config = normalizeLibraryClientConfig(config)
	canonicalClientDir, err := canonicalStateDir(clientDir)
	if err != nil {
		return nil, err
	}
	if err := checkStateDirFilesystem(canonicalClientDir, config.checkFilesystem); err != nil {
		return nil, err
	}
	canonicalWorktree, err := canonicalExistingPath(worktree)
	if err != nil {
		return nil, err
	}
	databasePath := filepath.Join(canonicalClientDir, _clientDatabaseName)
	var locks *clientLocks
	if !config.bindingLockHeld {
		locks, err = tryLockUnbind(ctx, canonicalClientDir, databasePath, canonicalWorktree, config)
		if err != nil {
			return nil, err
		}
	}
	state = &restoreClientState{locks: locks, config: config}
	var token []byte
	defer func() {
		if retErr != nil {
			clear(token)
			retErr = errors.Join(retErr, state.Close())
		}
	}()
	db, err := openClientDB(databasePath, false)
	if err != nil {
		return nil, err
	}
	state.db = db
	if err := initializeClientSchema(ctx, db); err != nil {
		return nil, err
	}
	if err := db.QueryRowContext(ctx, `SELECT server_url, library_id, worktree, user_id, device_id, sync_base_commit,
		sync_base_root, head_etag, access_token FROM bindings WHERE worktree = ?`, canonicalWorktree).Scan(
		&state.binding.ServerURL, &state.binding.LibraryID, &state.binding.Worktree, &state.binding.UserID,
		&state.binding.DeviceID, &state.binding.SyncBase, &state.binding.SyncBaseRoot, &state.binding.HeadETag, &token); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.New("worktree is not bound")
		}
		return nil, fmt.Errorf("read client binding: %w", err)
	}
	base, err := validateServerURL(state.binding.ServerURL)
	if err != nil {
		clear(token)
		return nil, err
	}
	authorityOptions := bindOptions{clientDir: canonicalClientDir, serverURL: state.binding.ServerURL,
		libraryID: state.binding.LibraryID, worktree: state.binding.Worktree, deviceID: state.binding.DeviceID,
		base: base, token: token}
	if err := rehydratePendingPromotionSeedAuthority(ctx, db, authorityOptions, state.binding); err != nil {
		clear(token)
		return nil, fmt.Errorf("rehydrate checkout promotion authority: %w", err)
	}
	root, err := openWorktreeRoot(canonicalWorktree, config.checkFilesystem)
	if err != nil {
		clear(token)
		return nil, err
	}
	state.root = root
	if err := validatePendingPromotionTargets(ctx, db, state.binding.Worktree); err != nil {
		return nil, fmt.Errorf("validate checkout promotion targets: %w", err)
	}
	if err := recoverFSActions(ctx, db, state.binding.Worktree, root, config.fsActionFault); err != nil {
		return nil, fmt.Errorf("recover checkout filesystem actions: %w", err)
	}
	tracked, err := loadTrackedPaths(ctx, db, state.binding.Worktree)
	if err != nil {
		return nil, err
	}
	state.options = bindOptions{clientDir: canonicalClientDir, serverURL: state.binding.ServerURL,
		libraryID: state.binding.LibraryID, worktree: state.binding.Worktree, deviceID: state.binding.DeviceID,
		base: base, token: token, worktreeRoot: root, fallbackOccupied: config.fallbackOccupied,
		scanConfig: worktreeScanConfig{trackedPaths: tracked, warning: stderr, fault: config.scanFault,
			ignoreUntrackedUnsupported: true}}
	if len(tracked) == 0 {
		tracked, err = loadRemoteTrackedPaths(ctx, state.options, state.binding)
		if err != nil {
			return nil, err
		}
		state.options.scanConfig.trackedPaths = tracked
	}
	return state, nil
}

func (state *restoreClientState) Close() error {
	if state == nil {
		return nil
	}
	var err error
	if state.root != nil {
		err = errors.Join(err, state.root.Close())
		state.root = nil
	}
	if state.db != nil {
		err = errors.Join(err, state.db.Close())
		state.db = nil
	}
	clear(state.options.token)
	state.options.token = nil
	if state.locks != nil {
		err = errors.Join(err, state.locks.Close())
		state.locks = nil
	}
	return err
}

func restorePendingState(ctx context.Context, state *restoreClientState) (*pendingPublication, error) {
	checkout, err := loadPendingCheckout(ctx, state.db, state.binding.ServerURL, state.binding.LibraryID, state.binding.Worktree)
	if err != nil {
		return nil, err
	}
	if checkout != nil {
		return nil, errors.New("restore cannot start while a checkout is pending")
	}
	pending, err := loadPendingPublication(ctx, state.db, state.binding.Worktree)
	if err != nil {
		return nil, err
	}
	return pending, nil
}

func runLibraryRestorePreview(ctx context.Context, clientDir, worktree, sourceCommit, sourcePath string,
	stdout, stderr io.Writer, config libraryClientConfig) (retErr error) {
	state, err := openRestoreClientState(ctx, clientDir, worktree, stderr, config)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, state.Close()) }()
	pending, err := restorePendingState(ctx, state)
	if err != nil {
		return err
	}
	if pending != nil {
		return errors.New("restore cannot start while a publication is pending")
	}
	snapshot, err := scanWorktreeWithConfig(state.root, state.options.scanConfig)
	if err != nil {
		return fmt.Errorf("scan worktree: %w", err)
	}
	head, err := getRemoteHead(ctx, state.options.base, state.binding.LibraryID, state.options.token)
	if err != nil {
		return err
	}
	if head.CommitID == nil {
		return errors.New("bound library Head is empty")
	}
	if snapshot.root != state.binding.SyncBaseRoot || *head.CommitID != state.binding.SyncBase {
		return errors.New("worktree is not converged with Sync Base and Head; run sync first")
	}
	source, err := fetchRestoreSource(ctx, state, sourceCommit)
	if err != nil {
		return err
	}
	plan, err := planRestoreSnapshot(ctx, state, snapshot, source, sourcePath)
	if errors.Is(err, _errRestoreSourcePathNotFound) {
		return errors.New("restore source path not found")
	}
	if err != nil {
		return err
	}
	if plan.resultRoot == snapshot.root {
		return writeRestoreNoOp(stdout, sourceCommit, sourcePath)
	}
	capturedData, _, err := fetchRestoreHeadCommit(ctx, state, *head.CommitID, snapshot.root)
	if err != nil {
		return err
	}
	createdAt := config.now()
	candidateData, candidateID, err := canonicalRestoreCommit(state.binding.UserID, state.binding.DeviceID,
		plan.resultRoot, sourceCommit, sourcePath, []string{*head.CommitID}, createdAt)
	if err != nil {
		return err
	}
	preview, err := _encodeRestorePreview(plan.changedPaths)
	if err != nil {
		return err
	}
	candidate := pendingPublication{Kind: PublicationKindRestore, BaseCommit: state.binding.SyncBase,
		BaseRoot: state.binding.SyncBaseRoot, ExpectedHead: *head.CommitID, ExpectedETag: head.ETag,
		CandidateCommit: candidateID, CandidateRoot: plan.resultRoot, CandidateData: candidateData,
		CapturedCommit: *head.CommitID, CapturedRoot: snapshot.root,
		CapturedData: capturedData, CandidateHistory: []byte{}, SourceCommit: sourceCommit, SourcePath: sourcePath,
		SourceRoot: source.Root, CreatedCount: plan.createdCount, UpdatedCount: plan.updatedCount,
		TypeReplacementCount: plan.typeReplacementCount, RemovedDescendantCount: plan.removedDescendantCount,
		PreservedCurrentOnlyCount: plan.preservedCurrentOnlyCount, ChangedPathPreview: preview,
		ChangedPathCount: plan.changedPathCount, PreviewTruncated: plan.previewTruncated}
	if err := verifyRestorePublication(candidate, state.binding); err != nil {
		return fmt.Errorf("verify restore candidate: %w", err)
	}
	tx, err := state.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := savePendingPublication(ctx, tx, state.binding.Worktree, candidate); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("save restore candidate: %w", err)
	}
	return writeRestorePreview(stdout, clientDir, worktree, candidate)
}

func runLibraryRestoreConfirm(ctx context.Context, clientDir, worktree, prefix string,
	stdout, stderr io.Writer, config libraryClientConfig) (retErr error) {
	state, err := openRestoreClientState(ctx, clientDir, worktree, stderr, config)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, state.Close()) }()
	pending, err := restorePendingState(ctx, state)
	if err != nil {
		return err
	}
	if pending == nil || pending.Kind != PublicationKindRestore {
		return errors.New("restore confirmation requires a pending restore candidate")
	}
	if len(pending.CandidateCommit) < deleteCandidatePrefixLen || prefix != pending.CandidateCommit[:deleteCandidatePrefixLen] {
		return errors.New("restore --confirm must exactly match this worktree's pending restore candidate")
	}
	if err := verifyRestorePublication(*pending, state.binding); err != nil {
		return fmt.Errorf("verify restore candidate: %w", err)
	}
	snapshot, err := scanWorktreeWithConfig(state.root, state.options.scanConfig)
	if err != nil {
		return fmt.Errorf("scan worktree: %w", err)
	}
	head, err := getRemoteHead(ctx, state.options.base, state.binding.LibraryID, state.options.token)
	if err != nil {
		return err
	}
	if head.CommitID == nil {
		return discardStaleRestore(ctx, state, *pending)
	}
	published, err := restoreCandidatePublished(ctx, state.options, *head.CommitID, pending.CandidateCommit,
		state.binding.UserID, _newReplayBudget())
	if err != nil {
		return err
	}
	if published {
		return dispatchPendingPublication(ctx, state.db, state.options, state.binding, snapshot, head, *pending,
			stdout, state.config, _newReplayBudget(), _publicationDispatchResume)
	}
	if *head.CommitID != pending.ExpectedHead || head.ETag != pending.ExpectedETag ||
		snapshot.root != pending.CapturedRoot || state.binding.SyncBase != pending.BaseCommit || state.binding.SyncBaseRoot != pending.BaseRoot {
		return discardStaleRestore(ctx, state, *pending)
	}
	source, err := fetchRestoreSource(ctx, state, pending.SourceCommit)
	if err != nil {
		return err
	}
	plan, err := planRestoreSnapshot(ctx, state, snapshot, source, pending.SourcePath)
	if errors.Is(err, _errRestoreSourcePathNotFound) {
		return discardStaleRestore(ctx, state, *pending)
	}
	if err != nil {
		return err
	}
	capturedData, captured, err := fetchRestoreHeadCommit(ctx, state, pending.ExpectedHead, pending.CapturedRoot)
	if err != nil {
		return err
	}
	if !bytes.Equal(capturedData, pending.CapturedData) || captured.AuthorUserID != state.binding.UserID {
		return discardStaleRestore(ctx, state, *pending)
	}
	candidate, err := object.VerifyCommit(pending.CandidateData, pending.CandidateCommit)
	if err != nil {
		return discardStaleRestore(ctx, state, *pending)
	}
	createdAt, err := parseCanonicalProtocolMtime(candidate.CreatedAt)
	if err != nil {
		return discardStaleRestore(ctx, state, *pending)
	}
	canonical, candidateID, err := canonicalRestoreCommit(state.binding.UserID, state.binding.DeviceID,
		pending.CandidateRoot, pending.SourceCommit, pending.SourcePath, []string{pending.ExpectedHead}, createdAt)
	if err != nil {
		return err
	}
	preview, err := _encodeRestorePreview(plan.changedPaths)
	if err != nil {
		return err
	}
	if candidateID != pending.CandidateCommit || !bytes.Equal(canonical, pending.CandidateData) ||
		candidate.AuthorUserID != state.binding.UserID || candidate.DeviceID != state.binding.DeviceID ||
		candidate.Root != pending.CandidateRoot || len(candidate.Parents) != 1 || candidate.Parents[0] != pending.ExpectedHead ||
		candidate.Message != "restore "+pending.SourceCommit+" "+pending.SourcePath ||
		plan.resultRoot != pending.CandidateRoot || plan.createdCount != pending.CreatedCount || plan.updatedCount != pending.UpdatedCount ||
		plan.typeReplacementCount != pending.TypeReplacementCount || plan.removedDescendantCount != pending.RemovedDescendantCount ||
		plan.preservedCurrentOnlyCount != pending.PreservedCurrentOnlyCount || plan.changedPathCount != pending.ChangedPathCount ||
		plan.previewTruncated != pending.PreviewTruncated || !bytes.Equal(preview, pending.ChangedPathPreview) ||
		source.Root != pending.SourceRoot {
		return discardStaleRestore(ctx, state, *pending)
	}
	if !pending.RestoreConfirmed {
		if err := _execPendingPublicationCAS(ctx, state.db, "UPDATE pending_publications SET restore_confirmed = 1", nil,
			state.binding.Worktree, *pending, "restore candidate changed before confirmation"); err != nil {
			return fmt.Errorf("confirm restore candidate: %w", err)
		}
		pending.RestoreConfirmed = true
	}
	return dispatchPendingPublication(ctx, state.db, state.options, state.binding, snapshot, head, *pending, stdout,
		state.config, _newReplayBudget(), _publicationDispatchStart)
}

func restoreCandidatePublished(ctx context.Context, options bindOptions, head, candidate, owner string, budget *_replayBudget) (bool, error) {
	if head == candidate {
		return true, nil
	}
	return _remoteCommitDescendsFrom(ctx, options, head, candidate, owner, budget)
}

func discardStaleRestore(ctx context.Context, state *restoreClientState, pending pendingPublication) error {
	if err := discardPendingPublication(ctx, state.db, state.binding.Worktree, pending); err != nil {
		return err
	}
	return errors.New("stale restore candidate discarded; rerun restore")
}

func fetchRestoreSource(ctx context.Context, state *restoreClientState, sourceID string) (historyInspectCommit, error) {
	return fetchRestoreSourceWithOptions(ctx, state.options, state.binding, sourceID)
}

func fetchRestoreSourceWithOptions(ctx context.Context, options bindOptions, binding clientBinding, sourceID string) (historyInspectCommit, error) {
	client := historyInspectClient{binding: binding, base: options.base, token: options.token}
	commit, err := client.fetchCommit(ctx, sourceID)
	if err != nil {
		return historyInspectCommit{}, err
	}
	if commit.Role != "mainline" && commit.Role != "merge-source" {
		return historyInspectCommit{}, errors.New("restore source commit does not have a supported published history role")
	}
	return commit, nil
}

func fetchRestoreObject(ctx context.Context, options bindOptions, kind, id string) ([]byte, error) {
	if !object.ValidID(id) || (kind != "commits" && kind != "directories" && kind != "files") {
		return nil, errors.New("restore object reference is invalid")
	}
	request, err := authenticatedRequest(ctx, http.MethodGet,
		options.base.JoinPath("v1/libraries", options.libraryID, "objects", kind, id).String(), options.token, nil)
	if err != nil {
		return nil, err
	}
	status, data, _, err := doClientRequest(request)
	if err != nil {
		return nil, fmt.Errorf("read restore %s object: %w", kind, err)
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("read restore %s object failed: server returned %s", kind, http.StatusText(status))
	}
	var verifyErr error
	switch kind {
	case "commits":
		_, verifyErr = object.VerifyCommit(data, id)
	case "directories":
		_, verifyErr = object.VerifyDirectory(data, id)
	case "files":
		_, verifyErr = object.VerifyFile(data, id)
	}
	if verifyErr != nil {
		return nil, fmt.Errorf("restore %s object is not canonical: %w", kind, verifyErr)
	}
	return data, nil
}

func restoreLocalObjectMap(snapshot worktreeSnapshot) map[string][]byte {
	objects := make(map[string][]byte, len(snapshot.objects))
	for _, value := range snapshot.objects {
		objects[value.kind+"\x00"+value.id] = append([]byte(nil), value.data...)
	}
	return objects
}

func planRestoreSnapshot(ctx context.Context, state *restoreClientState, snapshot worktreeSnapshot,
	source historyInspectCommit, sourcePath string) (restorePlan, error) {
	return planRestoreSnapshotWithBudget(ctx, state.options, snapshot, source, sourcePath, state.config.restorePlanBudget)
}

func planRestoreSnapshotWithBudget(ctx context.Context, options bindOptions, snapshot worktreeSnapshot,
	source historyInspectCommit, sourcePath string, budget *restorePlanBudget) (restorePlan, error) {
	local := restoreLocalObjectMap(snapshot)
	loader := func(kind, id string) ([]byte, error) {
		if data, ok := local[kind+"\x00"+id]; ok {
			return append([]byte(nil), data...), nil
		}
		return fetchRestoreObject(ctx, options, kind, id)
	}
	return planRestoreOverlay(restorePlanInput{CurrentRoot: snapshot.root, SourceRoot: source.Root,
		SourcePath: sourcePath, Load: loader, Budget: budget})
}

func fetchRestoreHeadCommit(ctx context.Context, state *restoreClientState, commitID, rootID string) ([]byte, object.Commit, error) {
	data, err := fetchRestoreObject(ctx, state.options, "commits", commitID)
	if err != nil {
		return nil, object.Commit{}, err
	}
	commit, err := object.VerifyCommit(data, commitID)
	if err != nil || commit.AuthorUserID != state.binding.UserID || commit.Root != rootID {
		return nil, object.Commit{}, errors.New("restore Head commit is not canonical for the binding")
	}
	return data, commit, nil
}

func canonicalRestoreCommit(owner, device, root, source, path string, parents []string, now time.Time) ([]byte, string, error) {
	message := "restore " + source + " " + path
	input, err := json.Marshal(struct {
		AuthorUserID string   `json:"AuthorUserId"`
		CreatedAt    string   `json:"CreatedAt"`
		DeviceID     string   `json:"DeviceId"`
		Message      string   `json:"Message"`
		Parents      []string `json:"Parents"`
		Root         string   `json:"Root"`
		Type         string   `json:"Type"`
		Version      int      `json:"Version"`
	}{AuthorUserID: owner, CreatedAt: canonicalProtocolMtime(now), DeviceID: device, Message: message,
		Parents: parents, Root: root, Type: "Commit", Version: 1})
	if err != nil {
		return nil, "", fmt.Errorf("construct restore commit: %w", err)
	}
	data, id, err := object.Canonicalize("commits", input)
	if err != nil {
		return nil, "", fmt.Errorf("construct restore commit: %w", err)
	}
	return data, id, nil
}

func writeRestoreNoOp(output io.Writer, source, path string) error {
	_, err := fmt.Fprintf(output, "restore no-op: source commit=%s path=%s already matches current state\n", source, path)
	if err != nil {
		return fmt.Errorf("write restore no-op: %w", err)
	}
	return nil
}

func writeRestorePreview(output io.Writer, clientDir, worktree string, pending pendingPublication) error {
	paths, err := _decodeRestorePreview(pending.ChangedPathPreview)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(output, "source commit: %s\nsource path: %s\nexpected head: %s\ncandidate: %s\ncreated paths: %d\nupdated paths: %d\ntype replacements: %d\nremoved descendants by type replacement: %d\npreserved current-only paths: %d\nchanged paths:\n", pending.SourceCommit, pending.SourcePath, pending.ExpectedHead, pending.CandidateCommit[:deleteCandidatePrefixLen], pending.CreatedCount, pending.UpdatedCount, pending.TypeReplacementCount, pending.RemovedDescendantCount, pending.PreservedCurrentOnlyCount); err != nil {
		return fmt.Errorf("write restore preview: %w", err)
	}
	for _, path := range paths {
		if _, err := fmt.Fprintf(output, "%s\n", path); err != nil {
			return fmt.Errorf("write restore preview path: %w", err)
		}
	}
	if _, err := fmt.Fprintf(output, "truncated: %t\nconfirm: filecloud library restore --client-dir %s --worktree %s --confirm %s\n", pending.PreviewTruncated, clientDir, worktree, pending.CandidateCommit[:deleteCandidatePrefixLen]); err != nil {
		return fmt.Errorf("write restore confirmation: %w", err)
	}
	return nil
}
