package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/mingming-cn/filecloud/internal/object"
	"golang.org/x/text/cases"
)

const (
	_mergeMaxDepth   = 256
	_mergeMaxObjects = 2000000
	_mergeMaxPath    = 1024
)

type _replayBudget struct {
	commitLimit, treeLimit, pathLimit    int
	commitFetches, commitWalks, treeWork int
	paths, pathBytes                     int
	commits                              map[string]object.Commit
	walked                               map[string]bool
}

func _newReplayBudget() *_replayBudget {
	return &_replayBudget{commitLimit: maxSyncParentWalk, treeLimit: _mergeMaxObjects,
		pathLimit: _mergeMaxObjects, commits: make(map[string]object.Commit), walked: make(map[string]bool)}
}

type _treeMerger struct {
	ctx         context.Context
	options     bindOptions
	directories map[string][]byte
	synthesized map[string][]byte
	active      map[string]bool
	seen        map[string]bool
	budget      *_replayBudget
}

type _preparedMerge struct {
	root, candidateID string
	candidateData     []byte
	directories       []scannedObject
	paths             []checkoutPath
}

func _startRecursiveMerge(ctx context.Context, db *sql.DB, options bindOptions, binding clientBinding,
	snapshot worktreeSnapshot, head remoteHead, stdout io.Writer, config libraryClientConfig, budget *_replayBudget) error {
	if head.CommitID == nil {
		return errors.New("remote merge Head is empty")
	}
	remote, err := getRemoteCommit(ctx, options.base, options.libraryID, options.token, *head.CommitID)
	if err != nil || remote.AuthorUserID != binding.UserID {
		return errors.Join(errors.New("verify remote Head for merge"), err)
	}
	capturedData, capturedID, err := canonicalCommit(binding.UserID, binding.DeviceID, snapshot.root,
		[]string{binding.SyncBase}, config.now)
	if err != nil {
		return err
	}
	prepared, err := _prepareRecursiveMerge(ctx, options, snapshot, binding.SyncBaseRoot, snapshot.root, remote.Root,
		*head.CommitID, capturedID, binding, config, budget)
	if err != nil {
		return err
	}
	verified, err := scanWorktreeWithConfig(options.worktreeRoot, options.scanConfig)
	if err != nil || verified.root != snapshot.root {
		return errors.New("worktree changed while preparing recursive merge")
	}
	basePaths, err := _verifiedRemotePaths(ctx, options, binding.SyncBaseRoot, budget)
	if err != nil {
		return fmt.Errorf("verify merge Base snapshot: %w", err)
	}
	deleted, tracked := deletionStats(_trackedPathSet(basePaths), prepared.paths)
	protected := protectedDeletion(deleted, tracked)
	pending := pendingPublication{BaseCommit: binding.SyncBase, BaseRoot: binding.SyncBaseRoot,
		ExpectedHead: *head.CommitID, ExpectedETag: head.ETag, CandidateCommit: prepared.candidateID,
		CandidateRoot: prepared.root, CandidateData: prepared.candidateData, CapturedCommit: capturedID,
		CapturedRoot: snapshot.root, CapturedData: capturedData, CandidateHistory: _emptyCandidateHistory,
		DeletionCount: deleted, TrackedCount: tracked, RequiresDeleteConfirmation: protected}
	if err := savePendingPublication(ctx, db, binding.Worktree, pending); err != nil {
		return err
	}
	if protected {
		return deletionConfirmationError(pending)
	}
	return _uploadAndPublishPrepared(ctx, db, options, binding, snapshot, pending, prepared.directories, stdout, config, budget)
}

func _prepareRecursiveMerge(ctx context.Context, options bindOptions, snapshot worktreeSnapshot, baseRoot, localRoot,
	remoteRoot, remoteHead, localCommit string, binding clientBinding, config libraryClientConfig,
	budget *_replayBudget) (_preparedMerge, error) {
	root, directories, paths, err := _mergeDirectoryTrees(ctx, options, snapshot, baseRoot, localRoot, remoteRoot, budget)
	if err != nil {
		return _preparedMerge{}, err
	}
	data, id, err := canonicalCommit(binding.UserID, binding.DeviceID, root, []string{remoteHead, localCommit}, config.now)
	if err != nil {
		return _preparedMerge{}, err
	}
	return _preparedMerge{root: root, candidateID: id, candidateData: data, directories: directories, paths: paths}, nil
}

type _rebuiltPendingMerge struct {
	directories []scannedObject
	snapshot    worktreeSnapshot
	paths       []checkoutPath
}

func _rebuildPendingMerge(ctx context.Context, options bindOptions, binding clientBinding, snapshot worktreeSnapshot,
	pending pendingPublication, budget *_replayBudget) (_rebuiltPendingMerge, error) {
	sequence, err := _pendingMergeSequence(pending, binding)
	if err != nil || len(sequence) == 0 {
		return _rebuiltPendingMerge{}, errors.Join(errors.New("pending recursive merge candidate is invalid"), err)
	}
	working := snapshot
	baseCommit, baseRoot, localRoot := binding.SyncBase, binding.SyncBaseRoot, pending.CapturedRoot
	synthesized := make(map[string]scannedObject)
	var paths []checkoutPath
	for index, entry := range sequence {
		if index == 0 && entry.id == pending.CapturedCommit && len(entry.commit.Parents) == 2 {
			remote, remoteErr := _budgetedRemoteCommit(ctx, options, entry.commit.Parents[0], budget)
			if remoteErr != nil || remote.AuthorUserID != binding.UserID {
				return _rebuiltPendingMerge{}, errors.Join(errors.New("verify migrated captured merge Remote"), remoteErr)
			}
			baseCommit, baseRoot, localRoot, paths = entry.commit.Parents[0], remote.Root, entry.commit.Root, snapshot.paths
			continue
		}
		if index == len(sequence)-1 && (pending.BaseCommit != baseCommit || pending.BaseRoot != baseRoot) {
			return _rebuiltPendingMerge{}, errors.New("pending recursive merge Base metadata does not match replay")
		}
		remote, remoteErr := _budgetedRemoteCommit(ctx, options, entry.commit.Parents[0], budget)
		if remoteErr != nil || remote.AuthorUserID != binding.UserID {
			return _rebuiltPendingMerge{}, errors.Join(fmt.Errorf("verify pending recursive merge Remote %d", index), remoteErr)
		}
		root, directories, mergedPaths, mergeErr := _mergeDirectoryTrees(ctx, options, working, baseRoot, localRoot, remote.Root, budget)
		if mergeErr != nil {
			return _rebuiltPendingMerge{}, mergeErr
		}
		created, parseErr := time.Parse("2006-01-02T15:04:05Z", entry.commit.CreatedAt)
		if parseErr != nil {
			return _rebuiltPendingMerge{}, errors.New("pending recursive merge timestamp is invalid")
		}
		canonical, id, canonicalErr := canonicalCommit(binding.UserID, binding.DeviceID, root, entry.commit.Parents,
			func() time.Time { return created })
		if canonicalErr != nil || id != entry.id || !bytes.Equal(canonical, entry.data) || entry.commit.Root != root {
			return _rebuiltPendingMerge{}, errors.Join(
				fmt.Errorf("pending recursive merge replay %d does not match canonical commit", index), canonicalErr)
		}
		for _, directory := range directories {
			if _, exists := synthesized[directory.id]; !exists {
				synthesized[directory.id] = directory
				working.objects = append(working.objects, directory)
			}
		}
		baseCommit, baseRoot, localRoot, paths = entry.commit.Parents[0], remote.Root, root, mergedPaths
	}
	basePaths, err := _verifiedRemotePaths(ctx, options, pending.BaseRoot, budget)
	if err != nil {
		return _rebuiltPendingMerge{}, fmt.Errorf("verify pending recursive merge Base snapshot: %w", err)
	}
	deleted, tracked := deletionStats(_trackedPathSet(basePaths), paths)
	if localRoot != pending.CandidateRoot || deleted != pending.DeletionCount || tracked != pending.TrackedCount {
		return _rebuiltPendingMerge{}, errors.New("pending recursive merge no longer matches its canonical graph or deletion statistics")
	}
	ids := make([]string, 0, len(synthesized))
	for id := range synthesized {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	directories := make([]scannedObject, 0, len(ids))
	for _, id := range ids {
		directories = append(directories, synthesized[id])
	}
	return _rebuiltPendingMerge{directories: directories, snapshot: working, paths: paths}, nil
}

func _verifiedRemotePaths(ctx context.Context, options bindOptions, root string, budget *_replayBudget) ([]checkoutPath, error) {
	cacheRoot, err := openVerifiedCacheRoot(options.clientDir)
	if err != nil {
		return nil, err
	}
	defer cacheRoot.Close()
	options.cacheRoot = cacheRoot
	merger := &_treeMerger{ctx: ctx, options: options, directories: make(map[string][]byte),
		synthesized: make(map[string][]byte), active: make(map[string]bool), seen: make(map[string]bool), budget: budget}
	return merger.paths(root, "", 0)
}

func _trackedPathSet(paths []checkoutPath) map[string]bool {
	result := make(map[string]bool, len(paths))
	for _, path := range paths {
		result[path.path] = true
	}
	return result
}

func _mergeDirectoryTrees(ctx context.Context, options bindOptions, snapshot worktreeSnapshot, baseRoot, localRoot,
	remoteRoot string, budget *_replayBudget) (string, []scannedObject, []checkoutPath, error) {
	cacheRoot, err := openVerifiedCacheRoot(options.clientDir)
	if err != nil {
		return "", nil, nil, err
	}
	defer cacheRoot.Close()
	options.cacheRoot = cacheRoot
	merger := &_treeMerger{ctx: ctx, options: options, directories: make(map[string][]byte),
		synthesized: make(map[string][]byte), active: make(map[string]bool), seen: make(map[string]bool), budget: budget}
	for _, value := range snapshot.objects {
		if value.kind == "directories" {
			merger.directories[value.id] = value.data
		}
	}
	root, err := merger.merge(baseRoot, localRoot, remoteRoot, "", 0)
	if err != nil {
		return "", nil, nil, err
	}
	paths, err := merger.paths(root, "", 0)
	if err != nil {
		return "", nil, nil, err
	}
	ids := make([]string, 0, len(merger.synthesized))
	for id := range merger.synthesized {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	objects := make([]scannedObject, 0, len(ids))
	for _, id := range ids {
		objects = append(objects, scannedObject{kind: "directories", id: id, data: merger.synthesized[id]})
	}
	return root, objects, paths, nil
}

func (merger *_treeMerger) merge(baseID, localID, remoteID, path string, depth int) (string, error) {
	if depth > _mergeMaxDepth {
		return "", errors.New("recursive merge exceeds directory depth limit")
	}
	key := baseID + "\x00" + localID + "\x00" + remoteID
	if merger.active[key] {
		return "", errors.New("recursive merge directory graph contains a cycle")
	}
	merger.active[key] = true
	defer delete(merger.active, key)
	base, err := merger.loadDirectory(baseID)
	if err != nil {
		return "", fmt.Errorf("load merge Base directory at %q: %w", path, err)
	}
	local, err := merger.loadDirectory(localID)
	if err != nil {
		return "", fmt.Errorf("load merge Local directory at %q: %w", path, err)
	}
	remote, err := merger.loadDirectory(remoteID)
	if err != nil {
		return "", fmt.Errorf("load merge Remote directory at %q: %w", path, err)
	}
	baseEntries := _entriesByName(base)
	localEntries := _entriesByName(local)
	remoteEntries := _entriesByName(remote)
	names := make(map[string]bool, len(baseEntries)+len(localEntries)+len(remoteEntries))
	folded := make(map[string]string, len(names))
	for _, directory := range []object.Directory{base, local, remote} {
		for _, entry := range directory.Entries {
			names[entry.Name] = true
			fold := cases.Fold().String(entry.Name)
			if previous, exists := folded[fold]; exists && previous != entry.Name {
				return "", fmt.Errorf("structural merge boundary: canonical name collision between %q and %q", previous, entry.Name)
			}
			folded[fold] = entry.Name
		}
	}
	ordered := make([]string, 0, len(names))
	for name := range names {
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)
	entries := make([]scanEntry, 0, len(ordered))
	for _, name := range ordered {
		childPath := name
		if path != "" {
			childPath = path + "/" + name
		}
		if len(childPath) > _mergeMaxPath {
			return "", errors.New("recursive merge path exceeds protocol limit")
		}
		entry, err := merger.mergeEntry(baseEntries[name], localEntries[name], remoteEntries[name], childPath, depth)
		if err != nil {
			return "", err
		}
		if entry != nil {
			entries = append(entries, scanEntry{name: entry.Name, kind: entry.Type, id: entry.ID, modified: entry.ModifiedAt})
		}
	}
	data, id, err := canonicalDirectory(path, entries)
	if err != nil {
		return "", fmt.Errorf("construct merged directory: %w", err)
	}
	if id != baseID && id != localID && id != remoteID {
		merger.directories[id] = data
		merger.synthesized[id] = data
	}
	return id, nil
}

func (merger *_treeMerger) mergeEntry(base, local, remote *object.DirectoryEntry, path string,
	depth int) (*object.DirectoryEntry, error) {
	if _sameMergeEntry(local, base) {
		return _copyMergeEntry(remote), nil
	}
	if _sameMergeEntry(remote, base) {
		return _copyMergeEntry(local), nil
	}
	if _sameMergeEntry(local, remote) {
		return _copyMergeEntry(local), nil
	}
	if local == nil || remote == nil {
		return nil, fmt.Errorf("merge boundary at %q: delete-vs-modify is owned by Issue #18", path)
	}
	if local.Type != remote.Type || base != nil && (base.Type != local.Type || base.Type != remote.Type) {
		return nil, fmt.Errorf("merge boundary at %q: file/directory type conflict is owned by Issue #18", path)
	}
	if local.Type == "File" {
		if local.ID != remote.ID {
			return nil, fmt.Errorf("merge boundary at %q: divergent file content is owned by Issue #17", path)
		}
		result := *local
		if remote.ModifiedAt > result.ModifiedAt {
			result.ModifiedAt = remote.ModifiedAt
		}
		return &result, nil
	}
	emptyData, emptyID, err := canonicalEmptyDirectory()
	if err != nil {
		return nil, err
	}
	merger.directories[emptyID] = emptyData
	baseID := emptyID
	if base != nil {
		baseID = base.ID
	}
	id, err := merger.merge(baseID, local.ID, remote.ID, path, depth+1)
	if err != nil {
		return nil, err
	}
	modified := local.ModifiedAt
	switch {
	case id == local.ID && id == remote.ID:
		if remote.ModifiedAt > modified {
			modified = remote.ModifiedAt
		}
	case id == remote.ID:
		modified = remote.ModifiedAt
	case id != local.ID:
		if remote.ModifiedAt > modified {
			modified = remote.ModifiedAt
		}
	}
	return &object.DirectoryEntry{ID: id, Name: local.Name, Type: "Directory", ModifiedAt: modified}, nil
}

func (merger *_treeMerger) paths(id, prefix string, depth int) ([]checkoutPath, error) {
	merger.ensureBudget()
	if depth > _mergeMaxDepth {
		return nil, errors.New("recursive merge exceeds directory depth limit")
	}
	directory, err := merger.loadDirectory(id)
	if err != nil {
		return nil, err
	}
	var paths []checkoutPath
	for _, entry := range directory.Entries {
		path := entry.Name
		if prefix != "" {
			path = prefix + "/" + entry.Name
		}
		merger.budget.paths++
		if len(path) > merger.budget.pathLimit || merger.budget.pathBytes > merger.budget.pathLimit-len(path) {
			return nil, errors.New("recursive merge exceeds cumulative path budget")
		}
		merger.budget.pathBytes += len(path)
		if merger.budget.paths > merger.budget.pathLimit {
			return nil, errors.New("recursive merge exceeds cumulative path budget")
		}
		paths = append(paths, checkoutPath{path: path, kind: entry.Type, id: entry.ID, mtime: entry.ModifiedAt})
		if entry.Type == "Directory" {
			children, err := merger.paths(entry.ID, path, depth+1)
			if err != nil {
				return nil, err
			}
			paths = append(paths, children...)
		}
	}
	return paths, nil
}

func (merger *_treeMerger) loadDirectory(id string) (object.Directory, error) {
	merger.ensureBudget()
	if !merger.seen[id] {
		merger.seen[id] = true
		merger.budget.treeWork++
		if merger.budget.treeWork > merger.budget.treeLimit {
			return object.Directory{}, errors.New("recursive merge exceeds cumulative object work budget")
		}
	}
	data, ok := merger.directories[id]
	if !ok {
		var err error
		data, err = cachedRemoteObject(merger.ctx, merger.options, "directories", id)
		if err != nil {
			return object.Directory{}, err
		}
		merger.directories[id] = data
	}
	directory, err := object.VerifyDirectory(data, id)
	if err != nil {
		return object.Directory{}, errors.New("merge directory is not valid canonical content")
	}
	if len(directory.Entries) > merger.budget.treeLimit-merger.budget.treeWork {
		return object.Directory{}, errors.New("recursive merge exceeds cumulative entry work budget")
	}
	merger.budget.treeWork += len(directory.Entries)
	return directory, nil
}

func (merger *_treeMerger) ensureBudget() {
	if merger.budget == nil {
		merger.budget = _newReplayBudget()
	}
}

func _uploadMergedDirectories(ctx context.Context, options bindOptions, values []scannedObject) error {
	byID := make(map[string]scannedObject, len(values))
	for _, value := range values {
		byID[value.id] = value
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	references := make([]clientObjectReference, 0, len(ids))
	for _, id := range ids {
		references = append(references, clientObjectReference{ObjectID: id, ObjectType: "Directory"})
	}
	missing, err := checkRemoteObjects(ctx, options, references)
	if err != nil {
		return err
	}
	for _, id := range ids {
		value := byID[id]
		if missing["directories\x00"+id] {
			if err := putMetadata(ctx, options.base, options.libraryID, options.token, "directories", id, value.data); err != nil {
				return err
			}
		}
	}
	return nil
}

func _entriesByName(directory object.Directory) map[string]*object.DirectoryEntry {
	result := make(map[string]*object.DirectoryEntry, len(directory.Entries))
	for index := range directory.Entries {
		entry := directory.Entries[index]
		result[entry.Name] = &entry
	}
	return result
}

func _sameMergeEntry(left, right *object.DirectoryEntry) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.Type == right.Type && left.ID == right.ID && left.ModifiedAt == right.ModifiedAt
}

func _copyMergeEntry(value *object.DirectoryEntry) *object.DirectoryEntry {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
