package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
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
	files       map[string][]byte
	synthesized map[string][]byte
	active      map[string]bool
	seen        map[string]bool
	budget      *_replayBudget
	localSeed   object.Commit
	lineage     map[string]_conflictPromotion
}

type _conflictPromotion struct {
	source, target, id, mtime string
	size                      int64
}

type _preparedMerge struct {
	root, candidateID string
	candidateData     []byte
	directories       []scannedObject
	paths             []checkoutPath
	promotions        []_conflictPromotion
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
	localSeed, err := object.VerifyCommit(capturedData, capturedID)
	if err != nil || localSeed.AuthorUserID != binding.UserID || localSeed.DeviceID != binding.DeviceID ||
		localSeed.Root != snapshot.root {
		return errors.New("captured local conflict-name seed is invalid")
	}
	prepared, err := _prepareRecursiveMerge(ctx, options, snapshot, binding.SyncBaseRoot, snapshot.root, remote.Root,
		*head.CommitID, capturedID, localSeed, binding, config, budget)
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
	remoteRoot, remoteHead, localCommit string, localSeed object.Commit, binding clientBinding, config libraryClientConfig,
	budget *_replayBudget) (_preparedMerge, error) {
	if localSeed.AuthorUserID != binding.UserID || localSeed.DeviceID != binding.DeviceID || localSeed.Root != localRoot {
		return _preparedMerge{}, errors.New("local conflict-name seed does not match merge Local")
	}
	lineage := _capturedFileLineage(snapshot.paths)
	root, directories, paths, err := _mergeDirectoryTrees(ctx, options, snapshot, baseRoot, localRoot, remoteRoot, localSeed, lineage, budget)
	if err != nil {
		return _preparedMerge{}, err
	}
	data, id, err := canonicalCommit(binding.UserID, binding.DeviceID, root, []string{remoteHead, localCommit}, config.now)
	if err != nil {
		return _preparedMerge{}, err
	}
	return _preparedMerge{root: root, candidateID: id, candidateData: data, directories: directories, paths: paths,
		promotions: _movedConflictPromotions(lineage)}, nil
}

type _rebuiltPendingMerge struct {
	directories []scannedObject
	snapshot    worktreeSnapshot
	paths       []checkoutPath
	promotions  []_conflictPromotion
}

func _rebuildPendingMerge(ctx context.Context, options bindOptions, binding clientBinding, snapshot worktreeSnapshot,
	pending pendingPublication, budget *_replayBudget) (_rebuiltPendingMerge, error) {
	sequence, err := _pendingMergeSequence(pending, binding)
	if err != nil || len(sequence) == 0 {
		return _rebuiltPendingMerge{}, errors.Join(errors.New("pending recursive merge candidate is invalid"), err)
	}
	working := snapshot
	baseCommit, baseRoot, localRoot := binding.SyncBase, binding.SyncBaseRoot, pending.CapturedRoot
	localSeed, err := object.VerifyCommit(pending.CapturedData, pending.CapturedCommit)
	if err != nil || localSeed.AuthorUserID != binding.UserID || localSeed.DeviceID != binding.DeviceID || localSeed.Root != localRoot {
		return _rebuiltPendingMerge{}, errors.New("pending captured conflict-name seed is invalid")
	}
	synthesized := make(map[string]scannedObject)
	lineage := _capturedFileLineage(snapshot.paths)
	var paths []checkoutPath
	for index, entry := range sequence {
		if index == 0 && entry.id == pending.CapturedCommit && len(entry.commit.Parents) == 2 {
			remote, remoteErr := _budgetedRemoteCommit(ctx, options, entry.commit.Parents[0], budget)
			if remoteErr != nil || remote.AuthorUserID != binding.UserID {
				return _rebuiltPendingMerge{}, errors.Join(errors.New("verify migrated captured merge Remote"), remoteErr)
			}
			baseCommit, baseRoot, localRoot, paths = entry.commit.Parents[0], remote.Root, entry.commit.Root, snapshot.paths
			localSeed = entry.commit
			continue
		}
		if index == len(sequence)-1 && (pending.BaseCommit != baseCommit || pending.BaseRoot != baseRoot) {
			return _rebuiltPendingMerge{}, errors.New("pending recursive merge Base metadata does not match replay")
		}
		remote, remoteErr := _budgetedRemoteCommit(ctx, options, entry.commit.Parents[0], budget)
		if remoteErr != nil || remote.AuthorUserID != binding.UserID {
			return _rebuiltPendingMerge{}, errors.Join(fmt.Errorf("verify pending recursive merge Remote %d", index), remoteErr)
		}
		root, directories, mergedPaths, mergeErr := _mergeDirectoryTrees(ctx, options, working, baseRoot, localRoot, remote.Root, localSeed, lineage, budget)
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
		localSeed = entry.commit
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
	return _rebuiltPendingMerge{directories: directories, snapshot: working, paths: paths,
		promotions: _movedConflictPromotions(lineage)}, nil
}

func _verifiedRemotePaths(ctx context.Context, options bindOptions, root string, budget *_replayBudget) ([]checkoutPath, error) {
	cacheRoot, err := openVerifiedCacheRoot(options.clientDir)
	if err != nil {
		return nil, err
	}
	defer cacheRoot.Close()
	options.cacheRoot = cacheRoot
	merger := &_treeMerger{ctx: ctx, options: options, directories: make(map[string][]byte), files: make(map[string][]byte),
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

func _capturedFileLineage(paths []checkoutPath) map[string]_conflictPromotion {
	result := make(map[string]_conflictPromotion)
	for _, path := range paths {
		if path.kind == "File" {
			result[path.path] = _conflictPromotion{source: path.path, target: path.path, id: path.id,
				mtime: path.mtime, size: path.size}
		}
	}
	return result
}

func _movedConflictPromotions(lineage map[string]_conflictPromotion) []_conflictPromotion {
	result := make([]_conflictPromotion, 0)
	for _, value := range lineage {
		if value.source != value.target {
			result = append(result, value)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].source < result[j].source })
	return result
}

func (merger *_treeMerger) moveLineage(path, target string) {
	for source, value := range merger.lineage {
		if value.target == path {
			value.target = target
			merger.lineage[source] = value
		}
	}
}

func (merger *_treeMerger) dropLineage(path string, directory bool) {
	prefix := path + "/"
	for source, value := range merger.lineage {
		if value.target == path || directory && strings.HasPrefix(value.target, prefix) {
			delete(merger.lineage, source)
		}
	}
}

func _mergeDirectoryTrees(ctx context.Context, options bindOptions, snapshot worktreeSnapshot, baseRoot, localRoot,
	remoteRoot string, localSeed object.Commit, lineage map[string]_conflictPromotion,
	budget *_replayBudget) (string, []scannedObject, []checkoutPath, error) {
	cacheRoot, err := openVerifiedCacheRoot(options.clientDir)
	if err != nil {
		return "", nil, nil, err
	}
	defer cacheRoot.Close()
	options.cacheRoot = cacheRoot
	merger := &_treeMerger{ctx: ctx, options: options, directories: make(map[string][]byte), files: make(map[string][]byte),
		synthesized: make(map[string][]byte), active: make(map[string]bool), seen: make(map[string]bool), budget: budget,
		localSeed: localSeed, lineage: lineage}
	for _, value := range snapshot.objects {
		switch value.kind {
		case "directories":
			merger.directories[value.id] = value.data
		case "files":
			merger.files[value.id] = value.data
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
	occupied := append([]string(nil), ordered...)
	for _, name := range ordered {
		childPath := name
		if path != "" {
			childPath = path + "/" + name
		}
		if len(childPath) > _mergeMaxPath {
			return "", errors.New("recursive merge path exceeds protocol limit")
		}
		baseEntry, localEntry, remoteEntry := baseEntries[name], localEntries[name], remoteEntries[name]
		if _divergentFileConflict(baseEntry, localEntry, remoteEntry) {
			conflictName, err := _conflictCopyName(name, path, merger.localSeed, occupied)
			if err != nil {
				return "", err
			}
			conflictPath := conflictName
			if path != "" {
				conflictPath = path + "/" + conflictName
			}
			merger.moveLineage(childPath, conflictPath)
			entries = append(entries,
				scanEntry{name: remoteEntry.Name, kind: remoteEntry.Type, id: remoteEntry.ID, modified: remoteEntry.ModifiedAt},
				scanEntry{name: conflictName, kind: localEntry.Type, id: localEntry.ID, modified: localEntry.ModifiedAt})
			occupied = append(occupied, conflictName)
			continue
		}
		if _sameMergeEntry(localEntry, baseEntry) && !_sameMergeEntry(remoteEntry, baseEntry) {
			merger.dropLineage(childPath, localEntry != nil && localEntry.Type == "Directory")
		}
		entry, err := merger.mergeEntry(baseEntry, localEntry, remoteEntry, childPath, depth)
		if err != nil {
			return "", err
		}
		if entry != nil {
			entries = append(entries, scanEntry{name: entry.Name, kind: entry.Type, id: entry.ID, modified: entry.ModifiedAt})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })
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
			return nil, errors.New("divergent file conflict was not allocated by its directory")
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

func _divergentFileConflict(base, local, remote *object.DirectoryEntry) bool {
	if _sameMergeEntry(local, base) || _sameMergeEntry(remote, base) || _sameMergeEntry(local, remote) ||
		local == nil || remote == nil || local.Type != "File" || remote.Type != "File" || local.ID == remote.ID {
		return false
	}
	return base == nil || base.Type == "File"
}

func _conflictCopyName(leaf, parent string, seed object.Commit, occupied []string) (string, error) {
	created, err := time.Parse("2006-01-02T15:04:05Z", seed.CreatedAt)
	deviceParts := strings.Split(seed.DeviceID, "-")
	if err != nil || created.Format("2006-01-02T15:04:05Z") != seed.CreatedAt || len(deviceParts) != 5 ||
		len(deviceParts[0]) != 8 || strings.ToLower(deviceParts[0]) != deviceParts[0] {
		return "", errors.New("local conflict-name seed is not canonical")
	}
	stem, extension := leaf, ""
	if index := strings.LastIndexByte(leaf, '.'); index > 0 {
		stem, extension = leaf[:index], leaf[index:]
	}
	folded := make(map[string]bool, len(occupied))
	for _, name := range occupied {
		folded[cases.Fold().String(name)] = true
	}
	marker := " (Filecloud conflict " + deviceParts[0] + " " + created.Format("20060102T150405Z") + ")"
	for number := 1; ; number++ {
		suffix := ""
		if number > 1 {
			suffix = " " + fmt.Sprint(number)
		}
		candidate := stem + marker + suffix + extension
		path := candidate
		if parent != "" {
			path = parent + "/" + candidate
		}
		if !validRecoveryVisibleName(candidate) || len(path) > _mergeMaxPath {
			return "", errors.New("conflict copy name exceeds Issue #17 limits; Issue #19 owns truncation and fallback")
		}
		if !folded[cases.Fold().String(candidate)] {
			return candidate, nil
		}
	}
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
		value := checkoutPath{path: path, kind: entry.Type, id: entry.ID, mtime: entry.ModifiedAt}
		if entry.Type == "File" {
			file, err := merger.loadFile(entry.ID)
			if err != nil {
				return nil, err
			}
			value.size = file.Size
		}
		paths = append(paths, value)
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

func (merger *_treeMerger) loadFile(id string) (object.File, error) {
	data, ok := merger.files[id]
	if !ok {
		var err error
		data, err = cachedRemoteObject(merger.ctx, merger.options, "files", id)
		if err != nil {
			return object.File{}, err
		}
		merger.files[id] = data
	}
	file, err := object.VerifyFile(data, id)
	if err != nil {
		return object.File{}, errors.New("merge file is not valid canonical content")
	}
	return file, nil
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
