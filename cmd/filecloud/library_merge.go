package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mingming-cn/filecloud/internal/object"
	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

const (
	_mergeMaxDepth        = 256
	_mergeMaxObjects      = 2000000
	_mergeMaxPath         = 1024
	_conflictMaxOrdinal   = 9999
	_fallbackConflictRoot = "Filecloud Conflicts"
	_conflictMarkerPrefix = " (Filecloud conflict "
)

var errConflictPathNeedsFallback = errors.New("conflict copy path requires root fallback")

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
	localSeedID string
	lineage     map[string]_conflictPromotion
	fallbacks   []_fallbackConflict
}

type _fallbackConflict struct {
	source string
	leaf   string
	entry  object.DirectoryEntry
}

type _conflictPromotion struct {
	source, target, id, mtime, namingSeed string
	size                                  int64
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
	root, directories, paths, err := _mergeDirectoryTrees(ctx, options, snapshot, baseRoot, localRoot, remoteRoot,
		localCommit, localSeed, lineage, budget)
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
	localSeedID := pending.CapturedCommit
	localSeed, err := object.VerifyCommit(pending.CapturedData, localSeedID)
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
			localSeedID, localSeed = entry.id, entry.commit
			continue
		}
		if index == len(sequence)-1 && (pending.BaseCommit != baseCommit || pending.BaseRoot != baseRoot) {
			return _rebuiltPendingMerge{}, errors.New("pending recursive merge Base metadata does not match replay")
		}
		remote, remoteErr := _budgetedRemoteCommit(ctx, options, entry.commit.Parents[0], budget)
		if remoteErr != nil || remote.AuthorUserID != binding.UserID {
			return _rebuiltPendingMerge{}, errors.Join(fmt.Errorf("verify pending recursive merge Remote %d", index), remoteErr)
		}
		root, directories, mergedPaths, mergeErr := _mergeDirectoryTrees(ctx, options, working, baseRoot, localRoot, remote.Root,
			localSeedID, localSeed, lineage, budget)
		if mergeErr != nil {
			return _rebuiltPendingMerge{}, mergeErr
		}
		created, parseErr := parseCanonicalProtocolMtime(entry.commit.CreatedAt)
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
		localSeedID, localSeed = entry.id, entry.commit
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

func _promotionListsEqual(left, right []_conflictPromotion) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func _authoritativePromotionReplay(ctx context.Context, options bindOptions, binding clientBinding, target string,
	promotions []_conflictPromotion, budget *_replayBudget) error {
	seeded := false
	for _, promotion := range promotions {
		seeded = seeded || promotion.namingSeed != ""
	}
	if !seeded {
		return nil
	}
	if budget == nil {
		budget = _newReplayBudget()
	}
	type cachedCommit struct {
		data   []byte
		commit object.Commit
	}
	loaded := make(map[string]cachedCommit)
	loadCommit := func(id string) (cachedCommit, error) {
		if value, ok := loaded[id]; ok {
			return value, nil
		}
		data, err := cachedRemoteObject(ctx, options, "commits", id)
		if err != nil {
			return cachedCommit{}, err
		}
		commit, err := object.VerifyCommit(data, id)
		if err != nil {
			return cachedCommit{}, err
		}
		if _, exists := budget.commits[id]; !exists {
			if budget.commitFetches >= budget.commitLimit {
				return cachedCommit{}, errors.New("promotion authority commit history exceeds cumulative synchronization budget")
			}
			budget.commitFetches++
			budget.commits[id] = commit
		}
		value := cachedCommit{data: data, commit: commit}
		loaded[id] = value
		return value, nil
	}
	walk := func(id string) error {
		if budget.walked[id] {
			return nil
		}
		if budget.commitWalks >= budget.commitLimit {
			return errors.New("promotion authority commit history exceeds cumulative synchronization budget")
		}
		budget.walked[id] = true
		budget.commitWalks++
		return nil
	}
	captured, err := loadCommit(binding.SyncBase)
	if err != nil || captured.commit.AuthorUserID != binding.UserID || captured.commit.DeviceID != binding.DeviceID ||
		captured.commit.Root != binding.SyncBaseRoot || (len(captured.commit.Parents) != 1 && len(captured.commit.Parents) != 2) {
		return errors.Join(errors.New("promotion authority Captured commit does not match the binding"), err)
	}

	chainFromCandidate := func(candidateID string) ([]_pendingMergeCommit, bool, error) {
		chain := make([]_pendingMergeCommit, 0)
		id := candidateID
		seen := make(map[string]bool)
		for id != binding.SyncBase {
			if seen[id] {
				return nil, false, errors.New("promotion authority Candidate second-parent chain contains a cycle")
			}
			seen[id] = true
			if err := walk(id); err != nil {
				return nil, false, err
			}
			value, err := loadCommit(id)
			if err != nil {
				return nil, false, fmt.Errorf("load promotion authority Candidate %s: %w", id, err)
			}
			if value.commit.AuthorUserID != binding.UserID || value.commit.DeviceID != binding.DeviceID ||
				len(value.commit.Parents) != 2 || value.commit.Parents[0] == value.commit.Parents[1] {
				return nil, false, nil
			}
			chain = append(chain, _pendingMergeCommit{id: id, data: value.data, commit: value.commit})
			id = value.commit.Parents[1]
		}
		for left, right := 0, len(chain)-1; left < right; left, right = left+1, right-1 {
			chain[left], chain[right] = chain[right], chain[left]
		}
		return chain, len(chain) != 0, nil
	}

	var candidateChain []_pendingMergeCommit
	firstParent := target
	for firstParent != "" {
		if err := walk(firstParent); err != nil {
			return err
		}
		value, err := loadCommit(firstParent)
		if err != nil {
			return fmt.Errorf("load promotion authority first-parent commit %s: %w", firstParent, err)
		}
		if value.commit.AuthorUserID != binding.UserID || len(value.commit.Parents) > 2 {
			return errors.New("promotion authority first-parent mainline contains an invalid commit")
		}
		if firstParent != binding.SyncBase && len(value.commit.Parents) == 2 && value.commit.DeviceID == binding.DeviceID {
			chain, anchored, err := chainFromCandidate(firstParent)
			if err != nil {
				return err
			}
			if anchored {
				if candidateChain != nil {
					return errors.New("promotion authority first-parent mainline has multiple Candidate chains")
				}
				candidateChain = chain
			}
		}
		if len(value.commit.Parents) == 0 {
			firstParent = ""
		} else {
			firstParent = value.commit.Parents[0]
		}
	}
	if len(candidateChain) == 0 {
		return fmt.Errorf("promotion authority first-parent mainline has no Candidate chain (target %s, Captured %s)", target, binding.SyncBase)
	}

	capturedPaths, err := _verifiedRemotePaths(ctx, options, binding.SyncBaseRoot, budget)
	if err != nil {
		return fmt.Errorf("load promotion authority Captured snapshot: %w", err)
	}
	working := worktreeSnapshot{root: binding.SyncBaseRoot, paths: capturedPaths}
	lineage := _capturedFileLineage(capturedPaths)
	baseID := captured.commit.Parents[0]
	base, err := loadCommit(baseID)
	if err != nil || base.commit.AuthorUserID != binding.UserID {
		return errors.Join(errors.New("promotion authority Captured Base is invalid"), err)
	}
	baseRoot, localRoot := base.commit.Root, binding.SyncBaseRoot
	localSeedID, localSeed := binding.SyncBase, captured.commit
	for index, entry := range candidateChain {
		if entry.commit.Parents[1] != localSeedID {
			return fmt.Errorf("promotion authority Candidate chain entry %d has a mismatched Local parent", index)
		}
		remote, err := loadCommit(entry.commit.Parents[0])
		if err != nil || remote.commit.AuthorUserID != binding.UserID {
			return errors.Join(fmt.Errorf("promotion authority Candidate Remote %d is invalid", index), err)
		}
		root, directories, paths, err := _mergeDirectoryTrees(ctx, options, working, baseRoot, localRoot,
			remote.commit.Root, localSeedID, localSeed, lineage, budget)
		if err != nil {
			return fmt.Errorf("replay promotion authority Candidate %d: %w", index, err)
		}
		created, err := parseCanonicalProtocolMtime(entry.commit.CreatedAt)
		if err != nil {
			return fmt.Errorf("promotion authority Candidate %d timestamp is invalid", index)
		}
		canonical, id, err := canonicalCommit(binding.UserID, binding.DeviceID, root, entry.commit.Parents,
			func() time.Time { return created })
		if err != nil || id != entry.id || entry.commit.Root != root || !bytes.Equal(canonical, entry.data) {
			return errors.Join(fmt.Errorf("promotion authority Candidate replay %d does not match its canonical commit", index), err)
		}
		working.root, working.paths = root, paths
		working.objects = append(working.objects, directories...)
		baseRoot, localRoot = remote.commit.Root, root
		localSeedID, localSeed = entry.id, entry.commit
	}
	if !_promotionListsEqual(_movedConflictPromotions(lineage), promotions) {
		return errors.New("pending checkout conflict provenance does not match authoritative Candidate replay")
	}
	return nil
}

func (merger *_treeMerger) moveLineage(path, target string) {
	prefix := path + "/"
	for source, value := range merger.lineage {
		if value.target == path {
			value.target = target
		} else if strings.HasPrefix(value.target, prefix) {
			value.target = target + strings.TrimPrefix(value.target, path)
		} else {
			continue
		}
		value.namingSeed = merger.localSeedID
		merger.lineage[source] = value
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
	remoteRoot, localSeedID string, localSeed object.Commit, lineage map[string]_conflictPromotion,
	budget *_replayBudget) (string, []scannedObject, []checkoutPath, error) {
	cacheRoot, err := openVerifiedCacheRoot(options.clientDir)
	if err != nil {
		return "", nil, nil, err
	}
	defer cacheRoot.Close()
	options.cacheRoot = cacheRoot
	merger := &_treeMerger{ctx: ctx, options: options, directories: make(map[string][]byte), files: make(map[string][]byte),
		synthesized: make(map[string][]byte), active: make(map[string]bool), seen: make(map[string]bool), budget: budget,
		localSeed: localSeed, localSeedID: localSeedID, lineage: lineage}
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
	root, err = merger.applyFallbacks(root)
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
		if _localConflictCopy(baseEntry, localEntry, remoteEntry) {
			conflictName, err := _conflictCopyName(name, path, merger.localSeed, occupied)
			if errors.Is(err, errConflictPathNeedsFallback) {
				merger.fallbacks = append(merger.fallbacks, _fallbackConflict{source: childPath, leaf: name, entry: *localEntry})
				if remoteEntry != nil {
					entries = append(entries, scanEntry{name: remoteEntry.Name, kind: remoteEntry.Type,
						id: remoteEntry.ID, modified: remoteEntry.ModifiedAt})
				}
				continue
			}
			if err != nil {
				return "", err
			}
			conflictPath := conflictName
			if path != "" {
				conflictPath = path + "/" + conflictName
			}
			merger.moveLineage(childPath, conflictPath)
			if remoteEntry != nil {
				entries = append(entries, scanEntry{name: remoteEntry.Name, kind: remoteEntry.Type,
					id: remoteEntry.ID, modified: remoteEntry.ModifiedAt})
			}
			entries = append(entries, scanEntry{name: conflictName, kind: localEntry.Type,
				id: localEntry.ID, modified: localEntry.ModifiedAt})
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
	if local == nil {
		return _copyMergeEntry(remote), nil
	}
	if remote == nil || local.Type != remote.Type || local.Type == "File" && local.ID != remote.ID {
		return nil, errors.New("local structural conflict was not allocated by its directory")
	}
	if local.Type == "File" {
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
	if base != nil && base.Type == "Directory" {
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

func _localConflictCopy(base, local, remote *object.DirectoryEntry) bool {
	if _sameMergeEntry(local, base) || _sameMergeEntry(remote, base) || _sameMergeEntry(local, remote) || local == nil {
		return false
	}
	return remote == nil || local.Type != remote.Type || local.Type == "File" && local.ID != remote.ID
}

func _conflictCopyName(leaf, parent string, seed object.Commit, occupied []string) (string, error) {
	created, err := parseCanonicalProtocolMtime(seed.CreatedAt)
	deviceParts := strings.Split(seed.DeviceID, "-")
	if err != nil || len(deviceParts) != 5 || len(deviceParts[0]) != 8 || strings.ToLower(deviceParts[0]) != deviceParts[0] {
		return "", errors.New("local conflict-name seed is not canonical")
	}
	leaf = norm.NFC.String(leaf)
	stem, extension := leaf, ""
	if index := strings.LastIndexByte(leaf, '.'); index > 0 {
		stem, extension = leaf[:index], leaf[index:]
	}
	marker := _conflictMarkerPrefix + deviceParts[0] + " " + created.Format("20060102T150405Z") + ")"
	available := _conflictLeafBudget(parent)
	folded := make(map[string]bool, len(occupied))
	for _, name := range occupied {
		folded[cases.Fold().String(name)] = true
	}
	for number := 1; number <= _conflictMaxOrdinal; number++ {
		suffix := ""
		if number > 1 {
			suffix = " " + strconv.Itoa(number)
		}
		if available < len(marker)+len(suffix)+len(extension) {
			return "", errConflictPathNeedsFallback
		}
		candidate := truncateUTF8(stem, available-len(marker)-len(suffix)-len(extension)) + marker + suffix + extension
		if !validRecoveryVisibleName(candidate) {
			return "", errConflictPathNeedsFallback
		}
		if !folded[cases.Fold().String(candidate)] {
			return candidate, nil
		}
	}
	return "", errors.New("conflict copy collision sequence exhausted")
}

func _conflictLeafBudget(parent string) int {
	available := _mergeMaxPath
	if parent != "" {
		available -= len(parent) + 1
	}
	return min(available, 240)
}

func _fallbackConflictName(leaf, parent, seedID string, ordinal int) (string, error) {
	if !object.ValidID(seedID) || strings.ToLower(seedID) != seedID || ordinal < 1 || ordinal > _conflictMaxOrdinal {
		return "", errors.New("fallback conflict-name seed is invalid")
	}
	leaf = norm.NFC.String(leaf)
	prefix := seedID[:12] + "-"
	marker := _conflictMarkerPrefix + strconv.Itoa(ordinal) + ")"
	available := _conflictLeafBudget(parent)
	if available < len(prefix)+len(marker) {
		return "", errors.New("fallback conflict path exceeds protocol limits")
	}
	candidate := prefix + truncateUTF8(leaf, available-len(prefix)-len(marker)) + marker
	if !validRecoveryVisibleName(candidate) {
		return "", errors.New("fallback conflict name is invalid")
	}
	return candidate, nil
}

func (merger *_treeMerger) applyFallbacks(rootID string) (string, error) {
	if len(merger.fallbacks) == 0 {
		return rootID, nil
	}
	if !object.ValidID(merger.localSeedID) || strings.ToLower(merger.localSeedID) != merger.localSeedID {
		return "", errors.New("fallback conflict-name seed is invalid")
	}
	sort.Slice(merger.fallbacks, func(i, j int) bool { return merger.fallbacks[i].source < merger.fallbacks[j].source })
	root, err := merger.loadDirectory(rootID)
	if err != nil {
		return "", err
	}
	rootEntries := _entriesByName(root)
	rootFolded := make(map[string]bool, len(root.Entries))
	for _, entry := range root.Entries {
		rootFolded[cases.Fold().String(entry.Name)] = true
	}
	var rootName string
	var fallbackID string
	var fallbackMtime string
	for ordinal := 1; ordinal <= _conflictMaxOrdinal; ordinal++ {
		candidate := _fallbackConflictRoot
		if ordinal > 1 {
			candidate += " " + strconv.Itoa(ordinal)
		}
		if entry := rootEntries[candidate]; entry != nil {
			if entry.Type != "Directory" {
				continue
			}
			rootName, fallbackID, fallbackMtime = candidate, entry.ID, entry.ModifiedAt
			break
		}
		if rootFolded[cases.Fold().String(candidate)] {
			continue
		}
		emptyData, emptyID, emptyErr := canonicalEmptyDirectory()
		if emptyErr != nil {
			return "", emptyErr
		}
		merger.directories[emptyID] = emptyData
		rootName, fallbackID = candidate, emptyID
		break
	}
	if rootName == "" {
		return "", errors.New("fallback conflict root collision sequence exhausted")
	}
	fallback, err := merger.loadDirectory(fallbackID)
	if err != nil {
		return "", err
	}
	entries := make([]scanEntry, 0, len(fallback.Entries)+len(merger.fallbacks))
	occupied := make(map[string]bool, len(fallback.Entries)+len(merger.fallbacks))
	for _, entry := range fallback.Entries {
		entries = append(entries, scanEntry{name: entry.Name, kind: entry.Type, id: entry.ID, modified: entry.ModifiedAt})
		occupied[cases.Fold().String(entry.Name)] = true
	}
	for _, request := range merger.fallbacks {
		var name string
		for ordinal := 1; ordinal <= _conflictMaxOrdinal; ordinal++ {
			name, err = _fallbackConflictName(request.leaf, rootName, merger.localSeedID, ordinal)
			if err != nil {
				return "", err
			}
			if !occupied[cases.Fold().String(name)] && (merger.options.fallbackOccupied == nil || !merger.options.fallbackOccupied(name)) {
				break
			}
			name = ""
		}
		if name == "" {
			return "", errors.New("fallback conflict collision sequence exhausted")
		}
		occupied[cases.Fold().String(name)] = true
		entries = append(entries, scanEntry{name: name, kind: request.entry.Type, id: request.entry.ID,
			modified: request.entry.ModifiedAt})
		if request.entry.ModifiedAt > fallbackMtime {
			fallbackMtime = request.entry.ModifiedAt
		}
		merger.moveLineage(request.source, rootName+"/"+name)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })
	fallbackData, mergedFallbackID, err := canonicalDirectory(rootName, entries)
	if err != nil {
		return "", err
	}
	if mergedFallbackID != fallbackID {
		merger.directories[mergedFallbackID] = fallbackData
		merger.synthesized[mergedFallbackID] = fallbackData
	}
	rootValues := make([]scanEntry, 0, len(root.Entries)+1)
	for _, entry := range root.Entries {
		if entry.Name == rootName {
			rootValues = append(rootValues, scanEntry{name: rootName, kind: "Directory", id: mergedFallbackID, modified: fallbackMtime})
		} else {
			rootValues = append(rootValues, scanEntry{name: entry.Name, kind: entry.Type, id: entry.ID, modified: entry.ModifiedAt})
		}
	}
	if rootEntries[rootName] == nil {
		rootValues = append(rootValues, scanEntry{name: rootName, kind: "Directory", id: mergedFallbackID, modified: fallbackMtime})
	}
	sort.Slice(rootValues, func(i, j int) bool { return rootValues[i].name < rootValues[j].name })
	rootData, mergedRootID, err := canonicalDirectory("", rootValues)
	if err != nil {
		return "", err
	}
	if mergedRootID != rootID {
		merger.directories[mergedRootID] = rootData
		merger.synthesized[mergedRootID] = rootData
	}
	return mergedRootID, nil
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
