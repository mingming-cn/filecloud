package main

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/mingming-cn/filecloud/internal/object"
)

const _restorePreviewPathLimit = 100

var _errRestoreSourcePathNotFound = errors.New("restore source path not found")

type restoreObjectLoader func(kind, id string) ([]byte, error)

type restorePlanBudget struct {
	maxDepth     int
	maxObjects   int
	maxPathBytes int
}

type restorePlanInput struct {
	CurrentRoot string
	SourceRoot  string
	SourcePath  string
	Load        restoreObjectLoader
	Budget      *restorePlanBudget
}

type restorePlan struct {
	resultRoot  string
	directories []scannedObject
	paths       []checkoutPath

	createdCount              int64
	updatedCount              int64
	typeReplacementCount      int64
	removedDescendantCount    int64
	preservedCurrentOnlyCount int64
	changedPaths              []string
	changedPathCount          int64
	previewTruncated          bool
}

type restorePlanNode struct {
	present bool
	kind    string
	id      string
	mtime   string
}

type restorePlanner struct {
	input  restorePlanInput
	budget restorePlanBudget

	directories map[string]object.Directory
	files       map[string]object.File
	synthesized map[string][]byte
	active      map[string]bool
	activeWalk  map[string]bool
	changed     map[string]struct{}
	objectCount int

	createdCount              int64
	updatedCount              int64
	typeReplacementCount      int64
	removedDescendantCount    int64
	preservedCurrentOnlyCount int64
}

func planRestoreOverlay(input restorePlanInput) (restorePlan, error) {
	planner, err := newRestorePlanner(input)
	if err != nil {
		return restorePlan{}, err
	}
	if _, err := planner.loadDirectory(input.CurrentRoot); err != nil {
		return restorePlan{}, fmt.Errorf("load current restore root: %w", err)
	}
	if _, err := planner.loadDirectory(input.SourceRoot); err != nil {
		return restorePlan{}, fmt.Errorf("load source restore root: %w", err)
	}
	parts := restorePathParts(input.SourcePath)
	if _, err := planner.resolvePath(input.SourceRoot, parts); err != nil {
		return restorePlan{}, err
	}
	result, err := planner.applyPath(restorePlanNode{present: true, kind: "Directory", id: input.CurrentRoot},
		restorePlanNode{present: true, kind: "Directory", id: input.SourceRoot}, parts, 0, "", true)
	if err != nil {
		return restorePlan{}, err
	}
	paths, err := planner.resultPaths(result.id, "", 0)
	if err != nil {
		return restorePlan{}, fmt.Errorf("build restore result paths: %w", err)
	}
	ids := make([]string, 0, len(planner.synthesized))
	for id := range planner.synthesized {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	directories := make([]scannedObject, 0, len(ids))
	for _, id := range ids {
		directories = append(directories, scannedObject{kind: "directories", id: id, data: planner.synthesized[id]})
	}
	changed := make([]string, 0, len(planner.changed))
	for path := range planner.changed {
		changed = append(changed, path)
	}
	sort.Strings(changed)
	plan := restorePlan{
		resultRoot:                result.id,
		directories:               directories,
		paths:                     paths,
		createdCount:              planner.createdCount,
		updatedCount:              planner.updatedCount,
		typeReplacementCount:      planner.typeReplacementCount,
		removedDescendantCount:    planner.removedDescendantCount,
		preservedCurrentOnlyCount: planner.preservedCurrentOnlyCount,
		changedPathCount:          int64(len(changed)),
		previewTruncated:          len(changed) > _restorePreviewPathLimit,
	}
	if len(changed) > _restorePreviewPathLimit {
		plan.changedPaths = changed[:_restorePreviewPathLimit]
	} else {
		plan.changedPaths = changed
	}
	return plan, nil
}

func newRestorePlanner(input restorePlanInput) (*restorePlanner, error) {
	if !object.ValidID(input.CurrentRoot) || !object.ValidID(input.SourceRoot) {
		return nil, errors.New("restore roots must be complete lowercase object IDs")
	}
	if !object.ValidPath(input.SourcePath) {
		return nil, errors.New("restore source path must be canonical")
	}
	if input.Load == nil {
		return nil, errors.New("restore object loader is required")
	}
	budget := restorePlanBudget{maxDepth: _mergeMaxDepth, maxObjects: _mergeMaxObjects, maxPathBytes: _mergeMaxPath}
	if input.Budget != nil {
		budget = *input.Budget
	}
	if budget.maxDepth < 1 || budget.maxObjects < 1 || budget.maxPathBytes < 1 {
		return nil, errors.New("restore planner budgets must be positive")
	}
	return &restorePlanner{input: input, budget: budget, directories: make(map[string]object.Directory),
		files: make(map[string]object.File), synthesized: make(map[string][]byte), active: make(map[string]bool),
		activeWalk: make(map[string]bool), changed: make(map[string]struct{})}, nil
}

func restorePathParts(path string) []string {
	if path == "." {
		return nil
	}
	return strings.Split(path, "/")
}

func (planner *restorePlanner) loadDirectory(id string) (object.Directory, error) {
	if !object.ValidID(id) {
		return object.Directory{}, errors.New("restore directory reference is invalid")
	}
	if directory, ok := planner.directories[id]; ok {
		return directory, nil
	}
	if data, ok := planner.synthesized[id]; ok {
		if err := planner.countObject(); err != nil {
			return object.Directory{}, err
		}
		directory, err := object.VerifyDirectory(data, id)
		if err != nil {
			return object.Directory{}, fmt.Errorf("restore synthesized directory %s is not canonical: %w", id, err)
		}
		planner.directories[id] = directory
		return directory, nil
	}
	if err := planner.countObject(); err != nil {
		return object.Directory{}, err
	}
	data, err := planner.input.Load("directories", id)
	if err != nil {
		return object.Directory{}, fmt.Errorf("load restore directory %s: %w", id, err)
	}
	directory, err := object.VerifyDirectory(data, id)
	if err != nil {
		return object.Directory{}, fmt.Errorf("restore directory %s is not canonical: %w", id, err)
	}
	planner.directories[id] = directory
	return directory, nil
}

func (planner *restorePlanner) loadFile(id string) (object.File, error) {
	if !object.ValidID(id) {
		return object.File{}, errors.New("restore file reference is invalid")
	}
	if file, ok := planner.files[id]; ok {
		return file, nil
	}
	if err := planner.countObject(); err != nil {
		return object.File{}, err
	}
	data, err := planner.input.Load("files", id)
	if err != nil {
		return object.File{}, fmt.Errorf("load restore file %s: %w", id, err)
	}
	file, err := object.VerifyFile(data, id)
	if err != nil {
		return object.File{}, fmt.Errorf("restore file %s is not canonical: %w", id, err)
	}
	planner.files[id] = file
	return file, nil
}

func (planner *restorePlanner) countObject() error {
	planner.objectCount++
	if planner.objectCount > planner.budget.maxObjects {
		return errors.New("restore planner exceeds object budget")
	}
	return nil
}

func (planner *restorePlanner) resolvePath(root string, parts []string) (restorePlanNode, error) {
	current := restorePlanNode{present: true, kind: "Directory", id: root}
	if len(parts) == 0 {
		return current, nil
	}
	seen := make(map[string]bool)
	for index, part := range parts {
		if current.kind != "Directory" {
			return restorePlanNode{}, _errRestoreSourcePathNotFound
		}
		if seen[current.id] {
			return restorePlanNode{}, errors.New("restore source tree contains a cycle")
		}
		seen[current.id] = true
		directory, err := planner.loadDirectory(current.id)
		if err != nil {
			return restorePlanNode{}, err
		}
		entry, ok := restoreDirectoryEntry(directory, part)
		if !ok {
			return restorePlanNode{}, _errRestoreSourcePathNotFound
		}
		current = restorePlanNode{present: true, kind: entry.Type, id: entry.ID, mtime: entry.ModifiedAt}
		if index == len(parts)-1 {
			if entry.Type == "File" {
				if _, err := planner.loadFile(entry.ID); err != nil {
					return restorePlanNode{}, err
				}
			} else if _, err := planner.loadDirectory(entry.ID); err != nil {
				return restorePlanNode{}, err
			}
			return current, nil
		}
	}
	return restorePlanNode{}, _errRestoreSourcePathNotFound
}

func restoreDirectoryEntry(directory object.Directory, name string) (object.DirectoryEntry, bool) {
	index := sort.Search(len(directory.Entries), func(index int) bool {
		return directory.Entries[index].Name >= name
	})
	if index == len(directory.Entries) || directory.Entries[index].Name != name {
		return object.DirectoryEntry{}, false
	}
	return directory.Entries[index], true
}

func (planner *restorePlanner) applyPath(current, source restorePlanNode, parts []string, index int, path string, root bool) (restorePlanNode, error) {
	if index == len(parts) {
		return planner.mergeNode(current, source, path, root)
	}
	if !source.present || source.kind != "Directory" {
		return restorePlanNode{}, _errRestoreSourcePathNotFound
	}
	if !current.present || current.kind != "Directory" {
		return planner.buildMissingPath(current, source, parts, index, path, root)
	}
	currentDirectory, err := planner.loadDirectory(current.id)
	if err != nil {
		return restorePlanNode{}, err
	}
	sourceDirectory, err := planner.loadDirectory(source.id)
	if err != nil {
		return restorePlanNode{}, err
	}
	name := parts[index]
	sourceEntry, ok := restoreDirectoryEntry(sourceDirectory, name)
	if !ok {
		return restorePlanNode{}, _errRestoreSourcePathNotFound
	}
	currentEntry, currentOK := restoreDirectoryEntry(currentDirectory, name)
	var currentChild restorePlanNode
	if currentOK {
		currentChild = restorePlanNode{present: true, kind: currentEntry.Type, id: currentEntry.ID, mtime: currentEntry.ModifiedAt}
	}
	childPath := name
	if path != "" {
		childPath = path + "/" + name
	}
	child, err := planner.applyPath(currentChild,
		restorePlanNode{present: true, kind: sourceEntry.Type, id: sourceEntry.ID, mtime: sourceEntry.ModifiedAt},
		parts, index+1, childPath, false)
	if err != nil {
		return restorePlanNode{}, err
	}
	entries := make([]scanEntry, 0, len(currentDirectory.Entries)+1)
	inserted := false
	for _, entry := range currentDirectory.Entries {
		if entry.Name == name {
			entries = append(entries, scanEntry{name: childPathLeaf(childPath), kind: child.kind, id: child.id, modified: child.mtime})
			inserted = true
			continue
		}
		entries = append(entries, scanEntry{name: entry.Name, kind: entry.Type, id: entry.ID, modified: entry.ModifiedAt})
	}
	if !inserted {
		entries = append(entries, scanEntry{name: name, kind: child.kind, id: child.id, modified: child.mtime})
	}
	sort.Slice(entries, func(left, right int) bool { return entries[left].name < entries[right].name })
	data, id, err := canonicalDirectory(path, entries)
	if err != nil {
		return restorePlanNode{}, err
	}
	planner.recordSynthesizedDirectory(id, data, current.id, source.id)
	mtime := maxRestoreMtime(current.mtime, source.mtime)
	if root {
		mtime = ""
	}
	result := restorePlanNode{present: true, kind: "Directory", id: id, mtime: mtime}
	if !root && (result.id != current.id || result.mtime != current.mtime) {
		planner.recordUpdated(path)
	}
	return result, nil
}

func (planner *restorePlanner) buildMissingPath(current, source restorePlanNode, parts []string, index int, path string, root bool) (restorePlanNode, error) {
	if !source.present || source.kind != "Directory" {
		return restorePlanNode{}, _errRestoreSourcePathNotFound
	}
	sourceDirectory, err := planner.loadDirectory(source.id)
	if err != nil {
		return restorePlanNode{}, err
	}
	name := parts[index]
	sourceEntry, ok := restoreDirectoryEntry(sourceDirectory, name)
	if !ok {
		return restorePlanNode{}, _errRestoreSourcePathNotFound
	}
	childPath := name
	if path != "" {
		childPath = path + "/" + name
	}
	var child restorePlanNode
	if index+1 == len(parts) {
		child, err = planner.mergeNode(restorePlanNode{}, restorePlanNode{present: true, kind: sourceEntry.Type,
			id: sourceEntry.ID, mtime: sourceEntry.ModifiedAt}, childPath, false)
	} else {
		child, err = planner.buildMissingPath(restorePlanNode{}, restorePlanNode{present: true, kind: sourceEntry.Type,
			id: sourceEntry.ID, mtime: sourceEntry.ModifiedAt}, parts, index+1, childPath, false)
	}
	if err != nil {
		return restorePlanNode{}, err
	}
	data, id, err := canonicalDirectory(path, []scanEntry{{name: name, kind: child.kind, id: child.id, modified: child.mtime}})
	if err != nil {
		return restorePlanNode{}, err
	}
	planner.recordSynthesizedDirectory(id, data, "", source.id)
	result := restorePlanNode{present: true, kind: "Directory", id: id, mtime: source.mtime}
	if root {
		result.mtime = ""
	}
	if !root {
		switch {
		case !current.present:
			planner.recordCreated(path)
		case current.kind != "Directory":
			planner.recordTypeReplacement(path)
		}
	}
	return result, nil
}

func (planner *restorePlanner) mergeNode(current, source restorePlanNode, path string, root bool) (restorePlanNode, error) {
	if !source.present {
		return restorePlanNode{}, _errRestoreSourcePathNotFound
	}
	if !current.present {
		if err := planner.recordCreatedSubtree(source, path, root); err != nil {
			return restorePlanNode{}, err
		}
		return source, nil
	}
	if current.kind != source.kind {
		if !root {
			planner.recordTypeReplacement(path)
		}
		if current.kind == "Directory" {
			removed, err := planner.countDescendants(current.id, path, strings.Count(path, "/")+1)
			if err != nil {
				return restorePlanNode{}, err
			}
			planner.removedDescendantCount += removed
		}
		return source, nil
	}
	switch source.kind {
	case "File":
		if _, err := planner.loadFile(current.id); err != nil {
			return restorePlanNode{}, err
		}
		if _, err := planner.loadFile(source.id); err != nil {
			return restorePlanNode{}, err
		}
		if current.id == source.id && current.mtime == source.mtime {
			return current, nil
		}
		if !root {
			planner.recordUpdated(path)
		}
		return source, nil
	case "Directory":
		return planner.mergeDirectories(current, source, path, root)
	default:
		return restorePlanNode{}, errors.New("restore tree contains an invalid object type")
	}
}

func (planner *restorePlanner) mergeDirectories(current, source restorePlanNode, path string, root bool) (restorePlanNode, error) {
	key := current.id + "\x00" + source.id
	if planner.active[key] {
		return restorePlanNode{}, errors.New("restore directory graph contains a cycle")
	}
	planner.active[key] = true
	defer delete(planner.active, key)
	currentDirectory, err := planner.loadDirectory(current.id)
	if err != nil {
		return restorePlanNode{}, err
	}
	sourceDirectory, err := planner.loadDirectory(source.id)
	if err != nil {
		return restorePlanNode{}, err
	}
	currentEntries := restoreEntriesByName(currentDirectory)
	sourceEntries := restoreEntriesByName(sourceDirectory)
	names := make(map[string]struct{}, len(currentEntries)+len(sourceEntries))
	for name := range currentEntries {
		names[name] = struct{}{}
	}
	for name := range sourceEntries {
		names[name] = struct{}{}
	}
	ordered := make([]string, 0, len(names))
	for name := range names {
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)
	entries := make([]scanEntry, 0, len(ordered))
	for _, name := range ordered {
		currentEntry, currentOK := currentEntries[name]
		sourceEntry, sourceOK := sourceEntries[name]
		childPath := name
		if path != "" {
			childPath = path + "/" + name
		}
		var child restorePlanNode
		switch {
		case !sourceOK:
			child = restorePlanNode{present: true, kind: currentEntry.Type, id: currentEntry.ID, mtime: currentEntry.ModifiedAt}
			if err := planner.recordPreservedSubtree(child, childPath); err != nil {
				return restorePlanNode{}, err
			}
		case !currentOK:
			child, err = planner.mergeNode(restorePlanNode{}, restorePlanNode{present: true, kind: sourceEntry.Type,
				id: sourceEntry.ID, mtime: sourceEntry.ModifiedAt}, childPath, false)
		default:
			child, err = planner.mergeNode(restorePlanNode{present: true, kind: currentEntry.Type,
				id: currentEntry.ID, mtime: currentEntry.ModifiedAt}, restorePlanNode{present: true, kind: sourceEntry.Type,
				id: sourceEntry.ID, mtime: sourceEntry.ModifiedAt}, childPath, false)
		}
		if err != nil {
			return restorePlanNode{}, err
		}
		entries = append(entries, scanEntry{name: name, kind: child.kind, id: child.id, modified: child.mtime})
	}
	data, id, err := canonicalDirectory(path, entries)
	if err != nil {
		return restorePlanNode{}, err
	}
	planner.recordSynthesizedDirectory(id, data, current.id, source.id)
	mtime := maxRestoreMtime(current.mtime, source.mtime)
	if root {
		mtime = ""
	}
	result := restorePlanNode{present: true, kind: "Directory", id: id, mtime: mtime}
	if !root && (result.id != current.id || result.mtime != current.mtime) {
		planner.recordUpdated(path)
	}
	return result, nil
}

func restoreEntriesByName(directory object.Directory) map[string]object.DirectoryEntry {
	entries := make(map[string]object.DirectoryEntry, len(directory.Entries))
	for _, entry := range directory.Entries {
		entries[entry.Name] = entry
	}
	return entries
}

func childPathLeaf(path string) string {
	if index := strings.LastIndexByte(path, '/'); index >= 0 {
		return path[index+1:]
	}
	return path
}

func maxRestoreMtime(left, right string) string {
	if right > left {
		return right
	}
	return left
}

func (planner *restorePlanner) recordSynthesizedDirectory(id string, data []byte, currentID, sourceID string) {
	if id == currentID || id == sourceID {
		return
	}
	planner.synthesized[id] = data
}

func (planner *restorePlanner) recordCreated(path string) {
	planner.createdCount++
	planner.changed[path] = struct{}{}
}

func (planner *restorePlanner) recordUpdated(path string) {
	planner.updatedCount++
	planner.changed[path] = struct{}{}
}

func (planner *restorePlanner) recordTypeReplacement(path string) {
	planner.typeReplacementCount++
	planner.changed[path] = struct{}{}
}

func (planner *restorePlanner) recordCreatedSubtree(node restorePlanNode, path string, root bool) error {
	if root {
		return errors.New("restore cannot create a root object")
	}
	return planner.walkSubtree(node, path, func(value string) { planner.recordCreated(value) })
}

func (planner *restorePlanner) recordPreservedSubtree(node restorePlanNode, path string) error {
	return planner.walkSubtree(node, path, func(string) { planner.preservedCurrentOnlyCount++ })
}

func (planner *restorePlanner) countDescendants(id, path string, depth int) (int64, error) {
	if depth > planner.budget.maxDepth {
		return 0, errors.New("restore planner exceeds directory depth budget")
	}
	if planner.activeWalk[id] {
		return 0, errors.New("restore removed directory graph contains a cycle")
	}
	planner.activeWalk[id] = true
	defer delete(planner.activeWalk, id)
	directory, err := planner.loadDirectory(id)
	if err != nil {
		return 0, err
	}
	var count int64
	for _, entry := range directory.Entries {
		childPath := path + "/" + entry.Name
		if err := planner.validateResultPath(childPath); err != nil {
			return 0, err
		}
		count++
		if entry.Type == "Directory" {
			children, err := planner.countDescendants(entry.ID, childPath, depth+1)
			if err != nil {
				return 0, err
			}
			count += children
		} else if _, err := planner.loadFile(entry.ID); err != nil {
			return 0, err
		}
	}
	return count, nil
}

func (planner *restorePlanner) walkSubtree(node restorePlanNode, path string, visit func(string)) error {
	if err := planner.validateResultPath(path); err != nil {
		return err
	}
	if node.kind == "File" {
		if _, err := planner.loadFile(node.id); err != nil {
			return err
		}
		visit(path)
		return nil
	}
	if node.kind != "Directory" {
		return errors.New("restore tree contains an invalid object type")
	}
	if planner.activeWalk[node.id] {
		return errors.New("restore directory graph contains a cycle")
	}
	planner.activeWalk[node.id] = true
	defer delete(planner.activeWalk, node.id)
	directory, err := planner.loadDirectory(node.id)
	if err != nil {
		return err
	}
	visit(path)
	for _, entry := range directory.Entries {
		childPath := path + "/" + entry.Name
		if err := planner.walkSubtree(restorePlanNode{present: true, kind: entry.Type, id: entry.ID,
			mtime: entry.ModifiedAt}, childPath, visit); err != nil {
			return err
		}
	}
	return nil
}

func (planner *restorePlanner) resultPaths(id, prefix string, depth int) ([]checkoutPath, error) {
	if depth > planner.budget.maxDepth {
		return nil, errors.New("restore planner exceeds directory depth budget")
	}
	if planner.activeWalk[id] {
		return nil, errors.New("restore result directory graph contains a cycle")
	}
	planner.activeWalk[id] = true
	defer delete(planner.activeWalk, id)
	directory, err := planner.loadDirectory(id)
	if err != nil {
		return nil, err
	}
	paths := make([]checkoutPath, 0, len(directory.Entries))
	for _, entry := range directory.Entries {
		path := entry.Name
		if prefix != "" {
			path = prefix + "/" + entry.Name
		}
		if err := planner.validateResultPath(path); err != nil {
			return nil, err
		}
		value := checkoutPath{path: path, kind: entry.Type, id: entry.ID, mtime: entry.ModifiedAt}
		if entry.Type == "File" {
			file, err := planner.loadFile(entry.ID)
			if err != nil {
				return nil, err
			}
			value.size = file.Size
			paths = append(paths, value)
			continue
		}
		paths = append(paths, value)
		children, err := planner.resultPaths(entry.ID, path, depth+1)
		if err != nil {
			return nil, err
		}
		paths = append(paths, children...)
	}
	return paths, nil
}

func (planner *restorePlanner) validateResultPath(path string) error {
	if !object.ValidPath(path) || path == "." || len(path) > planner.budget.maxPathBytes {
		return errors.New("restore planner exceeds path budget")
	}
	return nil
}
