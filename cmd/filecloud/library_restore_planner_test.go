package main

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

type restorePlannerTestLoader struct {
	objects map[string][]byte
	loads   []string
}

func (loader *restorePlannerTestLoader) loadRestoreObject(kind, id string) ([]byte, error) {
	loader.loads = append(loader.loads, kind+"/"+id)
	if kind == "blocks" {
		return nil, errors.New("block load is forbidden")
	}
	data, ok := loader.objects[kind+"/"+id]
	if !ok {
		return nil, errors.New("object not found")
	}
	return append([]byte(nil), data...), nil
}

func TestRestorePlannerRestoresFileAndMtimeOnlyChange(t *testing.T) {
	currentFileData, currentFileID, err := canonicalFile("report.txt", 1, []string{strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	sourceFileData, sourceFileID, err := canonicalFile("report.txt", 1, []string{strings.Repeat("b", 64)})
	if err != nil {
		t.Fatal(err)
	}
	currentRootData, currentRootID, err := canonicalDirectory("", []scanEntry{{
		name: "report.txt", kind: "File", id: currentFileID, modified: "2026-01-01T00:00:00Z",
	}})
	if err != nil {
		t.Fatal(err)
	}
	sourceRootData, sourceRootID, err := canonicalDirectory("", []scanEntry{{
		name: "report.txt", kind: "File", id: sourceFileID, modified: "2026-01-02T00:00:00Z",
	}})
	if err != nil {
		t.Fatal(err)
	}
	loader := &restorePlannerTestLoader{objects: map[string][]byte{
		"directories/" + currentRootID: currentRootData,
		"directories/" + sourceRootID:  sourceRootData,
		"files/" + currentFileID:       currentFileData,
		"files/" + sourceFileID:        sourceFileData,
	}}

	plan, err := planRestoreOverlay(restorePlanInput{
		CurrentRoot: currentRootID,
		SourceRoot:  sourceRootID,
		SourcePath:  "report.txt",
		Load:        loader.loadRestoreObject,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.resultRoot != sourceRootID {
		t.Fatalf("ResultRoot=%s, want source root %s", plan.resultRoot, sourceRootID)
	}
	if plan.updatedCount != 1 || plan.changedPathCount != 1 || len(plan.changedPaths) != 1 || plan.changedPaths[0] != "report.txt" {
		t.Fatalf("plan stats=%+v, want one updated report.txt", plan)
	}
	if plan.createdCount != 0 || plan.typeReplacementCount != 0 || plan.removedDescendantCount != 0 || plan.preservedCurrentOnlyCount != 0 {
		t.Fatalf("unexpected plan stats=%+v", plan)
	}
	if len(plan.paths) != 1 || plan.paths[0].path != "report.txt" || plan.paths[0].kind != "File" ||
		plan.paths[0].id != sourceFileID || plan.paths[0].mtime != "2026-01-02T00:00:00Z" || plan.paths[0].size != 1 {
		t.Fatalf("result paths=%+v", plan.paths)
	}
	for _, load := range loader.loads {
		if strings.HasPrefix(load, "blocks/") {
			t.Fatalf("planner loaded content block %q", load)
		}
	}
}

func TestRestorePlannerDirectoryOverlayFixedVector(t *testing.T) {
	currentOldData, currentOldID, err := canonicalFile("old.txt", 1, []string{strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	sourceOldData, sourceOldID, err := canonicalFile("old.txt", 1, []string{strings.Repeat("b", 64)})
	if err != nil {
		t.Fatal(err)
	}
	sourceNewData, sourceNewID, err := canonicalFile("new.txt", 2, []string{strings.Repeat("c", 64)})
	if err != nil {
		t.Fatal(err)
	}
	currentKeepData, currentKeepID, err := canonicalFile("keep.txt", 1, []string{strings.Repeat("d", 64)})
	if err != nil {
		t.Fatal(err)
	}
	currentDocsData, currentDocsID, err := canonicalDirectory("docs", []scanEntry{
		{name: "keep.txt", kind: "File", id: currentKeepID, modified: "2026-01-01T00:00:00Z"},
		{name: "old.txt", kind: "File", id: currentOldID, modified: "2026-01-01T00:00:00Z"},
	})
	if err != nil {
		t.Fatal(err)
	}
	sourceDocsData, sourceDocsID, err := canonicalDirectory("docs", []scanEntry{
		{name: "new.txt", kind: "File", id: sourceNewID, modified: "2026-01-04T00:00:00Z"},
		{name: "old.txt", kind: "File", id: sourceOldID, modified: "2026-01-03T00:00:00Z"},
	})
	if err != nil {
		t.Fatal(err)
	}
	currentRootData, currentRootID, err := canonicalDirectory("", []scanEntry{
		{name: "docs", kind: "Directory", id: currentDocsID, modified: "2026-01-01T00:00:00Z"},
	})
	if err != nil {
		t.Fatal(err)
	}
	sourceRootData, sourceRootID, err := canonicalDirectory("", []scanEntry{
		{name: "docs", kind: "Directory", id: sourceDocsID, modified: "2026-01-02T00:00:00Z"},
	})
	if err != nil {
		t.Fatal(err)
	}
	loader := &restorePlannerTestLoader{objects: map[string][]byte{
		"directories/" + currentRootID: currentRootData,
		"directories/" + currentDocsID: currentDocsData,
		"directories/" + sourceRootID:  sourceRootData,
		"directories/" + sourceDocsID:  sourceDocsData,
		"files/" + currentOldID:        currentOldData,
		"files/" + sourceOldID:         sourceOldData,
		"files/" + sourceNewID:         sourceNewData,
		"files/" + currentKeepID:       currentKeepData,
	}}

	plan, err := planRestoreOverlay(restorePlanInput{
		CurrentRoot: currentRootID,
		SourceRoot:  sourceRootID,
		SourcePath:  "docs",
		Load:        loader.loadRestoreObject,
	})
	if err != nil {
		t.Fatal(err)
	}
	const wantRoot = "cbdaed3bd32f21da9876622b002c75d8bd986be6e0bfd13c5f8e924146331059"
	if plan.resultRoot != wantRoot {
		t.Fatalf("fixed vector ResultRoot=%s, want %s", plan.resultRoot, wantRoot)
	}
	if plan.createdCount != 1 || plan.updatedCount != 2 || plan.preservedCurrentOnlyCount != 1 ||
		plan.typeReplacementCount != 0 || plan.removedDescendantCount != 0 {
		t.Fatalf("fixed vector stats=%+v", plan)
	}
	wantChanged := []string{"docs", "docs/new.txt", "docs/old.txt"}
	if plan.changedPathCount != int64(len(wantChanged)) || !equalStrings(plan.changedPaths, wantChanged) || plan.previewTruncated {
		t.Fatalf("fixed vector changed paths=%v count=%d truncated=%v", plan.changedPaths, plan.changedPathCount, plan.previewTruncated)
	}
	if len(plan.paths) != 4 || plan.paths[0].path != "docs" || plan.paths[0].mtime != "2026-01-02T00:00:00Z" ||
		plan.paths[1].path != "docs/keep.txt" || plan.paths[2].path != "docs/new.txt" || plan.paths[3].path != "docs/old.txt" {
		t.Fatalf("fixed vector paths=%+v", plan.paths)
	}
}

func TestRestorePlannerNoOpAndMissingSourcePath(t *testing.T) {
	fileData, fileID, err := canonicalFile("same.txt", 1, []string{strings.Repeat("e", 64)})
	if err != nil {
		t.Fatal(err)
	}
	rootData, rootID, err := canonicalDirectory("", []scanEntry{{
		name: "same.txt", kind: "File", id: fileID, modified: "2026-02-01T00:00:00Z",
	}})
	if err != nil {
		t.Fatal(err)
	}
	loader := &restorePlannerTestLoader{objects: map[string][]byte{
		"directories/" + rootID: rootData,
		"files/" + fileID:       fileData,
	}}
	plan, err := planRestoreOverlay(restorePlanInput{
		CurrentRoot: rootID, SourceRoot: rootID, SourcePath: "same.txt", Load: loader.loadRestoreObject,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.resultRoot != rootID || plan.createdCount != 0 || plan.updatedCount != 0 || plan.typeReplacementCount != 0 ||
		plan.removedDescendantCount != 0 || plan.preservedCurrentOnlyCount != 0 || plan.changedPathCount != 0 || len(plan.changedPaths) != 0 {
		t.Fatalf("no-op plan=%+v", plan)
	}
	if _, err := planRestoreOverlay(restorePlanInput{
		CurrentRoot: rootID, SourceRoot: rootID, SourcePath: "missing.txt", Load: loader.loadRestoreObject,
	}); !errors.Is(err, _errRestoreSourcePathNotFound) {
		t.Fatalf("missing source path error=%v, want _errRestoreSourcePathNotFound", err)
	}
}

func TestRestorePlannerTypeReplacementCountsRemovedDescendants(t *testing.T) {
	childData, childID, err := canonicalFile("child.txt", 1, []string{strings.Repeat("c", 64)})
	if err != nil {
		t.Fatal(err)
	}
	deepData, deepID, err := canonicalFile("deep.txt", 1, []string{strings.Repeat("d", 64)})
	if err != nil {
		t.Fatal(err)
	}
	nestedData, nestedID, err := canonicalDirectory("target/nested", []scanEntry{{
		name: "deep.txt", kind: "File", id: deepID, modified: "2026-03-01T00:00:00Z",
	}})
	if err != nil {
		t.Fatal(err)
	}
	currentTargetData, currentTargetID, err := canonicalDirectory("target", []scanEntry{
		{name: "child.txt", kind: "File", id: childID, modified: "2026-03-01T00:00:00Z"},
		{name: "nested", kind: "Directory", id: nestedID, modified: "2026-03-01T00:00:00Z"},
	})
	if err != nil {
		t.Fatal(err)
	}
	sourceData, sourceID, err := canonicalFile("target", 2, []string{strings.Repeat("e", 64)})
	if err != nil {
		t.Fatal(err)
	}
	currentRootData, currentRootID, err := canonicalDirectory("", []scanEntry{{
		name: "target", kind: "Directory", id: currentTargetID, modified: "2026-03-01T00:00:00Z",
	}})
	if err != nil {
		t.Fatal(err)
	}
	sourceRootData, sourceRootID, err := canonicalDirectory("", []scanEntry{{
		name: "target", kind: "File", id: sourceID, modified: "2026-03-02T00:00:00Z",
	}})
	if err != nil {
		t.Fatal(err)
	}
	loader := &restorePlannerTestLoader{objects: map[string][]byte{
		"directories/" + currentRootID:   currentRootData,
		"directories/" + currentTargetID: currentTargetData,
		"directories/" + nestedID:        nestedData,
		"directories/" + sourceRootID:    sourceRootData,
		"files/" + childID:               childData,
		"files/" + deepID:                deepData,
		"files/" + sourceID:              sourceData,
	}}
	plan, err := planRestoreOverlay(restorePlanInput{
		CurrentRoot: currentRootID, SourceRoot: sourceRootID, SourcePath: "target", Load: loader.loadRestoreObject,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.resultRoot != sourceRootID || plan.typeReplacementCount != 1 || plan.removedDescendantCount != 3 ||
		plan.createdCount != 0 || plan.updatedCount != 0 || plan.preservedCurrentOnlyCount != 0 ||
		plan.changedPathCount != 1 || len(plan.changedPaths) != 1 || plan.changedPaths[0] != "target" {
		t.Fatalf("type replacement plan=%+v", plan)
	}
	if len(plan.paths) != 1 || plan.paths[0].path != "target" || plan.paths[0].kind != "File" || plan.paths[0].size != 2 {
		t.Fatalf("type replacement paths=%+v", plan.paths)
	}
}

func TestRestorePlannerPreviewCapAndDeterminism(t *testing.T) {
	fileData, fileID, err := canonicalFile("file.txt", 1, []string{strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	currentEntries := make([]scanEntry, 101)
	sourceEntries := make([]scanEntry, 101)
	for index := range 101 {
		name := fmt.Sprintf("file-%03d.txt", index)
		currentEntries[index] = scanEntry{name: name, kind: "File", id: fileID, modified: "2026-03-01T00:00:00Z"}
		sourceEntries[index] = scanEntry{name: name, kind: "File", id: fileID, modified: "2026-04-01T00:00:00Z"}
	}
	currentData, currentID, err := canonicalDirectory("", currentEntries)
	if err != nil {
		t.Fatal(err)
	}
	sourceData, sourceID, err := canonicalDirectory("", sourceEntries)
	if err != nil {
		t.Fatal(err)
	}
	loader := &restorePlannerTestLoader{objects: map[string][]byte{
		"directories/" + currentID: currentData,
		"directories/" + sourceID:  sourceData,
		"files/" + fileID:          fileData,
	}}
	input := restorePlanInput{CurrentRoot: currentID, SourceRoot: sourceID, SourcePath: ".", Load: loader.loadRestoreObject}
	first, err := planRestoreOverlay(input)
	if err != nil {
		t.Fatal(err)
	}
	second, err := planRestoreOverlay(input)
	if err != nil {
		t.Fatal(err)
	}
	if first.resultRoot != second.resultRoot || !equalStrings(first.changedPaths, second.changedPaths) || first.changedPathCount != second.changedPathCount {
		t.Fatalf("planner is not deterministic: first=%+v second=%+v", first, second)
	}
	if first.resultRoot != sourceID || first.updatedCount != 101 || first.changedPathCount != 101 ||
		len(first.changedPaths) != _restorePreviewPathLimit || !first.previewTruncated {
		t.Fatalf("preview stats=%+v", first)
	}
	for index, path := range first.changedPaths {
		want := fmt.Sprintf("file-%03d.txt", index)
		if path != want {
			t.Fatalf("preview path %d=%q, want %q", index, path, want)
		}
	}
}

func TestRestorePlannerRejectsBudgetsAndNonCanonicalObjects(t *testing.T) {
	fileData, fileID, err := canonicalFile("same.txt", 1, []string{strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	rootData, rootID, err := canonicalDirectory("", []scanEntry{{
		name: "same.txt", kind: "File", id: fileID, modified: "2026-05-01T00:00:00Z",
	}})
	if err != nil {
		t.Fatal(err)
	}
	loader := &restorePlannerTestLoader{objects: map[string][]byte{
		"directories/" + rootID: rootData,
		"files/" + fileID:       fileData,
	}}
	if _, err := planRestoreOverlay(restorePlanInput{CurrentRoot: rootID, SourceRoot: rootID, SourcePath: "same.txt",
		Load: loader.loadRestoreObject, Budget: &restorePlanBudget{maxDepth: 256, maxObjects: 1, maxPathBytes: 1024}}); err == nil || !strings.Contains(err.Error(), "object budget") {
		t.Fatalf("object budget error=%v", err)
	}
	if _, err := planRestoreOverlay(restorePlanInput{CurrentRoot: rootID, SourceRoot: rootID, SourcePath: "same.txt",
		Load: loader.loadRestoreObject, Budget: &restorePlanBudget{maxDepth: 256, maxObjects: 100, maxPathBytes: 4}}); err == nil || !strings.Contains(err.Error(), "path budget") {
		t.Fatalf("path budget error=%v", err)
	}

	leafData, leafID, err := canonicalFile("leaf.txt", 1, []string{strings.Repeat("b", 64)})
	if err != nil {
		t.Fatal(err)
	}
	levelTwoData, levelTwoID, err := canonicalDirectory("a/b", []scanEntry{{
		name: "leaf.txt", kind: "File", id: leafID, modified: "2026-05-01T00:00:00Z",
	}})
	if err != nil {
		t.Fatal(err)
	}
	levelOneData, levelOneID, err := canonicalDirectory("a", []scanEntry{{
		name: "b", kind: "Directory", id: levelTwoID, modified: "2026-05-01T00:00:00Z",
	}})
	if err != nil {
		t.Fatal(err)
	}
	deepRootData, deepRootID, err := canonicalDirectory("", []scanEntry{{
		name: "a", kind: "Directory", id: levelOneID, modified: "2026-05-01T00:00:00Z",
	}})
	if err != nil {
		t.Fatal(err)
	}
	deepLoader := &restorePlannerTestLoader{objects: map[string][]byte{
		"directories/" + deepRootID: deepRootData,
		"directories/" + levelOneID: levelOneData,
		"directories/" + levelTwoID: levelTwoData,
		"files/" + leafID:           leafData,
	}}
	if _, err := planRestoreOverlay(restorePlanInput{CurrentRoot: deepRootID, SourceRoot: deepRootID, SourcePath: ".",
		Load: deepLoader.loadRestoreObject, Budget: &restorePlanBudget{maxDepth: 1, maxObjects: 100, maxPathBytes: 1024}}); err == nil || !strings.Contains(err.Error(), "depth budget") {
		t.Fatalf("depth budget error=%v", err)
	}
	badID := strings.Repeat("c", 64)
	badLoader := &restorePlannerTestLoader{objects: map[string][]byte{"directories/" + badID: []byte(`{"Type":"Directory"}`)}}
	if _, err := planRestoreOverlay(restorePlanInput{CurrentRoot: badID, SourceRoot: badID, SourcePath: ".", Load: badLoader.loadRestoreObject}); err == nil || !strings.Contains(err.Error(), "not canonical") {
		t.Fatalf("non-canonical object error=%v", err)
	}
}

func TestRestorePlannerDirectoryReplacesFile(t *testing.T) {
	currentData, currentID, err := canonicalFile("target", 1, []string{strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	sourceChildData, sourceChildID, err := canonicalFile("child.txt", 1, []string{strings.Repeat("b", 64)})
	if err != nil {
		t.Fatal(err)
	}
	sourceDirectoryData, sourceDirectoryID, err := canonicalDirectory("target", []scanEntry{{
		name: "child.txt", kind: "File", id: sourceChildID, modified: "2026-06-01T00:00:00Z",
	}})
	if err != nil {
		t.Fatal(err)
	}
	currentRootData, currentRootID, err := canonicalDirectory("", []scanEntry{{
		name: "target", kind: "File", id: currentID, modified: "2026-06-01T00:00:00Z",
	}})
	if err != nil {
		t.Fatal(err)
	}
	sourceRootData, sourceRootID, err := canonicalDirectory("", []scanEntry{{
		name: "target", kind: "Directory", id: sourceDirectoryID, modified: "2026-06-02T00:00:00Z",
	}})
	if err != nil {
		t.Fatal(err)
	}
	loader := &restorePlannerTestLoader{objects: map[string][]byte{
		"directories/" + currentRootID:     currentRootData,
		"directories/" + sourceRootID:      sourceRootData,
		"directories/" + sourceDirectoryID: sourceDirectoryData,
		"files/" + currentID:               currentData,
		"files/" + sourceChildID:           sourceChildData,
	}}
	plan, err := planRestoreOverlay(restorePlanInput{
		CurrentRoot: currentRootID, SourceRoot: sourceRootID, SourcePath: "target", Load: loader.loadRestoreObject,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.resultRoot != sourceRootID || plan.typeReplacementCount != 1 || plan.removedDescendantCount != 0 ||
		plan.createdCount != 0 || plan.updatedCount != 0 || plan.changedPathCount != 1 || plan.changedPaths[0] != "target" {
		t.Fatalf("directory replacement plan=%+v", plan)
	}
	if len(plan.paths) != 2 || plan.paths[0].path != "target" || plan.paths[0].kind != "Directory" ||
		plan.paths[1].path != "target/child.txt" || plan.paths[1].kind != "File" {
		t.Fatalf("directory replacement paths=%+v", plan.paths)
	}
}

func TestRestorePlannerCreatesMissingParentChainForSelectedPath(t *testing.T) {
	fileData, fileID, err := canonicalFile("target.txt", 1, []string{strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	sourceParentData, sourceParentID, err := canonicalDirectory("docs", []scanEntry{
		{name: "other.txt", kind: "File", id: fileID, modified: "2026-07-01T00:00:00Z"},
		{name: "target.txt", kind: "File", id: fileID, modified: "2026-07-02T00:00:00Z"},
	})
	if err != nil {
		t.Fatal(err)
	}
	sourceRootData, sourceRootID, err := canonicalDirectory("", []scanEntry{{
		name: "docs", kind: "Directory", id: sourceParentID, modified: "2026-07-03T00:00:00Z",
	}})
	if err != nil {
		t.Fatal(err)
	}
	currentRootData, currentRootID, err := canonicalDirectory("", nil)
	if err != nil {
		t.Fatal(err)
	}
	loader := &restorePlannerTestLoader{objects: map[string][]byte{
		"directories/" + currentRootID:  currentRootData,
		"directories/" + sourceRootID:   sourceRootData,
		"directories/" + sourceParentID: sourceParentData,
		"files/" + fileID:               fileData,
	}}
	plan, err := planRestoreOverlay(restorePlanInput{
		CurrentRoot: currentRootID, SourceRoot: sourceRootID, SourcePath: "docs/target.txt", Load: loader.loadRestoreObject,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.createdCount != 2 || plan.updatedCount != 0 || plan.typeReplacementCount != 0 || plan.changedPathCount != 2 ||
		!equalStrings(plan.changedPaths, []string{"docs", "docs/target.txt"}) || plan.resultRoot == currentRootID {
		t.Fatalf("missing parent plan=%+v", plan)
	}
	if len(plan.paths) != 2 || plan.paths[0].path != "docs" || plan.paths[0].mtime != "2026-07-03T00:00:00Z" ||
		plan.paths[1].path != "docs/target.txt" {
		t.Fatalf("missing parent paths=%+v", plan.paths)
	}
}
