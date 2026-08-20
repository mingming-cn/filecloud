package main

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
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

type restoreFixedVector struct {
	currentRoot string
	sourceRoot  string
	plan        restorePlan
	candidate   []byte
	candidateID string
}

func computeRestoreFixedVector() (restoreFixedVector, error) {
	currentOldData, currentOldID, err := canonicalFile("old.txt", 1, []string{strings.Repeat("a", 64)})
	if err != nil {
		return restoreFixedVector{}, err
	}
	sourceOldData, sourceOldID, err := canonicalFile("old.txt", 1, []string{strings.Repeat("b", 64)})
	if err != nil {
		return restoreFixedVector{}, err
	}
	sourceNewData, sourceNewID, err := canonicalFile("new.txt", 2, []string{strings.Repeat("c", 64)})
	if err != nil {
		return restoreFixedVector{}, err
	}
	currentKeepData, currentKeepID, err := canonicalFile("keep.txt", 1, []string{strings.Repeat("d", 64)})
	if err != nil {
		return restoreFixedVector{}, err
	}
	currentDocsData, currentDocsID, err := canonicalDirectory("docs", []scanEntry{
		{name: "keep.txt", kind: "File", id: currentKeepID, modified: "2026-01-01T00:00:00Z"},
		{name: "old.txt", kind: "File", id: currentOldID, modified: "2026-01-01T00:00:00Z"},
	})
	if err != nil {
		return restoreFixedVector{}, err
	}
	sourceDocsData, sourceDocsID, err := canonicalDirectory("docs", []scanEntry{
		{name: "new.txt", kind: "File", id: sourceNewID, modified: "2026-01-04T00:00:00Z"},
		{name: "old.txt", kind: "File", id: sourceOldID, modified: "2026-01-03T00:00:00Z"},
	})
	if err != nil {
		return restoreFixedVector{}, err
	}
	currentRootData, currentRootID, err := canonicalDirectory("", []scanEntry{{
		name: "docs", kind: "Directory", id: currentDocsID, modified: "2026-01-01T00:00:00Z",
	}})
	if err != nil {
		return restoreFixedVector{}, err
	}
	sourceRootData, sourceRootID, err := canonicalDirectory("", []scanEntry{{
		name: "docs", kind: "Directory", id: sourceDocsID, modified: "2026-01-02T00:00:00Z",
	}})
	if err != nil {
		return restoreFixedVector{}, err
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
		CurrentRoot: currentRootID, SourceRoot: sourceRootID, SourcePath: "docs", Load: loader.loadRestoreObject,
	})
	if err != nil {
		return restoreFixedVector{}, err
	}
	sourceCommit := strings.Repeat("e", 64)
	expectedHead := strings.Repeat("f", 64)
	candidate, candidateID, err := canonicalRestoreCommit(testClientUserID, testClientDeviceID, plan.resultRoot,
		sourceCommit, "docs", []string{expectedHead}, time.Date(2026, 8, 9, 1, 2, 3, 0, time.UTC))
	if err != nil {
		return restoreFixedVector{}, err
	}
	return restoreFixedVector{currentRoot: currentRootID, sourceRoot: sourceRootID, plan: plan,
		candidate: candidate, candidateID: candidateID}, nil
}

func TestRestorePlannerDirectoryOverlayFixedVector(t *testing.T) {
	vector, err := computeRestoreFixedVector()
	if err != nil {
		t.Fatal(err)
	}
	again, err := computeRestoreFixedVector()
	if err != nil {
		t.Fatal(err)
	}
	if vector.plan.resultRoot != again.plan.resultRoot || vector.candidateID != again.candidateID ||
		!bytes.Equal(vector.candidate, again.candidate) || !reflect.DeepEqual(vector.plan.paths, again.plan.paths) {
		t.Fatalf("restore fixed vector is not deterministic: first=%+v second=%+v", vector, again)
	}
	const wantRoot = "cbdaed3bd32f21da9876622b002c75d8bd986be6e0bfd13c5f8e924146331059"
	const wantCandidate = "f6439d57b0b22e7f068143b9e5c9d8a4bbee9efe97e6d995b48a7fab7a1916d1"
	const wantCandidateData = `{"AuthorUserId":"01234567-89ab-4def-8123-456789abcdef","CreatedAt":"2026-08-09T01:02:03Z","DeviceId":"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee","Message":"restore eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee docs","Parents":["ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"],"Root":"cbdaed3bd32f21da9876622b002c75d8bd986be6e0bfd13c5f8e924146331059","Type":"Commit","Version":1}`
	if vector.plan.resultRoot != wantRoot || vector.candidateID != wantCandidate || string(vector.candidate) != wantCandidateData {
		t.Fatalf("restore fixed vector current=%s source=%s result=%s candidate=%s data=%s",
			vector.currentRoot, vector.sourceRoot, vector.plan.resultRoot, vector.candidateID, vector.candidate)
	}
	if vector.plan.createdCount != 1 || vector.plan.updatedCount != 2 || vector.plan.preservedCurrentOnlyCount != 1 ||
		vector.plan.typeReplacementCount != 0 || vector.plan.removedDescendantCount != 0 {
		t.Fatalf("fixed vector stats=%+v", vector.plan)
	}
	wantChanged := []string{"docs", "docs/new.txt", "docs/old.txt"}
	if vector.plan.changedPathCount != int64(len(wantChanged)) ||
		!equalStrings(vector.plan.changedPaths, wantChanged) || vector.plan.previewTruncated {
		t.Fatalf("fixed vector changed paths=%v count=%d truncated=%v", vector.plan.changedPaths,
			vector.plan.changedPathCount, vector.plan.previewTruncated)
	}
	if len(vector.plan.paths) != 4 || vector.plan.paths[0].path != "docs" ||
		vector.plan.paths[0].mtime != "2026-01-02T00:00:00Z" || vector.plan.paths[1].path != "docs/keep.txt" ||
		vector.plan.paths[2].path != "docs/new.txt" || vector.plan.paths[3].path != "docs/old.txt" {
		t.Fatalf("fixed vector paths=%+v", vector.plan.paths)
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

func TestRestorePlannerRejectsFileReplacingDirectory(t *testing.T) {
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
	if !errors.Is(err, _errRestoreTypeConflict) {
		t.Fatalf("type conflict error=%v, want _errRestoreTypeConflict", err)
	}
	if !reflect.DeepEqual(plan, restorePlan{}) {
		t.Fatalf("rejected type conflict returned plan=%+v", plan)
	}
}

func TestRestorePlannerCreatesCompleteMissingSourceDirectory(t *testing.T) {
	fileData, fileID, err := canonicalFile("docs/root.txt", 1, []string{strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	emptyData, emptyID, err := canonicalDirectory("docs/nested/empty", nil)
	if err != nil {
		t.Fatal(err)
	}
	nestedData, nestedID, err := canonicalDirectory("docs/nested", []scanEntry{
		{name: "empty", kind: "Directory", id: emptyID, modified: "2026-04-01T00:00:01Z"},
		{name: "leaf.txt", kind: "File", id: fileID, modified: "2026-04-01T00:00:02Z"},
	})
	if err != nil {
		t.Fatal(err)
	}
	docsData, docsID, err := canonicalDirectory("docs", []scanEntry{
		{name: "nested", kind: "Directory", id: nestedID, modified: "2026-04-01T00:00:03Z"},
		{name: "root.txt", kind: "File", id: fileID, modified: "2026-04-01T00:00:04Z"},
	})
	if err != nil {
		t.Fatal(err)
	}
	currentRootData, currentRootID, err := canonicalDirectory("", nil)
	if err != nil {
		t.Fatal(err)
	}
	sourceRootData, sourceRootID, err := canonicalDirectory("", []scanEntry{{
		name: "docs", kind: "Directory", id: docsID, modified: "2026-04-01T00:00:05Z",
	}})
	if err != nil {
		t.Fatal(err)
	}
	loader := &restorePlannerTestLoader{objects: map[string][]byte{
		"directories/" + currentRootID: currentRootData,
		"directories/" + sourceRootID:  sourceRootData,
		"directories/" + docsID:        docsData,
		"directories/" + nestedID:      nestedData,
		"directories/" + emptyID:       emptyData,
		"files/" + fileID:              fileData,
	}}

	plan, err := planRestoreOverlay(restorePlanInput{
		CurrentRoot: currentRootID, SourceRoot: sourceRootID, SourcePath: "docs", Load: loader.loadRestoreObject,
	})
	if err != nil {
		t.Fatal(err)
	}
	wantChanged := []string{"docs", "docs/nested", "docs/nested/empty", "docs/nested/leaf.txt", "docs/root.txt"}
	if plan.createdCount != int64(len(wantChanged)) || plan.updatedCount != 0 ||
		plan.preservedCurrentOnlyCount != 0 || plan.typeReplacementCount != 0 || plan.removedDescendantCount != 0 ||
		plan.changedPathCount != int64(len(wantChanged)) || !equalStrings(plan.changedPaths, wantChanged) {
		t.Fatalf("missing directory plan=%+v, want created paths %v", plan, wantChanged)
	}
	if len(plan.paths) != 5 || plan.paths[0].path != "docs" || plan.paths[0].id != docsID ||
		plan.paths[0].mtime != "2026-04-01T00:00:05Z" || plan.paths[1].path != "docs/nested" ||
		plan.paths[1].id != nestedID || plan.paths[1].mtime != "2026-04-01T00:00:03Z" ||
		plan.paths[2].path != "docs/nested/empty" || plan.paths[2].id != emptyID ||
		plan.paths[2].mtime != "2026-04-01T00:00:01Z" || plan.paths[3].path != "docs/nested/leaf.txt" ||
		plan.paths[3].mtime != "2026-04-01T00:00:02Z" || plan.paths[4].path != "docs/root.txt" ||
		plan.paths[4].mtime != "2026-04-01T00:00:04Z" {
		t.Fatalf("missing directory paths=%+v", plan.paths)
	}
}

func TestRestorePlannerNestedDirectoryOverlayPreservesCurrentOnlySubtrees(t *testing.T) {
	fileData, fileID, err := canonicalFile("docs/nested/shared.txt", 1, []string{strings.Repeat("b", 64)})
	if err != nil {
		t.Fatal(err)
	}
	emptyData, emptyID, err := canonicalDirectory("docs/nested/current-empty", nil)
	if err != nil {
		t.Fatal(err)
	}
	currentOnlyData, currentOnlyID, err := canonicalDirectory("docs/nested/current-dir", []scanEntry{{
		name: "deep.txt", kind: "File", id: fileID, modified: "2026-05-01T00:00:01Z",
	}})
	if err != nil {
		t.Fatal(err)
	}
	currentNestedData, currentNestedID, err := canonicalDirectory("docs/nested", []scanEntry{
		{name: "current-dir", kind: "Directory", id: currentOnlyID, modified: "2026-05-01T00:00:02Z"},
		{name: "current-empty", kind: "Directory", id: emptyID, modified: "2026-05-01T00:00:03Z"},
		{name: "current.txt", kind: "File", id: fileID, modified: "2026-05-01T00:00:04Z"},
		{name: "shared.txt", kind: "File", id: fileID, modified: "2026-05-02T00:00:00Z"},
	})
	if err != nil {
		t.Fatal(err)
	}
	sourceNestedData, sourceNestedID, err := canonicalDirectory("docs/nested", []scanEntry{
		{name: "shared.txt", kind: "File", id: fileID, modified: "2026-05-01T00:00:00Z"},
		{name: "source.txt", kind: "File", id: fileID, modified: "2026-05-01T00:00:05Z"},
	})
	if err != nil {
		t.Fatal(err)
	}
	currentDocsData, currentDocsID, err := canonicalDirectory("docs", []scanEntry{{
		name: "nested", kind: "Directory", id: currentNestedID, modified: "2026-05-03T00:00:00Z",
	}})
	if err != nil {
		t.Fatal(err)
	}
	sourceDocsData, sourceDocsID, err := canonicalDirectory("docs", []scanEntry{{
		name: "nested", kind: "Directory", id: sourceNestedID, modified: "2026-05-02T00:00:00Z",
	}})
	if err != nil {
		t.Fatal(err)
	}
	currentRootData, currentRootID, err := canonicalDirectory("", []scanEntry{{
		name: "docs", kind: "Directory", id: currentDocsID, modified: "2026-05-04T00:00:00Z",
	}})
	if err != nil {
		t.Fatal(err)
	}
	sourceRootData, sourceRootID, err := canonicalDirectory("", []scanEntry{{
		name: "docs", kind: "Directory", id: sourceDocsID, modified: "2026-05-03T00:00:00Z",
	}})
	if err != nil {
		t.Fatal(err)
	}
	loader := &restorePlannerTestLoader{objects: map[string][]byte{
		"directories/" + currentRootID:   currentRootData,
		"directories/" + sourceRootID:    sourceRootData,
		"directories/" + currentDocsID:   currentDocsData,
		"directories/" + sourceDocsID:    sourceDocsData,
		"directories/" + currentNestedID: currentNestedData,
		"directories/" + sourceNestedID:  sourceNestedData,
		"directories/" + currentOnlyID:   currentOnlyData,
		"directories/" + emptyID:         emptyData,
		"files/" + fileID:                fileData,
	}}

	plan, err := planRestoreOverlay(restorePlanInput{
		CurrentRoot: currentRootID, SourceRoot: sourceRootID, SourcePath: "docs", Load: loader.loadRestoreObject,
	})
	if err != nil {
		t.Fatal(err)
	}
	wantChanged := []string{"docs", "docs/nested", "docs/nested/shared.txt", "docs/nested/source.txt"}
	if plan.createdCount != 1 || plan.updatedCount != 3 || plan.preservedCurrentOnlyCount != 4 ||
		plan.typeReplacementCount != 0 || plan.removedDescendantCount != 0 ||
		plan.changedPathCount != int64(len(wantChanged)) || !equalStrings(plan.changedPaths, wantChanged) {
		t.Fatalf("nested overlay plan=%+v, want changed %v", plan, wantChanged)
	}
	paths := make(map[string]checkoutPath, len(plan.paths))
	for _, path := range plan.paths {
		paths[path.path] = path
	}
	for _, path := range []string{"docs/nested/current-dir", "docs/nested/current-dir/deep.txt", "docs/nested/current-empty", "docs/nested/current.txt"} {
		if _, ok := paths[path]; !ok {
			t.Fatalf("nested overlay omitted current-only path %q: %+v", path, plan.paths)
		}
	}
	if paths["docs"].mtime != "2026-05-04T00:00:00Z" || paths["docs/nested"].mtime != "2026-05-03T00:00:00Z" ||
		paths["docs/nested/shared.txt"].mtime != "2026-05-01T00:00:00Z" ||
		paths["docs/nested/current-empty"].mtime != "2026-05-01T00:00:03Z" {
		t.Fatalf("nested overlay mtimes=%+v", paths)
	}
}

func TestRestorePlannerRootOverlayIsDeterministicWithoutRootMtime(t *testing.T) {
	fileData, fileID, err := canonicalFile("shared.txt", 1, []string{strings.Repeat("c", 64)})
	if err != nil {
		t.Fatal(err)
	}
	currentData, currentID, err := canonicalDirectory("", []scanEntry{
		{name: "current.txt", kind: "File", id: fileID, modified: "2026-06-01T00:00:00Z"},
		{name: "shared.txt", kind: "File", id: fileID, modified: "2026-06-02T00:00:00Z"},
	})
	if err != nil {
		t.Fatal(err)
	}
	sourceData, sourceID, err := canonicalDirectory("", []scanEntry{
		{name: "shared.txt", kind: "File", id: fileID, modified: "2026-06-01T00:00:00Z"},
		{name: "source.txt", kind: "File", id: fileID, modified: "2026-06-03T00:00:00Z"},
	})
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
	if first.resultRoot != second.resultRoot || !reflect.DeepEqual(first.paths, second.paths) ||
		first.createdCount != second.createdCount || first.updatedCount != second.updatedCount ||
		first.preservedCurrentOnlyCount != second.preservedCurrentOnlyCount ||
		!equalStrings(first.changedPaths, second.changedPaths) {
		t.Fatalf("root overlay is not deterministic: first=%+v second=%+v", first, second)
	}
	if first.createdCount != 1 || first.updatedCount != 1 || first.preservedCurrentOnlyCount != 1 ||
		first.typeReplacementCount != 0 || first.removedDescendantCount != 0 ||
		!equalStrings(first.changedPaths, []string{"shared.txt", "source.txt"}) {
		t.Fatalf("root overlay plan=%+v", first)
	}
	for _, path := range first.changedPaths {
		if path == "." {
			t.Fatal("root overlay exposed protocol-less root mtime path")
		}
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

func TestRestorePlannerRejectsDirectoryReplacingFile(t *testing.T) {
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
		CurrentRoot: currentRootID, SourceRoot: sourceRootID, SourcePath: "target/child.txt", Load: loader.loadRestoreObject,
	})
	if !errors.Is(err, _errRestoreTypeConflict) {
		t.Fatalf("ancestor type conflict error=%v, want _errRestoreTypeConflict", err)
	}
	if !reflect.DeepEqual(plan, restorePlan{}) {
		t.Fatalf("rejected ancestor type conflict returned plan=%+v", plan)
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
