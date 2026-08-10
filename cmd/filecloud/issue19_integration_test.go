package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	libraryapi "github.com/mingming-cn/filecloud/internal/library"
	"github.com/mingming-cn/filecloud/internal/object"
)

func TestLibrarySyncLongFallbackCandidateHistoryFixedReplayVector(t *testing.T) {
	environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
	publisherDir, publisherTree := newClientPaths(t)
	subscriberDir, subscriberTree := newClientPaths(t)
	relative := strings.Repeat("a", 240) + "/" + strings.Repeat("b", 240) + "/" +
		strings.Repeat("c", 240) + "/" + strings.Repeat("d", 240) + "/" + strings.Repeat("a", 13) + "/f"
	publisherPath := filepath.Join(publisherTree, filepath.FromSlash(relative))
	subscriberPath := filepath.Join(subscriberTree, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(publisherPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(publisherPath, []byte("base"), 0o600); err != nil {
		t.Fatal(err)
	}
	baseMtime := time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)
	if err := os.Chtimes(publisherPath, baseMtime, baseMtime); err != nil {
		t.Fatal(err)
	}
	if err := filepath.Walk(publisherTree, func(path string, info os.FileInfo, err error) error {
		if err == nil && info.IsDir() {
			return os.Chtimes(path, baseMtime, baseMtime)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	run := func(args []string, input string, now time.Time) error {
		if len(args) > 0 && args[0] == "library" {
			args = args[1:]
		}
		return runLibraryWithConfig(t.Context(), args, strings.NewReader(input), io.Discard, io.Discard,
			libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, now: func() time.Time { return now }})
	}
	publisherArgs := append(bindArgs(publisherDir, environment.server.URL, testClientLibraryID, publisherTree, testClientDeviceID), "--import-local")
	if err := run(publisherArgs, environment.token+"\n", time.Date(2026, 8, 9, 11, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if err := run(bindArgs(subscriberDir, environment.server.URL, testClientLibraryID, subscriberTree, testOtherDeviceID),
		environment.token+"\n", time.Date(2026, 8, 9, 11, 1, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	fixedSync := func(clientDir, worktree string, now time.Time) error {
		return run([]string{"sync", "--client-dir", clientDir, "--worktree", worktree}, "", now)
	}
	if err := os.WriteFile(subscriberPath, []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	localMtime := time.Date(2026, 8, 9, 10, 1, 0, 0, time.UTC)
	if err := os.Chtimes(subscriberPath, localMtime, localMtime); err != nil {
		t.Fatal(err)
	}
	var replacements []pendingPublication
	advances := 0
	config := libraryClientConfig{checkFilesystem: func(*os.File) error { return nil },
		now: func() time.Time { return time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC) },
		beforeHeadCAS: func() error {
			if advances == 2 {
				return nil
			}
			advances++
			if err := os.WriteFile(publisherPath, []byte(fmt.Sprintf("remote-%d", advances)), 0o600); err != nil {
				return err
			}
			mtime := time.Date(2026, 8, 9, 10, advances+1, 0, 0, time.UTC)
			if err := os.Chtimes(publisherPath, mtime, mtime); err != nil {
				return err
			}
			return fixedSync(publisherDir, publisherTree, time.Date(2026, 8, 9, 11, advances+1, 0, 0, time.UTC))
		},
		afterPendingReplacement: func() error {
			replacements = append(replacements, readTestPendingPublication(t, subscriberDir, subscriberTree))
			if len(replacements) == 2 {
				return errors.New("fixed pending replay seam")
			}
			return nil
		}}
	err := runLibraryWithConfig(t.Context(), []string{"sync", "--client-dir", subscriberDir, "--worktree", subscriberTree},
		strings.NewReader(""), io.Discard, io.Discard, config)
	if err == nil || !strings.Contains(err.Error(), "fixed pending replay seam") || len(replacements) != 2 {
		t.Fatalf("replacement seam error=%v replacements=%d", err, len(replacements))
	}
	const captured = "3d7251058f751e7c068f2fd479beb18ffb1dd8c39254c9bf3cb05e4da1c8557b"
	wantRoots := []string{"db54d9f084bf9e726eff106a1fb4ba76892be6caf37e13e762a49b11726cc930", "6e5d2226217d4d85b756afbbd0dcb0c464f810ed0f39cd81f5f153c7a4369a6d"}
	wantCommits := []string{"bd2a7a4c563532c8e122f5573946fa955d0368c6d1d0e1f2d649140862355eb0", "aa3bca72e2a915d8dd91a909badbd570a3fc80c829c08238d312259586bc62de"}
	for index, pending := range replacements {
		history, err := _decodeCandidateHistory(pending.CandidateHistory)
		if err != nil || pending.CapturedCommit != captured || pending.CandidateRoot != wantRoots[index] ||
			pending.CandidateCommit != wantCommits[index] || len(history) != index {
			t.Fatalf("replacement %d captured=%s root=%s commit=%s history=%d err=%v", index,
				pending.CapturedCommit, pending.CandidateRoot, pending.CandidateCommit, len(history), err)
		}
		if index == 1 && !bytes.Equal(history[0], replacements[0].CandidateData) {
			t.Fatal("second replacement did not preserve the first Candidate bytes")
		}
	}
	persisted := readTestPendingPublication(t, subscriberDir, subscriberTree)
	if persisted.CandidateRoot != wantRoots[1] || persisted.CandidateCommit != wantCommits[1] {
		t.Fatalf("persisted candidate=%s/%s", persisted.CandidateRoot, persisted.CandidateCommit)
	}
	if err := fixedSync(subscriberDir, subscriberTree, time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	const conflictName = "3d7251058f75-f (Filecloud conflict 1)"
	if data, err := os.ReadFile(filepath.Join(subscriberTree, "Filecloud Conflicts", conflictName)); err != nil || string(data) != "local" {
		t.Fatalf("fixed fallback conflict=%q/%v", data, err)
	}
	binding := assertTestConverged(t, environment, subscriberDir, subscriberTree)
	if binding.SyncBase != wantCommits[1] || binding.SyncBaseRoot != wantRoots[1] {
		t.Fatalf("replayed binding=%s/%s", binding.SyncBase, binding.SyncBaseRoot)
	}
	base, err := validateServerURL(environment.server.URL)
	if err != nil {
		t.Fatal(err)
	}
	options := bindOptions{clientDir: subscriberDir, libraryID: testClientLibraryID, base: base,
		token: []byte(environment.token)}
	cacheRoot, err := openVerifiedCacheRoot(subscriberDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cacheRoot.Close()
	options.cacheRoot = cacheRoot
	capturedCommit, err := downloadTargetCommit(t.Context(), options, captured, testClientUserID)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := deriveRemotePaths(t.Context(), options, binding.SyncBaseRoot, false)
	if err != nil {
		t.Fatal(err)
	}
	var promoted checkoutPath
	promotionTarget := _fallbackConflictRoot + "/" + conflictName
	for _, path := range paths {
		if path.path == promotionTarget {
			promoted = path
		}
	}
	promotion := _conflictPromotion{source: relative, target: promotionTarget, id: promoted.id,
		mtime: promoted.mtime, size: promoted.size, namingSeed: captured}
	authorityBinding := binding
	authorityBinding.SyncBase, authorityBinding.SyncBaseRoot = captured, capturedCommit.Root
	measuredBudget := _newReplayBudget()
	if err := _authoritativePromotionReplay(t.Context(), options, authorityBinding, binding.SyncBase,
		[]_conflictPromotion{promotion}, measuredBudget); err != nil {
		t.Fatalf("correct fixed-history promotion authority: %v", err)
	}
	exactLimit := max(measuredBudget.commitFetches, measuredBudget.commitWalks)
	budgetWithLimit := func(limit int) *_replayBudget {
		return &_replayBudget{commitLimit: limit, treeLimit: _mergeMaxObjects, pathLimit: _mergeMaxObjects,
			commits: make(map[string]object.Commit), walked: make(map[string]bool)}
	}
	if err := _authoritativePromotionReplay(t.Context(), options, authorityBinding, binding.SyncBase,
		[]_conflictPromotion{promotion}, budgetWithLimit(exactLimit)); err != nil {
		t.Fatalf("exact promotion authority commit budget %d: %v", exactLimit, err)
	}
	if err := _authoritativePromotionReplay(t.Context(), options, authorityBinding, binding.SyncBase,
		[]_conflictPromotion{promotion}, budgetWithLimit(exactLimit-1)); err == nil {
		t.Fatal("promotion authority accepted a pending frontier beyond its exact commit budget")
	}
	promotion.namingSeed = wantCommits[0]
	if err := _authoritativePromotionReplay(t.Context(), options, authorityBinding, binding.SyncBase,
		[]_conflictPromotion{promotion}, _newReplayBudget()); err == nil {
		t.Fatal("same CandidateHistory alternate legitimate seed was accepted")
	}
	if err := _authoritativePromotionReplay(t.Context(), options, authorityBinding, captured,
		[]_conflictPromotion{promotion}, _newReplayBudget()); err == nil {
		t.Fatal("zero Candidate chain was accepted")
	}
	ambiguousData, ambiguousID, err := canonicalCommit(testClientUserID, testClientDeviceID, capturedCommit.Root,
		[]string{wantCommits[0], captured}, func() time.Time { return time.Date(2026, 8, 9, 12, 1, 0, 0, time.UTC) })
	if err != nil {
		t.Fatal(err)
	}
	if err := putMetadata(t.Context(), base, testClientLibraryID, []byte(environment.token), "commits", ambiguousID, ambiguousData); err != nil {
		t.Fatal(err)
	}
	if err := _authoritativePromotionReplay(t.Context(), options, authorityBinding, ambiguousID,
		[]_conflictPromotion{promotion}, _newReplayBudget()); err == nil {
		t.Fatal("multiple Candidate chains were accepted")
	}
	if err := fixedSync(subscriberDir, subscriberTree, time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
}
