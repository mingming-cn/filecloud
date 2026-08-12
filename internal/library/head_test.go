package library

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mingming-cn/filecloud/internal/object"
	"github.com/mingming-cn/filecloud/internal/storage"
)

const _headLibraryID = "01234567-89ab-4def-8123-456789abcdef"

func TestHeadValidationAdmissionBoundsConcurrentWork(t *testing.T) {
	for _, test := range []struct {
		name             string
		globalCapacity   int
		occupiedLibrary  string
		requestedLibrary string
	}{
		{name: "same library", globalCapacity: 2, occupiedLibrary: _headLibraryID, requestedLibrary: _headLibraryID},
		{name: "global pool", globalCapacity: 1, occupiedLibrary: _headLibraryID, requestedLibrary: "00000000-0000-4000-8000-000000000001"},
	} {
		t.Run(test.name, func(t *testing.T) {
			limiter, err := newHeadValidationLimiter(test.globalCapacity)
			if err != nil {
				t.Fatalf("newHeadValidationLimiter: %v", err)
			}
			handler, store, _ := newTestHandlerWithConfig(t, Config{
				HeadValidation: HeadValidationConfig{GlobalConcurrency: test.globalCapacity},
				headLimiter:    limiter,
			})
			defer closeStore(t, store)
			createHeadLibrary(t, handler)
			if test.requestedLibrary != _headLibraryID {
				assertStatusCode(t, serve(handler, http.MethodPut, "/v1/libraries/"+test.requestedLibrary, `{"Name":"other"}`, _ownerToken), http.StatusCreated, 0)
			}

			release, ok := limiter.tryAcquire(_ownerID, test.occupiedLibrary)
			if !ok {
				t.Fatal("failed to occupy Head validation slot")
			}
			response := headRequest(handler, http.MethodPut, "/v1/libraries/"+test.requestedLibrary+"/head", `{"CommitId":"`+strings.Repeat("a", 64)+`"}`, `"head-version-0"`, "")
			assertStatusCode(t, response, http.StatusTooManyRequests, 4000)
			if response.Header().Get("Retry-After") != "1" {
				t.Fatalf("Retry-After = %q, want 1", response.Header().Get("Retry-After"))
			}
			release()

			secondRelease, ok := limiter.tryAcquire(_ownerID, test.requestedLibrary)
			if !ok {
				t.Fatal("Head validation slot was not released")
			}
			secondRelease()
		})
	}
}

func TestHeadValidationDeadlineReleasesWorkPool(t *testing.T) {
	limiter, err := newHeadValidationLimiter(1)
	if err != nil {
		t.Fatalf("newHeadValidationLimiter: %v", err)
	}
	handler, store, _ := newTestHandlerWithConfig(t, Config{
		HeadValidation: HeadValidationConfig{GlobalConcurrency: 1, RequestTimeout: time.Nanosecond},
		headLimiter:    limiter,
	})
	defer closeStore(t, store)
	createHeadLibrary(t, handler)
	root := putMetadata(t, store, "directories", `{"Entries":[],"Type":"Directory","Version":1}`)
	candidate := putCommit(t, store, _ownerID, nil, root)

	publishHead(t, handler, candidate, `"head-version-0"`, http.StatusServiceUnavailable, 5001)
	assertCurrentHead(t, handler, "", `"head-version-0"`)
	release, ok := limiter.tryAcquire(_ownerID, _headLibraryID)
	if !ok {
		t.Fatal("timed-out Head validation retained its work-pool slot")
	}
	release()
}

func TestHeadHTTPPreconditionsAndConditionalGet(t *testing.T) {
	handler, store, _ := newTestHandler(t)
	defer closeStore(t, store)
	createHeadLibrary(t, handler)
	path := "/v1/libraries/" + _headLibraryID + "/head"

	response := headRequest(handler, http.MethodGet, path, "", "", "")
	assertStatusCode(t, response, 200, 0)
	var envelope struct{ Head headResponse }
	decode(t, response, &envelope)
	if envelope.Head.CommitID != nil || envelope.Head.ETag != `"head-version-0"` || response.Header().Get("ETag") != envelope.Head.ETag {
		t.Fatalf("initial Head = %+v headers=%v", envelope.Head, response.Header())
	}
	response = headRequest(handler, http.MethodGet, path, "", "", `"head-version-0"`)
	if response.Code != http.StatusNotModified || response.Body.Len() != 0 || response.Header().Get("ETag") != `"head-version-0"` {
		t.Fatalf("conditional GET = %d %q headers=%v", response.Code, response.Body.String(), response.Header())
	}
	for _, value := range []string{`"other", "head-version-0"`, `W/"head-version-0"`, `*`} {
		response = headRequest(handler, http.MethodGet, path, "", "", value)
		if response.Code != http.StatusNotModified || response.Body.Len() != 0 || response.Header().Get("ETag") != `"head-version-0"` {
			t.Fatalf("If-None-Match %q = %d %q headers=%v", value, response.Code, response.Body.String(), response.Header())
		}
	}
	response = headRequest(handler, http.MethodGet, path, "", "", `*, "head-version-0"`)
	if response.Code != http.StatusOK {
		t.Fatalf("invalid If-None-Match = %d, want 200", response.Code)
	}

	body := `{"CommitId":"` + strings.Repeat("a", 64) + `"}`
	assertStatusCode(t, headRequest(handler, http.MethodPut, path, body, "", ""), 428, 3006)
	for _, value := range []string{`W/"head-version-0"`, `"head-version-0", "head-version-1"`, "*", "head-version-0", `"head-version-00"`, `"head-version--1"`} {
		assertStatusCode(t, headRequest(handler, http.MethodPut, path, body, value, ""), 400, 1000)
	}
	response = headRequest(handler, http.MethodPut, path, body, `"head-version-1"`, "")
	assertStatusCode(t, response, 412, 3002)
	decode(t, response, &envelope)
	if envelope.Head.CommitID != nil || envelope.Head.ETag != `"head-version-0"` || response.Header().Get("ETag") != envelope.Head.ETag {
		t.Fatalf("conflicting Head = %+v headers=%v", envelope.Head, response.Header())
	}
	for _, invalidBody := range []string{
		`{"CommitId":"` + strings.Repeat("a", 64) + `","Extra":true}`,
		`{"CommitId":"` + strings.Repeat("a", 64) + `","CommitId":"` + strings.Repeat("b", 64) + `"}`,
		`{"CommitId":"invalid"}`,
	} {
		assertStatusCode(t, headRequest(handler, http.MethodPut, path, invalidBody, `"head-version-0"`, ""), 400, 1000)
	}
	assertStatusCode(t, headRequest(handler, http.MethodPut, path, strings.Repeat("x", _maxHeadBody+1), `"head-version-0"`, ""), 413, 3005)

	foreign := headRequestWithToken(handler, http.MethodGet, path, "", "", "", _otherToken)
	missing := headRequestWithToken(handler, http.MethodGet, "/v1/libraries/00000000-0000-4000-8000-000000000001/head", "", "", "", _otherToken)
	assertStatusCode(t, foreign, 404, 2000)
	assertStatusCode(t, missing, 404, 2000)
	if foreign.Body.String() != missing.Body.String() {
		t.Fatalf("foreign and missing Head responses differ: %q vs %q", foreign.Body.String(), missing.Body.String())
	}
}

func TestHeadValidationSharesDeduplicatedObjectBudgetAcrossRoots(t *testing.T) {
	for _, test := range []struct {
		name       string
		uniqueRoot bool
		status     int
		code       int
	}{
		{name: "shared root stays within budget", status: http.StatusOK},
		{name: "unique second-parent root exceeds budget", uniqueRoot: true, status: http.StatusRequestEntityTooLarge, code: 3005},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler, store, _ := newTestHandlerWithConfig(t, Config{HeadValidation: HeadValidationConfig{MaxValidatedObjects: 3}})
			defer closeStore(t, store)
			createHeadLibrary(t, handler)
			root := putMetadata(t, store, "directories", `{"Entries":[],"Type":"Directory","Version":1}`)
			base := putCommit(t, store, _ownerID, nil, root)
			publishHead(t, handler, base, `"head-version-0"`, http.StatusOK, 0)

			branchRoot := root
			if test.uniqueRoot {
				branchRoot = putMetadata(t, store, "directories", `{"Entries":[{"Id":"`+root+`","ModifiedAt":"2026-08-09T00:00:00Z","Name":"child","Type":"Directory"}],"Type":"Directory","Version":1}`)
			}
			branch := putCommit(t, store, _ownerID, []string{base}, branchRoot)
			merge := putCommit(t, store, _ownerID, []string{base, branch}, root)
			publishHead(t, handler, merge, `"head-version-1"`, test.status, test.code)
			if test.status == http.StatusOK {
				assertCurrentHead(t, handler, merge, `"head-version-2"`)
			} else {
				assertCurrentHead(t, handler, base, `"head-version-1"`)
			}
		})
	}
}

func TestHeadValidationCommitBudgetBoundaries(t *testing.T) {
	for _, test := range []struct {
		name        string
		config      HeadValidationConfig
		branchDepth int
		status      int
		code        int
	}{
		{name: "introduced Commit boundary accepted", config: HeadValidationConfig{MaxIntroducedCommits: 2}, branchDepth: 2, status: http.StatusOK},
		{name: "introduced Commit over boundary rejected", config: HeadValidationConfig{MaxIntroducedCommits: 2}, branchDepth: 3, status: http.StatusRequestEntityTooLarge, code: 3005},
		{name: "parent depth boundary accepted", config: HeadValidationConfig{MaxCommitDepth: 2}, branchDepth: 2, status: http.StatusOK},
		{name: "parent depth over boundary rejected", config: HeadValidationConfig{MaxCommitDepth: 2}, branchDepth: 3, status: http.StatusRequestEntityTooLarge, code: 3005},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler, store, _ := newTestHandlerWithConfig(t, Config{HeadValidation: test.config})
			defer closeStore(t, store)
			createHeadLibrary(t, handler)
			root := putMetadata(t, store, "directories", `{"Entries":[],"Type":"Directory","Version":1}`)
			base := putCommit(t, store, _ownerID, nil, root)
			publishHead(t, handler, base, `"head-version-0"`, http.StatusOK, 0)

			branch := base
			for range test.branchDepth {
				branch = putCommit(t, store, _ownerID, []string{branch}, root)
			}
			merge := putCommit(t, store, _ownerID, []string{base, branch}, root)
			publishHead(t, handler, merge, `"head-version-1"`, test.status, test.code)
			if test.status == http.StatusOK {
				assertCurrentHead(t, handler, merge, `"head-version-2"`)
			} else {
				assertCurrentHead(t, handler, base, `"head-version-1"`)
			}
		})
	}
}

func TestHeadValidationConfiguredSnapshotDepthPreservesOldHead(t *testing.T) {
	handler, store, _ := newTestHandlerWithConfig(t, Config{HeadValidation: HeadValidationConfig{MaxSnapshotDepth: 1}})
	defer closeStore(t, store)
	createHeadLibrary(t, handler)
	empty := putMetadata(t, store, "directories", `{"Entries":[],"Type":"Directory","Version":1}`)
	root := putMetadata(t, store, "directories", `{"Entries":[{"Id":"`+empty+`","ModifiedAt":"2026-08-09T00:00:00Z","Name":"child","Type":"Directory"}],"Type":"Directory","Version":1}`)
	candidate := putCommit(t, store, _ownerID, nil, root)

	publishHead(t, handler, candidate, `"head-version-0"`, http.StatusBadRequest, 1000)
	assertCurrentHead(t, handler, "", `"head-version-0"`)
}

func TestDefaultHeadValidationProtocolBudgets(t *testing.T) {
	config := DefaultHeadValidationConfig()
	if config.MaxSnapshotDepth != 256 || config.MaxTraversalContexts != 65_536 || config.MaxCommitDepth != 1024 ||
		config.MaxIntroducedCommits != 1024 || config.MaxValidatedObjects != 2_000_000 {
		t.Fatalf("DefaultHeadValidationConfig() = %+v, want protocol depth and work budgets", config)
	}

	for _, invalid := range []HeadValidationConfig{
		{MaxSnapshotDepth: config.MaxSnapshotDepth + 1},
		{MaxTraversalContexts: config.MaxTraversalContexts + 1},
		{MaxCommitDepth: config.MaxCommitDepth + 1},
		{MaxIntroducedCommits: config.MaxIntroducedCommits + 1},
		{MaxValidatedObjects: config.MaxValidatedObjects + 1},
	} {
		if _, err := normalizeHeadValidationConfig(invalid); err == nil || !strings.Contains(err.Error(), "exceed protocol maximum") {
			t.Fatalf("normalizeHeadValidationConfig(%+v) error = %v, want protocol maximum error", invalid, err)
		}
	}
}

func TestUpdateHeadValidatesMergeAncestryAndSnapshots(t *testing.T) {
	t.Run("complete unpublished branch stops at published ancestor", func(t *testing.T) {
		handler, store, _ := newTestHandler(t)
		defer closeStore(t, store)
		createHeadLibrary(t, handler)
		root := putMetadata(t, store, "directories", `{"Entries":[],"Type":"Directory","Version":1}`)
		base := putCommit(t, store, _ownerID, nil, root)
		publishHead(t, handler, base, `"head-version-0"`, 200, 0)
		main := putCommit(t, store, _ownerID, []string{base}, root)
		publishHead(t, handler, main, `"head-version-1"`, 200, 0)
		branchRoot := putMetadata(t, store, "directories", `{"Entries":[{"Id":"`+root+`","ModifiedAt":"2026-08-09T00:00:00Z","Name":"branch","Type":"Directory"}],"Type":"Directory","Version":1}`)
		branch := putCommit(t, store, _ownerID, []string{base}, branchRoot)
		merge := putCommit(t, store, _ownerID, []string{main, branch}, root)
		publishHead(t, handler, merge, `"head-version-2"`, 200, 0)
		assertCurrentHead(t, handler, merge, `"head-version-3"`)
	})

	for _, test := range []struct {
		name   string
		build  func(*testing.T, *storage.Store, string, string) string
		status int
		code   int
	}{
		{name: "missing second parent", status: 422, code: 3003, build: func(_ *testing.T, _ *storage.Store, _, _ string) string {
			return strings.Repeat("b", 64)
		}},
		{name: "corrupt second parent", status: 422, code: 3004, build: func(t *testing.T, store *storage.Store, base, root string) string {
			id := putCommit(t, store, _ownerID, []string{base}, root)
			path := filepath.Join(store.ObjectsDir(), _ownerID, _headLibraryID, "commits", id[:2], id[2:])
			if err := os.WriteFile(path, []byte(`{"broken":true}`), 0o600); err != nil {
				t.Fatalf("corrupt second parent: %v", err)
			}
			return id
		}},
		{name: "second parent wrong author", status: 400, code: 1000, build: func(t *testing.T, store *storage.Store, base, root string) string {
			return putCommit(t, store, _otherID, []string{base}, root)
		}},
		{name: "missing branch ancestor", status: 422, code: 3003, build: func(t *testing.T, store *storage.Store, _, root string) string {
			return putCommit(t, store, _ownerID, []string{strings.Repeat("c", 64)}, root)
		}},
		{name: "missing branch root", status: 422, code: 3003, build: func(t *testing.T, store *storage.Store, base, _ string) string {
			return putCommit(t, store, _ownerID, []string{base}, strings.Repeat("d", 64))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler, store, _ := newTestHandler(t)
			defer closeStore(t, store)
			createHeadLibrary(t, handler)
			root := putMetadata(t, store, "directories", `{"Entries":[],"Type":"Directory","Version":1}`)
			base := putCommit(t, store, _ownerID, nil, root)
			publishHead(t, handler, base, `"head-version-0"`, 200, 0)
			second := test.build(t, store, base, root)
			merge := putCommit(t, store, _ownerID, []string{base, second}, root)
			publishHead(t, handler, merge, `"head-version-1"`, test.status, test.code)
			assertCurrentHead(t, handler, base, `"head-version-1"`)
		})
	}
}

func TestUpdateHeadStopsAtPersistentPublishedBoundaryAfterRestart(t *testing.T) {
	baseHandler, store, now := newTestHandler(t)
	defer closeStore(t, store)
	createHeadLibrary(t, baseHandler)
	root := putMetadata(t, store, "directories", `{"Entries":[],"Type":"Directory","Version":1}`)

	var head *string
	var oldest string
	for version := int64(0); version < _maxCommitDepth+2; version++ {
		parents := []string(nil)
		if head != nil {
			parents = []string{*head}
		}
		commitID := putCommit(t, store, _ownerID, parents, root)
		if version == 0 {
			oldest = commitID
		}
		updated, err := store.UpdateLibraryHead(t.Context(), _ownerID, _headLibraryID, head, version, commitID, nil, now)
		if err != nil {
			t.Fatalf("publish history version %d: %v", version, err)
		}
		head = updated.HeadCommitID
	}

	// Corrupt an existing ancestor: a single-parent publication must not read it.
	commitPath := filepath.Join(store.ObjectsDir(), _ownerID, _headLibraryID, "commits", oldest[:2], oldest[2:])
	if err := os.WriteFile(commitPath, []byte(`{"broken":true}`), 0o600); err != nil {
		t.Fatalf("corrupt published commit: %v", err)
	}

	handler := newHeadTestHandler(t, store, now, Config{})
	candidate := putCommit(t, store, _ownerID, []string{*head}, root)
	etag := fmt.Sprintf(`"head-version-%d"`, _maxCommitDepth+2)
	publishHead(t, handler, candidate, etag, http.StatusOK, 0)

	branchRoot := putMetadata(t, store, "directories", `{"Entries":[{"Id":"`+root+`","ModifiedAt":"2026-08-09T00:00:00Z","Name":"branch","Type":"Directory"}],"Type":"Directory","Version":1}`)
	branch := putCommit(t, store, _ownerID, []string{oldest}, branchRoot)
	merge := putCommit(t, store, _ownerID, []string{candidate, branch}, root)
	restarted := newHeadTestHandler(t, store, now, Config{})
	publishHead(t, restarted, merge, fmt.Sprintf(`"head-version-%d"`, _maxCommitDepth+3), http.StatusOK, 0)
}

func TestUpdateHeadDoesNotRevalidatePublishedAncestorAcrossMerges(t *testing.T) {
	baseHandler, store, now := newTestHandler(t)
	defer closeStore(t, store)
	createHeadLibrary(t, baseHandler)
	root := putMetadata(t, store, "directories", `{"Entries":[],"Type":"Directory","Version":1}`)
	base := putCommit(t, store, _ownerID, nil, root)
	publishHead(t, baseHandler, base, `"head-version-0"`, 200, 0)
	main := putCommit(t, store, _ownerID, []string{base}, root)
	publishHead(t, baseHandler, main, `"head-version-1"`, 200, 0)

	handler := newHeadTestHandler(t, store, now, Config{})
	branchRoot := putMetadata(t, store, "directories", `{"Entries":[{"Id":"`+root+`","ModifiedAt":"2026-08-09T00:00:00Z","Name":"branch","Type":"Directory"}],"Type":"Directory","Version":1}`)
	branch := putCommit(t, store, _ownerID, []string{base}, branchRoot)
	merge := putCommit(t, store, _ownerID, []string{main, branch}, root)
	publishHead(t, handler, merge, `"head-version-2"`, 200, 0)

	basePath := filepath.Join(store.ObjectsDir(), _ownerID, _headLibraryID, "commits", base[:2], base[2:])
	if err := os.Remove(basePath); err != nil {
		t.Fatalf("remove published ancestor: %v", err)
	}
	secondBranchRoot := putMetadata(t, store, "directories", `{"Entries":[{"Id":"`+root+`","ModifiedAt":"2026-08-09T00:00:00Z","Name":"second","Type":"Directory"}],"Type":"Directory","Version":1}`)
	secondBranch := putCommit(t, store, _ownerID, []string{main}, secondBranchRoot)
	secondMerge := putCommit(t, store, _ownerID, []string{merge, secondBranch}, root)
	publishHead(t, handler, secondMerge, `"head-version-3"`, 200, 0)
}

func TestUpdateHeadCASFailureDoesNotPublishCandidate(t *testing.T) {
	baseHandler, store, now := newTestHandler(t)
	defer closeStore(t, store)
	createHeadLibrary(t, baseHandler)
	root := putMetadata(t, store, "directories", `{"Entries":[],"Type":"Directory","Version":1}`)
	base := putCommit(t, store, _ownerID, nil, root)
	publishHead(t, baseHandler, base, `"head-version-0"`, 200, 0)
	candidateRoot := putMetadata(t, store, "directories", `{"Entries":[{"Id":"`+root+`","ModifiedAt":"2026-08-09T00:00:00Z","Name":"candidate","Type":"Directory"}],"Type":"Directory","Version":1}`)
	rivalRoot := putMetadata(t, store, "directories", `{"Entries":[{"Id":"`+root+`","ModifiedAt":"2026-08-09T00:00:00Z","Name":"rival","Type":"Directory"}],"Type":"Directory","Version":1}`)
	candidate := putCommit(t, store, _ownerID, []string{base}, candidateRoot)
	rival := putCommit(t, store, _ownerID, []string{base}, rivalRoot)

	injected := false
	handler := newHeadTestHandler(t, store, now, Config{BeforeHeadUpdate: func() error {
		if !injected {
			injected = true
			publishHead(t, baseHandler, rival, `"head-version-1"`, 200, 0)
		}
		return nil
	}})
	publishHead(t, handler, candidate, `"head-version-1"`, 412, 3002)

	merge := putCommit(t, store, _ownerID, []string{rival, candidate}, root)
	publishHead(t, handler, merge, `"head-version-2"`, 200, 0)
}

func TestUpdateHeadRejectsPublishedSecondParent(t *testing.T) {
	handler, store, _ := newTestHandler(t)
	defer closeStore(t, store)
	createHeadLibrary(t, handler)
	root := putMetadata(t, store, "directories", `{"Entries":[],"Type":"Directory","Version":1}`)
	base := putCommit(t, store, _ownerID, nil, root)
	publishHead(t, handler, base, `"head-version-0"`, 200, 0)

	duplicate := putCommit(t, store, _ownerID, []string{base, base}, root)
	publishHead(t, handler, duplicate, `"head-version-1"`, 400, 1000)
	main := putCommit(t, store, _ownerID, []string{base}, root)
	publishHead(t, handler, main, `"head-version-1"`, 200, 0)
	ancestor := putCommit(t, store, _ownerID, []string{main, base}, root)
	publishHead(t, handler, ancestor, `"head-version-2"`, 400, 1000)
}

func TestUpdateHeadValidatesEverySharedDirectoryPath(t *testing.T) {
	handler, store, _ := newTestHandler(t)
	defer closeStore(t, store)
	createHeadLibrary(t, handler)
	empty := putMetadata(t, store, "directories", `{"Entries":[],"Type":"Directory","Version":1}`)
	shared := putMetadata(t, store, "directories", `{"Entries":[{"Id":"`+empty+`","ModifiedAt":"2026-08-09T00:00:00Z","Name":"`+strings.Repeat("x", 200)+`","Type":"Directory"}],"Type":"Directory","Version":1}`)
	long := shared
	for range 4 {
		long = putMetadata(t, store, "directories", `{"Entries":[{"Id":"`+long+`","ModifiedAt":"2026-08-09T00:00:00Z","Name":"`+strings.Repeat("y", 220)+`","Type":"Directory"}],"Type":"Directory","Version":1}`)
	}
	root := putMetadata(t, store, "directories", `{"Entries":[{"Id":"`+shared+`","ModifiedAt":"2026-08-09T00:00:00Z","Name":"a","Type":"Directory"},{"Id":"`+long+`","ModifiedAt":"2026-08-09T00:00:00Z","Name":"z","Type":"Directory"}],"Type":"Directory","Version":1}`)
	commitID := putCommit(t, store, _ownerID, nil, root)
	publishHead(t, handler, commitID, `"head-version-0"`, http.StatusBadRequest, 1000)
	assertCurrentHead(t, handler, "", `"head-version-0"`)
}

func TestUpdateHeadValidatesSharedDirectoryDepth(t *testing.T) {
	handler, store, _ := newTestHandler(t)
	defer closeStore(t, store)
	createHeadLibrary(t, handler)
	empty := putMetadata(t, store, "directories", `{"Entries":[],"Type":"Directory","Version":1}`)
	shared := putMetadata(t, store, "directories", `{"Entries":[{"Id":"`+empty+`","ModifiedAt":"2026-08-09T00:00:00Z","Name":"child","Type":"Directory"}],"Type":"Directory","Version":1}`)
	deep := shared
	for range 254 {
		deep = putMetadata(t, store, "directories", `{"Entries":[{"Id":"`+deep+`","ModifiedAt":"2026-08-09T00:00:00Z","Name":"d","Type":"Directory"}],"Type":"Directory","Version":1}`)
	}
	root := putMetadata(t, store, "directories", `{"Entries":[{"Id":"`+shared+`","ModifiedAt":"2026-08-09T00:00:00Z","Name":"a","Type":"Directory"},{"Id":"`+deep+`","ModifiedAt":"2026-08-09T00:00:00Z","Name":"z","Type":"Directory"}],"Type":"Directory","Version":1}`)
	commitID := putCommit(t, store, _ownerID, nil, root)
	publishHead(t, handler, commitID, `"head-version-0"`, http.StatusBadRequest, 1000)
	assertCurrentHead(t, handler, "", `"head-version-0"`)
}

func TestUpdateHeadBoundsFileEntryDepth(t *testing.T) {
	for _, test := range []struct {
		name       string
		wrappers   int
		wantStatus int
		wantCode   int
	}{
		{name: "depth 256 accepted", wrappers: 254, wantStatus: 200},
		{name: "depth 257 rejected", wrappers: 255, wantStatus: 400, wantCode: 1000},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler, store, _ := newTestHandler(t)
			defer closeStore(t, store)
			createHeadLibrary(t, handler)
			fileID := putMetadata(t, store, "files", `{"Blocks":[],"Size":"0","Type":"File","Version":1}`)
			root := putMetadata(t, store, "directories", `{"Entries":[{"Id":"`+fileID+`","ModifiedAt":"2026-08-09T00:00:00Z","Name":"x","Type":"File"}],"Type":"Directory","Version":1}`)
			for range test.wrappers {
				root = putMetadata(t, store, "directories", `{"Entries":[{"Id":"`+root+`","ModifiedAt":"2026-08-09T00:00:00Z","Name":"d","Type":"Directory"}],"Type":"Directory","Version":1}`)
			}
			commitID := putCommit(t, store, _ownerID, nil, root)
			publishHead(t, handler, commitID, `"head-version-0"`, test.wantStatus, test.wantCode)
		})
	}
}

func TestDirectoryAncestorCycleDetection(t *testing.T) {
	path := &directoryPath{id: "child", parent: &directoryPath{id: "root"}}
	if !hasDirectoryAncestor(path, "root") || hasDirectoryAncestor(path, "other") {
		t.Fatal("directory ancestor cycle detection returned the wrong result")
	}
}

func TestUpdateHeadBoundsSharedDirectoryContexts(t *testing.T) {
	handler, store, _ := newTestHandler(t)
	defer closeStore(t, store)
	createHeadLibrary(t, handler)
	child := putMetadata(t, store, "directories", `{"Entries":[],"Type":"Directory","Version":1}`)
	for range 17 {
		child = putMetadata(t, store, "directories", `{"Entries":[{"Id":"`+child+`","ModifiedAt":"2026-08-09T00:00:00Z","Name":"a","Type":"Directory"},{"Id":"`+child+`","ModifiedAt":"2026-08-09T00:00:00Z","Name":"b","Type":"Directory"}],"Type":"Directory","Version":1}`)
	}
	commitID := putCommit(t, store, _ownerID, nil, child)
	publishHead(t, handler, commitID, `"head-version-0"`, http.StatusRequestEntityTooLarge, 3005)
	assertCurrentHead(t, handler, "", `"head-version-0"`)
}

func TestGetHeadIfNoneMatchOverHTTP(t *testing.T) {
	handler, store, _ := newTestHandler(t)
	defer closeStore(t, store)
	createHeadLibrary(t, handler)
	server := httptest.NewServer(handler)
	defer server.Close()

	for _, value := range []string{`"other", W/"head-version-0"`, `*`} {
		request, err := http.NewRequest(http.MethodGet, server.URL+"/v1/libraries/"+_headLibraryID+"/head", nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		request.Header.Set("Authorization", "Bearer "+_ownerToken)
		request.Header.Set("If-None-Match", value)
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatalf("GET Head: %v", err)
		}
		body, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if readErr != nil || closeErr != nil {
			t.Fatalf("read response: %v", errors.Join(readErr, closeErr))
		}
		if response.StatusCode != http.StatusNotModified || len(body) != 0 || response.Header.Get("ETag") != `"head-version-0"` {
			t.Fatalf("If-None-Match %q = %d %q headers=%v", value, response.StatusCode, body, response.Header)
		}
	}
}

func TestUpdateHeadPublishesOnlyCompleteValidSnapshot(t *testing.T) {
	handler, store, _ := newTestHandler(t)
	defer closeStore(t, store)
	createHeadLibrary(t, handler)

	emptyRoot := putMetadata(t, store, "directories", `{"Entries":[],"Type":"Directory","Version":1}`)
	initial := putCommit(t, store, _ownerID, nil, emptyRoot)
	publishHead(t, handler, initial, `"head-version-0"`, 200, 0)
	assertCurrentHead(t, handler, initial, `"head-version-1"`)

	block := []byte("x")
	blockID := object.ID(block)
	if _, err := store.PutObjectSized(t.Context(), _ownerID, _headLibraryID, "blocks", blockID, bytes.NewReader(block), 1); err != nil {
		t.Fatalf("PutObjectSized: %v", err)
	}
	fileID := putMetadata(t, store, "files", `{"Blocks":["`+blockID+`"],"Size":"1","Type":"File","Version":1}`)
	root := putMetadata(t, store, "directories", `{"Entries":[{"Id":"`+fileID+`","ModifiedAt":"2026-08-09T00:00:00Z","Name":"x.txt","Type":"File"}],"Type":"Directory","Version":1}`)
	second := putCommit(t, store, _ownerID, []string{initial}, root)
	publishHead(t, handler, second, `"head-version-1"`, 200, 0)
	assertCurrentHead(t, handler, second, `"head-version-2"`)

	// A stale ETag loses even when it repeats the already-published CommitId.
	publishHead(t, handler, second, `"head-version-1"`, 412, 3002)
	assertCurrentHead(t, handler, second, `"head-version-2"`)
}

func TestUpdateHeadRejectsAuthorParentMissingAndCorruptObjects(t *testing.T) {
	tests := []struct {
		name  string
		build func(*testing.T, *storage.Store, string) string
		code  int
	}{
		{name: "wrong author", code: 1000, build: func(t *testing.T, store *storage.Store, root string) string {
			return putCommit(t, store, _otherID, nil, root)
		}},
		{name: "wrong initial parent", code: 1000, build: func(t *testing.T, store *storage.Store, root string) string {
			return putCommit(t, store, _ownerID, []string{strings.Repeat("a", 64)}, root)
		}},
		{name: "missing root", code: 3003, build: func(t *testing.T, store *storage.Store, _ string) string {
			return putCommit(t, store, _ownerID, nil, strings.Repeat("b", 64))
		}},
		{name: "short non-tail block", code: 3004, build: func(t *testing.T, store *storage.Store, _ string) string {
			ids := make([]string, 2)
			for index, data := range [][]byte{{'a'}, {'b'}} {
				ids[index] = object.ID(data)
				if _, err := store.PutObjectSized(t.Context(), _ownerID, _headLibraryID, "blocks", ids[index], bytes.NewReader(data), 1); err != nil {
					t.Fatalf("PutObjectSized: %v", err)
				}
			}
			fileID := putMetadata(t, store, "files", fmt.Sprintf(`{"Blocks":["%s","%s"],"Size":"4194305","Type":"File","Version":1}`, ids[0], ids[1]))
			root := putMetadata(t, store, "directories", `{"Entries":[{"Id":"`+fileID+`","ModifiedAt":"2026-08-09T00:00:00Z","Name":"x","Type":"File"}],"Type":"Directory","Version":1}`)
			return putCommit(t, store, _ownerID, nil, root)
		}},
		{name: "corrupt directory", code: 3004, build: func(t *testing.T, store *storage.Store, root string) string {
			path := filepath.Join(store.ObjectsDir(), _ownerID, _headLibraryID, "directories", root[:2], root[2:])
			if err := os.WriteFile(path, []byte(`{"Entries":[],"Type":"File","Version":1}`), 0o600); err != nil {
				t.Fatalf("corrupt directory: %v", err)
			}
			return putCommit(t, store, _ownerID, nil, root)
		}},
		{name: "path over limit", code: 1000, build: func(t *testing.T, store *storage.Store, root string) string {
			child := root
			for range 5 {
				child = putMetadata(t, store, "directories", `{"Entries":[{"Id":"`+child+`","ModifiedAt":"2026-08-09T00:00:00Z","Name":"`+strings.Repeat("a", 240)+`","Type":"Directory"}],"Type":"Directory","Version":1}`)
			}
			return putCommit(t, store, _ownerID, nil, child)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler, store, _ := newTestHandler(t)
			defer closeStore(t, store)
			createHeadLibrary(t, handler)
			root := putMetadata(t, store, "directories", `{"Entries":[],"Type":"Directory","Version":1}`)
			commitID := test.build(t, store, root)
			status := http.StatusBadRequest
			if test.code == 3003 || test.code == 3004 {
				status = http.StatusUnprocessableEntity
			}
			publishHead(t, handler, commitID, `"head-version-0"`, status, test.code)
			assertCurrentHead(t, handler, "", `"head-version-0"`)
		})
	}
}

func TestUpdateHeadCapsMissingObjectResponse(t *testing.T) {
	handler, store, _ := newTestHandler(t)
	defer closeStore(t, store)
	createHeadLibrary(t, handler)

	entries := make([]map[string]string, 101)
	for index := range entries {
		entries[index] = map[string]string{
			"Id": object.ID([]byte(fmt.Sprintf("missing-%03d", index))), "ModifiedAt": "2026-08-09T00:00:00Z",
			"Name": fmt.Sprintf("%03d", index), "Type": "File",
		}
	}
	directoryBytes, err := json.Marshal(struct {
		Entries []map[string]string
		Type    string
		Version int
	}{Entries: entries, Type: "Directory", Version: 1})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	root := putMetadata(t, store, "directories", string(directoryBytes))
	commitID := putCommit(t, store, _ownerID, nil, root)
	response := publishHead(t, handler, commitID, `"head-version-0"`, 422, 3003)
	var envelope struct {
		MissingObjects []string
		Truncated      bool
	}
	decode(t, response, &envelope)
	if len(envelope.MissingObjects) != 100 || !envelope.Truncated {
		t.Fatalf("missing response = %d truncated=%v", len(envelope.MissingObjects), envelope.Truncated)
	}
	assertCurrentHead(t, handler, "", `"head-version-0"`)
}

func TestHeadUpdateFaultPointsPreserveReadableInvariant(t *testing.T) {
	for _, point := range []string{"before", "after"} {
		t.Run(point, func(t *testing.T) {
			dataDir, oldCommit, candidate := newHeadCrashFixture(t)
			command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestHeadUpdateCrashHelper$")
			command.Env = append(os.Environ(),
				"FILECLOUD_HEAD_CRASH_DATA_DIR="+dataDir,
				"FILECLOUD_HEAD_CRASH_POINT="+point,
				"FILECLOUD_HEAD_CRASH_CANDIDATE="+candidate,
			)
			assertHeadProcessSIGKILL(t, command.Run())

			store, err := storage.OpenForServe(t.Context(), dataDir)
			if err != nil {
				t.Fatalf("reopen Head store after %s: %v", point, err)
			}
			defer closeStore(t, store)
			handler := newHeadTestHandler(t, store, time.Now(), Config{})
			if point == "before" {
				assertCurrentHead(t, handler, oldCommit, `"head-version-1"`)
				assertHeadGraphReadable(t, store, oldCommit)
				return
			}
			assertCurrentHead(t, handler, candidate, `"head-version-2"`)
			assertHeadGraphReadable(t, store, candidate)
			assertHeadGraphReadable(t, store, oldCommit)
		})
	}
}

func TestHeadUpdateCrashHelper(t *testing.T) {
	dataDir := os.Getenv("FILECLOUD_HEAD_CRASH_DATA_DIR")
	point := os.Getenv("FILECLOUD_HEAD_CRASH_POINT")
	candidate := os.Getenv("FILECLOUD_HEAD_CRASH_CANDIDATE")
	if dataDir == "" || point == "" || candidate == "" {
		t.Skip("Head update crash subprocess helper")
	}
	store, err := storage.OpenForServe(t.Context(), dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	kill := func() error {
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		return nil
	}
	config := Config{}
	if point == "before" {
		config.BeforeHeadUpdate = kill
	} else {
		config.AfterHeadUpdate = kill
	}
	handler := newHeadTestHandler(t, store, time.Now(), config)
	publishHead(t, handler, candidate, `"head-version-1"`, http.StatusOK, 0)
	t.Fatalf("Head update did not reach crash point %q", point)
}

func newHeadCrashFixture(t *testing.T) (string, string, string) {
	t.Helper()
	root := os.Getenv("FILECLOUD_LINUX_EXT4_ROOT")
	if root == "" {
		root = t.TempDir()
	}
	dataDir, err := os.MkdirTemp(root, ".linux-ext4-head-store-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dataDir); err != nil {
			t.Errorf("remove Head crash store: %v", err)
		}
	})
	if err := storage.Init(t.Context(), dataDir); err != nil {
		t.Fatal(err)
	}
	store, err := storage.OpenForServe(t.Context(), dataDir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := store.CreateUser(t.Context(), storage.User{ID: _ownerID, Username: "alice", PasswordHash: "hash"}, now); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(_ownerToken))
	if err := store.CreateSession(t.Context(), _ownerID, digest, "crash", now, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	handler := newHeadTestHandler(t, store, now, Config{})
	createHeadLibrary(t, handler)
	block := []byte("old Head confirmed content")
	blockID := object.ID(block)
	if _, err := store.PutObject(t.Context(), _ownerID, _headLibraryID, "blocks", blockID, bytes.NewReader(block)); err != nil {
		t.Fatal(err)
	}
	file := putMetadata(t, store, "files", fmt.Sprintf(`{"Blocks":["%s"],"Size":"%d","Type":"File","Version":1}`, blockID, len(block)))
	oldRoot := putMetadata(t, store, "directories", fmt.Sprintf(`{"Entries":[{"Id":"%s","ModifiedAt":"2026-08-09T00:00:00Z","Name":"old.txt","Type":"File"}],"Type":"Directory","Version":1}`, file))
	oldCommit := putCommit(t, store, _ownerID, nil, oldRoot)
	publishHead(t, handler, oldCommit, `"head-version-0"`, http.StatusOK, 0)
	candidateRoot := putMetadata(t, store, "directories", fmt.Sprintf(`{"Entries":[{"Id":"%s","ModifiedAt":"2026-08-09T00:00:00Z","Name":"renamed.txt","Type":"File"}],"Type":"Directory","Version":1}`, file))
	candidate := putCommit(t, store, _ownerID, []string{oldCommit}, candidateRoot)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return dataDir, oldCommit, candidate
}

func assertHeadGraphReadable(t *testing.T, store *storage.Store, commitID string) {
	t.Helper()
	commitData := readHeadObject(t, store, "commits", commitID)
	commit, err := object.VerifyCommit(commitData, commitID)
	if err != nil {
		t.Fatalf("verify Head commit %s: %v", commitID, err)
	}
	directoryData := readHeadObject(t, store, "directories", commit.Root)
	directory, err := object.VerifyDirectory(directoryData, commit.Root)
	if err != nil || len(directory.Entries) != 1 {
		t.Fatalf("verify Head directory %s: %+v, %v", commit.Root, directory, err)
	}
	fileData := readHeadObject(t, store, "files", directory.Entries[0].ID)
	file, err := object.VerifyFile(fileData, directory.Entries[0].ID)
	if err != nil || len(file.Blocks) != 1 {
		t.Fatalf("verify Head file %s: %+v, %v", directory.Entries[0].ID, file, err)
	}
	block := readHeadObject(t, store, "blocks", file.Blocks[0])
	if object.ID(block) != file.Blocks[0] || int64(len(block)) != file.Size {
		t.Fatalf("verify Head block %s size=%d want=%d", file.Blocks[0], len(block), file.Size)
	}
}

func readHeadObject(t *testing.T, store *storage.Store, kind, id string) []byte {
	t.Helper()
	reader, _, err := store.GetObject(t.Context(), _ownerID, _headLibraryID, kind, id)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read Head object %s/%s: read=%v close=%v", kind, id, readErr, closeErr)
	}
	return data
}

func assertHeadProcessSIGKILL(t *testing.T, err error) {
	t.Helper()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("Head process was not killed: %v", err)
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("Head process status=%v err=%v", status, err)
	}
}

func newHeadTestHandler(t *testing.T, store *storage.Store, now time.Time, config Config) http.Handler {
	t.Helper()
	config.Now = func() time.Time { return now }
	config.PageTokenKey = bytes.Repeat([]byte{8}, 32)
	handler, err := NewHandler(store, log.New(io.Discard, "", 0), config)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return handler
}

func createHeadLibrary(t *testing.T, handler http.Handler) {
	t.Helper()
	assertStatusCode(t, serve(handler, http.MethodPut, "/v1/libraries/"+_headLibraryID, `{"Name":"head"}`, _ownerToken), 201, 0)
}

func putMetadata(t *testing.T, store *storage.Store, kind, input string) string {
	t.Helper()
	canonical, id, err := object.Canonicalize(kind, []byte(input))
	if err != nil {
		t.Fatalf("Canonicalize(%s): %v", kind, err)
	}
	if _, err := store.PutObject(t.Context(), _ownerID, _headLibraryID, kind, id, bytes.NewReader(canonical)); err != nil {
		t.Fatalf("PutObject(%s): %v", kind, err)
	}
	return id
}

func putCommit(t *testing.T, store *storage.Store, author string, parents []string, root string) string {
	t.Helper()
	if parents == nil {
		parents = []string{}
	}
	parentBytes, err := json.Marshal(parents)
	if err != nil {
		t.Fatalf("Marshal parents: %v", err)
	}
	return putMetadata(t, store, "commits", fmt.Sprintf(`{"AuthorUserId":"%s","CreatedAt":"2026-08-09T00:00:00Z","DeviceId":"01234567-89ab-4def-8123-456789abcdef","Message":"sync","Parents":%s,"Root":"%s","Type":"Commit","Version":1}`, author, parentBytes, root))
}

func publishHead(t *testing.T, handler http.Handler, commitID, etag string, status, code int) *httptest.ResponseRecorder {
	t.Helper()
	response := headRequest(handler, http.MethodPut, "/v1/libraries/"+_headLibraryID+"/head", `{"CommitId":"`+commitID+`"}`, etag, "")
	assertStatusCode(t, response, status, code)
	return response
}

func assertCurrentHead(t *testing.T, handler http.Handler, commitID, etag string) {
	t.Helper()
	response := headRequest(handler, http.MethodGet, "/v1/libraries/"+_headLibraryID+"/head", "", "", "")
	assertStatusCode(t, response, 200, 0)
	var envelope struct{ Head headResponse }
	decode(t, response, &envelope)
	if envelope.Head.ETag != etag || (commitID == "" && envelope.Head.CommitID != nil) || (commitID != "" && (envelope.Head.CommitID == nil || *envelope.Head.CommitID != commitID)) {
		t.Fatalf("current Head = %+v, want commit=%q etag=%q", envelope.Head, commitID, etag)
	}
}

func headRequest(handler http.Handler, method, path, body, ifMatch, ifNoneMatch string) *httptest.ResponseRecorder {
	return headRequestWithToken(handler, method, path, body, ifMatch, ifNoneMatch, _ownerToken)
}

func headRequestWithToken(handler http.Handler, method, path, body, ifMatch, ifNoneMatch, token string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	if ifMatch != "" {
		request.Header.Set("If-Match", ifMatch)
	}
	if ifNoneMatch != "" {
		request.Header.Set("If-None-Match", ifNoneMatch)
	}
	handler.ServeHTTP(response, request)
	return response
}
