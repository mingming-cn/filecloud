package library

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestListHistoryUsesHeadFirstParentOrderAndStableCursor(t *testing.T) {
	handler, store, _ := newTestHandler(t)
	defer closeStore(t, store)
	createHeadLibrary(t, handler)
	root := putMetadata(t, store, "directories", `{"Entries":[],"Type":"Directory","Version":1}`)
	first := putCommit(t, store, _ownerID, nil, root)
	publishHead(t, handler, first, `"head-version-0"`, http.StatusOK, 0)
	second := putCommit(t, store, _ownerID, []string{first}, root)
	publishHead(t, handler, second, `"head-version-1"`, http.StatusOK, 0)
	third := putCommit(t, store, _ownerID, []string{second}, root)
	publishHead(t, handler, third, `"head-version-2"`, http.StatusOK, 0)

	response := serve(handler, http.MethodGet, "/v1/libraries/"+_headLibraryID+"/history?PageSize=2", "", _ownerToken)
	assertStatusCode(t, response, http.StatusOK, 0)
	var firstPage struct {
		History struct {
			AnchorCommitID *string
			Commits        []historyCommitResponse
			NextPageToken  string
		}
	}
	decode(t, response, &firstPage)
	if firstPage.History.AnchorCommitID == nil || *firstPage.History.AnchorCommitID != third || len(firstPage.History.Commits) != 2 ||
		firstPage.History.Commits[0].CommitID != third || firstPage.History.Commits[1].CommitID != second || firstPage.History.NextPageToken == "" {
		t.Fatalf("first history page = %+v", firstPage.History)
	}
	if firstPage.History.Commits[0].Parents == nil {
		t.Fatal("history commit parents were null")
	}

	response = serve(handler, http.MethodGet, "/v1/libraries/"+_headLibraryID+"/history?PageSize=2&PageToken="+firstPage.History.NextPageToken, "", _ownerToken)
	assertStatusCode(t, response, http.StatusOK, 0)
	var secondPage struct {
		History struct {
			AnchorCommitID *string
			Commits        []historyCommitResponse
			NextPageToken  string
		}
	}
	decode(t, response, &secondPage)
	if secondPage.History.AnchorCommitID == nil || *secondPage.History.AnchorCommitID != third || len(secondPage.History.Commits) != 1 ||
		secondPage.History.Commits[0].CommitID != first || secondPage.History.NextPageToken != "" {
		t.Fatalf("second history page = %+v", secondPage.History)
	}
}

func TestListHistoryIgnoresCommitCreatedAtForOrdering(t *testing.T) {
	handler, store, _ := newTestHandler(t)
	defer closeStore(t, store)
	createHeadLibrary(t, handler)
	root := putMetadata(t, store, "directories", `{"Entries":[],"Type":"Directory","Version":1}`)
	putAt := func(createdAt string, parents []string) string {
		parentBytes := "[]"
		if len(parents) != 0 {
			parentBytes = fmt.Sprintf(`[%q]`, parents[0])
		}
		return putMetadata(t, store, "commits", fmt.Sprintf(`{"AuthorUserId":%q,"CreatedAt":%q,"DeviceId":"01234567-89ab-4def-8123-456789abcdef","Message":"sync","Parents":%s,"Root":%q,"Type":"Commit","Version":1}`,
			_ownerID, createdAt, parentBytes, root))
	}
	first := putAt("2026-08-09T03:00:00Z", nil)
	publishHead(t, handler, first, `"head-version-0"`, http.StatusOK, 0)
	second := putAt("2026-08-09T01:00:00Z", []string{first})
	publishHead(t, handler, second, `"head-version-1"`, http.StatusOK, 0)
	third := putAt("2026-08-09T01:00:00Z", []string{second})
	publishHead(t, handler, third, `"head-version-2"`, http.StatusOK, 0)

	response := serve(handler, http.MethodGet, "/v1/libraries/"+_headLibraryID+"/history", "", _ownerToken)
	assertStatusCode(t, response, http.StatusOK, 0)
	var page struct{ History historyListResponse }
	decode(t, response, &page)
	if len(page.History.Commits) != 3 || page.History.Commits[0].CommitID != third ||
		page.History.Commits[1].CommitID != second || page.History.Commits[2].CommitID != first {
		t.Fatalf("history order = %+v, want first-parent order", page.History.Commits)
	}
}

func TestListHistoryTokenStaysAnchoredAfterNewHead(t *testing.T) {
	handler, store, _ := newTestHandler(t)
	defer closeStore(t, store)
	createHeadLibrary(t, handler)
	root := putMetadata(t, store, "directories", `{"Entries":[],"Type":"Directory","Version":1}`)
	first := putCommit(t, store, _ownerID, nil, root)
	publishHead(t, handler, first, `"head-version-0"`, http.StatusOK, 0)
	second := putCommit(t, store, _ownerID, []string{first}, root)
	publishHead(t, handler, second, `"head-version-1"`, http.StatusOK, 0)
	third := putCommit(t, store, _ownerID, []string{second}, root)
	publishHead(t, handler, third, `"head-version-2"`, http.StatusOK, 0)

	response := serve(handler, http.MethodGet, "/v1/libraries/"+_headLibraryID+"/history?PageSize=1", "", _ownerToken)
	var page struct {
		History struct {
			NextPageToken string
			Commits       []historyCommitResponse
		}
	}
	decode(t, response, &page)
	fourth := putCommit(t, store, _ownerID, []string{third}, root)
	publishHead(t, handler, fourth, `"head-version-3"`, http.StatusOK, 0)

	response = serve(handler, http.MethodGet, "/v1/libraries/"+_headLibraryID+"/history?PageSize=1&PageToken="+page.History.NextPageToken, "", _ownerToken)
	assertStatusCode(t, response, http.StatusOK, 0)
	decode(t, response, &page)
	if len(page.History.Commits) != 1 || page.History.Commits[0].CommitID != second {
		t.Fatalf("anchored page = %+v", page.History)
	}
	response = serve(handler, http.MethodGet, "/v1/libraries/"+_headLibraryID+"/history?PageSize=1", "", _ownerToken)
	assertStatusCode(t, response, http.StatusOK, 0)
	decode(t, response, &page)
	if len(page.History.Commits) != 1 || page.History.Commits[0].CommitID != fourth {
		t.Fatalf("refreshed page = %+v", page.History)
	}
}

func TestListHistoryEmptyHeadUsesNonNullCommitArray(t *testing.T) {
	handler, store, _ := newTestHandler(t)
	defer closeStore(t, store)
	createHeadLibrary(t, handler)
	response := serve(handler, http.MethodGet, "/v1/libraries/"+_headLibraryID+"/history", "", _ownerToken)
	assertStatusCode(t, response, http.StatusOK, 0)
	var envelope struct {
		History struct {
			AnchorCommitID *string
			Commits        []historyCommitResponse
			NextPageToken  string
		}
	}
	decode(t, response, &envelope)
	if envelope.History.AnchorCommitID != nil || envelope.History.Commits == nil || len(envelope.History.Commits) != 0 || envelope.History.NextPageToken != "" {
		t.Fatalf("empty history = %+v", envelope.History)
	}
}

func TestListHistoryValidationAndAuthenticationOrder(t *testing.T) {
	handler, store, _ := newTestHandler(t)
	defer closeStore(t, store)
	createHeadLibrary(t, handler)
	unauthenticated := serve(handler, http.MethodGet, "/v1/libraries/not-a-uuid/history?Unknown=x", "", "")
	assertStatusCode(t, unauthenticated, http.StatusUnauthorized, 1001)
	invalid := serve(handler, http.MethodGet, "/v1/libraries/not-a-uuid/history?Unknown=x", "", _ownerToken)
	assertStatusCode(t, invalid, http.StatusBadRequest, 1000)
	foreign := serve(handler, http.MethodGet, "/v1/libraries/22345678-9abc-4def-8123-456789abcdef/history", "", _otherToken)
	assertStatusCode(t, foreign, http.StatusNotFound, 2000)
	tooLong := serve(handler, http.MethodGet, "/v1/libraries/"+_headLibraryID+"/history?PageToken="+strings.Repeat("a", _maxHistoryTokenSize+1), "", _ownerToken)
	assertStatusCode(t, tooLong, http.StatusBadRequest, 1000)
}

func TestListHistoryRejectsExpiredAndCrossBoundTokens(t *testing.T) {
	current := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	handler, store, _ := newTestHandlerWithConfig(t, Config{Now: func() time.Time { return current }})
	defer closeStore(t, store)
	createHeadLibrary(t, handler)
	root := putMetadata(t, store, "directories", `{"Entries":[],"Type":"Directory","Version":1}`)
	first := putCommit(t, store, _ownerID, nil, root)
	publishHead(t, handler, first, `"head-version-0"`, http.StatusOK, 0)
	second := putCommit(t, store, _ownerID, []string{first}, root)
	publishHead(t, handler, second, `"head-version-1"`, http.StatusOK, 0)

	response := serve(handler, http.MethodGet, "/v1/libraries/"+_headLibraryID+"/history?PageSize=1", "", _ownerToken)
	assertStatusCode(t, response, http.StatusOK, 0)
	var page struct {
		History struct {
			NextPageToken string
		}
	}
	decode(t, response, &page)
	if page.History.NextPageToken == "" {
		t.Fatal("history page did not return a continuation token")
	}
	token := page.History.NextPageToken

	assertStatusCode(t, serve(handler, http.MethodGet, "/v1/libraries/"+_headLibraryID+"/history?PageToken="+token, "", _otherToken), http.StatusBadRequest, 1000)
	assertStatusCode(t, serve(handler, http.MethodGet, "/v1/libraries/22345678-9abc-4def-8123-456789abcdef/history?PageToken="+token, "", _ownerToken), http.StatusBadRequest, 1000)
	assertStatusCode(t, serve(handler, http.MethodGet, "/v1/libraries?PageToken="+token, "", _ownerToken), http.StatusBadRequest, 1000)
	assertStatusCode(t, serve(handler, http.MethodGet, "/v1/libraries/"+_headLibraryID+"/history?PageToken="+strings.Repeat("a", _maxHistoryTokenSize), "", _ownerToken), http.StatusBadRequest, 1000)
	assertStatusCode(t, serve(handler, http.MethodGet, "/v1/libraries/"+_headLibraryID+"/history?PageToken="+strings.Repeat("a", _maxHistoryTokenSize+1), "", _ownerToken), http.StatusBadRequest, 1000)

	current = current.Add(_pageTokenLifetime + time.Nanosecond)
	assertStatusCode(t, serve(handler, http.MethodGet, "/v1/libraries/"+_headLibraryID+"/history?PageToken="+token, "", _ownerToken), http.StatusBadRequest, 1000)
}

func TestListHistoryLimiterRejectsBeforeLibraryLookup(t *testing.T) {
	limiter, err := newHistoryLimiter(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	handler, store, _ := newTestHandlerWithConfig(t, Config{historyLimiter: limiter})
	defer closeStore(t, store)
	release, ok := limiter.tryAcquire(_ownerID)
	if !ok {
		t.Fatal("occupy history limiter")
	}
	missingLibrary := "32345678-9abc-4def-8123-456789abcdef"
	response := serve(handler, http.MethodGet, "/v1/libraries/"+missingLibrary+"/history", "", _ownerToken)
	assertStatusCode(t, response, http.StatusTooManyRequests, 4000)
	release()
	response = serve(handler, http.MethodGet, "/v1/libraries/"+missingLibrary+"/history", "", _ownerToken)
	assertStatusCode(t, response, http.StatusNotFound, 2000)
}

func TestListHistoryLimiterReleasesOnDeadline(t *testing.T) {
	limiter, err := newHistoryLimiter(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	handler, store, _ := newTestHandlerWithConfig(t, Config{
		History:        HistoryConfig{GlobalConcurrency: 1, UserConcurrency: 1, RequestTimeout: time.Nanosecond},
		historyLimiter: limiter,
		afterHistoryAdmit: func(ctx context.Context) {
			<-ctx.Done()
		},
	})
	defer closeStore(t, store)
	createHeadLibrary(t, handler)
	root := putMetadata(t, store, "directories", `{"Entries":[],"Type":"Directory","Version":1}`)
	first := putCommit(t, store, _ownerID, nil, root)
	publishHead(t, handler, first, `"head-version-0"`, http.StatusOK, 0)
	response := serve(handler, http.MethodGet, "/v1/libraries/"+_headLibraryID+"/history", "", _ownerToken)
	assertStatusCode(t, response, http.StatusServiceUnavailable, 5001)
	release, ok := limiter.tryAcquire(_ownerID)
	if !ok {
		t.Fatal("history limiter slot was not released")
	}
	release()
}

func TestListHistoryCorruptionLogMasksCommitAndRequestsIntegrityCheck(t *testing.T) {
	handler, store, now := newTestHandler(t)
	defer closeStore(t, store)
	createHeadLibrary(t, handler)
	root := putMetadata(t, store, "directories", `{"Entries":[],"Type":"Directory","Version":1}`)
	commitID := putCommit(t, store, _ownerID, nil, root)
	publishHead(t, handler, commitID, `"head-version-0"`, http.StatusOK, 0)
	objectPath := filepath.Join(store.ObjectsDir(), _ownerID, _headLibraryID, "commits", commitID[:2], commitID[2:])
	if err := os.Remove(objectPath); err != nil {
		t.Fatalf("remove published Commit: %v", err)
	}
	var logs bytes.Buffer
	loggingHandler, err := NewHandler(store, log.New(&logs, "", 0), Config{
		Now:          func() time.Time { return now },
		PageTokenKey: bytes.Repeat([]byte{7}, 32),
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	response := serve(loggingHandler, http.MethodGet, "/v1/libraries/"+_headLibraryID+"/history", "", _ownerToken)
	assertStatusCode(t, response, http.StatusInternalServerError, 5000)
	logOutput := logs.String()
	if !strings.Contains(logOutput, "history_commit_"+commitID[:8]+"_run_integrity_check") || strings.Contains(logOutput, commitID) {
		t.Fatalf("corrupt history log = %q, want masked identity and integrity guidance", logOutput)
	}
}

func TestGetHistoryCommitReturnsPublishedRolesAndImmutableMetadata(t *testing.T) {
	handler, store, _ := newTestHandler(t)
	defer closeStore(t, store)
	createHeadLibrary(t, handler)
	root := putMetadata(t, store, "directories", `{"Entries":[],"Type":"Directory","Version":1}`)
	base := putCommit(t, store, _ownerID, nil, root)
	publishHead(t, handler, base, `"head-version-0"`, http.StatusOK, 0)
	captured := putCommit(t, store, _ownerID, []string{base}, root)
	merged := putCommit(t, store, _ownerID, []string{base, captured}, root)
	publishHead(t, handler, merged, `"head-version-1"`, http.StatusOK, 0)

	for _, test := range []struct {
		name       string
		commitID   string
		role       string
		mainlineID string
		parents    []string
	}{
		{name: "mainline", commitID: merged, role: "mainline", mainlineID: merged, parents: []string{base, captured}},
		{name: "merge source", commitID: captured, role: "merge-source", mainlineID: merged, parents: []string{base}},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := serve(handler, http.MethodGet, "/v1/libraries/"+_headLibraryID+"/history/"+test.commitID, "", _ownerToken)
			assertStatusCode(t, response, http.StatusOK, 0)
			if got := response.Header().Get("ETag"); got != `"`+test.commitID+`"` {
				t.Fatalf("history Commit ETag = %q", got)
			}
			if got := response.Header().Get("Cache-Control"); got != "private, immutable" {
				t.Fatalf("history Commit Cache-Control = %q", got)
			}
			var envelope struct {
				RetCode       int
				Message       string
				HistoryCommit struct {
					CommitID         string `json:"CommitId"`
					Role             string
					MainlineCommitID string `json:"MainlineCommitId"`
					AuthorUserID     string `json:"AuthorUserId"`
					CreatedAt        string
					DeviceID         string `json:"DeviceId"`
					Message          string
					Parents          []string
					Root             string
				}
			}
			decode(t, response, &envelope)
			got := envelope.HistoryCommit
			if envelope.RetCode != 0 || envelope.Message != "success" || got.CommitID != test.commitID || got.Role != test.role ||
				got.MainlineCommitID != test.mainlineID || got.AuthorUserID != _ownerID || got.CreatedAt != "2026-08-09T00:00:00Z" ||
				got.DeviceID != "01234567-89ab-4def-8123-456789abcdef" || got.Message != "sync" || got.Root != root || !slices.Equal(got.Parents, test.parents) {
				t.Fatalf("history Commit response = %+v", envelope)
			}
		})
	}

	missingMainline := strings.Repeat("f", 64)
	if _, err := store.DB().ExecContext(t.Context(), `
		UPDATE published_commit_roles SET mainline_commit_id = ?
		WHERE owner_user_id = ? AND library_id = ? AND commit_id = ?`,
		missingMainline, _ownerID, _headLibraryID, captured); err != nil {
		t.Fatalf("history detail test operation: %v", err)
	}
	assertStatusCode(t, serve(handler, http.MethodGet, "/v1/libraries/"+_headLibraryID+"/history/"+captured, "", _ownerToken), http.StatusInternalServerError, 5000)
}

func TestGetHistoryCommitValidationAndVisibility(t *testing.T) {
	handler, store, _ := newTestHandler(t)
	defer closeStore(t, store)
	createHeadLibrary(t, handler)
	root := putMetadata(t, store, "directories", `{"Entries":[],"Type":"Directory","Version":1}`)
	published := putCommit(t, store, _ownerID, nil, root)
	publishHead(t, handler, published, `"head-version-0"`, http.StatusOK, 0)
	onlyPut := putCommit(t, store, _ownerID, []string{published}, root)
	onlyReachable := putCommit(t, store, _ownerID, []string{published}, root)
	if _, err := store.DB().ExecContext(t.Context(), `
		INSERT INTO published_commits(owner_user_id, library_id, commit_id) VALUES (?, ?, ?)`,
		_ownerID, _headLibraryID, onlyReachable); err != nil {
		t.Fatalf("history detail test operation: %v", err)
	}
	otherLibraryID := "32345678-9abc-4def-8123-456789abcdef"
	assertStatusCode(t, serve(handler, http.MethodPut, "/v1/libraries/"+otherLibraryID, `{"Name":"other"}`, _ownerToken), http.StatusCreated, 0)

	for _, path := range []string{
		"/v1/libraries/" + _headLibraryID + "/history/" + onlyPut,
		"/v1/libraries/" + _headLibraryID + "/history/" + onlyReachable,
		"/v1/libraries/" + otherLibraryID + "/history/" + published,
	} {
		assertStatusCode(t, serve(handler, http.MethodGet, path, "", _ownerToken), http.StatusNotFound, 2000)
	}
	assertStatusCode(t, serve(handler, http.MethodGet, "/v1/libraries/"+_headLibraryID+"/history/"+published, "", _otherToken), http.StatusNotFound, 2000)

	invalidPath := "/v1/libraries/not-a-uuid/history/" + strings.Repeat("A", 63) + "?Unknown=path-canary"
	assertStatusCode(t, serve(handler, http.MethodGet, invalidPath, "", ""), http.StatusUnauthorized, 1001)
	assertStatusCode(t, serve(handler, http.MethodGet, invalidPath, "", _ownerToken), http.StatusBadRequest, 1000)
	for _, id := range []string{published[:63], published + "0", strings.ToUpper(published), strings.Repeat("g", 64)} {
		assertStatusCode(t, serve(handler, http.MethodGet, "/v1/libraries/"+_headLibraryID+"/history/"+id, "", _ownerToken), http.StatusBadRequest, 1000)
	}
	assertStatusCode(t, serve(handler, http.MethodGet, "/v1/libraries/"+_headLibraryID+"/history/"+published+"?Unknown=x", "", _ownerToken), http.StatusBadRequest, 1000)

	objectPath := filepath.Join(store.ObjectsDir(), _ownerID, _headLibraryID, "commits", published[:2], published[2:])
	if err := os.Remove(objectPath); err != nil {
		t.Fatalf("history detail test operation: %v", err)
	}
	assertStatusCode(t, serve(handler, http.MethodGet, "/v1/libraries/"+_headLibraryID+"/history/"+published, "", _ownerToken), http.StatusInternalServerError, 5000)
}

func TestUpdateHeadRejectsUnpublishedFirstParentSourceBranch(t *testing.T) {
	handler, store, _ := newTestHandler(t)
	defer closeStore(t, store)
	createHeadLibrary(t, handler)
	root := putMetadata(t, store, "directories", `{"Entries":[],"Type":"Directory","Version":1}`)
	base := putCommit(t, store, _ownerID, nil, root)
	publishHead(t, handler, base, `"head-version-0"`, http.StatusOK, 0)
	branchParent := putCommit(t, store, _ownerID, []string{base}, root)
	branch := putCommit(t, store, _ownerID, []string{branchParent}, root)
	candidate := putCommit(t, store, _ownerID, []string{base, branch}, root)
	publishHead(t, handler, candidate, `"head-version-1"`, http.StatusBadRequest, 1000)
	assertCurrentHead(t, handler, base, `"head-version-1"`)
}

func TestPublishedCommitRoleIsAtomicWithHead(t *testing.T) {
	handler, store, _ := newTestHandler(t)
	defer closeStore(t, store)
	createHeadLibrary(t, handler)
	root := putMetadata(t, store, "directories", `{"Entries":[],"Type":"Directory","Version":1}`)
	base := putCommit(t, store, _ownerID, nil, root)
	publishHead(t, handler, base, `"head-version-0"`, http.StatusOK, 0)
	if _, err := store.DB().ExecContext(t.Context(), `CREATE TRIGGER fail_history_role BEFORE INSERT ON published_commit_roles BEGIN SELECT RAISE(ABORT, 'injected'); END`); err != nil {
		t.Fatalf("history detail test operation: %v", err)
	}
	candidate := putCommit(t, store, _ownerID, []string{base}, root)
	publishHead(t, handler, candidate, `"head-version-1"`, http.StatusInternalServerError, 5000)
	assertCurrentHead(t, handler, base, `"head-version-1"`)
}
