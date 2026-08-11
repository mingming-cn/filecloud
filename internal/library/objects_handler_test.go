package library

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mingming-cn/filecloud/internal/object"
	"github.com/mingming-cn/filecloud/internal/storage"
)

func TestObjectPUTRejectsBusyRequestsBeforeReadingBody(t *testing.T) {
	handler, store, _ := newTestHandlerWithConfig(t, Config{Upload: storage.UploadConfig{
		GlobalConcurrency: 1, UserConcurrency: 1,
	}})
	defer closeStore(t, store)
	libraryID := "01234567-89ab-4def-8123-456789abcdef"
	assertStatusCode(t, serve(handler, http.MethodPut, "/v1/libraries/"+libraryID, `{"Name":"upload slots"}`, _ownerToken), 201, 0)

	firstData := []byte("first")
	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		reader := &blockingReader{data: firstData, started: started, release: release}
		request := httptest.NewRequest(http.MethodPut, "/v1/libraries/"+libraryID+"/blocks/"+digestBytes(firstData), reader)
		request.ContentLength = int64(len(firstData))
		request.Header.Set("Content-Type", "application/octet-stream")
		request.Header.Set("Authorization", "Bearer "+_ownerToken)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		firstDone <- response
	}()
	<-started

	secondData := []byte("second")
	secondBody := &observedBody{reader: bytes.NewReader(secondData)}
	request := httptest.NewRequest(http.MethodPut, "/v1/libraries/"+libraryID+"/blocks/"+digestBytes(secondData), secondBody)
	request.ContentLength = int64(len(secondData))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Authorization", "Bearer "+_ownerToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assertStatusCode(t, response, http.StatusTooManyRequests, 4000)
	if response.Header().Get("Retry-After") != "1" {
		t.Fatalf("Retry-After = %q, want 1", response.Header().Get("Retry-After"))
	}
	if secondBody.read {
		t.Fatal("busy PUT read its body")
	}

	close(release)
	assertStatusCode(t, <-firstDone, http.StatusCreated, 0)
}

func TestObjectPUTRollsBudgetAcrossRestartWithoutChargingReplays(t *testing.T) {
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	config := Config{Now: func() time.Time { return now }, Upload: storage.UploadConfig{
		BudgetBytes: 1, BudgetWindow: time.Hour,
	}}
	handler, store, _ := newTestHandlerWithConfig(t, config)
	libraryID := "01234567-89ab-4def-8123-456789abcdef"
	assertStatusCode(t, serve(handler, http.MethodPut, "/v1/libraries/"+libraryID, `{"Name":"upload budget"}`, _ownerToken), 201, 0)

	first := []byte("a")
	firstPath := "/v1/libraries/" + libraryID + "/blocks/" + digestBytes(first)
	assertStatusCode(t, serveBlock(handler, http.MethodPut, firstPath, first, 1, _ownerToken), http.StatusCreated, 0)
	assertStatusCode(t, serveBlock(handler, http.MethodPut, firstPath, first, 1, _ownerToken), http.StatusOK, 0)

	dataDir := filepath.Dir(store.ObjectsDir())
	closeStore(t, store)
	store, err := storage.OpenForServe(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer closeStore(t, store)
	handler, err = NewHandler(store, nil, config)
	if err != nil {
		t.Fatalf("reopen handler: %v", err)
	}
	assertStatusCode(t, serveBlock(handler, http.MethodPut, firstPath, first, 1, _ownerToken), http.StatusOK, 0)

	second := []byte("b")
	response := serveBlock(handler, http.MethodPut, "/v1/libraries/"+libraryID+"/blocks/"+digestBytes(second), second, 1, _ownerToken)
	assertStatusCode(t, response, http.StatusTooManyRequests, 4000)
	if response.Header().Get("Retry-After") != "1" {
		t.Fatalf("Retry-After = %q, want 1", response.Header().Get("Retry-After"))
	}
	now = now.Add(55 * time.Minute)
	assertStatusCode(t, serveBlock(handler, http.MethodPut, "/v1/libraries/"+libraryID+"/blocks/"+digestBytes(second), second, 1, _ownerToken), http.StatusTooManyRequests, 4000)
	now = now.Add(5 * time.Minute)
	assertStatusCode(t, serveBlock(handler, http.MethodPut, "/v1/libraries/"+libraryID+"/blocks/"+digestBytes(second), second, 1, _ownerToken), http.StatusCreated, 0)
}

func TestBlockPUTReleasesTemporaryReservationAfterFailure(t *testing.T) {
	free := uint64((1 << 30) + 2)
	handler, store, _ := newTestHandlerWithConfig(t, Config{Upload: storage.UploadConfig{
		DiskUsage: func(string) (uint64, uint64, error) { return free, 2 << 30, nil },
	}})
	defer closeStore(t, store)
	libraryID := "01234567-89ab-4def-8123-456789abcdef"
	assertStatusCode(t, serve(handler, http.MethodPut, "/v1/libraries/"+libraryID, `{"Name":"reservation release"}`, _ownerToken), 201, 0)

	truncated := serveBlock(handler, http.MethodPut, "/v1/libraries/"+libraryID+"/blocks/"+strings.Repeat("a", 64), []byte("x"), 2, _ownerToken)
	assertStatusCode(t, truncated, http.StatusBadRequest, 1000)
	data := []byte("ok")
	assertStatusCode(t, serveBlock(handler, http.MethodPut, "/v1/libraries/"+libraryID+"/blocks/"+digestBytes(data), data, 2, _ownerToken), http.StatusCreated, 0)
}

func TestObjectPUTRejectsLowDiskBeforeReadingBody(t *testing.T) {
	free := uint64(2 << 30)
	handler, store, _ := newTestHandlerWithConfig(t, Config{Upload: storage.UploadConfig{
		DiskUsage: func(string) (uint64, uint64, error) { return free, 2 << 30, nil },
	}})
	defer closeStore(t, store)
	libraryID := "01234567-89ab-4def-8123-456789abcdef"
	assertStatusCode(t, serve(handler, http.MethodPut, "/v1/libraries/"+libraryID, `{"Name":"disk waterline"}`, _ownerToken), 201, 0)

	data := []byte("a")
	path := "/v1/libraries/" + libraryID + "/blocks/" + digestBytes(data)
	assertStatusCode(t, serveBlock(handler, http.MethodPut, path, data, 1, _ownerToken), http.StatusCreated, 0)
	free = 1 << 30
	assertStatusCode(t, serveBlock(handler, http.MethodPut, path, data, 1, _ownerToken), http.StatusOK, 0)

	newData := []byte("b")
	body := &observedBody{reader: bytes.NewReader(newData)}
	request := httptest.NewRequest(http.MethodPut, "/v1/libraries/"+libraryID+"/blocks/"+digestBytes(newData), body)
	request.ContentLength = int64(len(newData))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Authorization", "Bearer "+_ownerToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assertStatusCode(t, response, http.StatusServiceUnavailable, 5001)
	if body.read {
		t.Fatal("low disk PUT read its body")
	}
}

func TestMetadataObjectHTTPContract(t *testing.T) {
	handler, store, _ := newTestHandler(t)
	defer closeStore(t, store)
	libraryID := "01234567-89ab-4def-8123-456789abcdef"
	assertStatusCode(t, serve(handler, http.MethodPut, "/v1/libraries/"+libraryID, `{"Name":"objects"}`, _ownerToken), 201, 0)

	canonical := `{"Blocks":[],"Size":"0","Type":"File","Version":1}`
	id := digestString(canonical)
	path := "/v1/libraries/" + libraryID + "/objects/files/" + id
	response := serve(handler, http.MethodPut, path, `{"Version":1,"Type":"File","Size":"0","Blocks":[]}`, _ownerToken)
	assertStatusCode(t, response, 201, 0)
	var metadataPut struct{ Object struct{ Created bool } }
	decode(t, response, &metadataPut)
	if !metadataPut.Object.Created {
		t.Fatal("first metadata PUT returned Created:false")
	}
	response = serve(handler, http.MethodPut, path, canonical, _ownerToken)
	assertStatusCode(t, response, 200, 0)
	decode(t, response, &metadataPut)
	if metadataPut.Object.Created {
		t.Fatal("replayed metadata PUT returned Created:true")
	}

	response = serve(handler, http.MethodGet, path, "", _ownerToken)
	if response.Code != 200 || response.Body.String() != canonical || response.Header().Get("Content-Type") != "application/json" ||
		response.Header().Get("Content-Length") != "50" || response.Header().Get("ETag") != `"`+id+`"` ||
		response.Header().Get("Cache-Control") != "private, immutable" {
		t.Fatalf("GET metadata = %d %q headers=%v", response.Code, response.Body.String(), response.Header())
	}

	badID := strings.Repeat("a", 64)
	for _, body := range []string{
		`{"Blocks":[],"Size":"0","Type":"File","Version":1,"Extra":true}`,
		`{"Blocks":[],"Size":"0","Size":"0","Type":"File","Version":1}`,
		`{"Blocks":[],"Size":"0","Type":"Directory","Version":1}`,
	} {
		assertStatusCode(t, serve(handler, http.MethodPut, "/v1/libraries/"+libraryID+"/objects/files/"+badID, body, _ownerToken), 400, 1000)
	}
	assertStatusCode(t, serve(handler, http.MethodPut, "/v1/libraries/"+libraryID+"/objects/files/"+badID, canonical, _ownerToken), 422, 3004)
	assertStatusCode(t, serve(handler, http.MethodGet, "/v1/libraries/"+libraryID+"/objects/files/"+badID, "", _ownerToken), 404, 2000)
}

func TestMetadataNestingLimitHTTPContract(t *testing.T) {
	handler, store, _ := newTestHandler(t)
	defer closeStore(t, store)
	libraryID := "01234567-89ab-4def-8123-456789abcdef"
	assertStatusCode(t, serve(handler, http.MethodPut, "/v1/libraries/"+libraryID, `{"Name":"metadata-depth"}`, _ownerToken), 201, 0)

	tests := []struct {
		kind string
		body string
	}{
		{kind: "files", body: `{"Blocks":[],"Size":` + strings.Repeat("[", 256) + `not-json`},
		{kind: "directories", body: `{"Entries":[],"Type":` + strings.Repeat("[", 256) + `not-json`},
		{kind: "commits", body: `{"AuthorUserId":"12345678-9abc-4def-8123-456789abcdef","CreatedAt":"2026-08-09T00:00:00Z","DeviceId":"01234567-89ab-4def-8123-456789abcdef","Message":` + strings.Repeat("[", 256) + `not-json`},
	}
	for _, test := range tests {
		t.Run(test.kind, func(t *testing.T) {
			path := "/v1/libraries/" + libraryID + "/objects/" + test.kind + "/" + strings.Repeat("a", 64)
			assertStatusCode(t, serve(handler, http.MethodPut, path, test.body, _ownerToken), 413, 3005)
		})
	}
}

func TestBlockHTTPContract(t *testing.T) {
	handler, store, _ := newTestHandler(t)
	defer closeStore(t, store)
	libraryID := "01234567-89ab-4def-8123-456789abcdef"
	assertStatusCode(t, serve(handler, http.MethodPut, "/v1/libraries/"+libraryID, `{"Name":"blocks"}`, _ownerToken), 201, 0)
	data := []byte("a")
	id := digestBytes(data)
	path := "/v1/libraries/" + libraryID + "/blocks/" + id

	response := serveBlock(handler, http.MethodPut, path, data, int64(len(data)), _ownerToken)
	assertStatusCode(t, response, 201, 0)
	var blockPut struct{ Block struct{ Created bool } }
	decode(t, response, &blockPut)
	if !blockPut.Block.Created {
		t.Fatal("first block PUT returned Created:false")
	}
	response = serveBlock(handler, http.MethodPut, path, data, int64(len(data)), _ownerToken)
	assertStatusCode(t, response, 200, 0)
	decode(t, response, &blockPut)
	if blockPut.Block.Created {
		t.Fatal("replayed block PUT returned Created:true")
	}
	response = serveBlock(handler, http.MethodGet, path, nil, 0, _ownerToken)
	if response.Code != 200 || !bytes.Equal(response.Body.Bytes(), data) || response.Header().Get("Content-Type") != "application/octet-stream" ||
		response.Header().Get("Content-Length") != "1" || response.Header().Get("ETag") != `"`+id+`"` ||
		response.Header().Get("Cache-Control") != "private, immutable" {
		t.Fatalf("GET block = %d %q headers=%v", response.Code, response.Body.Bytes(), response.Header())
	}

	maximumBlock := bytes.Repeat([]byte("m"), 4<<20)
	maximumID := digestBytes(maximumBlock)
	assertStatusCode(t, serveBlock(handler, http.MethodPut, "/v1/libraries/"+libraryID+"/blocks/"+maximumID, maximumBlock, int64(len(maximumBlock)), _ownerToken), 201, 0)
	maximumGET := serveBlock(handler, http.MethodGet, "/v1/libraries/"+libraryID+"/blocks/"+maximumID, nil, 0, _ownerToken)
	if maximumGET.Code != 200 || maximumGET.Body.Len() != 4<<20 || maximumGET.Header().Get("Content-Length") != "4194304" {
		t.Fatalf("maximum block GET = %d/%d headers=%v", maximumGET.Code, maximumGET.Body.Len(), maximumGET.Header())
	}

	missingLength := serveBlock(handler, http.MethodPut, "/v1/libraries/"+libraryID+"/blocks/"+digestBytes([]byte("b")), []byte("b"), -1, _ownerToken)
	assertStatusCode(t, missingLength, 400, 1000)
	assertStatusCode(t, serveBlock(handler, http.MethodPut, "/v1/libraries/"+libraryID+"/blocks/"+digestBytes(nil), nil, 0, _ownerToken), 400, 1000)
	truncatedID := strings.Repeat("b", 64)
	assertStatusCode(t, serveBlock(handler, http.MethodPut, "/v1/libraries/"+libraryID+"/blocks/"+truncatedID, []byte("x"), 2, _ownerToken), 400, 1000)
	assertStatusCode(t, serveBlock(handler, http.MethodGet, "/v1/libraries/"+libraryID+"/blocks/"+truncatedID, nil, 0, _ownerToken), 404, 2000)
	extraID := strings.Repeat("f", 64)
	assertStatusCode(t, serveBlock(handler, http.MethodPut, "/v1/libraries/"+libraryID+"/blocks/"+extraID, []byte("xy"), 1, _ownerToken), 400, 1000)
	assertStatusCode(t, serveBlock(handler, http.MethodGet, "/v1/libraries/"+libraryID+"/blocks/"+extraID, nil, 0, _ownerToken), 404, 2000)
	assertStatusCode(t, serveBlock(handler, http.MethodPut, "/v1/libraries/"+libraryID+"/blocks/"+strings.Repeat("c", 64), nil, (4<<20)+1, _ownerToken), 413, 3005)
	assertStatusCode(t, serveBlock(handler, http.MethodPut, "/v1/libraries/"+libraryID+"/blocks/"+strings.Repeat("d", 64), []byte("x"), 1, _ownerToken), 422, 3004)
}

func TestBlockResponseLossReplayReturnsCreatedFalse(t *testing.T) {
	handler, store, _ := newTestHandler(t)
	defer closeStore(t, store)
	libraryID := "01234567-89ab-4def-8123-456789abcdef"
	assertStatusCode(t, serve(handler, http.MethodPut, "/v1/libraries/"+libraryID, `{"Name":"response loss"}`, _ownerToken), 201, 0)
	data := []byte("durable replay")
	id := digestBytes(data)
	lost := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && !lost {
			lost = true
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, r)
			connection, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack response-loss PUT: %v", err)
				return
			}
			_ = connection.Close()
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	path := server.URL + "/v1/libraries/" + libraryID + "/blocks/" + id
	request, err := http.NewRequest(http.MethodPut, path, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Authorization", "Bearer "+_ownerToken)
	if response, err := server.Client().Do(request); err == nil {
		response.Body.Close()
		t.Fatal("lost PUT response was received")
	}
	request, err = http.NewRequest(http.MethodPut, path, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Authorization", "Bearer "+_ownerToken)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var replay struct {
		RetCode int
		Block   struct{ Created bool }
	}
	if err := json.NewDecoder(response.Body).Decode(&replay); err != nil || response.StatusCode != http.StatusOK || replay.RetCode != 0 || replay.Block.Created {
		t.Fatalf("replay response=%d %+v err=%v", response.StatusCode, replay, err)
	}
	get := serveBlock(handler, http.MethodGet, "/v1/libraries/"+libraryID+"/blocks/"+id, nil, 0, _ownerToken)
	if get.Code != http.StatusOK || !bytes.Equal(get.Body.Bytes(), data) {
		t.Fatalf("replayed object bytes=%q status=%d", get.Body.Bytes(), get.Code)
	}
	head := serve(handler, http.MethodGet, "/v1/libraries/"+libraryID+"/head", "", _ownerToken)
	var headEnvelope struct {
		Head struct {
			CommitID *string `json:"CommitId"`
		}
	}
	decode(t, head, &headEnvelope)
	if head.Code != http.StatusOK || headEnvelope.Head.CommitID != nil {
		t.Fatalf("response-loss object PUT changed Head: %d %+v", head.Code, headEnvelope.Head)
	}
}

func TestObjectHTTPWireContract(t *testing.T) {
	handler, store, _ := newTestHandler(t)
	defer closeStore(t, store)
	libraryID := "01234567-89ab-4def-8123-456789abcdef"
	assertStatusCode(t, serve(handler, http.MethodPut, "/v1/libraries/"+libraryID, `{"Name":"wire"}`, _ownerToken), 201, 0)
	server := httptest.NewServer(handler)
	defer server.Close()

	chunkedID := digestBytes([]byte("chunked"))
	request, err := http.NewRequest(http.MethodPut, server.URL+"/v1/libraries/"+libraryID+"/blocks/"+chunkedID, io.NopCloser(bytes.NewReader([]byte("chunked"))))
	if err != nil {
		t.Fatalf("NewRequest chunked: %v", err)
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Authorization", "Bearer "+_ownerToken)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("chunked request: %v", err)
	}
	assertHTTPEnvelope(t, response, http.StatusBadRequest, 1000)

	maximum := bytes.Repeat([]byte("w"), object.MaxBlockSize)
	maximumID := digestBytes(maximum)
	request, err = http.NewRequest(http.MethodPut, server.URL+"/v1/libraries/"+libraryID+"/blocks/"+maximumID, bytes.NewReader(maximum))
	if err != nil {
		t.Fatalf("NewRequest maximum: %v", err)
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Authorization", "Bearer "+_ownerToken)
	response, err = server.Client().Do(request)
	if err != nil {
		t.Fatalf("maximum request: %v", err)
	}
	assertHTTPEnvelope(t, response, http.StatusCreated, 0)

	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse server URL: %v", err)
	}
	rawCases := []struct {
		name            string
		headers         string
		body            string
		applicationJSON bool
	}{
		{name: "malformed content length", headers: "Content-Length: nope\r\n", body: "x"},
		{name: "conflicting content length", headers: "Content-Length: 1\r\nContent-Length: 2\r\n", body: "xx"},
		{name: "content length and transfer encoding", headers: "Content-Length: 1\r\nTransfer-Encoding: chunked\r\n", body: "1\r\nx\r\n0\r\n\r\n", applicationJSON: true},
	}
	for _, test := range rawCases {
		t.Run(test.name, func(t *testing.T) {
			connection, err := net.Dial("tcp", serverURL.Host)
			if err != nil {
				t.Fatalf("Dial: %v", err)
			}
			defer connection.Close()
			path := "/v1/libraries/" + libraryID + "/blocks/" + strings.Repeat("a", 64)
			if _, err := fmt.Fprintf(connection, "PUT %s HTTP/1.1\r\nHost: %s\r\nContent-Type: application/octet-stream\r\nAuthorization: Bearer %s\r\n%sConnection: close\r\n\r\n%s", path, serverURL.Host, _ownerToken, test.headers, test.body); err != nil {
				t.Fatalf("write raw request: %v", err)
			}
			response, err := http.ReadResponse(bufio.NewReader(connection), nil)
			if err != nil {
				t.Fatalf("ReadResponse: %v", err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatalf("read raw response: %v", err)
			}
			if response.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", response.StatusCode)
			}
			contentType := response.Header.Get("Content-Type")
			if test.applicationJSON {
				var envelope struct{ RetCode int }
				if err := json.Unmarshal(body, &envelope); err != nil || contentType != "application/json" || envelope.RetCode != 1000 {
					t.Fatalf("application response = type %q/body %q/error %v", contentType, body, err)
				}
			} else if contentType != "text/plain; charset=utf-8" || string(body) != "400 Bad Request" {
				t.Fatalf("transport response = type %q/body %q", contentType, body)
			}
		})
	}
}

func assertHTTPEnvelope(t *testing.T, response *http.Response, status, code int) {
	t.Helper()
	defer response.Body.Close()
	var envelope struct{ RetCode int }
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode response envelope: %v", err)
	}
	if response.StatusCode != status || envelope.RetCode != code || response.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("response = %d/%d headers=%v, want %d/%d JSON", response.StatusCode, envelope.RetCode, response.Header, status, code)
	}
}

func TestCheckObjectsReturnsOnlyOwnerLibraryMissingObjects(t *testing.T) {
	handler, store, _ := newTestHandler(t)
	defer closeStore(t, store)
	libraryID := "01234567-89ab-4def-8123-456789abcdef"
	assertStatusCode(t, serve(handler, http.MethodPut, "/v1/libraries/"+libraryID, `{"Name":"owner objects"}`, _ownerToken), 201, 0)
	assertStatusCode(t, serve(handler, http.MethodPut, "/v1/libraries/"+libraryID, `{"Name":"other objects"}`, _otherToken), 201, 0)
	presentID := digestBytes([]byte("present"))
	assertStatusCode(t, serveBlock(handler, http.MethodPut, "/v1/libraries/"+libraryID+"/blocks/"+presentID, []byte("present"), 7, _ownerToken), 201, 0)
	missingID := strings.Repeat("e", 64)
	body := `{"Objects":[{"ObjectId":"` + presentID + `","ObjectType":"Block"},{"ObjectId":"` + missingID + `","ObjectType":"File"}]}`

	response := serve(handler, http.MethodPost, "/v1/libraries/"+libraryID+"/object-checks", body, _ownerToken)
	assertStatusCode(t, response, 200, 0)
	var result struct {
		MissingObjects []struct{ ObjectID, ObjectType string }
	}
	decode(t, response, &result)
	if len(result.MissingObjects) != 1 || result.MissingObjects[0].ObjectID != missingID || result.MissingObjects[0].ObjectType != "File" {
		t.Fatalf("missing objects = %+v", result.MissingObjects)
	}
	response = serve(handler, http.MethodPost, "/v1/libraries/"+libraryID+"/object-checks", body, _otherToken)
	assertStatusCode(t, response, 200, 0)
	decode(t, response, &result)
	if len(result.MissingObjects) != 2 {
		t.Fatalf("other owner missing objects = %+v", result.MissingObjects)
	}

	objects := make([]map[string]string, 1001)
	for i := range objects {
		objects[i] = map[string]string{"ObjectId": missingID, "ObjectType": "File"}
	}
	oversize, err := json.Marshal(map[string]any{"Objects": objects})
	if err != nil {
		t.Fatalf("marshal oversized object checks: %v", err)
	}
	assertStatusCode(t, serve(handler, http.MethodPost, "/v1/libraries/"+libraryID+"/object-checks", string(oversize), _ownerToken), 413, 3005)

	invalidBodies := []string{
		`{"Objects":[],"Objects":[]}`,
		`{"Objects":[],"Extra":true}`,
		`{"Extra":true}`,
		`{"Objects":[{"ObjectId":"` + missingID + `","ObjectId":"` + missingID + `","ObjectType":"File"}]}`,
		`{"Objects":[{"ObjectId":"` + missingID + `","ObjectType":"File","Extra":true}]}`,
		`{"Objects":[{"ObjectId":"` + missingID + `"}]}`,
		`{"Objects":[]} true`,
	}
	for _, invalidBody := range invalidBodies {
		assertStatusCode(t, serve(handler, http.MethodPost, "/v1/libraries/"+libraryID+"/object-checks", invalidBody, _ownerToken), 400, 1000)
	}

	var limitFirst strings.Builder
	limitFirst.WriteString(`{"Objects":[`)
	for i := 0; i < 1000; i++ {
		if i > 0 {
			limitFirst.WriteByte(',')
		}
		limitFirst.WriteString(`{"ObjectId":"` + missingID + `","ObjectType":"File"}`)
	}
	limitFirst.WriteString(`,{"not":"decoded"}]}`)
	assertStatusCode(t, serve(handler, http.MethodPost, "/v1/libraries/"+libraryID+"/object-checks", limitFirst.String(), _ownerToken), 413, 3005)
}

type blockingReader struct {
	data             []byte
	started, release chan struct{}
	read             bool
}

func (reader *blockingReader) Read(buffer []byte) (int, error) {
	if !reader.read {
		reader.read = true
		close(reader.started)
		<-reader.release
	}
	if len(reader.data) == 0 {
		return 0, io.EOF
	}
	count := copy(buffer, reader.data)
	reader.data = reader.data[count:]
	return count, nil
}

type observedBody struct {
	reader io.Reader
	read   bool
}

func (reader *observedBody) Read(buffer []byte) (int, error) {
	reader.read = true
	return reader.reader.Read(buffer)
}

func serveBlock(handler http.Handler, method, path string, body []byte, contentLength int64, token string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	request := httptest.NewRequest(method, path, bytes.NewReader(body))
	request.ContentLength = contentLength
	if method == http.MethodPut {
		request.Header.Set("Content-Type", "application/octet-stream")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(response, request)
	return response
}

func digestString(value string) string { return digestBytes([]byte(value)) }
func digestBytes(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}
