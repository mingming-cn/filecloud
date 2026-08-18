package library

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/mingming-cn/filecloud/internal/auth"
	"github.com/mingming-cn/filecloud/internal/object"
	"github.com/mingming-cn/filecloud/internal/opslog"
	"github.com/mingming-cn/filecloud/internal/storage"
)

const (
	_historyTokenPurpose    = "library-history"
	_historyRoleMainline    = "mainline"
	_historyRoleMergeSource = "merge-source"
)

var (
	errHistoryCorrupt     = errors.New("published history is corrupt")
	errHistoryUnavailable = errors.New("published history is unavailable")
)

type historyPageToken struct {
	Version        int    `json:"Version"`
	Purpose        string `json:"Purpose"`
	OwnerUserID    string `json:"OwnerUserId"`
	LibraryID      string `json:"LibraryId"`
	AnchorCommitID string `json:"AnchorCommitId"`
	NextCommitID   string `json:"NextCommitId"`
	ExpiresAt      string `json:"ExpiresAt"`
}

type historyCommitResponse struct {
	CommitID     string   `json:"CommitId"`
	AuthorUserID string   `json:"AuthorUserId"`
	CreatedAt    string   `json:"CreatedAt"`
	DeviceID     string   `json:"DeviceId"`
	Message      string   `json:"Message"`
	Parents      []string `json:"Parents"`
	Root         string   `json:"Root"`
}

type historyListResponse struct {
	AnchorCommitID *string                 `json:"AnchorCommitId"`
	Commits        []historyCommitResponse `json:"Commits"`
	NextPageToken  string                  `json:"NextPageToken"`
}

func (h *handler) listHistory(w http.ResponseWriter, r *http.Request) {
	owner, ok := auth.UserID(r.Context())
	if !ok {
		h.historyFailure(w, errors.New("missing authenticated user"), "")
		return
	}
	libraryID := r.PathValue("LibraryId")
	pageSize, encodedToken, err := parseHistoryQuery(r.URL.RawQuery)
	if err != nil || !validUUID(libraryID) {
		h.writeError(w, http.StatusBadRequest, 1000, "invalid history request")
		return
	}

	var token historyPageToken
	if encodedToken != "" {
		token, err = h.decodeHistoryToken(encodedToken, owner)
		if err != nil || token.LibraryID != libraryID {
			h.writeError(w, http.StatusBadRequest, 1000, "invalid history request")
			return
		}
	}

	request, release, ok := h.admitHistory(w, r, owner)
	if !ok {
		return
	}
	defer release()
	if h.afterHistoryAdmit != nil {
		h.afterHistoryAdmit(request.Context())
	}

	library, err := h.store.GetLibrary(request.Context(), owner, libraryID)
	if errors.Is(err, storage.ErrLibraryNotFound) {
		h.writeError(w, http.StatusNotFound, 2000, "library not found")
		return
	}
	if err != nil {
		h.historyFailure(w, fmt.Errorf("%w: metadata lookup", errHistoryUnavailable), "")
		return
	}

	anchorID := library.HeadCommitID
	nextID := ""
	if encodedToken != "" {
		anchorID = &token.AnchorCommitID
		nextID = token.NextCommitID
	}
	if anchorID == nil || *anchorID == "" {
		h.writeJSON(w, http.StatusOK, struct {
			RetCode int
			Message string
			History historyListResponse
		}{RetCode: 0, Message: "success", History: historyListResponse{Commits: make([]historyCommitResponse, 0)}})
		return
	}
	if !object.ValidID(*anchorID) || (encodedToken != "" && !object.ValidID(nextID)) {
		h.historyFailure(w, errHistoryCorrupt, *anchorID)
		return
	}
	if encodedToken == "" {
		nextID = *anchorID
	}

	commits := make([]historyCommitResponse, 0, pageSize)
	for len(commits) < pageSize && nextID != "" {
		commitID := nextID
		commit, err := h.readHistoryCommit(request.Context(), owner, libraryID, commitID)
		if err != nil {
			h.historyFailure(w, err, commitID)
			return
		}
		commits = append(commits, historyCommitResponse{
			CommitID:     commitID,
			AuthorUserID: commit.AuthorUserID,
			CreatedAt:    commit.CreatedAt,
			DeviceID:     commit.DeviceID,
			Message:      commit.Message,
			Parents:      append([]string{}, commit.Parents...),
			Root:         commit.Root,
		})
		if len(commit.Parents) == 0 {
			nextID = ""
		} else {
			nextID = commit.Parents[0]
		}
	}

	nextPageToken := ""
	if nextID != "" {
		nextPageToken, err = h.encodeHistoryToken(owner, libraryID, *anchorID, nextID)
		if err != nil {
			h.historyFailure(w, errHistoryUnavailable, "")
			return
		}
	}
	response := historyListResponse{AnchorCommitID: anchorID, Commits: commits, NextPageToken: nextPageToken}
	h.writeJSON(w, http.StatusOK, struct {
		RetCode int
		Message string
		History historyListResponse
	}{RetCode: 0, Message: "success", History: response})
}

func parseHistoryQuery(rawQuery string) (int, string, error) {
	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return 0, "", err
	}
	for key, values := range query {
		if (key != "PageSize" && key != "PageToken") || len(values) != 1 {
			return 0, "", errors.New("invalid history query")
		}
	}
	if values, exists := query["PageSize"]; exists && values[0] == "" {
		return 0, "", errors.New("invalid history page size")
	}
	if values, exists := query["PageToken"]; exists {
		if values[0] == "" || len(values[0]) > _maxHistoryTokenSize {
			return 0, "", errors.New("invalid history page token")
		}
	}
	pageSize := _defaultPageSize
	if encoded := query.Get("PageSize"); encoded != "" {
		pageSize, err = strconv.Atoi(encoded)
		if err != nil || pageSize < 1 || pageSize > _maxPageSize || strconv.Itoa(pageSize) != encoded {
			return 0, "", errors.New("invalid history page size")
		}
	}
	return pageSize, query.Get("PageToken"), nil
}

func (h *handler) encodeHistoryToken(owner, libraryID, anchorID, nextID string) (string, error) {
	if !validUUID(libraryID) || !object.ValidID(anchorID) || !object.ValidID(nextID) {
		return "", errHistoryUnavailable
	}
	payload, err := json.Marshal(historyPageToken{
		Version: 1, Purpose: _historyTokenPurpose, OwnerUserID: owner, LibraryID: libraryID,
		AnchorCommitID: anchorID, NextCommitID: nextID,
		ExpiresAt: h.now().UTC().Add(_pageTokenLifetime).Format(time.RFC3339Nano),
	})
	if err != nil {
		return "", err
	}
	nonce := make([]byte, h.historyTokenAEAD.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	sealed := h.historyTokenAEAD.Seal(nonce, nonce, payload, []byte(_historyTokenPurpose))
	encoded := base64.RawURLEncoding.EncodeToString(sealed)
	if len(encoded) > _maxHistoryTokenSize {
		return "", errHistoryUnavailable
	}
	return encoded, nil
}

func (h *handler) decodeHistoryToken(encoded, owner string) (historyPageToken, error) {
	if len(encoded) == 0 || len(encoded) > _maxHistoryTokenSize {
		return historyPageToken{}, errors.New("invalid history page token")
	}
	sealed, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil || len(sealed) < h.historyTokenAEAD.NonceSize()+h.historyTokenAEAD.Overhead() {
		return historyPageToken{}, errors.New("invalid history page token")
	}
	nonce := sealed[:h.historyTokenAEAD.NonceSize()]
	payload, err := h.historyTokenAEAD.Open(nil, nonce, sealed[h.historyTokenAEAD.NonceSize():], []byte(_historyTokenPurpose))
	if err != nil {
		return historyPageToken{}, errors.New("invalid history page token")
	}
	var token historyPageToken
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&token); err != nil {
		return historyPageToken{}, errors.New("invalid history page token")
	}
	if trailing, err := decoder.Token(); !errors.Is(err, io.EOF) || trailing != nil {
		return historyPageToken{}, errors.New("invalid history page token")
	}
	if token.Version != 1 || token.Purpose != _historyTokenPurpose || token.OwnerUserID != owner ||
		!validUUID(token.LibraryID) || !object.ValidID(token.AnchorCommitID) || !object.ValidID(token.NextCommitID) {
		return historyPageToken{}, errors.New("invalid history page token")
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, token.ExpiresAt)
	if err != nil || !expiresAt.After(h.now().UTC()) {
		return historyPageToken{}, errors.New("expired history page token")
	}
	return token, nil
}

func (h *handler) readHistoryCommit(ctx context.Context, owner, libraryID, id string) (object.Commit, error) {
	role, found, err := h.store.GetPublishedCommitRole(ctx, owner, libraryID, id)
	if err != nil {
		return object.Commit{}, fmt.Errorf("%w: role lookup", errHistoryUnavailable)
	}
	if !found || role.Role != _historyRoleMainline || role.MainlineCommitID != id {
		return object.Commit{}, errHistoryCorrupt
	}
	file, size, err := h.store.GetObject(ctx, owner, libraryID, "commits", id)
	if errors.Is(err, storage.ErrObjectNotFound) {
		return object.Commit{}, errHistoryCorrupt
	}
	if err != nil {
		return object.Commit{}, fmt.Errorf("%w: object lookup", errHistoryUnavailable)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, object.MaxCommitSize+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || size > object.MaxCommitSize || int64(len(data)) != size {
		return object.Commit{}, errHistoryCorrupt
	}
	commit, err := object.VerifyCommit(data, id)
	if err != nil || commit.AuthorUserID != owner {
		return object.Commit{}, errHistoryCorrupt
	}
	return commit, nil
}

func (h *handler) historyFailure(w http.ResponseWriter, err error, objectID string) {
	if objectID != "" {
		maskedID := "invalid"
		if object.ValidID(objectID) {
			maskedID = objectID[:8]
		}
		opslog.Error(h.logger, "serve", "", "history_commit_"+maskedID+"_run_integrity_check", errHistoryCorrupt)
	}
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded), errors.Is(err, errHistoryUnavailable):
		h.writeError(w, http.StatusServiceUnavailable, 5001, "storage unavailable")
	case errors.Is(err, errHistoryCorrupt):
		h.writeError(w, http.StatusInternalServerError, 5000, "internal server error")
	default:
		h.writeError(w, http.StatusInternalServerError, 5000, "internal server error")
	}
}
