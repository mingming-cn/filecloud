// Package library implements owner-isolated library control-plane endpoints.
package library

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mingming-cn/filecloud/internal/auth"
	"github.com/mingming-cn/filecloud/internal/storage"
)

const (
	_maxRequestBody    = 8 << 10
	_defaultPageSize   = 100
	_maxPageSize       = 500
	_pageTokenLifetime = 15 * time.Minute
	_pageTokenKeySize  = 32
)

// Config contains deterministic pagination seams.
type Config struct {
	Now          func() time.Time
	PageTokenKey []byte
}

type handler struct {
	store         *storage.Store
	logger        *log.Logger
	now           func() time.Time
	pageTokenAEAD cipher.AEAD
}

// NewHandler constructs authenticated create, list, and get library endpoints.
func NewHandler(store *storage.Store, logger *log.Logger, config Config) (http.Handler, error) {
	if store == nil {
		return nil, errors.New("library store is required")
	}
	if logger == nil {
		logger = log.Default()
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if len(config.PageTokenKey) == 0 {
		config.PageTokenKey = make([]byte, _pageTokenKeySize)
		if _, err := io.ReadFull(rand.Reader, config.PageTokenKey); err != nil {
			return nil, fmt.Errorf("generate page token key: %w", err)
		}
	}
	if len(config.PageTokenKey) != _pageTokenKeySize {
		return nil, errors.New("page token key must be 32 bytes")
	}
	block, err := aes.NewCipher(config.PageTokenKey)
	if err != nil {
		return nil, fmt.Errorf("create page token cipher: %w", err)
	}
	pageTokenAEAD, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create page token aead: %w", err)
	}
	h := &handler{store: store, logger: logger, now: config.Now, pageTokenAEAD: pageTokenAEAD}
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/libraries/{LibraryId}", h.create)
	mux.HandleFunc("GET /v1/libraries/{LibraryId}", h.get)
	mux.HandleFunc("GET /v1/libraries", h.list)
	return auth.RequireSession(store, config.Now, logger, mux), nil
}

func (h *handler) create(w http.ResponseWriter, r *http.Request) {
	libraryID := r.PathValue("LibraryId")
	if !validUUID(libraryID) {
		h.invalid(w)
		return
	}
	contentType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" {
		h.invalid(w)
		return
	}
	if r.ContentLength > _maxRequestBody {
		h.writeError(w, http.StatusRequestEntityTooLarge, 3005, "request body too large")
		return
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, _maxRequestBody+1))
	if err != nil {
		h.internal(w, "read library request", err)
		return
	}
	if len(data) > _maxRequestBody {
		h.writeError(w, http.StatusRequestEntityTooLarge, 3005, "request body too large")
		return
	}
	name, err := decodeCreateRequest(data)
	if err != nil {
		h.invalid(w)
		return
	}
	name, err = validateName(name)
	if err != nil {
		h.invalid(w)
		return
	}
	userID, ok := auth.UserID(r.Context())
	if !ok {
		h.internal(w, "read authenticated user", errors.New("missing authenticated user"))
		return
	}
	createdLibrary, created, err := h.store.CreateLibrary(r.Context(), storage.Library{
		ID: libraryID, OwnerUserID: userID, Name: name,
	}, h.now().UTC().Truncate(time.Second))
	if errors.Is(err, storage.ErrLibraryObjectConflict) {
		h.writeError(w, http.StatusConflict, 3001, "library conflicts with existing id")
		return
	}
	if errors.Is(err, storage.ErrLibraryExists) {
		h.writeError(w, http.StatusConflict, 3000, "library already exists")
		return
	}
	if err != nil {
		h.internal(w, "create library", err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	h.writeLibrary(w, status, createdLibrary)
}

func (h *handler) get(w http.ResponseWriter, r *http.Request) {
	libraryID := r.PathValue("LibraryId")
	if !validUUID(libraryID) {
		h.invalid(w)
		return
	}
	userID, ok := auth.UserID(r.Context())
	if !ok {
		h.internal(w, "read authenticated user", errors.New("missing authenticated user"))
		return
	}
	found, err := h.store.GetLibrary(r.Context(), userID, libraryID)
	if errors.Is(err, storage.ErrLibraryNotFound) {
		h.writeError(w, http.StatusNotFound, 2000, "library not found")
		return
	}
	if err != nil {
		h.internal(w, "get library", err)
		return
	}
	h.writeLibrary(w, http.StatusOK, found)
}

func (h *handler) list(w http.ResponseWriter, r *http.Request) {
	pageSize, token, err := parseListQuery(r.URL.RawQuery)
	if err != nil {
		h.invalid(w)
		return
	}
	userID, ok := auth.UserID(r.Context())
	if !ok {
		h.internal(w, "read authenticated user", errors.New("missing authenticated user"))
		return
	}
	var cursorTime time.Time
	var cursorID string
	if token != "" {
		cursorTime, cursorID, err = h.decodePageToken(token, userID)
		if err != nil {
			h.invalid(w)
			return
		}
	}
	libraries, err := h.store.ListLibraries(r.Context(), userID, cursorTime, cursorID, pageSize+1)
	if err != nil {
		h.internal(w, "list libraries", err)
		return
	}
	nextPageToken := ""
	if len(libraries) > pageSize {
		last := libraries[pageSize-1]
		nextPageToken, err = h.encodePageToken(userID, last)
		if err != nil {
			h.internal(w, "encode page token", err)
			return
		}
		libraries = libraries[:pageSize]
	}
	responses := make([]libraryResponse, len(libraries))
	for i, item := range libraries {
		responses[i] = responseFromLibrary(item)
	}
	h.writeJSON(w, http.StatusOK, listResponse{
		RetCode: 0, Message: "success", Libraries: responses, NextPageToken: nextPageToken,
	})
}

type libraryResponse struct {
	LibraryID    string `json:"LibraryId"`
	Name         string
	HeadCommitID *string `json:"HeadCommitId"`
	ETag         string
	CreatedAt    string
	UpdatedAt    string
}

type listResponse struct {
	RetCode       int
	Message       string
	Libraries     []libraryResponse
	NextPageToken string
}

func responseFromLibrary(library storage.Library) libraryResponse {
	return libraryResponse{
		LibraryID: library.ID, Name: library.Name, HeadCommitID: library.HeadCommitID,
		ETag: headETag(library.HeadVersion), CreatedAt: library.CreatedAt.Format(time.RFC3339),
		UpdatedAt: library.UpdatedAt.Format(time.RFC3339),
	}
}

func (h *handler) writeLibrary(w http.ResponseWriter, status int, library storage.Library) {
	response := responseFromLibrary(library)
	w.Header().Set("ETag", response.ETag)
	h.writeJSON(w, status, struct {
		RetCode int
		Message string
		Library libraryResponse
	}{RetCode: 0, Message: "success", Library: response})
}

func headETag(version int64) string {
	return `"head-version-` + strconv.FormatInt(version, 10) + `"`
}

func decodeCreateRequest(data []byte) (string, error) {
	if !utf8.Valid(data) {
		return "", errors.New("request is not valid utf-8")
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return "", errors.New("request must be an object")
	}
	var name string
	seen := false
	for decoder.More() {
		field, err := decoder.Token()
		if err != nil || field != "Name" || seen {
			return "", errors.New("invalid field")
		}
		seen = true
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil || !validJSONSurrogates(raw) {
			return "", errors.New("invalid name")
		}
		if err := json.Unmarshal(raw, &name); err != nil {
			return "", err
		}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') || !seen {
		return "", errors.New("invalid object")
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) || token != nil {
		return "", errors.New("trailing data")
	}
	return name, nil
}

func validJSONSurrogates(raw []byte) bool {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		i++
		if i >= len(raw) || raw[i] != 'u' {
			continue
		}
		if i+4 >= len(raw) {
			return false
		}
		value, err := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		if err != nil {
			return false
		}
		i += 4
		if value >= 0xdc00 && value <= 0xdfff {
			return false
		}
		if value < 0xd800 || value > 0xdbff {
			continue
		}
		if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
			return false
		}
		low, err := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
		if err != nil || low < 0xdc00 || low > 0xdfff {
			return false
		}
		i += 6
	}
	return true
}

func validateName(name string) (string, error) {
	if !utf8.ValidString(name) {
		return "", errors.New("name is not valid utf-8")
	}
	display, _ := storage.CanonicalLibraryName(name)
	count := utf8.RuneCountInString(display)
	if count < 1 || count > 128 || len(display) > 512 {
		return "", errors.New("name is outside limits")
	}
	return display, nil
}

func validUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' || strings.ToLower(value) != value {
		return false
	}
	encoded := strings.ReplaceAll(value, "-", "")
	decoded, err := hex.DecodeString(encoded)
	if err != nil || len(decoded) != 16 {
		return false
	}
	version := decoded[6] >> 4
	return version >= 1 && version <= 8 && decoded[8]&0xc0 == 0x80
}

func parseListQuery(rawQuery string) (int, string, error) {
	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return 0, "", err
	}
	for key, values := range query {
		if (key != "PageSize" && key != "PageToken") || len(values) != 1 {
			return 0, "", errors.New("invalid query")
		}
	}
	if values, exists := query["PageSize"]; exists && values[0] == "" {
		return 0, "", errors.New("invalid page size")
	}
	if values, exists := query["PageToken"]; exists && values[0] == "" {
		return 0, "", errors.New("invalid page token")
	}
	pageSize := _defaultPageSize
	if encoded := query.Get("PageSize"); encoded != "" {
		pageSize, err = strconv.Atoi(encoded)
		if err != nil || pageSize < 1 || pageSize > _maxPageSize || strconv.Itoa(pageSize) != encoded {
			return 0, "", errors.New("invalid page size")
		}
	}
	return pageSize, query.Get("PageToken"), nil
}

type pageToken struct {
	UserID    string
	CreatedAt string
	LibraryID string
	ExpiresAt string
}

func (h *handler) encodePageToken(userID string, library storage.Library) (string, error) {
	payload, err := json.Marshal(pageToken{
		UserID: userID, CreatedAt: library.CreatedAt.Format(time.RFC3339Nano), LibraryID: library.ID,
		ExpiresAt: h.now().UTC().Add(_pageTokenLifetime).Format(time.RFC3339Nano),
	})
	if err != nil {
		return "", fmt.Errorf("marshal page token: %w", err)
	}
	nonce := make([]byte, h.pageTokenAEAD.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("generate page token nonce: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(h.pageTokenAEAD.Seal(nonce, nonce, payload, nil)), nil
}

func (h *handler) decodePageToken(encoded, userID string) (time.Time, string, error) {
	sealed, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil || len(sealed) < h.pageTokenAEAD.NonceSize()+h.pageTokenAEAD.Overhead() {
		return time.Time{}, "", errors.New("invalid page token")
	}
	nonce := sealed[:h.pageTokenAEAD.NonceSize()]
	payload, err := h.pageTokenAEAD.Open(nil, nonce, sealed[h.pageTokenAEAD.NonceSize():], nil)
	if err != nil {
		return time.Time{}, "", errors.New("invalid page token")
	}
	var token pageToken
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&token); err != nil {
		return time.Time{}, "", errors.New("invalid page token")
	}
	if trailing, err := decoder.Token(); !errors.Is(err, io.EOF) || trailing != nil {
		return time.Time{}, "", errors.New("invalid page token")
	}
	createdAt, err := time.Parse(time.RFC3339Nano, token.CreatedAt)
	if err != nil || !validUUID(token.LibraryID) || token.UserID != userID {
		return time.Time{}, "", errors.New("invalid page token")
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, token.ExpiresAt)
	if err != nil || !expiresAt.After(h.now().UTC()) {
		return time.Time{}, "", errors.New("expired page token")
	}
	return createdAt, token.LibraryID, nil
}

func (h *handler) invalid(w http.ResponseWriter) {
	h.writeError(w, http.StatusBadRequest, 1000, "invalid request")
}

func (h *handler) writeError(w http.ResponseWriter, status, code int, message string) {
	h.writeJSON(w, status, struct {
		RetCode int
		Message string
	}{RetCode: code, Message: message})
}

func (h *handler) writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		h.logger.Printf("write JSON response: %v", err)
	}
}

func (h *handler) internal(w http.ResponseWriter, operation string, err error) {
	h.logger.Printf("%s: %v", operation, err)
	h.writeError(w, http.StatusInternalServerError, 5000, "internal error")
}
