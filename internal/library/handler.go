// Package library implements owner-isolated library control-plane endpoints.
package library

import (
	"bytes"
	"context"
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
	"github.com/mingming-cn/filecloud/internal/object"
	"github.com/mingming-cn/filecloud/internal/storage"
)

const (
	_maxRequestBody      = 8 << 10
	_defaultPageSize     = 100
	_maxPageSize         = 500
	_pageTokenLifetime   = 15 * time.Minute
	_pageTokenKeySize    = 32
	_maxObjectCheckBody  = 1 << 20
	_maxObjectCheckCount = 1000
	_maxCommitBody       = object.MaxCommitSize
	_maxFileBody         = object.MaxFileObjectSize
	_maxDirectoryBody    = object.MaxDirectoryObjectSize
)

// Config contains deterministic handler seams.
type Config struct {
	Now              func() time.Time
	PageTokenKey     []byte
	BeforeHeadUpdate func() error
	AfterHeadUpdate  func() error
	Upload           storage.UploadConfig
	HeadValidation   HeadValidationConfig
	headLimiter      *headValidationLimiter
}

type handler struct {
	store            *storage.Store
	logger           *log.Logger
	now              func() time.Time
	pageTokenAEAD    cipher.AEAD
	beforeHeadUpdate func() error
	afterHeadUpdate  func() error
	uploadTimeout    time.Duration
	headValidation   HeadValidationConfig
	headLimiter      *headValidationLimiter
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
	if config.Upload.Now == nil {
		config.Upload.Now = config.Now
	}
	upload, err := store.ConfigureUpload(config.Upload)
	if err != nil {
		return nil, err
	}
	headValidation, err := normalizeHeadValidationConfig(config.HeadValidation)
	if err != nil {
		return nil, err
	}
	if config.headLimiter == nil {
		config.headLimiter, err = newHeadValidationLimiter(headValidation.GlobalConcurrency)
		if err != nil {
			return nil, err
		}
	}
	block, err := aes.NewCipher(config.PageTokenKey)
	if err != nil {
		return nil, fmt.Errorf("create page token cipher: %w", err)
	}
	pageTokenAEAD, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create page token aead: %w", err)
	}
	h := &handler{
		store:            store,
		logger:           logger,
		now:              config.Now,
		pageTokenAEAD:    pageTokenAEAD,
		beforeHeadUpdate: config.BeforeHeadUpdate,
		afterHeadUpdate:  config.AfterHeadUpdate,
		uploadTimeout:    upload.RequestTimeout,
		headValidation:   headValidation,
		headLimiter:      config.headLimiter,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/libraries/{LibraryId}", h.create)
	mux.HandleFunc("GET /v1/libraries/{LibraryId}", h.get)
	mux.HandleFunc("GET /v1/libraries/{LibraryId}/head", h.getHead)
	mux.HandleFunc("PUT /v1/libraries/{LibraryId}/head", h.updateHead)
	mux.HandleFunc("GET /v1/libraries", h.list)
	mux.HandleFunc("POST /v1/libraries/{LibraryId}/object-checks", h.checkObjects)
	mux.HandleFunc("PUT /v1/libraries/{LibraryId}/objects/{ObjectType}/{ObjectId}", h.putMetadataObject)
	mux.HandleFunc("GET /v1/libraries/{LibraryId}/objects/{ObjectType}/{ObjectId}", h.getMetadataObject)
	mux.HandleFunc("PUT /v1/libraries/{LibraryId}/blocks/{ObjectId}", h.putBlock)
	mux.HandleFunc("GET /v1/libraries/{LibraryId}/blocks/{ObjectId}", h.getBlock)
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

type objectReference struct {
	ObjectID   string `json:"ObjectId"`
	ObjectType string `json:"ObjectType"`
}

func (h *handler) checkObjects(w http.ResponseWriter, r *http.Request) {
	owner, libraryID, ok := h.objectLibrary(w, r)
	if !ok {
		return
	}
	data, ok := h.readJSONBody(w, r, _maxObjectCheckBody)
	if !ok {
		return
	}
	references, err := decodeObjectChecks(data)
	if errors.Is(err, object.ErrPayloadTooLarge) {
		h.writeError(w, http.StatusRequestEntityTooLarge, 3005, "too many objects")
		return
	}
	if err != nil {
		h.invalid(w)
		return
	}
	missing := make([]objectReference, 0)
	for _, reference := range references {
		kind, valid := objectKind(reference.ObjectType)
		if !valid || !object.ValidID(reference.ObjectID) {
			h.invalid(w)
			return
		}
		exists, err := h.store.HasObject(r.Context(), owner, libraryID, kind, reference.ObjectID)
		if err != nil {
			h.internal(w, "check object", err)
			return
		}
		if !exists {
			missing = append(missing, reference)
		}
	}
	h.writeJSON(w, http.StatusOK, struct {
		RetCode        int
		Message        string
		MissingObjects []objectReference
	}{RetCode: 0, Message: "success", MissingObjects: missing})
}

func decodeObjectChecks(data []byte) ([]objectReference, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return nil, errors.New("object checks must be an object")
	}
	var references []objectReference
	seenObjects := false
	for decoder.More() {
		field, err := decoder.Token()
		if err != nil || field != "Objects" || seenObjects {
			return nil, errors.New("invalid object checks field")
		}
		seenObjects = true
		if token, err := decoder.Token(); err != nil || token != json.Delim('[') {
			return nil, errors.New("objects must be an array")
		}
		references = make([]objectReference, 0)
		for decoder.More() {
			if len(references) == _maxObjectCheckCount {
				return nil, object.ErrPayloadTooLarge
			}
			reference, err := decodeObjectReference(decoder)
			if err != nil {
				return nil, err
			}
			references = append(references, reference)
		}
		if token, err := decoder.Token(); err != nil || token != json.Delim(']') {
			return nil, errors.New("invalid objects array")
		}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') || !seenObjects {
		return nil, errors.New("invalid object checks")
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) || token != nil {
		return nil, errors.New("trailing object checks data")
	}
	return references, nil
}

func decodeObjectReference(decoder *json.Decoder) (objectReference, error) {
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return objectReference{}, errors.New("object reference must be an object")
	}
	var reference objectReference
	seenID := false
	seenType := false
	for decoder.More() {
		field, err := decoder.Token()
		if err != nil {
			return objectReference{}, fmt.Errorf("decode object reference field: %w", err)
		}
		switch field {
		case "ObjectId":
			if seenID {
				return objectReference{}, errors.New("duplicate object id")
			}
			seenID = true
			if err := decoder.Decode(&reference.ObjectID); err != nil {
				return objectReference{}, fmt.Errorf("decode object id: %w", err)
			}
		case "ObjectType":
			if seenType {
				return objectReference{}, errors.New("duplicate object type")
			}
			seenType = true
			if err := decoder.Decode(&reference.ObjectType); err != nil {
				return objectReference{}, fmt.Errorf("decode object type: %w", err)
			}
		default:
			return objectReference{}, errors.New("unknown object reference field")
		}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') || !seenID || !seenType {
		return objectReference{}, errors.New("invalid object reference")
	}
	return reference, nil
}

func (h *handler) putMetadataObject(w http.ResponseWriter, r *http.Request) {
	owner, libraryID, ok := h.objectLibrary(w, r)
	if !ok {
		return
	}
	kind := r.PathValue("ObjectType")
	objectID := r.PathValue("ObjectId")
	maximum, typeName, valid := metadataType(kind)
	if !valid || !object.ValidID(objectID) {
		h.invalid(w)
		return
	}
	r, release, ok := h.admitObjectPut(w, r, owner, libraryID, kind, objectID, 0)
	if !ok {
		return
	}
	defer release()
	data, ok := h.readJSONBody(w, r, maximum)
	if !ok {
		return
	}
	canonical, actualID, err := object.Canonicalize(kind, data)
	if errors.Is(err, object.ErrPayloadTooLarge) {
		h.writeError(w, http.StatusRequestEntityTooLarge, 3005, "object too large")
		return
	}
	if err != nil {
		h.invalid(w)
		return
	}
	if actualID != objectID {
		h.writeError(w, http.StatusUnprocessableEntity, 3004, "object hash mismatch")
		return
	}
	created, err := h.store.PutObject(r.Context(), owner, libraryID, kind, objectID, bytes.NewReader(canonical))
	if !h.handlePutError(w, err) {
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	h.writeJSON(w, status, struct {
		RetCode int
		Message string
		Object  struct {
			ObjectID   string `json:"ObjectId"`
			ObjectType string `json:"ObjectType"`
			Created    bool
		}
	}{RetCode: 0, Message: "success", Object: struct {
		ObjectID   string `json:"ObjectId"`
		ObjectType string `json:"ObjectType"`
		Created    bool
	}{ObjectID: objectID, ObjectType: typeName, Created: created}})
}

func (h *handler) getMetadataObject(w http.ResponseWriter, r *http.Request) {
	owner, libraryID, ok := h.objectLibrary(w, r)
	if !ok {
		return
	}
	kind := r.PathValue("ObjectType")
	objectID := r.PathValue("ObjectId")
	if _, _, valid := metadataType(kind); !valid || !object.ValidID(objectID) {
		h.invalid(w)
		return
	}
	h.getObject(w, r, owner, libraryID, kind, objectID, "application/json")
}

func (h *handler) putBlock(w http.ResponseWriter, r *http.Request) {
	owner, libraryID, ok := h.objectLibrary(w, r)
	if !ok {
		return
	}
	objectID := r.PathValue("ObjectId")
	contentType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if !object.ValidID(objectID) || err != nil || contentType != "application/octet-stream" || r.ContentLength <= 0 {
		h.invalid(w)
		return
	}
	if r.ContentLength > object.MaxBlockSize {
		h.writeError(w, http.StatusRequestEntityTooLarge, 3005, "block too large")
		return
	}
	r, release, ok := h.admitObjectPut(w, r, owner, libraryID, "blocks", objectID, r.ContentLength)
	if !ok {
		return
	}
	defer release()
	created, err := h.store.PutObjectSized(r.Context(), owner, libraryID, "blocks", objectID, r.Body, r.ContentLength)
	if errors.Is(err, io.ErrUnexpectedEOF) {
		h.invalid(w)
		return
	}
	if !h.handlePutError(w, err) {
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	h.writeJSON(w, status, struct {
		RetCode int
		Message string
		Block   struct {
			ObjectID string `json:"ObjectId"`
			Size     string
			Created  bool
		}
	}{RetCode: 0, Message: "success", Block: struct {
		ObjectID string `json:"ObjectId"`
		Size     string
		Created  bool
	}{ObjectID: objectID, Size: strconv.FormatInt(r.ContentLength, 10), Created: created}})
}

func (h *handler) getBlock(w http.ResponseWriter, r *http.Request) {
	owner, libraryID, ok := h.objectLibrary(w, r)
	if !ok {
		return
	}
	objectID := r.PathValue("ObjectId")
	if !object.ValidID(objectID) {
		h.invalid(w)
		return
	}
	h.getObject(w, r, owner, libraryID, "blocks", objectID, "application/octet-stream")
}

func (h *handler) objectLibrary(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	libraryID := r.PathValue("LibraryId")
	owner, authenticated := auth.UserID(r.Context())
	if !validUUID(libraryID) || !authenticated {
		h.invalid(w)
		return "", "", false
	}
	if _, err := h.store.GetLibrary(r.Context(), owner, libraryID); errors.Is(err, storage.ErrLibraryNotFound) {
		h.writeError(w, http.StatusNotFound, 2000, "library not found")
		return "", "", false
	} else if err != nil {
		h.internal(w, "get object library", err)
		return "", "", false
	}
	return owner, libraryID, true
}

func (h *handler) readJSONBody(w http.ResponseWriter, r *http.Request, maximum int64) ([]byte, bool) {
	contentType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" {
		h.invalid(w)
		return nil, false
	}
	if r.ContentLength > maximum {
		h.writeError(w, http.StatusRequestEntityTooLarge, 3005, "request body too large")
		return nil, false
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, maximum+1))
	if err != nil {
		h.internal(w, "read object request", err)
		return nil, false
	}
	if int64(len(data)) > maximum {
		h.writeError(w, http.StatusRequestEntityTooLarge, 3005, "request body too large")
		return nil, false
	}
	return data, true
}

func (h *handler) getObject(w http.ResponseWriter, r *http.Request, owner, libraryID, kind, objectID, contentType string) {
	file, size, err := h.store.GetObject(r.Context(), owner, libraryID, kind, objectID)
	if errors.Is(err, storage.ErrObjectNotFound) {
		h.writeError(w, http.StatusNotFound, 2000, "object not found")
		return
	}
	if err != nil {
		h.internal(w, "get object", err)
		return
	}
	defer func() {
		if err := file.Close(); err != nil {
			h.logger.Printf("close object: %v", err)
		}
	}()
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.Header().Set("ETag", `"`+objectID+`"`)
	w.Header().Set("Cache-Control", "private, immutable")
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, file); err != nil {
		h.logger.Printf("write object response: %v", err)
	}
}

func (h *handler) admitObjectPut(w http.ResponseWriter, r *http.Request, owner, libraryID, kind, objectID string, blockSize int64) (*http.Request, func(), bool) {
	ctx, cancel := context.WithTimeout(r.Context(), h.uploadTimeout)
	releaseSlot, err := h.store.AcquireObjectUpload(owner)
	if err != nil {
		cancel()
		h.handleUploadError(w, err)
		return nil, nil, false
	}
	if err := http.NewResponseController(w).SetReadDeadline(time.Now().Add(h.uploadTimeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		releaseSlot()
		cancel()
		h.internal(w, "set object upload deadline", err)
		return nil, nil, false
	}
	var releaseBytes func()
	if kind == "blocks" {
		releaseBytes, err = h.store.ReserveBlockUpload(ctx, owner, libraryID, objectID, blockSize)
	} else {
		err = h.store.CheckObjectUpload(ctx, owner, libraryID, kind, objectID)
	}
	if err != nil {
		_ = http.NewResponseController(w).SetReadDeadline(time.Time{})
		releaseSlot()
		cancel()
		h.handleUploadError(w, err)
		return nil, nil, false
	}
	return r.WithContext(ctx), func() {
		if releaseBytes != nil {
			releaseBytes()
		}
		_ = http.NewResponseController(w).SetReadDeadline(time.Time{})
		releaseSlot()
		cancel()
	}, true
}

func (h *handler) handlePutError(w http.ResponseWriter, err error) bool {
	switch {
	case err == nil:
		return true
	case errors.Is(err, storage.ErrObjectHashMismatch):
		h.writeError(w, http.StatusUnprocessableEntity, 3004, "object hash mismatch")
	case errors.Is(err, storage.ErrObjectConflict):
		h.writeError(w, http.StatusConflict, 3001, "object conflicts with existing id")
	case errors.Is(err, storage.ErrUploadRateLimited), errors.Is(err, storage.ErrUploadUnavailable):
		h.handleUploadError(w, err)
	default:
		h.internal(w, "put object", err)
	}
	return false
}

func (h *handler) handleUploadError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, storage.ErrUploadRateLimited):
		w.Header().Set("Retry-After", "1")
		h.writeError(w, http.StatusTooManyRequests, 4000, "upload rate limited")
	case errors.Is(err, storage.ErrUploadUnavailable):
		h.writeError(w, http.StatusServiceUnavailable, 5001, "upload unavailable")
	default:
		h.internal(w, "admit object upload", err)
	}
}

func metadataType(kind string) (int64, string, bool) {
	switch kind {
	case "files":
		return _maxFileBody, "File", true
	case "directories":
		return _maxDirectoryBody, "Directory", true
	case "commits":
		return _maxCommitBody, "Commit", true
	default:
		return 0, "", false
	}
}

func objectKind(typeName string) (string, bool) {
	switch typeName {
	case "Block":
		return "blocks", true
	case "File":
		return "files", true
	case "Directory":
		return "directories", true
	case "Commit":
		return "commits", true
	default:
		return "", false
	}
}

type libraryResponse struct {
	LibraryID    string `json:"LibraryId"`
	OwnerUserID  string `json:"OwnerUserId,omitempty"`
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
		LibraryID: library.ID, OwnerUserID: library.OwnerUserID, Name: library.Name, HeadCommitID: library.HeadCommitID,
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
