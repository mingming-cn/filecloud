package library

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/mingming-cn/filecloud/internal/object"
	"github.com/mingming-cn/filecloud/internal/storage"
)

const (
	_maxHeadBody          = 4 << 10
	_maxSnapshotDepth     = 256
	_maxSnapshotPathBytes = 1024
	_maxSnapshotContexts  = 65_536
	_maxCommitDepth       = 1024
	_maxIntroducedCommits = 1024
	_maxValidatedObjects  = 2_000_000
	_maxMissingObjects    = 100
)

var (
	errInvalidSnapshot = errors.New("invalid snapshot")
	errCorruptObject   = errors.New("corrupt object")
)

type headResponse struct {
	CommitID *string `json:"CommitId"`
	ETag     string
}

type missingObjects struct {
	ids       []string
	seen      map[string]struct{}
	truncated bool
}

func (h *handler) getHead(w http.ResponseWriter, r *http.Request) {
	_, _, library, ok := h.headLibrary(w, r)
	if !ok {
		return
	}
	etag := headETag(library.HeadVersion)
	if ifNoneMatch(r.Header.Values("If-None-Match"), etag) {
		w.Header().Set("ETag", etag)
		w.WriteHeader(http.StatusNotModified)
		return
	}
	h.writeHead(w, http.StatusOK, library)
}

func (h *handler) updateHead(w http.ResponseWriter, r *http.Request) {
	owner, libraryID, current, ok := h.headLibrary(w, r)
	if !ok {
		return
	}
	ifMatch := r.Header.Values("If-Match")
	if len(ifMatch) == 0 {
		h.writeError(w, http.StatusPreconditionRequired, 3006, "head precondition required")
		return
	}
	expectedVersion, valid := parseHeadETag(ifMatch)
	if !valid {
		h.invalid(w)
		return
	}
	if expectedVersion != current.HeadVersion {
		h.writeHeadConflict(w, current)
		return
	}

	data, ok := h.readJSONBody(w, r, _maxHeadBody)
	if !ok {
		return
	}
	commitID, err := decodeHeadRequest(data)
	if err != nil || !object.ValidID(commitID) {
		h.invalid(w)
		return
	}
	validationRequest, releaseValidation, ok := h.admitHeadValidation(w, r, owner, libraryID)
	if !ok {
		return
	}
	defer releaseValidation()
	if h.afterHeadValidationAdmit != nil {
		h.afterHeadValidationAdmit(validationRequest.Context())
	}
	missing, roles, err := h.validateCandidate(validationRequest, owner, libraryID, current.HeadCommitID, commitID)
	releaseValidation()
	switch {
	case errors.Is(err, object.ErrPayloadTooLarge):
		h.writeError(w, http.StatusRequestEntityTooLarge, 3005, "snapshot too large")
		return
	case errors.Is(err, errInvalidSnapshot):
		h.invalid(w)
		return
	case errors.Is(err, errCorruptObject):
		h.writeError(w, http.StatusUnprocessableEntity, 3004, "object validation failed")
		return
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		h.writeError(w, http.StatusServiceUnavailable, 5001, "head validation unavailable")
		return
	case err != nil:
		h.internal(w, "validate library head", err)
		return
	case len(missing.ids) != 0:
		h.writeJSON(w, http.StatusUnprocessableEntity, struct {
			RetCode       int
			Message       string
			MissingObject []string `json:"MissingObjects"`
			Truncated     bool
		}{RetCode: 3003, Message: "snapshot has missing objects", MissingObject: missing.ids, Truncated: missing.truncated})
		return
	}

	if h.beforeHeadUpdate != nil {
		if err := h.beforeHeadUpdate(); err != nil {
			h.internal(w, "before library head update", err)
			return
		}
	}
	updated, err := h.store.UpdateLibraryHeadWithRoles(r.Context(), owner, libraryID, current.HeadCommitID, current.HeadVersion, commitID, roles, h.now().UTC().Truncate(0))
	if errors.Is(err, storage.ErrHeadConflict) {
		latest, getErr := h.store.GetLibrary(r.Context(), owner, libraryID)
		if getErr != nil {
			h.internal(w, "read conflicting library head", getErr)
			return
		}
		h.writeHeadConflict(w, latest)
		return
	}
	if err != nil {
		h.internal(w, "update library head", err)
		return
	}
	if h.afterHeadUpdate != nil {
		if err := h.afterHeadUpdate(); err != nil {
			h.internal(w, "after library head update", err)
			return
		}
	}
	h.writeHead(w, http.StatusOK, updated)
}

func (h *handler) headLibrary(w http.ResponseWriter, r *http.Request) (string, string, storage.Library, bool) {
	owner, libraryID, ok := h.objectLibrary(w, r)
	if !ok {
		return "", "", storage.Library{}, false
	}
	library, err := h.store.GetLibrary(r.Context(), owner, libraryID)
	if err != nil {
		h.internal(w, "get library head", err)
		return "", "", storage.Library{}, false
	}
	return owner, libraryID, library, true
}

func parseHeadETag(values []string) (int64, bool) {
	if len(values) != 1 {
		return 0, false
	}
	value := values[0]
	const prefix = `"head-version-`
	if !strings.HasPrefix(value, prefix) || !strings.HasSuffix(value, `"`) || strings.Contains(value, ",") {
		return 0, false
	}
	number := value[len(prefix) : len(value)-1]
	if number == "" || (len(number) > 1 && number[0] == '0') {
		return 0, false
	}
	version, err := strconv.ParseInt(number, 10, 64)
	return version, err == nil && version >= 0 && strconv.FormatInt(version, 10) == number
}

func ifNoneMatch(values []string, current string) bool {
	if len(values) == 1 && strings.TrimSpace(values[0]) == "*" {
		return true
	}
	matched := false
	for _, value := range values {
		for index := 0; index < len(value); {
			for index < len(value) && (value[index] == ' ' || value[index] == '\t') {
				index++
			}
			if index == len(value) {
				return false
			}
			if value[index] == '*' {
				return false
			} else {
				if strings.HasPrefix(value[index:], "W/") {
					index += 2
				}
				if index == len(value) || value[index] != '"' {
					return false
				}
				start := index
				index++
				for index < len(value) && value[index] != '"' {
					if value[index] < 0x21 || value[index] == 0x7f {
						return false
					}
					index++
				}
				if index == len(value) {
					return false
				}
				index++
				matched = matched || value[start:index] == current
			}
			for index < len(value) && (value[index] == ' ' || value[index] == '\t') {
				index++
			}
			if index == len(value) {
				break
			}
			if value[index] != ',' {
				return false
			}
			index++
		}
	}
	return matched
}

func decodeHeadRequest(data []byte) (string, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return "", errors.New("head request must be an object")
	}
	var commitID string
	seen := false
	for decoder.More() {
		field, err := decoder.Token()
		if err != nil || field != "CommitId" || seen {
			return "", errors.New("invalid head request field")
		}
		seen = true
		if err := decoder.Decode(&commitID); err != nil {
			return "", errors.New("invalid commit id")
		}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') || !seen {
		return "", errors.New("invalid head request")
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) || token != nil {
		return "", errors.New("trailing head request data")
	}
	return commitID, nil
}

type validationState struct {
	h           *handler
	r           *http.Request
	owner       string
	libraryID   string
	missing     missingObjects
	planned     map[string]struct{}
	commits     map[string]commitResult
	directories map[string]directoryResult
	files       map[string]struct{}
	blocks      map[string]blockResult
	contexts    int
}

type commitResult struct {
	value object.Commit
	found bool
}

type directoryResult struct {
	value object.Directory
	found bool
}

type blockResult struct {
	size  int64
	found bool
}

type directoryPath struct {
	id     string
	parent *directoryPath
}

type directoryWork struct {
	id, path string
	depth    int
	ancestor *directoryPath
}

func (h *handler) validateCandidate(r *http.Request, owner, libraryID string, currentHead *string, commitID string) (missingObjects, []storage.PublishedCommitRole, error) {
	if err := r.Context().Err(); err != nil {
		return missingObjects{}, nil, err
	}
	state := validationState{
		h: h, r: r, owner: owner, libraryID: libraryID,
		missing: missingObjects{seen: make(map[string]struct{})},
		planned: make(map[string]struct{}), commits: make(map[string]commitResult),
		directories: make(map[string]directoryResult), files: make(map[string]struct{}),
		blocks: make(map[string]blockResult),
	}
	commit, found, err := state.loadCommit(commitID)
	if err != nil || !found {
		return state.missing, nil, err
	}
	if commit.AuthorUserID != owner || !validCommitParents(commit.Parents, currentHead) {
		return state.missing, nil, errInvalidSnapshot
	}
	if currentHead != nil {
		parentRole, found, err := h.store.GetPublishedCommitRole(r.Context(), owner, libraryID, *currentHead)
		if err != nil {
			return state.missing, nil, err
		}
		if !found || parentRole.Role != _historyRoleMainline || parentRole.MainlineCommitID != *currentHead {
			return state.missing, nil, errInvalidSnapshot
		}
	}
	if err := state.validateRoot(commit.Root); err != nil {
		return state.missing, nil, err
	}
	roles := []storage.PublishedCommitRole{{CommitID: commitID, Role: _historyRoleMainline, MainlineCommitID: commitID}}
	if len(commit.Parents) < 2 {
		return state.missing, roles, nil
	}

	seen := make(map[string]struct{})
	current := commit.Parents[1]
	for depth := 1; current != ""; depth++ {
		if err := state.r.Context().Err(); err != nil {
			return state.missing, nil, err
		}
		if depth > state.h.headValidation.MaxCommitDepth {
			return state.missing, nil, object.ErrPayloadTooLarge
		}
		if _, exists := seen[current]; exists {
			return state.missing, nil, errInvalidSnapshot
		}
		seen[current] = struct{}{}
		if err := state.addContext(); err != nil {
			return state.missing, nil, err
		}
		role, roleFound, err := state.h.store.GetPublishedCommitRole(state.r.Context(), owner, libraryID, current)
		if err != nil {
			return state.missing, nil, err
		}
		if roleFound || role.CommitID != "" {
			return state.missing, nil, errInvalidSnapshot
		}
		published, err := state.h.store.IsCommitPublished(state.r.Context(), owner, libraryID, current)
		if err != nil {
			return state.missing, nil, err
		}
		if published {
			return state.missing, nil, errInvalidSnapshot
		}
		value, found, err := state.loadCommit(current)
		if err != nil {
			return state.missing, nil, err
		}
		if !found {
			return state.missing, roles, nil
		}
		if value.AuthorUserID != owner || len(value.Parents) == 0 || len(value.Parents) > 2 {
			return state.missing, nil, errInvalidSnapshot
		}
		if len(roles)-1 == state.h.headValidation.MaxIntroducedCommits {
			return state.missing, nil, object.ErrPayloadTooLarge
		}
		parentRole, parentFound, err := state.h.store.GetPublishedCommitRole(state.r.Context(), owner, libraryID, value.Parents[0])
		if err != nil {
			return state.missing, nil, err
		}
		if !parentFound {
			published, err := state.h.store.IsCommitPublished(state.r.Context(), owner, libraryID, value.Parents[0])
			if err != nil {
				return state.missing, nil, err
			}
			if !published {
				if _, found, err := state.loadCommit(value.Parents[0]); err != nil {
					return state.missing, nil, err
				} else if !found {
					return state.missing, roles, nil
				}
			}
			return state.missing, nil, errInvalidSnapshot
		}
		if parentRole.Role != _historyRoleMainline || parentRole.MainlineCommitID != value.Parents[0] {
			return state.missing, nil, errInvalidSnapshot
		}
		if err := state.validateRoot(value.Root); err != nil {
			return state.missing, nil, err
		}
		roles = append(roles, storage.PublishedCommitRole{CommitID: current, Role: _historyRoleMergeSource, MainlineCommitID: commitID})
		if len(value.Parents) == 1 {
			break
		}
		current = value.Parents[1]
	}
	return state.missing, roles, nil
}

func validCommitParents(parents []string, currentHead *string) bool {
	if currentHead == nil {
		return len(parents) == 0
	}
	return (len(parents) == 1 || len(parents) == 2) && parents[0] == *currentHead
}

func (state *validationState) loadCommit(id string) (object.Commit, bool, error) {
	if cached, exists := state.commits[id]; exists {
		return cached.value, cached.found, nil
	}
	if err := state.plan("commits", id); err != nil {
		return object.Commit{}, false, err
	}
	data, found, err := state.h.readMetadata(state.r, state.owner, state.libraryID, "commits", id, _maxCommitBody)
	if err != nil {
		return object.Commit{}, false, err
	}
	if !found {
		state.missing.add(id)
		state.commits[id] = commitResult{}
		return object.Commit{}, false, nil
	}
	commit, err := object.VerifyCommit(data, id)
	if err != nil {
		return object.Commit{}, false, errCorruptObject
	}
	state.commits[id] = commitResult{value: commit, found: true}
	return commit, true, nil
}

func (state *validationState) validateRoot(root string) error {
	if err := state.plan("directories", root); err != nil {
		return err
	}
	if err := state.addContext(); err != nil {
		return err
	}
	rootPath := &directoryPath{id: root}
	queue := []directoryWork{{id: root, depth: 1, ancestor: rootPath}}
	for len(queue) > 0 {
		if err := state.r.Context().Err(); err != nil {
			return err
		}
		work := queue[0]
		queue = queue[1:]
		directory, found, err := state.loadDirectory(work.id)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		for _, entry := range directory.Entries {
			pathBytes := len(entry.Name)
			if work.path != "" {
				pathBytes += len(work.path) + 1
			}
			if pathBytes > _maxSnapshotPathBytes || work.depth == state.h.headValidation.MaxSnapshotDepth {
				return errInvalidSnapshot
			}
			switch entry.Type {
			case "Directory":
				if hasDirectoryAncestor(work.ancestor, entry.ID) {
					return errInvalidSnapshot
				}
				if err := state.addContext(); err != nil {
					return err
				}
				if err := state.plan("directories", entry.ID); err != nil {
					return err
				}
				path := entry.Name
				if work.path != "" {
					path = work.path + "/" + entry.Name
				}
				ancestor := &directoryPath{id: entry.ID, parent: work.ancestor}
				queue = append(queue, directoryWork{id: entry.ID, path: path, depth: work.depth + 1, ancestor: ancestor})
			case "File":
				if err := state.validateFile(entry.ID); err != nil {
					return err
				}
			default:
				return errInvalidSnapshot
			}
		}
	}
	return nil
}

func hasDirectoryAncestor(path *directoryPath, id string) bool {
	for current := path; current != nil; current = current.parent {
		if current.id == id {
			return true
		}
	}
	return false
}

func (state *validationState) addContext() error {
	if err := state.r.Context().Err(); err != nil {
		return err
	}
	if state.contexts == state.h.headValidation.MaxTraversalContexts {
		return object.ErrPayloadTooLarge
	}
	state.contexts++
	return nil
}

func (state *validationState) plan(kind, id string) error {
	if err := state.r.Context().Err(); err != nil {
		return err
	}
	key := kind + "/" + id
	if _, exists := state.planned[key]; exists {
		return nil
	}
	if len(state.planned) == state.h.headValidation.MaxValidatedObjects {
		return object.ErrPayloadTooLarge
	}
	state.planned[key] = struct{}{}
	return nil
}

func (state *validationState) loadDirectory(id string) (object.Directory, bool, error) {
	if cached, exists := state.directories[id]; exists {
		return cached.value, cached.found, nil
	}
	data, found, err := state.h.readMetadata(state.r, state.owner, state.libraryID, "directories", id, _maxDirectoryBody)
	if err != nil {
		return object.Directory{}, false, err
	}
	if !found {
		state.missing.add(id)
		state.directories[id] = directoryResult{}
		return object.Directory{}, false, nil
	}
	directory, err := object.VerifyDirectory(data, id)
	if err != nil {
		return object.Directory{}, false, errCorruptObject
	}
	state.directories[id] = directoryResult{value: directory, found: true}
	return directory, true, nil
}

func (state *validationState) validateFile(fileID string) error {
	if _, exists := state.files[fileID]; exists {
		return nil
	}
	if err := state.plan("files", fileID); err != nil {
		return err
	}
	state.files[fileID] = struct{}{}
	data, found, err := state.h.readMetadata(state.r, state.owner, state.libraryID, "files", fileID, _maxFileBody)
	if err != nil {
		return err
	}
	if !found {
		state.missing.add(fileID)
		return nil
	}
	file, err := object.VerifyFile(data, fileID)
	if err != nil {
		return errCorruptObject
	}
	var total int64
	complete := true
	for index, blockID := range file.Blocks {
		block, exists := state.blocks[blockID]
		if !exists {
			if err := state.plan("blocks", blockID); err != nil {
				return err
			}
			reader, actualSize, err := state.h.store.GetObject(state.r.Context(), state.owner, state.libraryID, "blocks", blockID)
			if errors.Is(err, storage.ErrObjectNotFound) {
				state.missing.add(blockID)
				state.blocks[blockID] = blockResult{}
				complete = false
				continue
			}
			if err != nil {
				return err
			}
			hash := sha256.New()
			_, copyErr := io.Copy(hash, contextReader{ctx: state.r.Context(), reader: reader})
			closeErr := reader.Close()
			if copyErr != nil || closeErr != nil {
				return errors.Join(copyErr, closeErr)
			}
			if hex.EncodeToString(hash.Sum(nil)) != blockID || actualSize < 1 || actualSize > object.MaxBlockSize {
				return errCorruptObject
			}
			block = blockResult{size: actualSize, found: true}
			state.blocks[blockID] = block
		}
		if !block.found {
			complete = false
			continue
		}
		if index < len(file.Blocks)-1 && block.size != object.MaxBlockSize {
			return errCorruptObject
		}
		total += block.size
	}
	if complete && total != file.Size {
		return errCorruptObject
	}
	return nil
}

func (h *handler) readMetadata(r *http.Request, owner, libraryID, kind, id string, maximum int64) ([]byte, bool, error) {
	file, size, err := h.store.GetObject(r.Context(), owner, libraryID, kind, id)
	if errors.Is(err, storage.ErrObjectNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer file.Close()
	if size > maximum {
		return nil, false, errCorruptObject
	}
	data, err := io.ReadAll(io.LimitReader(contextReader{ctx: r.Context(), reader: file}, maximum+1))
	if err != nil {
		return nil, false, fmt.Errorf("read persisted object: %w", err)
	}
	if int64(len(data)) != size {
		return nil, false, errCorruptObject
	}
	return data, true, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	count, err := r.reader.Read(buffer)
	if err == nil {
		if contextErr := r.ctx.Err(); contextErr != nil {
			return count, contextErr
		}
	}
	return count, err
}

func (missing *missingObjects) add(id string) {
	if _, exists := missing.seen[id]; exists {
		return
	}
	missing.seen[id] = struct{}{}
	if len(missing.ids) < _maxMissingObjects {
		missing.ids = append(missing.ids, id)
	} else {
		missing.truncated = true
	}
}

func (h *handler) writeHead(w http.ResponseWriter, status int, library storage.Library) {
	head := headResponse{CommitID: library.HeadCommitID, ETag: headETag(library.HeadVersion)}
	w.Header().Set("ETag", head.ETag)
	h.writeJSON(w, status, struct {
		RetCode int
		Message string
		Head    headResponse
	}{RetCode: 0, Message: "success", Head: head})
}

func (h *handler) writeHeadConflict(w http.ResponseWriter, library storage.Library) {
	head := headResponse{CommitID: library.HeadCommitID, ETag: headETag(library.HeadVersion)}
	w.Header().Set("ETag", head.ETag)
	h.writeJSON(w, http.StatusPreconditionFailed, struct {
		RetCode int
		Message string
		Head    headResponse
	}{RetCode: 3002, Message: "library head changed", Head: head})
}
