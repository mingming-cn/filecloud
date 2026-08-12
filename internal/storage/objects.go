package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/mingming-cn/filecloud/internal/object"
)

type objectPublication struct {
	gate chan struct{}
	refs int
}

const (
	_objectBeforeTemporaryWrite = "before_temporary_write"
	_objectAfterTemporaryWrite  = "after_temporary_write"
	_objectBeforeTemporarySync  = "before_temporary_sync"
	_objectAfterTemporarySync   = "after_temporary_sync"
	_objectBeforeInstall        = "before_install"
	_objectAfterInstall         = "after_install"
	_objectBeforeParentSync     = "before_parent_sync"
	_objectAfterParentSync      = "after_parent_sync"
)

var (
	// ErrObjectNotFound reports no object in the owner-isolated library.
	ErrObjectNotFound = errors.New("object not found")
	// ErrObjectConflict reports different persisted bytes at an ObjectId.
	ErrObjectConflict = errors.New("object conflict")
	// ErrObjectHashMismatch reports bytes whose digest differs from ObjectId.
	ErrObjectHashMismatch = errors.New("object hash mismatch")
)

// PutObject verifies and durably publishes an object without replacing an existing path.
func (s *Store) PutObject(ctx context.Context, ownerUserID, libraryID, kind, objectID string, source io.Reader) (bool, error) {
	return s.putObject(ctx, ownerUserID, libraryID, kind, objectID, source, -1)
}

// PutObjectSized publishes an object only when the source has exactly expectedSize bytes.
func (s *Store) PutObjectSized(ctx context.Context, ownerUserID, libraryID, kind, objectID string, source io.Reader, expectedSize int64) (bool, error) {
	if expectedSize < 0 {
		return false, errors.New("invalid expected object size")
	}
	return s.putObject(ctx, ownerUserID, libraryID, kind, objectID, source, expectedSize)
}

func (s *Store) putObject(ctx context.Context, ownerUserID, libraryID, kind, objectID string, source io.Reader, expectedSize int64) (created bool, retErr error) {
	if err := validateObjectLocation(s.objectsDir, ownerUserID, libraryID, kind, objectID); err != nil {
		return false, err
	}
	if _, err := s.GetLibrary(ctx, ownerUserID, libraryID); err != nil {
		return false, err
	}
	destination := s.objectPath(ownerUserID, libraryID, kind, objectID)
	release, err := s.lockObjectPublication(ctx, destination)
	if err != nil {
		return false, err
	}
	defer release()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if _, err := os.Stat(destination); err == nil {
		written, digest, err := objectDigest(ctx, source, expectedSize)
		if err != nil {
			return false, err
		}
		if digest != objectID {
			return false, ErrObjectHashMismatch
		}
		matches, err := objectFileMatches(destination, objectID, written)
		if err != nil {
			return false, fmt.Errorf("verify existing object: %w", err)
		}
		if !matches {
			return false, ErrObjectConflict
		}
		if err := s.syncObjectDirectory(filepath.Dir(destination)); err != nil {
			return false, fmt.Errorf("sync replayed object directory: %w", err)
		}
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("stat existing object: %w", err)
	}
	if err := s.reserveUploadBytes(0); err != nil {
		return false, err
	}

	parent := filepath.Dir(destination)
	if err := s.mkdirObjectPath(parent); err != nil {
		return false, err
	}
	temporary, err := os.CreateTemp(parent, ".filecloud-object-")
	if err != nil {
		return false, fmt.Errorf("create temporary object: %w", err)
	}
	temporaryName := temporary.Name()
	defer func() {
		if temporary != nil {
			retErr = errors.Join(retErr, temporary.Close())
		}
		if err := os.Remove(temporaryName); err != nil && !errors.Is(err, os.ErrNotExist) {
			retErr = errors.Join(retErr, fmt.Errorf("remove temporary object: %w", err))
		}
	}()

	if err := s.runObjectPublicationFault(_objectBeforeTemporaryWrite); err != nil {
		return false, err
	}
	hash := sha256.New()
	limited := source
	if expectedSize >= 0 {
		limited = io.LimitReader(source, expectedSize+1)
	}
	written, err := io.Copy(io.MultiWriter(temporary, hash), limited)
	if err != nil {
		return false, fmt.Errorf("write temporary object: %w", err)
	}
	if err := s.runObjectPublicationFault(_objectAfterTemporaryWrite); err != nil {
		return false, err
	}
	if expectedSize >= 0 && written != expectedSize {
		return false, io.ErrUnexpectedEOF
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if digest := hex.EncodeToString(hash.Sum(nil)); digest != objectID {
		return false, ErrObjectHashMismatch
	}
	releaseBudget, err := s.reserveUploadBudget(ctx, ownerUserID, written)
	if err != nil {
		return false, err
	}
	published := false
	defer func() {
		if !published {
			retErr = errors.Join(retErr, releaseBudget())
		}
	}()
	if err := s.runObjectPublicationFault(_objectBeforeTemporarySync); err != nil {
		return false, err
	}
	if err := temporary.Sync(); err != nil {
		return false, fmt.Errorf("sync temporary object: %w", err)
	}
	if err := s.runObjectPublicationFault(_objectAfterTemporarySync); err != nil {
		return false, err
	}
	if err := temporary.Close(); err != nil {
		return false, fmt.Errorf("close temporary object: %w", err)
	}
	temporary = nil
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := s.runObjectPublicationFault(_objectBeforeInstall); err != nil {
		return false, err
	}
	if err := os.Link(temporaryName, destination); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return false, fmt.Errorf("publish object: %w", err)
		}
		matches, verifyErr := objectFileMatches(destination, objectID, written)
		if verifyErr != nil {
			return false, fmt.Errorf("verify existing object: %w", verifyErr)
		}
		if !matches {
			return false, ErrObjectConflict
		}
		if err := s.syncObjectDirectory(parent); err != nil {
			return false, fmt.Errorf("sync replayed object directory: %w", err)
		}
		return false, nil
	}
	published = true
	if err := s.runObjectPublicationFault(_objectAfterInstall); err != nil {
		return false, err
	}
	if err := s.runObjectPublicationFault(_objectBeforeParentSync); err != nil {
		return false, err
	}
	if err := s.syncObjectDirectory(parent); err != nil {
		return false, fmt.Errorf("sync published object directory: %w", err)
	}
	if err := s.runObjectPublicationFault(_objectAfterParentSync); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) runObjectPublicationFault(point string) error {
	if s.objectPublicationFault == nil {
		return nil
	}
	if err := s.objectPublicationFault(point); err != nil {
		return fmt.Errorf("object publication %s: %w", point, err)
	}
	return nil
}

// GetObject opens an object only within its owner-isolated library.
func (s *Store) GetObject(ctx context.Context, ownerUserID, libraryID, kind, objectID string) (*os.File, int64, error) {
	if err := validateObjectLocation(s.objectsDir, ownerUserID, libraryID, kind, objectID); err != nil {
		return nil, 0, err
	}
	if _, err := s.GetLibrary(ctx, ownerUserID, libraryID); err != nil {
		return nil, 0, err
	}
	file, err := os.Open(s.objectPath(ownerUserID, libraryID, kind, objectID))
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, ErrObjectNotFound
	}
	if err != nil {
		return nil, 0, fmt.Errorf("open object: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		return nil, 0, errors.Join(fmt.Errorf("stat object: %w", err), file.Close())
	}
	if !info.Mode().IsRegular() {
		return nil, 0, errors.Join(ErrObjectNotFound, file.Close())
	}
	return file, info.Size(), nil
}

// HasObject reports whether an object is present in its owner-isolated library.
func (s *Store) HasObject(ctx context.Context, ownerUserID, libraryID, kind, objectID string) (bool, error) {
	file, _, err := s.GetObject(ctx, ownerUserID, libraryID, kind, objectID)
	if errors.Is(err, ErrObjectNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, file.Close()
}

func (s *Store) objectPath(ownerUserID, libraryID, kind, objectID string) string {
	return filepath.Join(s.objectsDir, ownerUserID, libraryID, kind, objectID[:2], objectID[2:])
}

func validateObjectLocation(objectsDir, ownerUserID, libraryID, kind, objectID string) error {
	if !validObjectScopeID(ownerUserID) || !validObjectScopeID(libraryID) || !object.ValidID(objectID) {
		return errors.New("invalid object location")
	}
	switch kind {
	case "blocks", "files", "directories", "commits":
	default:
		return errors.New("invalid object type")
	}
	destination := filepath.Join(objectsDir, ownerUserID, libraryID, kind, objectID[:2], objectID[2:])
	relative, err := filepath.Rel(objectsDir, destination)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return errors.New("object location escapes object directory")
	}
	return nil
}

func validObjectScopeID(value string) bool {
	return value != "" && value != "." && value != ".." && !strings.ContainsAny(value, `/\\`)
}

func (s *Store) lockObjectPublication(ctx context.Context, key string) (func(), error) {
	s.objectLocksMu.Lock()
	publication := s.objectLocks[key]
	if publication == nil {
		publication = &objectPublication{gate: make(chan struct{}, 1)}
		publication.gate <- struct{}{}
		s.objectLocks[key] = publication
	}
	publication.refs++
	queued := s.objectLockQueued
	s.objectLocksMu.Unlock()
	if queued != nil {
		queued(key)
	}
	select {
	case <-publication.gate:
		return func() {
			publication.gate <- struct{}{}
			s.releaseObjectPublication(key, publication)
		}, nil
	case <-ctx.Done():
		s.releaseObjectPublication(key, publication)
		return nil, ctx.Err()
	}
}

func (s *Store) releaseObjectPublication(key string, publication *objectPublication) {
	s.objectLocksMu.Lock()
	publication.refs--
	if publication.refs == 0 {
		delete(s.objectLocks, key)
	}
	s.objectLocksMu.Unlock()
}

func objectDigest(ctx context.Context, source io.Reader, expectedSize int64) (int64, string, error) {
	limited := source
	if expectedSize >= 0 {
		limited = io.LimitReader(source, expectedSize+1)
	}
	hash := sha256.New()
	written, err := io.Copy(hash, limited)
	if err != nil {
		return 0, "", fmt.Errorf("read object: %w", err)
	}
	if expectedSize >= 0 && written != expectedSize {
		return 0, "", io.ErrUnexpectedEOF
	}
	if err := ctx.Err(); err != nil {
		return 0, "", err
	}
	return written, hex.EncodeToString(hash.Sum(nil)), nil
}

func objectFileMatches(path, objectID string, size int64) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	hash := sha256.New()
	buffer := make([]byte, 32<<10)
	written, copyErr := io.CopyBuffer(hash, file, buffer)
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil {
		return false, errors.Join(copyErr, closeErr)
	}
	return written == size && hex.EncodeToString(hash.Sum(nil)) == objectID, nil
}

func (s *Store) mkdirObjectPath(path string) error {
	relative, err := filepath.Rel(s.objectsDir, path)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return errors.New("object directory escapes object root")
	}
	current := s.objectsDir
	for _, segment := range strings.Split(relative, string(filepath.Separator)) {
		next := filepath.Join(current, segment)
		if err := os.Mkdir(next, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("create object directory: %w", err)
		}
		// Sync existing parents too: a prior failed fsync may have left the child visible.
		if err := s.syncObjectDirectory(current); err != nil {
			return fmt.Errorf("sync object directory chain: %w", err)
		}
		current = next
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open object directory: %w", err)
	}
	return errors.Join(directory.Sync(), directory.Close())
}
