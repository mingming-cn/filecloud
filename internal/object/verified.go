package object

import (
	"bytes"
	"errors"
)

// ErrInvalidPersistedObject reports bytes that do not match their immutable identity.
var ErrInvalidPersistedObject = errors.New("invalid persisted object")

// File describes references needed while validating a persisted snapshot.
type File struct {
	Blocks []string
	Size   int64
}

// DirectoryEntry describes one typed child reference.
type DirectoryEntry struct {
	ID         string
	ModifiedAt string
	Name       string
	Type       string
}

// Directory describes references needed while validating a persisted snapshot.
type Directory struct {
	Entries []DirectoryEntry
}

// Commit describes publication fields from a persisted commit.
type Commit struct {
	AuthorUserID string
	CreatedAt    string
	DeviceID     string
	Message      string
	Parents      []string
	Root         string
}

// VerifyFile revalidates canonical bytes and identity before exposing references.
func VerifyFile(data []byte, id string) (File, error) {
	canonical, actualID, err := Canonicalize("files", data)
	if err != nil || actualID != id || !bytes.Equal(canonical, data) {
		return File{}, ErrInvalidPersistedObject
	}
	value, err := decodeFile(data)
	if err != nil {
		return File{}, ErrInvalidPersistedObject
	}
	size, err := parseDecimal(value.Size, _maxFileSize)
	if err != nil {
		return File{}, ErrInvalidPersistedObject
	}
	return File{Blocks: value.Blocks, Size: size}, nil
}

// VerifyDirectory revalidates canonical bytes and identity before exposing references.
func VerifyDirectory(data []byte, id string) (Directory, error) {
	canonical, actualID, err := Canonicalize("directories", data)
	if err != nil || actualID != id || !bytes.Equal(canonical, data) {
		return Directory{}, ErrInvalidPersistedObject
	}
	value, err := decodeDirectory(data)
	if err != nil {
		return Directory{}, ErrInvalidPersistedObject
	}
	result := Directory{Entries: make([]DirectoryEntry, len(value.Entries))}
	for index, entry := range value.Entries {
		result.Entries[index] = DirectoryEntry{ID: entry.ID, ModifiedAt: entry.ModifiedAt, Name: entry.Name, Type: entry.Type}
	}
	return result, nil
}

// VerifyCommit revalidates canonical bytes and identity before exposing publication fields.
func VerifyCommit(data []byte, id string) (Commit, error) {
	canonical, actualID, err := Canonicalize("commits", data)
	if err != nil || actualID != id || !bytes.Equal(canonical, data) {
		return Commit{}, ErrInvalidPersistedObject
	}
	value, err := decodeCommit(data)
	if err != nil {
		return Commit{}, ErrInvalidPersistedObject
	}
	return Commit{
		AuthorUserID: value.AuthorUserID,
		CreatedAt:    value.CreatedAt,
		DeviceID:     value.DeviceID,
		Message:      *value.Message,
		Parents:      value.Parents,
		Root:         value.Root,
	}, nil
}
