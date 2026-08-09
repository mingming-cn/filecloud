package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"golang.org/x/text/unicode/norm"
)

var (
	// ErrLibraryExists reports a canonical name conflict for one owner.
	ErrLibraryExists = errors.New("library already exists")
	// ErrLibraryObjectConflict reports an idempotent create with a different name.
	ErrLibraryObjectConflict = errors.New("library object conflict")
	// ErrLibraryNotFound reports no library visible to the owner.
	ErrLibraryNotFound = errors.New("library not found")
	// ErrHeadConflict reports that a conditional Head update lost a race.
	ErrHeadConflict = errors.New("library head conflict")
)

// Library is one owner-isolated library control-plane record.
type Library struct {
	ID           string
	OwnerUserID  string
	Name         string
	HeadCommitID *string
	HeadVersion  int64
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// CanonicalLibraryName returns the NFC display form and uniqueness key.
func CanonicalLibraryName(name string) (display, key string) {
	display = norm.NFC.String(name)
	return display, display
}

// CreateLibrary creates a library or recognizes an idempotent replay.
func (s *Store) CreateLibrary(ctx context.Context, library Library, now time.Time) (ret Library, created bool, retErr error) {
	display, key := CanonicalLibraryName(library.Name)
	timestamp := formatTime(now)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Library{}, false, fmt.Errorf("begin create library: %w", err)
	}
	defer func() {
		if retErr == nil {
			return
		}
		if rollbackErr := tx.Rollback(); !errors.Is(rollbackErr, sql.ErrTxDone) {
			retErr = errors.Join(retErr, rollbackErr)
		}
	}()

	result, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO libraries(id, owner_user_id, name, name_key, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		library.ID, library.OwnerUserID, display, key, timestamp, timestamp)
	if err != nil {
		return Library{}, false, fmt.Errorf("create library: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return Library{}, false, fmt.Errorf("read create library result: %w", err)
	}
	if changed == 1 {
		if err := tx.Commit(); err != nil {
			return Library{}, false, fmt.Errorf("commit create library: %w", err)
		}
		library.Name = display
		library.CreatedAt = now.UTC()
		library.UpdatedAt = now.UTC()
		return library, true, nil
	}

	existing, err := scanLibrary(tx.QueryRowContext(ctx, `
		SELECT id, owner_user_id, name, head_commit_id, head_version, created_at, updated_at
		FROM libraries WHERE owner_user_id = ? AND id = ?`, library.OwnerUserID, library.ID))
	if err == nil {
		if existing.Name != display {
			return Library{}, false, ErrLibraryObjectConflict
		}
		if err := tx.Commit(); err != nil {
			return Library{}, false, fmt.Errorf("commit replayed library: %w", err)
		}
		return existing, false, nil
	}
	if !errors.Is(err, ErrLibraryNotFound) {
		return Library{}, false, err
	}
	return Library{}, false, ErrLibraryExists
}

// GetLibrary returns one library only when it belongs to ownerUserID.
func (s *Store) GetLibrary(ctx context.Context, ownerUserID, libraryID string) (Library, error) {
	return scanLibrary(s.db.QueryRowContext(ctx, `
		SELECT id, owner_user_id, name, head_commit_id, head_version, created_at, updated_at
		FROM libraries WHERE owner_user_id = ? AND id = ?`, ownerUserID, libraryID))
}

// IsCommitPublished reports whether commitID is reachable from a successfully published Head.
func (s *Store) IsCommitPublished(ctx context.Context, ownerUserID, libraryID, commitID string) (bool, error) {
	var published bool
	err := s.db.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM published_commits
			WHERE owner_user_id = ? AND library_id = ? AND commit_id = ?
		)`, ownerUserID, libraryID, commitID).Scan(&published)
	if err != nil {
		return false, fmt.Errorf("query published commit: %w", err)
	}
	return published, nil
}

// UpdateLibraryHead atomically advances Head and records its newly published ancestry.
func (s *Store) UpdateLibraryHead(ctx context.Context, ownerUserID, libraryID string, expectedHead *string, expectedVersion int64, commitID string, introducedCommitIDs []string, now time.Time) (ret Library, retErr error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Library{}, fmt.Errorf("begin update library head: %w", err)
	}
	defer func() {
		if retErr == nil {
			return
		}
		if rollbackErr := tx.Rollback(); !errors.Is(rollbackErr, sql.ErrTxDone) {
			retErr = errors.Join(retErr, rollbackErr)
		}
	}()

	result, err := tx.ExecContext(ctx, `
		UPDATE libraries
		SET head_commit_id = ?, head_version = head_version + 1, updated_at = ?
		WHERE owner_user_id = ? AND id = ? AND head_version = ?
		  AND ((? IS NULL AND head_commit_id IS NULL) OR head_commit_id = ?)`,
		commitID, formatTime(now), ownerUserID, libraryID, expectedVersion, expectedHead, expectedHead)
	if err != nil {
		return Library{}, fmt.Errorf("update library head: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return Library{}, fmt.Errorf("read update library head result: %w", err)
	}
	if changed != 1 {
		return Library{}, ErrHeadConflict
	}
	for _, publishedCommitID := range append([]string{commitID}, introducedCommitIDs...) {
		if _, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO published_commits(owner_user_id, library_id, commit_id)
			VALUES (?, ?, ?)`, ownerUserID, libraryID, publishedCommitID); err != nil {
			return Library{}, fmt.Errorf("record published commit: %w", err)
		}
	}
	ret, err = scanLibrary(tx.QueryRowContext(ctx, `
		SELECT id, owner_user_id, name, head_commit_id, head_version, created_at, updated_at
		FROM libraries WHERE owner_user_id = ? AND id = ?`, ownerUserID, libraryID))
	if err != nil {
		return Library{}, err
	}
	if err := tx.Commit(); err != nil {
		return Library{}, fmt.Errorf("commit update library head: %w", err)
	}
	return ret, nil
}

// ListLibraries returns libraries after the exclusive cursor in stable order.
func (s *Store) ListLibraries(ctx context.Context, ownerUserID string, cursorTime time.Time, cursorID string, limit int) (ret []Library, retErr error) {
	cursor := ""
	if !cursorTime.IsZero() {
		cursor = formatTime(cursorTime)
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, owner_user_id, name, head_commit_id, head_version, created_at, updated_at
		FROM libraries
		WHERE owner_user_id = ? AND (? = '' OR created_at > ? OR (created_at = ? AND id > ?))
		ORDER BY created_at, id
		LIMIT ?`, ownerUserID, cursor, cursor, cursor, cursorID, limit)
	if err != nil {
		return nil, fmt.Errorf("list libraries: %w", err)
	}
	defer func() {
		retErr = errors.Join(retErr, rows.Close())
	}()
	libraries := make([]Library, 0, limit)
	for rows.Next() {
		library, err := scanLibrary(rows)
		if err != nil {
			return nil, err
		}
		libraries = append(libraries, library)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate libraries: %w", err)
	}
	return libraries, nil
}

type libraryScanner interface {
	Scan(...any) error
}

func scanLibrary(scanner libraryScanner) (Library, error) {
	var library Library
	var head sql.NullString
	var createdAt, updatedAt string
	if err := scanner.Scan(&library.ID, &library.OwnerUserID, &library.Name, &head, &library.HeadVersion, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Library{}, ErrLibraryNotFound
		}
		return Library{}, fmt.Errorf("scan library: %w", err)
	}
	if head.Valid {
		library.HeadCommitID = new(head.String)
	}
	var err error
	library.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return Library{}, fmt.Errorf("parse library created time: %w", err)
	}
	library.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		return Library{}, fmt.Errorf("parse library updated time: %w", err)
	}
	return library, nil
}
