// Package storage manages the single-node data directory and metadata database.
package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const (
	_databaseName      = "metadata.db"
	_lockName          = ".filecloud.lock"
	_objectsName       = "objects"
	_tmpName           = "tmp"
	_busyTimeoutMillis = 5000
)

func metadataMigrations() []migration {
	return []migration{
		{version: 1},
		{version: 2, statements: []string{
			`CREATE TABLE users (
				id TEXT PRIMARY KEY NOT NULL,
				username TEXT NOT NULL,
				username_key TEXT NOT NULL UNIQUE,
				password_hash TEXT NOT NULL,
				is_admin INTEGER NOT NULL DEFAULT 0 CHECK(is_admin IN (0, 1)),
				created_at TEXT NOT NULL,
				updated_at TEXT NOT NULL
			)`,
			`CREATE TABLE access_tokens (
				id INTEGER PRIMARY KEY NOT NULL,
				user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
				token_hash BLOB NOT NULL UNIQUE CHECK(length(token_hash) = 32),
				device_name TEXT NOT NULL,
				created_at TEXT NOT NULL,
				expires_at TEXT NOT NULL,
				revoked_at TEXT
			)`,
			`CREATE INDEX access_tokens_user_id ON access_tokens(user_id)`,
		}},
		{version: 3, statements: []string{
			`CREATE TABLE libraries (
				id TEXT NOT NULL,
				owner_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
				name TEXT NOT NULL,
				name_key TEXT NOT NULL,
				head_commit_id TEXT,
				head_version INTEGER NOT NULL DEFAULT 0 CHECK(head_version >= 0),
				created_at TEXT NOT NULL,
				updated_at TEXT NOT NULL,
				PRIMARY KEY(owner_user_id, id),
				UNIQUE(owner_user_id, name_key)
			)`,
			`CREATE INDEX libraries_owner_page ON libraries(owner_user_id, created_at, id)`,
		}},
		{version: 4, statements: []string{
			`CREATE TABLE published_commits (
				owner_user_id TEXT NOT NULL,
				library_id TEXT NOT NULL,
				commit_id TEXT NOT NULL,
				PRIMARY KEY(owner_user_id, library_id, commit_id),
				FOREIGN KEY(owner_user_id, library_id)
					REFERENCES libraries(owner_user_id, id) ON DELETE CASCADE
			)`,
			`INSERT INTO published_commits(owner_user_id, library_id, commit_id)
			 SELECT owner_user_id, id, head_commit_id FROM libraries WHERE head_commit_id IS NOT NULL`,
		}},
		{version: 5, statements: []string{
			`CREATE TABLE upload_charges (
				id INTEGER PRIMARY KEY NOT NULL,
				user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
				accepted_at INTEGER NOT NULL,
				bytes INTEGER NOT NULL CHECK(bytes > 0)
			)`,
			`CREATE INDEX upload_charges_window ON upload_charges(user_id, accepted_at)`,
		}},
	}
}

// Store is an open metadata database protected by the data-directory lock.
type Store struct {
	db                     *sql.DB
	objectsDir             string
	lock                   *dataLock
	objectLocksMu          sync.Mutex
	objectLocks            map[string]*objectPublication
	objectLockQueued       func(string)
	objectPublicationFault func(string) error
	syncObjectDirectory    func(string) error
	uploadMu               sync.Mutex
	upload                 uploadState
}

// Init creates a usable data directory and applies all metadata migrations.
func Init(ctx context.Context, dataDir string) (retErr error) {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}

	lock, err := openDataLock(dataDir, true)
	if err != nil {
		return err
	}
	defer func() {
		retErr = errors.Join(retErr, lock.Close())
	}()
	if err := lock.exclusive(); err != nil {
		return err
	}
	if err := os.Chmod(dataDir, 0o700); err != nil {
		return fmt.Errorf("secure data directory: %w", err)
	}
	if err := os.Chmod(filepath.Join(dataDir, _lockName), 0o600); err != nil {
		return fmt.Errorf("secure data-directory lock: %w", err)
	}

	if err := ensureLayout(dataDir, syncDirectory); err != nil {
		return err
	}
	databasePath := filepath.Join(dataDir, _databaseName)
	if err := os.Chmod(databasePath, 0o600); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("secure metadata database: %w", err)
	}
	db, err := openDB(databasePath)
	if err != nil {
		return err
	}
	defer func() {
		retErr = errors.Join(retErr, db.Close())
	}()
	if err := os.Chmod(databasePath, 0o600); err != nil {
		return fmt.Errorf("secure metadata database: %w", err)
	}
	if err := migrate(ctx, db, metadataMigrations()); err != nil {
		return err
	}
	return nil
}

// OpenForServe migrates an initialized data directory and retains its lock.
func OpenForServe(ctx context.Context, dataDir string) (*Store, error) {
	return openForServe(ctx, dataDir, metadataMigrations())
}

// OpenForAdmin opens an initialized data directory for local user management.
func OpenForAdmin(ctx context.Context, dataDir string) (*Store, error) {
	return openForServe(ctx, dataDir, metadataMigrations())
}

func openForServe(ctx context.Context, dataDir string, migrations []migration) (*Store, error) {
	info, err := os.Stat(dataDir)
	if err != nil {
		return nil, fmt.Errorf("open data directory: %w", err)
	}
	if !info.IsDir() {
		return nil, errors.New("data directory is not a directory")
	}

	lock, err := openDataLock(dataDir, false)
	if err != nil {
		return nil, err
	}
	if err := lock.exclusive(); err != nil {
		return nil, errors.Join(err, lock.Close())
	}

	if err := ensureLayout(dataDir, syncDirectory); err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	db, err := openDB(filepath.Join(dataDir, _databaseName))
	if err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	if err := migrate(ctx, db, migrations); err != nil {
		return nil, errors.Join(err, db.Close(), lock.Close())
	}
	if err := lock.shared(); err != nil {
		return nil, errors.Join(err, db.Close(), lock.Close())
	}

	return &Store{
		db:                  db,
		objectsDir:          filepath.Join(dataDir, _objectsName),
		lock:                lock,
		objectLocks:         make(map[string]*objectPublication),
		syncObjectDirectory: syncDirectory,
		upload: uploadState{
			config: DefaultUploadConfig(),
			users:  make(map[string]int),
		},
	}, nil
}

// DB returns the metadata database used by readiness checks.
func (s *Store) DB() *sql.DB {
	return s.db
}

// ObjectsDir returns the local object directory.
func (s *Store) ObjectsDir() string {
	return s.objectsDir
}

// CheckReady verifies metadata access, object storage writes, and the held data-directory lock.
func (s *Store) CheckReady(ctx context.Context) error {
	if err := s.lock.check(); err != nil {
		return fmt.Errorf("check data-directory lock: %w", err)
	}
	var version int
	if err := s.db.QueryRowContext(ctx,
		"SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1").Scan(&version); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	probe, err := os.CreateTemp(s.objectsDir, ".ready-*")
	if err != nil {
		return fmt.Errorf("create object storage probe: %w", err)
	}
	name := probe.Name()
	if err := probe.Close(); err != nil {
		return errors.Join(fmt.Errorf("close object storage probe: %w", err), removeReadyProbe(name))
	}
	return removeReadyProbe(name)
}

func removeReadyProbe(name string) error {
	if err := os.Remove(name); err != nil {
		return fmt.Errorf("remove object storage probe: %w", err)
	}
	return nil
}

// Close closes the metadata database and releases the data-directory lock.
func (s *Store) Close() error {
	return errors.Join(s.db.Close(), s.lock.Close())
}

func ensureLayout(dataDir string, syncDir func(string) error) error {
	for _, name := range []string{_objectsName, _tmpName} {
		path := filepath.Join(dataDir, name)
		if err := os.MkdirAll(path, 0o700); err != nil {
			return fmt.Errorf("create %s directory: %w", name, err)
		}
		if err := os.Chmod(path, 0o700); err != nil {
			return fmt.Errorf("secure %s directory: %w", name, err)
		}
	}
	if err := syncDir(dataDir); err != nil {
		return fmt.Errorf("sync data directory layout: %w", err)
	}
	return nil
}

func openDB(path string) (*sql.DB, error) {
	u := &url.URL{Scheme: "file", Path: sqliteURLPath(path)}
	query := u.Query()
	query.Add("_pragma", "foreign_keys(1)")
	query.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", _busyTimeoutMillis))
	u.RawQuery = query.Encode()

	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, fmt.Errorf("open metadata database: %w", err)
	}
	db.SetMaxOpenConns(4)
	if err := db.Ping(); err != nil {
		return nil, errors.Join(fmt.Errorf("ping metadata database: %w", err), db.Close())
	}
	return db, nil
}

func sqliteURLPath(path string) string {
	value := filepath.ToSlash(path)
	if filepath.VolumeName(path) != "" && !strings.HasPrefix(value, "/") {
		return "/" + value
	}
	return value
}

type migration struct {
	version    int
	statements []string
}

func migrate(ctx context.Context, db *sql.DB, migrations []migration) (retErr error) {
	latest := 0
	for _, migration := range migrations {
		if migration.version <= latest {
			return errors.New("metadata migrations are not strictly ordered")
		}
		latest = migration.version
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin metadata migration: %w", err)
	}
	defer func() {
		if retErr == nil {
			return
		}
		if rollbackErr := tx.Rollback(); !errors.Is(rollbackErr, sql.ErrTxDone) {
			retErr = errors.Join(retErr, rollbackErr)
		}
	}()

	if _, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY NOT NULL,
			applied_at TEXT NOT NULL
		)`); err != nil {
		return fmt.Errorf("create migration table: %w", err)
	}

	var current int
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&current); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	for _, migration := range migrations {
		if migration.version <= current {
			continue
		}
		for _, statement := range migration.statements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("apply metadata migration %d: %w", migration.version, err)
			}
		}
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
			migration.version, time.Now().UTC().Format(time.RFC3339)); err != nil {
			return fmt.Errorf("record metadata migration %d: %w", migration.version, err)
		}
	}
	if current > latest {
		return fmt.Errorf("metadata schema version %d is newer than supported version %d", current, latest)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit metadata migration: %w", err)
	}
	return nil
}
