package storage

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMetadataMigrationsReturnsFreshData(t *testing.T) {
	first := metadataMigrations()
	first[0].version = 99
	if got := metadataMigrations()[0].version; got != 1 {
		t.Fatalf("fresh migration version = %d, want 1", got)
	}
}

func TestEnsureLayoutSyncsDataDirectoryAfterCreatingObjectRoot(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := os.Mkdir(dataDir, 0o700); err != nil {
		t.Fatalf("Mkdir data: %v", err)
	}
	var synced []string
	syncDir := func(path string) error {
		if _, err := os.Stat(filepath.Join(dataDir, _objectsName)); err != nil {
			t.Fatalf("objects directory did not exist before sync: %v", err)
		}
		synced = append(synced, path)
		return nil
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := ensureLayout(dataDir, syncDir); err != nil {
			t.Fatalf("ensureLayout attempt %d: %v", attempt, err)
		}
	}
	if len(synced) != 2 || synced[0] != dataDir || synced[1] != dataDir {
		t.Fatalf("synced paths = %v, want data directory twice", synced)
	}
}

func TestInitCreatesLayoutAndSchemaIdempotently(t *testing.T) {
	ctx := t.Context()
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := Init(ctx, dataDir); err != nil {
		t.Fatalf("first Init: %v", err)
	}
	if err := Init(ctx, dataDir); err != nil {
		t.Fatalf("second Init: %v", err)
	}

	for _, name := range []string{_objectsName, _tmpName} {
		info, err := os.Stat(filepath.Join(dataDir, name))
		if err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
		if !info.IsDir() {
			t.Errorf("%s is not a directory", name)
		}
		if got := info.Mode().Perm(); got != 0o700 {
			t.Errorf("%s mode = %o, want 700", name, got)
		}
	}
	for _, name := range []string{_databaseName, _lockName} {
		info, err := os.Stat(filepath.Join(dataDir, name))
		if err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
		if got := info.Mode().Perm(); got != 0o600 {
			t.Errorf("%s mode = %o, want 600", name, got)
		}
	}

	db, err := openDB(filepath.Join(dataDir, _databaseName))
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer closeDB(t, db)

	rows, err := db.Query(`
		SELECT name
		FROM sqlite_schema
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
		ORDER BY name`)
	if err != nil {
		t.Fatalf("query tables: %v", err)
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table: %v", err)
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate tables: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close table rows: %v", err)
	}
	if got := strings.Join(tables, ","); got != "access_tokens,libraries,schema_migrations,users" {
		t.Fatalf("tables = %q, want library schema", got)
	}

	var count, version int
	if err := db.QueryRow("SELECT COUNT(*), MAX(version) FROM schema_migrations").Scan(&count, &version); err != nil {
		t.Fatalf("query schema version: %v", err)
	}
	if count != 3 || version != 3 {
		t.Fatalf("migration rows = %d, version = %d; want 3, 3", count, version)
	}
}

func TestInitRepairsPermissiveModes(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := Init(t.Context(), dataDir); err != nil {
		t.Fatalf("first Init: %v", err)
	}

	paths := []struct {
		path       string
		permissive os.FileMode
		want       os.FileMode
	}{
		{path: dataDir, permissive: 0o777, want: 0o700},
		{path: filepath.Join(dataDir, _objectsName), permissive: 0o777, want: 0o700},
		{path: filepath.Join(dataDir, _tmpName), permissive: 0o777, want: 0o700},
		{path: filepath.Join(dataDir, _databaseName), permissive: 0o666, want: 0o600},
		{path: filepath.Join(dataDir, _lockName), permissive: 0o666, want: 0o600},
	}
	for _, item := range paths {
		if err := os.Chmod(item.path, item.permissive); err != nil {
			t.Fatalf("make %s permissive: %v", item.path, err)
		}
	}

	if err := Init(t.Context(), dataDir); err != nil {
		t.Fatalf("repairing Init: %v", err)
	}
	for _, item := range paths {
		info, err := os.Stat(item.path)
		if err != nil {
			t.Fatalf("stat %s: %v", item.path, err)
		}
		if got := info.Mode().Perm(); got != item.want {
			t.Errorf("%s mode = %o, want %o", item.path, got, item.want)
		}
	}
}

func TestMigrateRollsBackFailedVersion(t *testing.T) {
	ctx := t.Context()
	db, err := openDB(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer closeDB(t, db)

	first := []migration{{version: 1, statements: []string{
		"CREATE TABLE retained (id INTEGER PRIMARY KEY)",
	}}}
	if err := migrate(ctx, db, first); err != nil {
		t.Fatalf("apply first migration: %v", err)
	}

	failed := append(first, migration{version: 2, statements: []string{
		"CREATE TABLE rolled_back (id INTEGER PRIMARY KEY)",
		"not valid SQL",
	}})
	if err := migrate(ctx, db, failed); err == nil {
		t.Fatal("failed migration unexpectedly succeeded")
	}

	var version int
	if err := db.QueryRow("SELECT MAX(version) FROM schema_migrations").Scan(&version); err != nil {
		t.Fatalf("query schema version: %v", err)
	}
	if version != 1 {
		t.Fatalf("schema version = %d, want 1", version)
	}
	if tableExists(t, db, "rolled_back") {
		t.Fatal("table from failed migration was not rolled back")
	}
	if !tableExists(t, db, "retained") {
		t.Fatal("previously committed migration was lost")
	}
}

func TestMigrateRejectsUnorderedVersions(t *testing.T) {
	db, err := openDB(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer closeDB(t, db)

	for _, migrations := range [][]migration{
		{{version: 1}, {version: 1}},
		{{version: 2}, {version: 1}},
		{{version: 0}},
	} {
		if err := migrate(t.Context(), db, migrations); err == nil || !strings.Contains(err.Error(), "strictly ordered") {
			t.Fatalf("migrate(%v) error = %v, want ordering error", migrations, err)
		}
	}
	if tableExists(t, db, "schema_migrations") {
		t.Fatal("ordering validation mutated the database")
	}
}

func TestMigrateRejectsNewerSchemaWithoutMutation(t *testing.T) {
	ctx := t.Context()
	db, err := openDB(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer closeDB(t, db)

	if err := migrate(ctx, db, metadataMigrations()); err != nil {
		t.Fatalf("apply supported migration: %v", err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE newer_feature (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create newer schema: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		"INSERT INTO schema_migrations(version, applied_at) VALUES (4, 'test')"); err != nil {
		t.Fatalf("record newer schema: %v", err)
	}
	before := schemaSnapshot(t, db)

	err = migrate(ctx, db, metadataMigrations())
	if err == nil || !strings.Contains(err.Error(), "newer than supported") {
		t.Fatalf("migrate newer schema error = %v, want unsupported version error", err)
	}

	var count, version int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*), MAX(version) FROM schema_migrations").Scan(&count, &version); err != nil {
		t.Fatalf("query schema version: %v", err)
	}
	if count != 4 || version != 4 {
		t.Fatalf("migration rows = %d, version = %d; want 4, 4", count, version)
	}
	if after := schemaSnapshot(t, db); after != before {
		t.Fatalf("schema changed after rejection:\nbefore: %s\nafter:  %s", before, after)
	}
}

func TestOpenDBAppliesPragmasToEveryConnection(t *testing.T) {
	db, err := openDB(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer closeDB(t, db)

	ctx := t.Context()
	connections := make([]*sql.Conn, 0, 4)
	for range 4 {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("acquire connection: %v", err)
		}
		connections = append(connections, conn)
	}
	defer func() {
		for _, conn := range connections {
			if err := conn.Close(); err != nil {
				t.Errorf("close connection: %v", err)
			}
		}
	}()

	for i, conn := range connections {
		var foreignKeys, busyTimeout int
		if err := conn.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
			t.Fatalf("connection %d foreign_keys: %v", i, err)
		}
		if err := conn.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&busyTimeout); err != nil {
			t.Fatalf("connection %d busy_timeout: %v", i, err)
		}
		if foreignKeys != 1 || busyTimeout != _busyTimeoutMillis {
			t.Errorf("connection %d pragmas = foreign_keys:%d busy_timeout:%d", i, foreignKeys, busyTimeout)
		}
	}
}

func TestOpenForServeAppliesPendingMigration(t *testing.T) {
	ctx := t.Context()
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := Init(ctx, dataDir); err != nil {
		t.Fatalf("Init: %v", err)
	}

	migrations := append(metadataMigrations(), migration{
		version:    4,
		statements: []string{"CREATE TABLE migrated_on_serve (id INTEGER PRIMARY KEY)"},
	})
	store, err := openForServe(ctx, dataDir, migrations)
	if err != nil {
		t.Fatalf("OpenForServe: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	}()

	if !tableExists(t, store.DB(), "migrated_on_serve") {
		t.Fatal("serve did not apply pending migration")
	}
	var version int
	if err := store.DB().QueryRow("SELECT MAX(version) FROM schema_migrations").Scan(&version); err != nil {
		t.Fatalf("query schema version: %v", err)
	}
	if version != 4 {
		t.Fatalf("schema version = %d, want 4", version)
	}
}

func TestOpenForServeRejectsConflictBeforeMigration(t *testing.T) {
	ctx := t.Context()
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := Init(ctx, dataDir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	store, err := OpenForServe(ctx, dataDir)
	if err != nil {
		t.Fatalf("first OpenForServe: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	}()

	migrations := append(metadataMigrations(), migration{
		version:    3,
		statements: []string{"CREATE TABLE must_not_exist (id INTEGER PRIMARY KEY)"},
	})
	_, err = openForServe(ctx, dataDir, migrations)
	if err == nil || !strings.Contains(err.Error(), "locked by another process") {
		t.Fatalf("second OpenForServe error = %v, want lock conflict", err)
	}
	if tableExists(t, store.DB(), "must_not_exist") {
		t.Fatal("conflicting serve applied a migration before acquiring the lock")
	}
}

func tableExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var count int
	if err := db.QueryRow(
		"SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = ?", name,
	).Scan(&count); err != nil {
		t.Fatalf("query table %s: %v", name, err)
	}
	return count != 0
}

func schemaSnapshot(t *testing.T, db *sql.DB) string {
	t.Helper()
	var snapshot string
	if err := db.QueryRowContext(t.Context(), `
		SELECT COALESCE(group_concat(type || ':' || name || ':' || COALESCE(sql, ''), '|'), '')
		FROM (
			SELECT type, name, sql
			FROM sqlite_schema
			WHERE name NOT LIKE 'sqlite_%'
			ORDER BY type, name
		)`).Scan(&snapshot); err != nil {
		t.Fatalf("snapshot schema: %v", err)
	}
	return snapshot
}

func closeDB(t *testing.T, db *sql.DB) {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Errorf("close database: %v", err)
	}
}
