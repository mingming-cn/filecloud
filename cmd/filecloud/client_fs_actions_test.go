package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mingming-cn/filecloud/internal/acceptance"
	"github.com/mingming-cn/filecloud/internal/fscompat"
	libraryapi "github.com/mingming-cn/filecloud/internal/library"
	"github.com/mingming-cn/filecloud/internal/object"
)

func TestClientDBUsesDurableWAL(t *testing.T) {
	clientDir := t.TempDir()
	db, err := initializeClientDB(t.Context(), clientDir, syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var journal string
	var synchronous int
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&journal); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("PRAGMA synchronous").Scan(&synchronous); err != nil {
		t.Fatal(err)
	}
	if journal != "wal" || synchronous != 2 {
		t.Fatalf("journal_mode=%q synchronous=%d", journal, synchronous)
	}
}

func TestRecoveryVisibleLeafPathBoundaries(t *testing.T) {
	const actionID = "0123456789abcdef0123456789abcdef"
	t.Run("preferred", func(t *testing.T) {
		name, err := recoveryVisibleLeaf("", actionID, 1)
		if err != nil || name != "Filecloud recovered 0123456789ab" || !validRecoveryVisibleName(name) {
			t.Fatalf("name=%q err=%v", name, err)
		}
		if !validRecoveryVisibleName(strings.Repeat("n", 240)) || validRecoveryVisibleName(strings.Repeat("n", 241)) {
			t.Fatal("visible recovery segment boundary was not enforced")
		}
	})
	t.Run("exact 1024", func(t *testing.T) {
		parent := strings.Repeat("p", 1018)
		name, err := recoveryVisibleLeaf(parent, actionID, 1)
		if err != nil || len(parent)+1+len(name) != 1024 || len(name) != 5 || !validRecoveryVisibleName(name) {
			t.Fatalf("parent=%d name=%q path=%d err=%v", len(parent), name, len(parent)+1+len(name), err)
		}
	})
	t.Run("over 1024", func(t *testing.T) {
		if name, err := recoveryVisibleLeaf(strings.Repeat("p", 1024), actionID, 1); err == nil || name != "" {
			t.Fatalf("name=%q err=%v", name, err)
		}
	})
	t.Run("deep successors", func(t *testing.T) {
		parent := strings.Repeat("p", 1020)
		seen := make(map[string]bool)
		for counter := 1; counter <= 100; counter++ {
			name, err := recoveryVisibleLeaf(parent, actionID, counter)
			if err != nil || len(name) != 3 || seen[name] || len(parent)+1+len(name) != 1024 || !validRecoveryVisibleName(name) {
				t.Fatalf("counter=%d name=%q duplicate=%v err=%v", counter, name, seen[name], err)
			}
			seen[name] = true
		}
	})
	t.Run("bounded exhaustion", func(t *testing.T) {
		parent := strings.Repeat("p", 1022)
		if _, err := recoveryVisibleLeaf(parent, actionID, 62); err != nil {
			t.Fatal(err)
		}
		if name, err := recoveryVisibleLeaf(parent, actionID, 63); err == nil || name != "" {
			t.Fatalf("name=%q err=%v", name, err)
		}
	})
}

func TestFSActionSchemaAndValidation(t *testing.T) {
	clientDir := t.TempDir()
	db, err := initializeClientDB(t.Context(), clientDir, syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, table := range []string{"fs_journal_bindings", "fs_actions"} {
		var found int
		if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&found); err != nil || found != 1 {
			t.Fatalf("table %s found=%d err=%v", table, found, err)
		}
	}
	for _, column := range []string{"internal_source", "internal_target", "action_outcome", "origin_action_id", "attempt"} {
		var found int
		if err := db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('fs_actions') WHERE name=?", column).Scan(&found); err != nil || found != 1 {
			t.Fatalf("column %s found=%d err=%v", column, found, err)
		}
	}
	valid := fsAction{Worktree: "/work", ActionID: "0123456789abcdef0123456789abcdef", Order: 1, Phase: fsPhasePreBase,
		Op: fsOpRename, Parent: "a/b", ParentDevice: 1, ParentInode: 2, Source: "old", Target: "new",
		ExpectedKind: "File", ExpectedDevice: 1, ExpectedInode: 3, State: fsStateIntent}
	if err := validateFSAction(valid); err != nil {
		t.Fatal(err)
	}
	bad := []fsAction{valid, valid, valid, valid, valid, {
		Worktree: "/work", ActionID: "fedcba9876543210fedcba9876543210", Order: 2, Phase: fsPhasePreBase,
		Op: fsOpCreateFile, ParentDevice: 1, ParentInode: 2, Source: fsActionInternalPrefix + "00112233445566778899aabbccddeeff",
		ExpectedKind: "File", InternalSource: fsActionInternalPrefix + "00112233445566778899aabbccddeeff", State: fsStateCompleted,
	}}
	bad[0].Parent = "../escape"
	bad[1].Source = "a/b"
	bad[2].Target = ".filecloud-internal-user"
	bad[3].Source = ".filecloud-internal-bad"
	bad[3].InternalSource = ".filecloud-internal-bad"
	bad[4].State = "done"
	for _, value := range bad {
		if err := validateFSAction(value); err == nil {
			t.Fatalf("accepted corrupt action: %+v", value)
		}
	}
}

const legacyClientV12FixtureSQL = `
CREATE TABLE bindings (server_url TEXT NOT NULL, library_id TEXT NOT NULL, worktree TEXT PRIMARY KEY NOT NULL,
 user_id TEXT NOT NULL, device_id TEXT NOT NULL, sync_base_commit TEXT NOT NULL, sync_base_root TEXT NOT NULL,
 head_etag TEXT NOT NULL, access_token BLOB NOT NULL, UNIQUE(server_url, library_id));
CREATE TABLE bind_intents (server_url TEXT NOT NULL, library_id TEXT NOT NULL, worktree TEXT PRIMARY KEY NOT NULL,
 user_id TEXT NOT NULL, device_id TEXT NOT NULL, expected_etag TEXT NOT NULL, candidate_commit TEXT NOT NULL,
 candidate_root TEXT NOT NULL, candidate_data BLOB NOT NULL, import_local INTEGER NOT NULL DEFAULT 0, UNIQUE(server_url, library_id));
CREATE TABLE pending_checkouts (server_url TEXT NOT NULL, library_id TEXT NOT NULL, worktree TEXT PRIMARY KEY NOT NULL,
 user_id TEXT NOT NULL, device_id TEXT NOT NULL, target_commit TEXT NOT NULL, target_root TEXT NOT NULL,
 head_etag TEXT NOT NULL, apply_state TEXT NOT NULL DEFAULT 'pending', UNIQUE(server_url, library_id));
CREATE TABLE pending_publications (worktree TEXT PRIMARY KEY NOT NULL, base_commit TEXT NOT NULL, base_root TEXT NOT NULL,
 expected_etag TEXT NOT NULL, candidate_commit TEXT NOT NULL, candidate_root TEXT NOT NULL, candidate_data BLOB NOT NULL);
CREATE TABLE sync_recoveries (worktree TEXT NOT NULL, path TEXT NOT NULL, recovery_name TEXT NOT NULL, type TEXT NOT NULL,
 object_id TEXT NOT NULL DEFAULT '', canonical_mtime TEXT NOT NULL DEFAULT '', size INTEGER NOT NULL DEFAULT 0,
 device INTEGER NOT NULL DEFAULT 0, inode INTEGER NOT NULL DEFAULT 0, completed INTEGER NOT NULL DEFAULT 0,
 tombstone_name TEXT NOT NULL DEFAULT '', PRIMARY KEY(worktree, path));
CREATE TABLE checkout_paths (worktree TEXT NOT NULL, path TEXT NOT NULL, type TEXT NOT NULL, object_id TEXT NOT NULL,
 canonical_mtime TEXT NOT NULL, actual_mtime TEXT NOT NULL DEFAULT '', size INTEGER NOT NULL DEFAULT 0,
 temp_name TEXT NOT NULL DEFAULT '', temp_device INTEGER NOT NULL DEFAULT 0, temp_inode INTEGER NOT NULL DEFAULT 0,
 target_device INTEGER NOT NULL DEFAULT 0, target_inode INTEGER NOT NULL DEFAULT 0, completed INTEGER NOT NULL DEFAULT 0,
 rollback_name TEXT NOT NULL DEFAULT '', PRIMARY KEY(worktree, path));
CREATE TABLE path_index (worktree TEXT NOT NULL, path TEXT NOT NULL, type TEXT NOT NULL, object_id TEXT NOT NULL,
 canonical_mtime TEXT NOT NULL, actual_mtime TEXT NOT NULL, size INTEGER NOT NULL, PRIMARY KEY(worktree, path));`

func TestClientLegacyV12FingerprintBeforeDDL(t *testing.T) {
	openFixture := func(t *testing.T, schema string) *sql.DB {
		t.Helper()
		db, err := openClientDB(filepath.Join(t.TempDir(), _clientDatabaseName), false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(schema); err != nil {
			db.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		return db
	}
	t.Run("valid", func(t *testing.T) {
		db := openFixture(t, legacyClientV12FixtureSQL)
		if err := initializeClientSchema(t.Context(), db); err != nil {
			t.Fatal(err)
		}
		var versions int
		if err := db.QueryRow("SELECT COUNT(*) FROM client_schema_migrations").Scan(&versions); err != nil || versions != _clientSchemaVersion-12 {
			t.Fatalf("versions=%d err=%v", versions, err)
		}
	})
	cases := []struct{ name, schema string }{
		{"partial bindings", `CREATE TABLE bindings(x TEXT)`},
		{"missing table", strings.Replace(legacyClientV12FixtureSQL, "CREATE TABLE path_index", "CREATE TABLE missing_path_index", 1)},
		{"missing column", strings.Replace(legacyClientV12FixtureSQL, "target_inode INTEGER NOT NULL DEFAULT 0,", "", 1)},
		{"extra table", legacyClientV12FixtureSQL + `; CREATE TABLE extra(value TEXT)`},
		{"extra view", legacyClientV12FixtureSQL + `; CREATE VIEW extra_view AS SELECT worktree FROM bindings`},
		{"extra trigger", legacyClientV12FixtureSQL + `; CREATE TRIGGER extra_trigger AFTER INSERT ON bindings BEGIN SELECT 1; END`},
		{"extra index", legacyClientV12FixtureSQL + `; CREATE INDEX extra_index ON bindings(user_id)`},
		{"altered default", strings.Replace(legacyClientV12FixtureSQL, "apply_state TEXT NOT NULL DEFAULT 'pending'", "apply_state TEXT NOT NULL DEFAULT 'bad'", 1)},
		{"altered index", strings.Replace(legacyClientV12FixtureSQL, ", UNIQUE(server_url, library_id));", ");", 1)},
		{"altered collate", strings.Replace(legacyClientV12FixtureSQL, "server_url TEXT NOT NULL", "server_url TEXT NOT NULL COLLATE NOCASE", 1)},
		{"altered conflict", strings.Replace(legacyClientV12FixtureSQL, "UNIQUE(server_url, library_id)", "UNIQUE(server_url, library_id) ON CONFLICT IGNORE", 1)},
		{"altered check", strings.Replace(legacyClientV12FixtureSQL, "server_url TEXT NOT NULL", "server_url TEXT NOT NULL CHECK(server_url <> '')", 1)},
		{"altered foreign key", strings.Replace(legacyClientV12FixtureSQL, "base_commit TEXT NOT NULL", "base_commit TEXT NOT NULL REFERENCES bindings(worktree)", 1)},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			db := openFixture(t, test.schema)
			var before, databasePath string
			if err := db.QueryRow("SELECT group_concat(name || ':' || sql, char(10)) FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name").Scan(&before); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&databasePath); err != nil {
				t.Fatal(err)
			}
			beforeBytes, err := os.ReadFile(databasePath)
			if err != nil {
				t.Fatal(err)
			}
			if err := initializeClientSchema(t.Context(), db); err == nil {
				t.Fatal("invalid legacy schema was migrated")
			}
			var after string
			if err := db.QueryRow("SELECT group_concat(name || ':' || sql, char(10)) FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name").Scan(&after); err != nil {
				t.Fatal(err)
			}
			afterBytes, err := os.ReadFile(databasePath)
			if err != nil {
				t.Fatal(err)
			}
			if before != after || !bytes.Equal(beforeBytes, afterBytes) {
				t.Fatal("legacy database bytes or schema changed before rejection")
			}
			var migrations int
			if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='client_schema_migrations'").Scan(&migrations); err != nil || migrations != 0 {
				t.Fatalf("migration table=%d err=%v", migrations, err)
			}
		})
	}
}

func TestClientSchemaRejectsFutureVersionAndGapWithoutDDL(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*sql.DB) error
	}{
		{"future", func(db *sql.DB) error {
			_, err := db.Exec("INSERT INTO client_schema_migrations(version) VALUES (?)", _clientSchemaVersion+1)
			return err
		}},
		{"gap", func(db *sql.DB) error {
			_, err := db.Exec("DELETE FROM client_schema_migrations WHERE version = 13")
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, err := initializeClientDB(t.Context(), t.TempDir(), syncDirectory)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Exec("CREATE TABLE migration_sentinel(value TEXT)"); err != nil {
				t.Fatal(err)
			}
			if err := test.mutate(db); err != nil {
				t.Fatal(err)
			}
			var before string
			if err := db.QueryRow("SELECT group_concat(version, ',') FROM client_schema_migrations ORDER BY version").Scan(&before); err != nil {
				t.Fatal(err)
			}
			if err := initializeClientSchema(t.Context(), db); err == nil {
				t.Fatal("invalid migration history was accepted")
			}
			var after string
			if err := db.QueryRow("SELECT group_concat(version, ',') FROM client_schema_migrations ORDER BY version").Scan(&after); err != nil {
				t.Fatal(err)
			}
			var sentinel int
			if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='migration_sentinel'").Scan(&sentinel); err != nil || before != after || sentinel != 1 {
				t.Fatalf("schema changed before=%q after=%q sentinel=%d err=%v", before, after, sentinel, err)
			}
		})
	}
}

func TestFSActionV15ProvenanceMigration(t *testing.T) {
	setup := func(t *testing.T, preserve bool) *sql.DB {
		t.Helper()
		db, err := initializeClientDB(t.Context(), t.TempDir(), syncDirectory)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		if _, err := db.Exec(`PRAGMA foreign_keys=OFF;
			DROP INDEX fs_actions_preserve_attempt;
			DROP INDEX fs_actions_pending;
			ALTER TABLE fs_actions RENAME TO new_fs_actions;
			CREATE TABLE fs_actions (
				worktree TEXT NOT NULL, action_id TEXT NOT NULL, action_order INTEGER NOT NULL,
				phase TEXT NOT NULL, op TEXT NOT NULL, parent_path TEXT NOT NULL, parent_device INTEGER NOT NULL,
				parent_inode INTEGER NOT NULL, source_name TEXT NOT NULL, target_name TEXT NOT NULL,
				expected_kind TEXT NOT NULL, expected_device INTEGER NOT NULL, expected_inode INTEGER NOT NULL,
				expected_object TEXT NOT NULL DEFAULT '', expected_size INTEGER NOT NULL DEFAULT 0,
				expected_mtime TEXT NOT NULL DEFAULT '', internal_name TEXT NOT NULL DEFAULT '',
				internal_source TEXT NOT NULL DEFAULT '', internal_target TEXT NOT NULL DEFAULT '',
				action_outcome TEXT NOT NULL DEFAULT '', state TEXT NOT NULL, PRIMARY KEY(worktree,action_id),
				UNIQUE(worktree,action_order));
			DROP TABLE new_fs_actions;
			DELETE FROM client_schema_migrations WHERE version>=16`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO fs_journal_bindings VALUES('/work',1,2,1);
			INSERT INTO fs_actions VALUES('/work','11111111111111111111111111111111',1,
			'pre_base','create_file','',1,2,'.filecloud-internal-action-00112233445566778899aabbccddeeff','',
			'File',0,0,'',0,'','.filecloud-internal-action-00112233445566778899aabbccddeeff',
			'.filecloud-internal-action-00112233445566778899aabbccddeeff','',?,'intent')`,
			map[bool]string{true: "preserve_unknown"}[preserve]); err != nil {
			t.Fatal(err)
		}
		return db
	}
	t.Run("ordinary rows preserved", func(t *testing.T) {
		db := setup(t, false)
		if err := initializeClientSchema(t.Context(), db); err != nil {
			t.Fatal(err)
		}
		var rows, version, foreignKeys int
		if err := db.QueryRow("SELECT COUNT(*) FROM fs_actions WHERE origin_action_id IS NULL AND attempt=0").Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow("SELECT MAX(version) FROM client_schema_migrations").Scan(&version); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow("PRAGMA foreign_keys").Scan(&foreignKeys); err != nil || rows != 1 || version != _clientSchemaVersion || foreignKeys != 1 {
			t.Fatalf("rows=%d version=%d foreign_keys=%d err=%v", rows, version, foreignKeys, err)
		}
	})
	t.Run("active preserve rejected", func(t *testing.T) {
		db := setup(t, true)
		if err := initializeClientSchema(t.Context(), db); err == nil {
			t.Fatal("ambiguous legacy preserve action was migrated")
		}
		var hasOrigin int
		if err := db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('fs_actions') WHERE name='origin_action_id'").Scan(&hasOrigin); err != nil || hasOrigin != 0 {
			t.Fatalf("failed migration changed schema: origin=%d err=%v", hasOrigin, err)
		}
	})
}

func TestClientV20PendingPublicationFingerprint(t *testing.T) {
	rebuild := func(db *sql.DB, createSQL string) error {
		_, err := db.Exec(`ALTER TABLE pending_publications RENAME TO old_pending_publications`)
		if err != nil {
			return err
		}
		if _, err = db.Exec(createSQL); err != nil {
			return err
		}
		_, err = db.Exec(`INSERT INTO pending_publications SELECT * FROM old_pending_publications;
			DROP TABLE old_pending_publications`)
		return err
	}
	cases := []struct {
		name   string
		mutate func(*sql.DB) error
	}{
		{"removed check", func(db *sql.DB) error {
			return rebuild(db, strings.Replace(clientV20PendingSQL,
				"CHECK(deletion_count >= 0 AND tracked_count >= deletion_count)", "CHECK(deletion_count >= 0)", 1))
		}},
		{"altered check", func(db *sql.DB) error {
			return rebuild(db, strings.Replace(clientV20PendingSQL, "deletion_count > 100", "deletion_count > 101", 1))
		}},
		{"altered default", func(db *sql.DB) error {
			return rebuild(db, strings.Replace(clientV20PendingSQL,
				"deletion_count INTEGER NOT NULL DEFAULT 0", "deletion_count INTEGER NOT NULL DEFAULT 1", 1))
		}},
		{"altered notnull", func(db *sql.DB) error {
			return rebuild(db, strings.Replace(clientV20PendingSQL,
				"tracked_count INTEGER NOT NULL DEFAULT 0", "tracked_count INTEGER DEFAULT 0", 1))
		}},
		{"altered type", func(db *sql.DB) error {
			return rebuild(db, strings.Replace(clientV20PendingSQL,
				"tracked_count INTEGER NOT NULL DEFAULT 0", "tracked_count TEXT NOT NULL DEFAULT 0", 1))
		}},
		{"extra trigger", func(db *sql.DB) error {
			_, err := db.Exec(`CREATE TRIGGER confirm_candidate AFTER UPDATE OF candidate_commit ON pending_publications
				BEGIN UPDATE pending_publications SET delete_confirmed=1 WHERE worktree=NEW.worktree; END`)
			return err
		}},
		{"extra view", func(db *sql.DB) error {
			_, err := db.Exec(`CREATE VIEW pending_candidates AS SELECT worktree,candidate_commit FROM pending_publications`)
			return err
		}},
		{"extra explicit index", func(db *sql.DB) error {
			_, err := db.Exec(`CREATE INDEX pending_candidate_commit ON pending_publications(candidate_commit)`)
			return err
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			clientDir := t.TempDir()
			db, err := initializeClientDB(t.Context(), clientDir, syncDirectory)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Exec(`DROP TABLE pending_publications;
				DELETE FROM client_schema_migrations WHERE version=21`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(clientV20PendingSQL); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO pending_publications VALUES
				('/work','base','root','base','etag','candidate','candidate-root',X'0102',101,101,1,0,0)`); err != nil {
				t.Fatal(err)
			}
			if err := test.mutate(db); err != nil {
				t.Fatal(err)
			}
			var databasePath, beforeSchema, beforeData string
			if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&databasePath); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT group_concat(type || ':' || name || ':' || COALESCE(sql,''), char(10))
				FROM (SELECT type,name,sql FROM sqlite_schema ORDER BY type,name)`).Scan(&beforeSchema); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT quote(worktree)||'|'||quote(candidate_commit)||'|'||delete_confirmed
				FROM pending_publications ORDER BY worktree`).Scan(&beforeData); err != nil {
				t.Fatal(err)
			}
			beforeBytes, err := os.ReadFile(databasePath)
			if err != nil {
				t.Fatal(err)
			}
			if err := initializeClientSchema(t.Context(), db); err == nil {
				t.Fatal("invalid v20 schema was accepted")
			}
			var afterSchema, afterData string
			if err := db.QueryRow(`SELECT group_concat(type || ':' || name || ':' || COALESCE(sql,''), char(10))
				FROM (SELECT type,name,sql FROM sqlite_schema ORDER BY type,name)`).Scan(&afterSchema); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT quote(worktree)||'|'||quote(candidate_commit)||'|'||delete_confirmed
				FROM pending_publications ORDER BY worktree`).Scan(&afterData); err != nil {
				t.Fatal(err)
			}
			afterBytes, err := os.ReadFile(databasePath)
			if err != nil {
				t.Fatal(err)
			}
			if beforeSchema != afterSchema || beforeData != afterData || !bytes.Equal(beforeBytes, afterBytes) {
				t.Fatal("v20 rejection changed schema, data, or database bytes")
			}
			if test.name == "extra trigger" && afterData != "'/work'|'candidate'|0" {
				t.Fatalf("trigger confirmed candidate: %s", afterData)
			}
		})
	}
	t.Run("valid", func(t *testing.T) {
		db, err := initializeClientDB(t.Context(), t.TempDir(), syncDirectory)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if err := initializeClientSchema(t.Context(), db); err != nil {
			t.Fatal(err)
		}
	})
}

func TestClientV21PendingPublicationFingerprint(t *testing.T) {
	rebuild := func(db *sql.DB, createSQL string) error {
		if _, err := db.Exec(`ALTER TABLE pending_publications RENAME TO old_pending_publications`); err != nil {
			return err
		}
		if _, err := db.Exec(createSQL); err != nil {
			return err
		}
		_, err := db.Exec(`DROP TABLE old_pending_publications`)
		return err
	}
	cases := []struct {
		name   string
		mutate func(*sql.DB) error
	}{
		{"removed check", func(db *sql.DB) error {
			return rebuild(db, strings.Replace(_clientV21PendingSQL,
				"CHECK(deletion_count >= 0 AND tracked_count >= deletion_count)", "CHECK(deletion_count >= 0)", 1))
		}},
		{"altered default", func(db *sql.DB) error {
			return rebuild(db, strings.Replace(_clientV21PendingSQL,
				"deletion_count INTEGER NOT NULL DEFAULT 0", "deletion_count INTEGER NOT NULL DEFAULT 1", 1))
		}},
		{"altered nullability", func(db *sql.DB) error {
			return rebuild(db, strings.Replace(_clientV21PendingSQL, "captured_data BLOB NOT NULL", "captured_data BLOB", 1))
		}},
		{"altered type", func(db *sql.DB) error {
			return rebuild(db, strings.Replace(_clientV21PendingSQL, "captured_data BLOB NOT NULL", "captured_data TEXT NOT NULL", 1))
		}},
		{"altered captured default", func(db *sql.DB) error {
			return rebuild(db, strings.Replace(_clientV21PendingSQL, "captured_data BLOB NOT NULL", "captured_data BLOB NOT NULL DEFAULT X''", 1))
		}},
		{"altered history nullability", func(db *sql.DB) error {
			return rebuild(db, strings.Replace(_clientV21PendingSQL, "candidate_history BLOB NOT NULL", "candidate_history BLOB", 1))
		}},
		{"altered history type", func(db *sql.DB) error {
			return rebuild(db, strings.Replace(_clientV21PendingSQL, "candidate_history BLOB NOT NULL", "candidate_history TEXT NOT NULL", 1))
		}},
		{"altered history default", func(db *sql.DB) error {
			return rebuild(db, strings.Replace(_clientV21PendingSQL, "candidate_history BLOB NOT NULL", "candidate_history BLOB NOT NULL DEFAULT X''", 1))
		}},
		{"intermediate schema without history", func(db *sql.DB) error {
			createSQL := strings.Replace(_clientV21PendingSQL, ", candidate_history BLOB NOT NULL", "", 1)
			createSQL = strings.Replace(createSQL, "\n\tCHECK(length(candidate_history) BETWEEN 8 AND 67112968),", "", 1)
			return rebuild(db, createSQL)
		}},
		{"extra trigger", func(db *sql.DB) error {
			_, err := db.Exec(`CREATE TRIGGER captured_update AFTER UPDATE ON pending_publications BEGIN SELECT 1; END`)
			return err
		}},
		{"extra view", func(db *sql.DB) error {
			_, err := db.Exec(`CREATE VIEW captured_publications AS SELECT captured_commit FROM pending_publications`)
			return err
		}},
		{"extra index", func(db *sql.DB) error {
			_, err := db.Exec(`CREATE INDEX captured_commit_index ON pending_publications(captured_commit)`)
			return err
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			db, err := initializeClientDB(t.Context(), t.TempDir(), syncDirectory)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err := test.mutate(db); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
				t.Fatal(err)
			}
			var databasePath, beforeSchema string
			if err := db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&databasePath); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT group_concat(type || ':' || name || ':' || COALESCE(sql,''), char(10))
				FROM (SELECT type,name,sql FROM sqlite_schema ORDER BY type,name)`).Scan(&beforeSchema); err != nil {
				t.Fatal(err)
			}
			beforeBytes, err := os.ReadFile(databasePath)
			if err != nil {
				t.Fatal(err)
			}
			if err := initializeClientSchema(t.Context(), db); err == nil {
				t.Fatal("invalid v21 schema was accepted")
			}
			var afterSchema string
			if err := db.QueryRow(`SELECT group_concat(type || ':' || name || ':' || COALESCE(sql,''), char(10))
				FROM (SELECT type,name,sql FROM sqlite_schema ORDER BY type,name)`).Scan(&afterSchema); err != nil {
				t.Fatal(err)
			}
			afterBytes, err := os.ReadFile(databasePath)
			if err != nil {
				t.Fatal(err)
			}
			if beforeSchema != afterSchema || !bytes.Equal(beforeBytes, afterBytes) {
				t.Fatal("v21 rejection changed schema or database bytes")
			}
			var version int
			if err := db.QueryRow(`SELECT MAX(version) FROM client_schema_migrations`).Scan(&version); err != nil || version != _clientSchemaVersion {
				t.Fatalf("schema rejection changed version=%d err=%v", version, err)
			}
		})
	}
}

func TestPendingPublicationBlobLimits(t *testing.T) {
	for _, test := range []struct {
		name, candidate, captured, history string
	}{
		{"candidate_data", "zeroblob(65537)", "X'01'", "X'4643483100000000'"},
		{"captured_data", "X'01'", "zeroblob(65537)", "X'4643483100000000'"},
		{"candidate_history", "X'01'", "X'01'", "zeroblob(67112969)"},
	} {
		t.Run("current load/"+test.name, func(t *testing.T) {
			db, err := initializeClientDB(t.Context(), t.TempDir(), syncDirectory)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			statement := `PRAGMA ignore_check_constraints=ON;
				INSERT INTO pending_publications(worktree,publication_kind,base_commit,base_root,expected_head,expected_etag,
					candidate_commit,candidate_root,candidate_data,captured_commit,captured_root,captured_data,candidate_history,
					deletion_count,tracked_count,requires_delete_confirmation,delete_confirmed,legacy_revalidation_required)
				VALUES('/work','sync','base','root','base','etag','candidate','root',` + test.candidate +
				`,'candidate','root',` + test.captured + `,` + test.history + `,0,0,0,0,0)`
			if _, err := db.Exec(statement); err != nil {
				t.Fatal(err)
			}
			if pending, err := loadPendingPublication(t.Context(), db, "/work"); err == nil || pending != nil ||
				!strings.Contains(err.Error(), "exceeds synchronization budget") {
				t.Fatalf("oversized pending load=%+v err=%v", pending, err)
			}
			var rows int
			if err := db.QueryRow(`SELECT COUNT(*) FROM pending_publications WHERE worktree='/work'`).Scan(&rows); err != nil || rows != 1 {
				t.Fatalf("oversized load changed row count=%d err=%v", rows, err)
			}
		})
	}

	t.Run("v20 migration preflight", func(t *testing.T) {
		clientDir := t.TempDir()
		db, err := initializeClientDB(t.Context(), clientDir, syncDirectory)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if _, err := db.Exec(`DROP TABLE pending_publications;
			DELETE FROM client_schema_migrations WHERE version>=21`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(clientV20PendingSQL); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO pending_publications VALUES
			('/work','base','root','base','etag','candidate','root',zeroblob(65537),0,0,0,0,0);
			PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
			t.Fatal(err)
		}
		var databasePath, beforeSchema string
		if err := db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&databasePath); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT group_concat(type || ':' || name || ':' || COALESCE(sql,''), char(10))
			FROM (SELECT type,name,sql FROM sqlite_schema ORDER BY type,name)`).Scan(&beforeSchema); err != nil {
			t.Fatal(err)
		}
		beforeBytes, err := os.ReadFile(databasePath)
		if err != nil {
			t.Fatal(err)
		}
		if err := initializeClientSchema(t.Context(), db); err == nil || !strings.Contains(err.Error(), "exceeds synchronization budget") {
			t.Fatalf("oversized v20 migration error=%v", err)
		}
		var afterSchema string
		if err := db.QueryRow(`SELECT group_concat(type || ':' || name || ':' || COALESCE(sql,''), char(10))
			FROM (SELECT type,name,sql FROM sqlite_schema ORDER BY type,name)`).Scan(&afterSchema); err != nil {
			t.Fatal(err)
		}
		afterBytes, err := os.ReadFile(databasePath)
		if err != nil {
			t.Fatal(err)
		}
		if beforeSchema != afterSchema || !bytes.Equal(beforeBytes, afterBytes) {
			t.Fatal("oversized v20 rejection changed schema or database bytes")
		}
	})
}

func TestPendingPublicationV18ThroughV20CapturedDataMigration(t *testing.T) {
	const (
		worktree      = "/work"
		baseRoot      = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		candidateRoot = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	)
	baseCommit := strings.Repeat("a", 64)
	candidateData, candidateCommit, err := canonicalCommit(testClientUserID, testClientDeviceID, candidateRoot,
		[]string{baseCommit}, func() time.Time { return time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC) })
	if err != nil {
		t.Fatal(err)
	}
	for _, version := range []int{18, 19, 20} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			db, err := initializeClientDB(t.Context(), t.TempDir(), syncDirectory)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Exec(`INSERT INTO bindings(server_url,library_id,worktree,user_id,device_id,sync_base_commit,
				sync_base_root,head_etag,access_token) VALUES(?,?,?,?,?,?,?,?,?)`, "https://example.invalid", testClientLibraryID,
				worktree, testClientUserID, testClientDeviceID, baseCommit, baseRoot, "etag", []byte("token")); err != nil {
				t.Fatal(err)
			}
			createSQL := legacyClientV18PendingSQL
			if version == 19 {
				createSQL = clientV19PendingSQL
			} else if version == 20 {
				createSQL = clientV20PendingSQL
			}
			if _, err := db.Exec(`ALTER TABLE pending_publications RENAME TO new_pending_publications`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(createSQL); err != nil {
				t.Fatal(err)
			}
			var insertErr error
			switch version {
			case 18:
				_, insertErr = db.Exec(`INSERT INTO pending_publications VALUES(?,?,?,?,?,?,?,0,0,0,0)`, worktree,
					baseCommit, baseRoot, "etag", candidateCommit, candidateRoot, candidateData)
			case 19:
				_, insertErr = db.Exec(`INSERT INTO pending_publications VALUES(?,?,?,?,?,?,?,0,0,0,0,0)`, worktree,
					baseCommit, baseRoot, "etag", candidateCommit, candidateRoot, candidateData)
			case 20:
				_, insertErr = db.Exec(`INSERT INTO pending_publications VALUES(?,?,?,?,?,?,?,?,0,0,0,0,0)`, worktree,
					baseCommit, baseRoot, baseCommit, "etag", candidateCommit, candidateRoot, candidateData)
			}
			if insertErr != nil {
				t.Fatal(insertErr)
			}
			if _, err := db.Exec(`DROP TABLE new_pending_publications;
				DELETE FROM client_schema_migrations WHERE version > ?`, version); err != nil {
				t.Fatal(err)
			}
			if err := initializeClientSchema(t.Context(), db); err != nil {
				t.Fatal(err)
			}
			var kind PublicationKind
			var expected, base string
			if err := db.QueryRow(`SELECT publication_kind,expected_head,base_commit FROM pending_publications WHERE worktree=?`, worktree).Scan(
				&kind, &expected, &base); err != nil {
				t.Fatal(err)
			}
			if kind != PublicationKindSync || expected != baseCommit || base != baseCommit {
				t.Fatalf("v%d migration kind=%q expected=%q base=%q", version, kind, expected, base)
			}
			var migratedCommit, migratedRoot string
			var migratedCandidateData, capturedData, history []byte
			if err := db.QueryRow(`SELECT captured_commit,captured_root,candidate_data,captured_data,candidate_history
				FROM pending_publications WHERE worktree=?`, worktree).Scan(&migratedCommit, &migratedRoot, &migratedCandidateData,
				&capturedData, &history); err != nil || migratedCommit != candidateCommit || migratedRoot != candidateRoot ||
				!bytes.Equal(migratedCandidateData, candidateData) || !bytes.Equal(capturedData, candidateData) ||
				!bytes.Equal(history, _emptyCandidateHistory) {
				t.Fatalf("v%d migration captured=%q/%q data=%x/%x history=%x err=%v", version, migratedCommit,
					migratedRoot, migratedCandidateData, capturedData, history, err)
			}
		})
	}
}

func TestClientV22PendingPublicationMigrationRejectsInvalidRowsAtomically(t *testing.T) {
	const (
		worktree = "/work"
		baseRoot = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		otherID  = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	)
	baseCommit := strings.Repeat("a", 64)
	now := func() time.Time { return time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC) }
	candidateData, candidateCommit, err := canonicalCommit(testClientUserID, testClientDeviceID, baseRoot,
		[]string{baseCommit}, now)
	if err != nil {
		t.Fatal(err)
	}
	wrongParentData, wrongParentCommit, err := canonicalCommit(testClientUserID, testClientDeviceID, baseRoot,
		[]string{otherID}, now)
	if err != nil {
		t.Fatal(err)
	}
	wrongOwnerData, wrongOwnerCommit, err := canonicalCommit("11111111-2222-4333-8444-555555555555",
		testClientDeviceID, baseRoot, []string{baseCommit}, now)
	if err != nil {
		t.Fatal(err)
	}
	wrongDeviceData, wrongDeviceCommit, err := canonicalCommit(testClientUserID,
		"11111111-2222-4333-8444-555555555555", baseRoot, []string{baseCommit}, now)
	if err != nil {
		t.Fatal(err)
	}
	nonemptyHistory, err := _encodeCandidateHistory([][]byte{candidateData})
	if err != nil {
		t.Fatal(err)
	}

	readLegacy := func(t *testing.T, db *sql.DB) pendingPublication {
		t.Helper()
		var value pendingPublication
		if err := db.QueryRow(`SELECT base_commit,base_root,expected_head,expected_etag,candidate_commit,candidate_root,
			candidate_data,captured_commit,captured_root,captured_data,candidate_history,deletion_count,tracked_count,
			requires_delete_confirmation,delete_confirmed,legacy_revalidation_required
			FROM pending_publications WHERE worktree=?`, worktree).Scan(&value.BaseCommit, &value.BaseRoot, &value.ExpectedHead,
			&value.ExpectedETag, &value.CandidateCommit, &value.CandidateRoot, &value.CandidateData, &value.CapturedCommit,
			&value.CapturedRoot, &value.CapturedData, &value.CandidateHistory, &value.DeletionCount, &value.TrackedCount,
			&value.RequiresDeleteConfirmation, &value.DeleteConfirmed, &value.LegacyRevalidationRequired); err != nil {
			t.Fatal(err)
		}
		return value
	}
	tests := []struct {
		name      string
		statement string
		args      []any
	}{
		{name: "candidate id does not match body", statement: "UPDATE pending_publications SET candidate_commit=?", args: []any{otherID}},
		{name: "captured id does not match body", statement: "UPDATE pending_publications SET captured_commit=?", args: []any{otherID}},
		{name: "candidate root does not match body", statement: "UPDATE pending_publications SET candidate_root=?", args: []any{otherID}},
		{name: "captured root does not match body", statement: "UPDATE pending_publications SET captured_root=?", args: []any{otherID}},
		{name: "parent does not match binding", statement: `UPDATE pending_publications SET candidate_commit=?,candidate_data=?,
			captured_commit=?,captured_data=?`, args: []any{wrongParentCommit, wrongParentData, wrongParentCommit, wrongParentData}},
		{name: "expected head does not match parent", statement: "UPDATE pending_publications SET expected_head=?", args: []any{otherID}},
		{name: "owner does not match binding", statement: `UPDATE pending_publications SET candidate_commit=?,candidate_data=?,
			captured_commit=?,captured_data=?`, args: []any{wrongOwnerCommit, wrongOwnerData, wrongOwnerCommit, wrongOwnerData}},
		{name: "device does not match binding", statement: `UPDATE pending_publications SET candidate_commit=?,candidate_data=?,
			captured_commit=?,captured_data=?`, args: []any{wrongDeviceCommit, wrongDeviceData, wrongDeviceCommit, wrongDeviceData}},
		{name: "candidate history does not match local candidate", statement: "UPDATE pending_publications SET candidate_history=?", args: []any{nonemptyHistory}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := initializeClientDB(t.Context(), t.TempDir(), syncDirectory)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Exec(`DROP TABLE pending_publications;
				DELETE FROM client_schema_migrations WHERE version>=23`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(_clientV21PendingSQL); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO bindings(server_url,library_id,worktree,user_id,device_id,sync_base_commit,
				sync_base_root,head_etag,access_token) VALUES(?,?,?,?,?,?,?,?,?)`, "https://example.invalid", testClientLibraryID,
				worktree, testClientUserID, testClientDeviceID, baseCommit, baseRoot, "etag", []byte("token")); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO pending_publications(worktree,base_commit,base_root,expected_head,expected_etag,
				candidate_commit,candidate_root,candidate_data,captured_commit,captured_root,captured_data,candidate_history,
				deletion_count,tracked_count,requires_delete_confirmation,delete_confirmed,legacy_revalidation_required)
				VALUES(?,?,?,?,?,?,?,?,?,?,?,?,0,0,0,0,0)`, worktree, baseCommit, baseRoot, baseCommit, "etag",
				candidateCommit, baseRoot, candidateData, candidateCommit, baseRoot, candidateData, _emptyCandidateHistory); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(test.statement, test.args...); err != nil {
				t.Fatal(err)
			}
			before := readLegacy(t, db)
			var beforeSchema string
			if err := db.QueryRow(`SELECT sql FROM sqlite_schema WHERE type='table' AND name='pending_publications'`).Scan(&beforeSchema); err != nil {
				t.Fatal(err)
			}

			if err := initializeClientSchema(t.Context(), db); err == nil {
				t.Fatal("invalid v22 pending publication was migrated")
			}

			after := readLegacy(t, db)
			var afterSchema string
			if err := db.QueryRow(`SELECT sql FROM sqlite_schema WHERE type='table' AND name='pending_publications'`).Scan(&afterSchema); err != nil {
				t.Fatal(err)
			}
			var version, kindColumns int
			if err := db.QueryRow(`SELECT MAX(version) FROM client_schema_migrations`).Scan(&version); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('pending_publications')
				WHERE name='publication_kind'`).Scan(&kindColumns); err != nil {
				t.Fatal(err)
			}
			if beforeSchema != afterSchema || !reflect.DeepEqual(before, after) || version != 22 || kindColumns != 0 {
				t.Fatalf("failed migration changed state: schema=%t row_equal=%t version=%d kind_columns=%d",
					beforeSchema == afterSchema, reflect.DeepEqual(before, after), version, kindColumns)
			}
		})
	}
}

func TestClientV19PendingPublicationFingerprintBeforeMigration(t *testing.T) {
	db, err := initializeClientDB(t.Context(), t.TempDir(), syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`ALTER TABLE pending_publications RENAME TO new_pending_publications`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(clientV19PendingSQL); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE new_pending_publications;
		DELETE FROM client_schema_migrations WHERE version >= 20;
		CREATE VIEW pending_candidates AS SELECT worktree,candidate_commit FROM pending_publications`); err != nil {
		t.Fatal(err)
	}
	if err := initializeClientSchema(t.Context(), db); err == nil || !strings.Contains(err.Error(), "unexpected trigger or view") {
		t.Fatalf("invalid v19 pre-migration schema error=%v", err)
	}
	var version int
	if err := db.QueryRow("SELECT MAX(version) FROM client_schema_migrations").Scan(&version); err != nil || version != 19 {
		t.Fatalf("failed validation changed migration version=%d err=%v", version, err)
	}
}

func TestPendingPublicationV17DeletionConfirmationMigration(t *testing.T) {
	const (
		worktree      = "/work"
		baseRoot      = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		candidateRoot = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	)
	baseCommit := strings.Repeat("a", 64)
	candidateData, candidateCommit, err := canonicalCommit(testClientUserID, testClientDeviceID, candidateRoot,
		[]string{baseCommit}, func() time.Time { return time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC) })
	if err != nil {
		t.Fatal(err)
	}
	db, err := initializeClientDB(t.Context(), t.TempDir(), syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`INSERT INTO bindings(server_url,library_id,worktree,user_id,device_id,sync_base_commit,
		sync_base_root,head_etag,access_token) VALUES(?,?,?,?,?,?,?,?,?)`, "https://example.invalid", testClientLibraryID,
		worktree, testClientUserID, testClientDeviceID, baseCommit, baseRoot, "etag", []byte("token")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER TABLE pending_publications RENAME TO new_pending_publications;
		CREATE TABLE pending_publications (worktree TEXT PRIMARY KEY NOT NULL, base_commit TEXT NOT NULL,
		base_root TEXT NOT NULL, expected_etag TEXT NOT NULL, candidate_commit TEXT NOT NULL,
		candidate_root TEXT NOT NULL, candidate_data BLOB NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO pending_publications VALUES(?,?,?,?,?,?,?)`, worktree, baseCommit, baseRoot,
		"etag", candidateCommit, candidateRoot, candidateData); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE new_pending_publications;
		DELETE FROM client_schema_migrations WHERE version>=18`); err != nil {
		t.Fatal(err)
	}
	if err := initializeClientSchema(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	var columns, version, rows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('pending_publications') WHERE name IN
		('expected_head','captured_commit','captured_root','captured_data','candidate_history','deletion_count','tracked_count',
		'requires_delete_confirmation','delete_confirmed','legacy_revalidation_required')`).Scan(&columns); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT MAX(version) FROM client_schema_migrations").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM pending_publications WHERE worktree='/work' AND expected_head=base_commit
		AND captured_commit=candidate_commit AND captured_root=candidate_root AND captured_data=candidate_data
		AND candidate_history=X'4643483100000000' AND deletion_count=0 AND tracked_count=0
		AND requires_delete_confirmation=0 AND delete_confirmed=0 AND legacy_revalidation_required=1`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if columns != 10 || version != _clientSchemaVersion || rows != 1 {
		t.Fatalf("columns=%d version=%d preserved_rows=%d", columns, version, rows)
	}
	for _, values := range []string{
		"'/bad','base','root','base','etag','candidate','root',X'00','candidate','root',X'00',X'4643483100000000',1,20,0,0,1",
		"'/bad','base','root','base','etag','candidate','root',X'00','candidate','root',X'00',X'4643483100000000',1,20,0,1,0",
	} {
		if _, err := db.Exec("INSERT INTO pending_publications VALUES(" + values + ")"); err == nil {
			t.Fatal("pending publication CHECK accepted inconsistent legacy or confirmation state")
		}
	}
}

func TestCheckoutCreateOriginV16Migration(t *testing.T) {
	const actionID = "11111111111111111111111111111111"
	setup := func(t *testing.T, ambiguous bool) *sql.DB {
		t.Helper()
		db, err := initializeClientDB(t.Context(), t.TempDir(), syncDirectory)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		if _, err := db.Exec(`INSERT INTO fs_journal_bindings VALUES('/work',1,2,1);
			INSERT INTO checkout_paths(worktree,path,type,object_id,canonical_mtime,temp_name)
			VALUES('/work','dir/final','File','object','2000-01-01T00:00:00Z','.filecloud-internal-action-00112233445566778899aabbccddeeff')`); err != nil {
			t.Fatal(err)
		}
		action := fsAction{Worktree: "/work", ActionID: actionID, Order: 1, Phase: fsPhasePreBase, Op: fsOpCreateFile,
			Parent: "dir", ParentDevice: 1, ParentInode: 2, Source: fsActionInternalPrefix + "00112233445566778899aabbccddeeff",
			ExpectedKind: "File", InternalSource: fsActionInternalPrefix + "00112233445566778899aabbccddeeff", State: fsStateIntent}
		if err := insertFSActionIntent(t.Context(), db, action); err != nil {
			t.Fatal(err)
		}
		if ambiguous {
			action.ActionID, action.Order = "22222222222222222222222222222222", 2
			if err := insertFSActionIntent(t.Context(), db, action); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := db.Exec(`ALTER TABLE checkout_paths DROP COLUMN create_action_id;
			DELETE FROM client_schema_migrations WHERE version>=17`); err != nil {
			t.Fatal(err)
		}
		return db
	}
	t.Run("unique active create", func(t *testing.T) {
		db := setup(t, false)
		if err := initializeClientSchema(t.Context(), db); err != nil {
			t.Fatal(err)
		}
		var got string
		if err := db.QueryRow("SELECT create_action_id FROM checkout_paths WHERE worktree='/work' AND path='dir/final'").Scan(&got); err != nil || got != actionID {
			t.Fatalf("create_action_id=%q err=%v", got, err)
		}
	})
	t.Run("ambiguous create", func(t *testing.T) {
		db := setup(t, true)
		if err := initializeClientSchema(t.Context(), db); err == nil {
			t.Fatal("ambiguous creation origins were migrated")
		}
		var column, version int
		if err := db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('checkout_paths') WHERE name='create_action_id'").Scan(&column); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow("SELECT MAX(version) FROM client_schema_migrations").Scan(&version); err != nil || column != 0 || version != 16 {
			t.Fatalf("column=%d version=%d err=%v", column, version, err)
		}
	})
}

func TestFSActionLegacyInternalOwnershipMigration(t *testing.T) {
	clientDir, rootPath := t.TempDir(), t.TempDir()
	db, err := initializeClientDB(t.Context(), clientDir, syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	root, err := openWorktreeRoot(rootPath, func(*os.File) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := bindFSJournalRoot(t.Context(), db, rootPath, root); err != nil {
		t.Fatal(err)
	}
	first := fsActionInternalPrefix + "00112233445566778899aabbccddeeff"
	second := fsActionInternalPrefix + "ffeeddccbbaa99887766554433221100"
	insertRawFSAction(t, db, rootPath, "11111111111111111111111111111111", 1, fsPhaseRollback, fsOpRename,
		"", root.device, root.inode, first, "visible", "File", root.device, root.inode, "", 0, "", first, fsStateIntent)
	if err := initializeClientSchema(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	values, err := loadIntentFSActions(t.Context(), db, rootPath)
	if err != nil || len(values) != 1 || values[0].InternalSource != first || values[0].InternalTarget != "" {
		t.Fatalf("unambiguous legacy action=%+v err=%v", values, err)
	}
	insertRawFSAction(t, db, rootPath, "22222222222222222222222222222222", 2, fsPhasePostBase, fsOpRename,
		"", root.device, root.inode, first, second, "File", root.device, root.inode, "", 0, "", first, fsStateIntent)
	if err := initializeClientSchema(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	if _, err := loadIntentFSActions(t.Context(), db, rootPath); err == nil {
		t.Fatal("ambiguous legacy internal ownership was accepted")
	}
}

func TestFSActionRejectsReplacedParent(t *testing.T) {
	rootPath := t.TempDir()
	if err := os.Mkdir(filepath.Join(rootPath, "parent"), 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := openWorktreeRoot(rootPath, func(*os.File) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	parent, err := openFSActionParent(root, "parent", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	var stat fscompat.Stat_t
	if err := fscompat.Fstat(int(parent.Fd()), &stat); err != nil {
		t.Fatal(err)
	}
	parent.Close()
	if err := os.Rename(filepath.Join(rootPath, "parent"), filepath.Join(rootPath, "old")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(rootPath, "parent"), 0o700); err != nil {
		t.Fatal(err)
	}
	_, err = openFSActionParent(root, "parent", uint64(stat.Dev), stat.Ino)
	if err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("replaced parent error=%v", err)
	}
}

func TestFSActionRejectsSymlinkParentAndCrossMount(t *testing.T) {
	rootPath := t.TempDir()
	if err := os.Mkdir(filepath.Join(rootPath, "real"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := createTestSymlink("real", filepath.Join(rootPath, "link")); err != nil {
		t.Fatal(err)
	}
	root, err := openWorktreeRoot(rootPath, func(*os.File) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if _, err := openFSActionParent(root, "link", 0, 0); err == nil {
		t.Fatal("symlink action parent was accepted")
	}
	old := _openActionParent
	_openActionParent = func(*openedWorktree, string) (int, error) { return -1, testCrossDevice }
	t.Cleanup(func() { _openActionParent = old })
	if _, err := openFSActionParent(root, "real", 0, 0); !errors.Is(err, testCrossDevice) {
		t.Fatalf("cross-mount error=%v", err)
	}
}

func TestFSActionRenameRejectsDifferentExistingInode(t *testing.T) {
	clientDir, rootPath := t.TempDir(), t.TempDir()
	for name, data := range map[string]string{"source": "source", "target": "target"} {
		if err := os.WriteFile(filepath.Join(rootPath, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	db, err := initializeClientDB(t.Context(), clientDir, syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	root, err := openWorktreeRoot(rootPath, func(*os.File) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	var stat fscompat.Stat_t
	if err := fscompat.Fstatat(int(root.directory.Fd()), "source", &stat, fscompat.AT_SYMLINK_NOFOLLOW); err != nil {
		t.Fatal(err)
	}
	action := fsAction{Worktree: rootPath, ActionID: "1234567890abcdef1234567890abcdef", Order: 1,
		Phase: fsPhasePreBase, Op: fsOpRename, Parent: "", ParentDevice: root.device, ParentInode: root.inode,
		Source: "source", Target: "target", ExpectedKind: "File", ExpectedDevice: uint64(stat.Dev), ExpectedInode: stat.Ino,
		State: fsStateIntent}
	if err := executeFSAction(t.Context(), db, root, action, nil); err == nil {
		t.Fatal("different existing target inode was accepted")
	}
	for name, want := range map[string]string{"source": "source", "target": "target"} {
		if data, err := os.ReadFile(filepath.Join(rootPath, name)); err != nil || string(data) != want {
			t.Fatalf("%s=%q err=%v", name, data, err)
		}
	}
}

func TestPublicSyncRejectsRootReplacementAndCorruptJournal(t *testing.T) {
	t.Run("root replacement", func(t *testing.T) {
		state := newImportedBinding(t)
		old := state.worktree + "-old"
		t.Cleanup(func() {
			if err := os.RemoveAll(old); err != nil {
				t.Errorf("remove old worktree: %v", err)
			}
		})
		if err := os.Rename(state.worktree, old); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(state.worktree, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(state.worktree, "replacement"), []byte("replacement"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := syncTestWorktree(t, state.clientDir, state.worktree); !errors.Is(err, errFSJournalRootChanged) {
			t.Fatalf("root replacement error=%v", err)
		}
		if data, err := os.ReadFile(filepath.Join(state.worktree, "replacement")); err != nil || string(data) != "replacement" {
			t.Fatalf("replacement=%q err=%v", data, err)
		}
	})

	t.Run("corrupt journal ownership", func(t *testing.T) {
		state := newImportedBinding(t)
		db, err := openClientDB(filepath.Join(state.clientDir, _clientDatabaseName), false)
		if err != nil {
			t.Fatal(err)
		}
		root, err := openWorktreeRoot(state.worktree, func(*os.File) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		if err := bindFSJournalRoot(t.Context(), db, state.worktree, root); err != nil {
			t.Fatal(err)
		}
		first := fsActionInternalPrefix + "00112233445566778899aabbccddeeff"
		second := fsActionInternalPrefix + "ffeeddccbbaa99887766554433221100"
		insertRawFSAction(t, db, state.worktree, "33333333333333333333333333333333", 1, fsPhaseRollback, fsOpRename,
			"", root.device, root.inode, first, second, "File", root.device, root.inode, "", 0, "", first, fsStateIntent)
		root.Close()
		db.Close()
		if err := syncTestWorktree(t, state.clientDir, state.worktree); err == nil {
			t.Fatal("public sync accepted corrupt internal ownership")
		}
	})
}

func TestFSActionPreserveUnknownRequiresZeroIdentityOrigin(t *testing.T) {
	clientDir, rootPath := t.TempDir(), t.TempDir()
	db, err := initializeClientDB(t.Context(), clientDir, syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	root, err := openWorktreeRoot(rootPath, func(*os.File) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := bindFSJournalRoot(t.Context(), db, rootPath, root); err != nil {
		t.Fatal(err)
	}
	source := fsActionInternalPrefix + "00112233445566778899aabbccddeeff"
	if err := os.WriteFile(filepath.Join(rootPath, source), []byte("unknown"), 0o600); err != nil {
		t.Fatal(err)
	}
	insertRawFSAction(t, db, rootPath, "33333333333333333333333333333333", 1, fsPhaseRollback, fsOpRename,
		"", root.device, root.inode, source, "visible", "File", 0, 0, "", 0, "", source, fsStateIntent)
	if _, err := db.Exec(`UPDATE fs_actions SET internal_source=?, action_outcome='preserve_unknown'
		WHERE worktree=? AND action_id=?`, source, rootPath, "33333333333333333333333333333333"); err != nil {
		t.Fatal(err)
	}
	if err := recoverFSActions(t.Context(), db, rootPath, root, nil); err == nil {
		t.Fatal("preserve-unknown action without a zero-identity creation origin was accepted")
	}
	if data, err := os.ReadFile(filepath.Join(rootPath, source)); err != nil || string(data) != "unknown" {
		t.Fatalf("source=%q err=%v", data, err)
	}
	if _, err := os.Lstat(filepath.Join(rootPath, "visible")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("visible target created: %v", err)
	}
}

func TestFSActionPreserveProvenanceValidation(t *testing.T) {
	const originID = "11111111111111111111111111111111"
	const preserveID = "22222222222222222222222222222222"
	const wrongID = "33333333333333333333333333333333"

	setup := func(t *testing.T) (*sql.DB, *openedWorktree, string, string) {
		t.Helper()
		db, err := initializeClientDB(t.Context(), t.TempDir(), syncDirectory)
		if err != nil {
			t.Fatal(err)
		}
		rootPath := t.TempDir()
		root, err := openWorktreeRoot(rootPath, func(*os.File) error { return nil })
		if err != nil {
			db.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() { root.Close(); db.Close() })
		if err := bindFSJournalRoot(t.Context(), db, rootPath, root); err != nil {
			t.Fatal(err)
		}
		source := checkoutTempPrefix + "00112233445566778899aabbccddeeff"
		target, err := recoveryVisibleLeaf("", originID, 1)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(rootPath, source), []byte("unknown"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO pending_checkouts(server_url,library_id,worktree,user_id,device_id,target_commit,target_root,
			head_etag,apply_state,rollback_root_mtime_ns,rollback_root_mtime_valid)
			VALUES('s','l',?,'u','d','c','r','e','rolling_back',0,1)`, rootPath); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO checkout_paths(worktree,path,type,object_id,canonical_mtime,temp_name)
			VALUES(?, 'final', 'File', 'object', '2000-01-01T00:00:00Z', ?)`, rootPath, source); err != nil {
			t.Fatal(err)
		}
		origin := fsAction{Worktree: rootPath, ActionID: originID, Order: 1, Phase: fsPhasePreBase,
			Op: fsOpCreateFile, ParentDevice: root.device, ParentInode: root.inode, Source: source,
			ExpectedKind: "File", InternalSource: source, State: fsStateIntent}
		if err := insertFSActionIntent(t.Context(), db, origin); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("UPDATE checkout_paths SET create_action_id=? WHERE worktree=? AND path='final'", originID, rootPath); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`UPDATE fs_actions SET state='completed', action_outcome='rolled_back'
			WHERE worktree=? AND action_id=?`, rootPath, originID); err != nil {
			t.Fatal(err)
		}
		preserve := fsAction{Worktree: rootPath, ActionID: preserveID, OriginActionID: originID, Attempt: 1,
			Order: 2, Phase: fsPhaseRollback, Op: fsOpRename, ParentDevice: root.device, ParentInode: root.inode,
			Source: source, Target: target, ExpectedKind: "File", InternalSource: source,
			Outcome: "preserve_unknown", State: fsStateIntent}
		if err := insertFSActionIntent(t.Context(), db, preserve); err != nil {
			t.Fatal(err)
		}
		return db, root, rootPath, target
	}

	cases := []struct {
		name   string
		mutate func(*testing.T, *sql.DB, string, string)
	}{
		{"without origin", func(t *testing.T, db *sql.DB, worktree, _ string) {
			_, err := db.Exec("UPDATE fs_actions SET origin_action_id=NULL, attempt=0 WHERE worktree=? AND action_id=?", worktree, preserveID)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{"without checkout context", func(t *testing.T, db *sql.DB, worktree, _ string) {
			_, err := db.Exec("DELETE FROM checkout_paths WHERE worktree=?", worktree)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{"self-consistent forged origin mismatches binding", func(t *testing.T, db *sql.DB, worktree, _ string) {
			_, err := db.Exec(`INSERT INTO fs_actions SELECT worktree, ?, NULL, 0, 3, phase, op, parent_path,
				parent_device,parent_inode,source_name,target_name,expected_kind,expected_device,expected_inode,
				expected_object,expected_size,expected_mtime,internal_name,internal_source,internal_target,action_outcome,state
				FROM fs_actions WHERE worktree=? AND action_id=?`, wrongID, worktree, originID)
			if err != nil {
				t.Fatal(err)
			}
			target, err := recoveryVisibleLeaf("", wrongID, 1)
			if err != nil {
				t.Fatal(err)
			}
			_, err = db.Exec("UPDATE fs_actions SET origin_action_id=?, target_name=? WHERE worktree=? AND action_id=?", wrongID, target, worktree, preserveID)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{"wrong attempt", func(t *testing.T, db *sql.DB, worktree, _ string) {
			_, err := db.Exec("UPDATE fs_actions SET attempt=2 WHERE worktree=? AND action_id=?", worktree, preserveID)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{"wrong target", func(t *testing.T, db *sql.DB, worktree, _ string) {
			_, err := db.Exec("UPDATE fs_actions SET target_name='forged' WHERE worktree=? AND action_id=?", worktree, preserveID)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{"invalid origin outcome", func(t *testing.T, db *sql.DB, worktree, _ string) {
			_, err := db.Exec("UPDATE fs_actions SET action_outcome='' WHERE worktree=? AND action_id=?", worktree, originID)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{"collision without successor", func(t *testing.T, db *sql.DB, worktree, _ string) {
			_, err := db.Exec("UPDATE fs_actions SET state='completed', action_outcome='collision' WHERE worktree=? AND action_id=?", worktree, preserveID)
			if err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			db, root, worktree, target := setup(t)
			test.mutate(t, db, worktree, target)
			if err := recoverFSActions(t.Context(), db, worktree, root, nil); err == nil {
				t.Fatal("forged preserve provenance was accepted")
			}
			if _, err := os.Lstat(filepath.Join(worktree, target)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("forged preserve changed target: %v", err)
			}
		})
	}
	t.Run("duplicate attempt constraint", func(t *testing.T) {
		db, _, worktree, _ := setup(t)
		_, err := db.Exec(`INSERT INTO fs_actions SELECT worktree, ?, origin_action_id, attempt, 3, phase, op, parent_path,
			parent_device,parent_inode,source_name,target_name,expected_kind,expected_device,expected_inode,
			expected_object,expected_size,expected_mtime,internal_name,internal_source,internal_target,action_outcome,state
			FROM fs_actions WHERE worktree=? AND action_id=?`, wrongID, worktree, preserveID)
		if err == nil {
			t.Fatal("duplicate preserve attempt was accepted")
		}
	})
	t.Run("invalid collision successor rolls back transaction", func(t *testing.T) {
		db, root, worktree, _ := setup(t)
		values, err := loadFSActions(t.Context(), db, worktree)
		if err != nil {
			t.Fatal(err)
		}
		predecessor := values[1]
		predecessor.Source = fsActionInternalPrefix + "ffeeddccbbaa99887766554433221100"
		predecessor.InternalSource = predecessor.Source
		if err := advancePreserveUnknownCollision(t.Context(), db, root, predecessor, nil); err == nil {
			t.Fatal("invalid collision successor was committed")
		}
		var rows int
		var state, outcome string
		if err := db.QueryRow("SELECT COUNT(*) FROM fs_actions WHERE worktree=?", worktree).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow("SELECT state,action_outcome FROM fs_actions WHERE worktree=? AND action_id=?", worktree, preserveID).Scan(&state, &outcome); err != nil {
			t.Fatal(err)
		}
		if rows != 2 || state != fsStateIntent || outcome != "preserve_unknown" {
			t.Fatalf("rows=%d predecessor=%s/%s", rows, state, outcome)
		}
	})
}

func TestFSActionRenameRecovery(t *testing.T) {
	ctx := context.Background()
	clientDir, rootPath := t.TempDir(), t.TempDir()
	db, err := initializeClientDB(ctx, clientDir, syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	root, err := openWorktreeRoot(rootPath, func(*os.File) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := bindFSJournalRoot(ctx, db, rootPath, root); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootPath, "source"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stat fscompat.Stat_t
	if err := fscompat.Fstatat(int(root.directory.Fd()), "source", &stat, fscompat.AT_SYMLINK_NOFOLLOW); err != nil {
		t.Fatal(err)
	}
	action := fsAction{Worktree: rootPath, ActionID: "fedcba9876543210fedcba9876543210", Order: 1,
		Phase: fsPhasePreBase, Op: fsOpRename, Parent: "", ParentDevice: root.device, ParentInode: root.inode,
		Source: "source", Target: "target", ExpectedKind: "File", ExpectedDevice: uint64(stat.Dev), ExpectedInode: stat.Ino,
		State: fsStateIntent}
	if err := insertFSActionIntent(ctx, db, action); err != nil {
		t.Fatal(err)
	}
	if err := renameNoReplace(int(root.directory.Fd()), "source", int(root.directory.Fd()), "target"); err != nil {
		t.Fatal(err)
	}
	if err := recoverFSActions(ctx, db, rootPath, root, nil); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := db.QueryRow("SELECT state FROM fs_actions WHERE action_id=?", action.ActionID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != fsStateCompleted {
		t.Fatalf("state=%q", state)
	}
	if data, err := os.ReadFile(filepath.Join(rootPath, "target")); err != nil || string(data) != "data" {
		t.Fatalf("target=%q err=%v", data, err)
	}
}

func TestFSJournalRootReplacementRejected(t *testing.T) {
	ctx := context.Background()
	clientDir, rootPath := t.TempDir(), filepath.Join(t.TempDir(), "work")
	if err := os.Mkdir(rootPath, 0o700); err != nil {
		t.Fatal(err)
	}
	db, err := initializeClientDB(ctx, clientDir, syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	root, err := openWorktreeRoot(rootPath, func(*os.File) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := bindFSJournalRoot(ctx, db, rootPath, root); err != nil {
		t.Fatal(err)
	}
	root.Close()
	oldRoot := rootPath + "-old"
	t.Cleanup(func() {
		if err := os.RemoveAll(oldRoot); err != nil {
			t.Errorf("remove old root: %v", err)
		}
	})
	if err := os.Rename(rootPath, oldRoot); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(rootPath, 0o700); err != nil {
		t.Fatal(err)
	}
	replacement, err := openWorktreeRoot(rootPath, func(*os.File) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	err = bindFSJournalRoot(ctx, db, rootPath, replacement)
	if err == nil || !errors.Is(err, errFSJournalRootChanged) {
		t.Fatalf("replacement error=%v", err)
	}
}

func TestPromotionTargetParentIdentityCodecIsCanonical(t *testing.T) {
	encoded := encodePromotionTargetParent(0x1234, 0x5678)
	device, inode, err := decodePromotionTargetParent(encoded)
	if err != nil || device != 0x1234 || inode != 0x5678 {
		t.Fatalf("promotion target parent identity=%q device=%x inode=%x err=%v", encoded, device, inode, err)
	}
	for name, value := range map[string]string{
		"trailing":  encoded + "0",
		"short":     encoded[:len(encoded)-1],
		"zero":      encodePromotionTargetParent(0, 1),
		"uppercase": strings.ToUpper(encoded),
		"untagged":  "0000000000001234:0000000000005678",
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := decodePromotionTargetParent(value); err == nil {
				t.Fatalf("noncanonical promotion target parent identity %q accepted", value)
			}
		})
	}
}

func TestFSActionRecoveryRejectsCorruptPath(t *testing.T) {
	clientDir, rootPath := t.TempDir(), t.TempDir()
	db, err := initializeClientDB(t.Context(), clientDir, syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	root, err := openWorktreeRoot(rootPath, func(*os.File) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := bindFSJournalRoot(t.Context(), db, rootPath, root); err != nil {
		t.Fatal(err)
	}
	insertRawFSAction(t, db, rootPath, "ffeeddccbbaa99887766554433221100", 1, fsPhasePreBase, fsOpRename,
		"../escape", root.device, root.inode, "source", "target", "File", root.device, root.inode, "", 0, "", "", fsStateIntent)
	if err := recoverFSActions(t.Context(), db, rootPath, root, nil); err == nil {
		t.Fatal("corrupt journal parent was accepted")
	}
}

func TestFSActionBaseBoundaryGuard(t *testing.T) {
	db, root, rootPath, _ := setupRecoveryDirectory(t)
	if err := bindFSJournalRoot(t.Context(), db, rootPath, root); err != nil {
		t.Fatal(err)
	}
	action := fsAction{Worktree: rootPath, ActionID: "abcdefabcdefabcdefabcdefabcdefab", Order: 1,
		Phase: fsPhasePreBase, Op: fsOpRename, Parent: "", ParentDevice: root.device, ParentInode: root.inode,
		Source: "source", Target: "target", ExpectedKind: "File", ExpectedDevice: root.device, ExpectedInode: root.inode,
		State: fsStateIntent}
	if err := insertFSActionIntent(t.Context(), db, action); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := assertNoIncompletePreBase(t.Context(), tx, rootPath); err == nil {
		t.Fatal("Base advance accepted an incomplete pre-base Intent")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE fs_actions SET state = 'completed' WHERE worktree = ? AND action_id = ?", rootPath, action.ActionID); err != nil {
		t.Fatal(err)
	}
	tx, err = db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := assertNoIncompletePreBase(t.Context(), tx, rootPath); err != nil {
		t.Fatalf("Base advance rejected Completed action: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func assertRecoveryCollisionState(t *testing.T, clientDir, worktree string) {
	t.Helper()
	db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var collisions, preserved int
	if err := db.QueryRow("SELECT COUNT(*) FROM fs_actions WHERE worktree=? AND state='completed' AND action_outcome='collision'", worktree).Scan(&collisions); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM fs_actions WHERE worktree=? AND state='completed' AND action_outcome='preserve_unknown'", worktree).Scan(&preserved); err != nil {
		t.Fatal(err)
	}
	if collisions != 2 || preserved != 1 {
		t.Fatalf("collision actions=%d preserved=%d", collisions, preserved)
	}
}

func assertCreateRolledBackState(t *testing.T, clientDir, worktree string) {
	t.Helper()
	db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var actions, pending int
	if err := db.QueryRow("SELECT COUNT(*) FROM fs_actions WHERE worktree=? AND action_outcome='rolled_back'", worktree).Scan(&actions); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM pending_checkouts WHERE worktree=? AND apply_state='rolling_back'", worktree).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if actions != 1 || pending != 1 {
		t.Fatalf("rolled-back actions=%d pending=%d", actions, pending)
	}
}

type exactPathState struct {
	info          os.FileInfo
	data          []byte
	linkTarget    string
	device, inode uint64
	nlink         uint64
}

func captureExactTree(t *testing.T, root string) map[string]exactPathState {
	t.Helper()
	result := make(map[string]exactPathState)
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		state := exactPathState{info: info}
		if stat, statErr := testPathStat(path); statErr == nil {
			state.device, state.inode, state.nlink = uint64(stat.Dev), stat.Ino, uint64(stat.Nlink)
		}
		if info.Mode().IsRegular() {
			state.data, err = os.ReadFile(path)
		} else if info.Mode()&os.ModeSymlink != 0 {
			state.linkTarget, err = os.Readlink(path)
		}
		if err != nil {
			return err
		}
		result[relative] = state
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func assertExactTree(t *testing.T, root string, before map[string]exactPathState) {
	t.Helper()
	after := captureExactTree(t, root)
	if len(after) != len(before) {
		t.Fatalf("tree entry count=%d want=%d", len(after), len(before))
	}
	var changed []string
	for path, want := range before {
		got, ok := after[path]
		if !ok || want.device != got.device || want.inode != got.inode ||
			filesystemMtimeNS(want.info.ModTime()) != filesystemMtimeNS(got.info.ModTime()) ||
			want.info.Mode() != got.info.Mode() || want.linkTarget != got.linkTarget ||
			want.nlink != got.nlink || !bytes.Equal(want.data, got.data) {
			changed = append(changed, fmt.Sprintf("%q: before=%v after=%v same=%t mtime=%d/%d mode=%v/%v nlink=%d/%d link=%q/%q data=%q want=%q",
				path, want.info, got.info, ok && want.device == got.device && want.inode == got.inode, filesystemMtimeNS(want.info.ModTime()),
				filesystemMtimeNS(got.info.ModTime()), want.info.Mode(), got.info.Mode(), want.nlink, got.nlink,
				want.linkTarget, got.linkTarget, got.data, want.data))
		}
	}
	if len(changed) != 0 {
		sort.Strings(changed)
		t.Fatalf("tree changed:\n%s", strings.Join(changed, "\n"))
	}
}

func TestFSActionSubprocessCrashMatrix(t *testing.T) {
	cases := []struct{ scenario, point string }{
		{"create-file", "after_intent_commit"},
		{"create-directory", "after_action"},
		{"install-file-rename", "after_parent_sync"},
		{"install-directory-rename", "after_action"},
		{"capture-rename", "before_action"},
		{"rollback-rename", "after_completed"},
		{"mtime", "after_action"},
		{"post-base-file-cleanup", "after_parent_sync"},
		{"post-base-directory-cleanup", "after_intent_commit"},
	}
	for _, test := range cases {
		t.Run(test.scenario+"/"+test.point, func(t *testing.T) {
			clientDir, rootPath := t.TempDir(), t.TempDir()
			command := exec.Command(os.Args[0], "-test.run=^TestFSActionCrashHelper$")
			command.Env = append(os.Environ(), "FILECLOUD_FS_CRASH_HELPER=1", "FILECLOUD_FS_CRASH_POINT="+test.point,
				"FILECLOUD_FS_CRASH_SCENARIO="+test.scenario, "FILECLOUD_FS_CRASH_CLIENT="+clientDir,
				"FILECLOUD_FS_CRASH_ROOT="+rootPath)
			assertProcessSIGKILL(t, command.Run())
			db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			root, err := openWorktreeRoot(rootPath, func(*os.File) error { return nil })
			if err != nil {
				db.Close()
				t.Fatal(err)
			}
			if err := initializeClientSchema(t.Context(), db); err != nil {
				t.Fatal(err)
			}
			if err := recoverFSActions(t.Context(), db, rootPath, root, nil); err != nil {
				t.Fatal(err)
			}
			root.Close()
			db.Close()
			assertFSActionCrashOracle(t, rootPath, test.scenario)
		})
	}
}

func assertFSActionCrashOracle(t *testing.T, rootPath, scenario string) {
	t.Helper()
	switch scenario {
	case "create-file", "create-directory":
		name := fsActionInternalPrefix + "00112233445566778899aabbccddeeff"
		if info, err := os.Stat(filepath.Join(rootPath, name)); err != nil || (scenario == "create-directory") != info.IsDir() {
			t.Fatalf("created path info=%v err=%v", info, err)
		}
	case "install-file-rename", "capture-rename", "rollback-rename":
		if data, err := os.ReadFile(filepath.Join(rootPath, "target")); err != nil || string(data) != "durable" {
			t.Fatalf("target=%q err=%v", data, err)
		}
		if _, err := os.Lstat(filepath.Join(rootPath, "source")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("source remains: %v", err)
		}
	case "install-directory-rename":
		if info, err := os.Stat(filepath.Join(rootPath, "target")); err != nil || !info.IsDir() {
			t.Fatalf("installed directory info=%v err=%v", info, err)
		}
		if _, err := os.Lstat(filepath.Join(rootPath, "source")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("source remains: %v", err)
		}
	case "mtime":
		info, err := os.Stat(filepath.Join(rootPath, "source"))
		if err != nil || info.ModTime().UTC().Format("2006-01-02T15:04:05Z") != "2000-01-02T03:04:05Z" {
			t.Fatalf("mtime=%v err=%v", info, err)
		}
	case "post-base-file-cleanup", "post-base-directory-cleanup":
		if _, err := os.Lstat(filepath.Join(rootPath, "trash")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("cleanup path remains: %v", err)
		}
	}
}

func scanPublicCheckout(t *testing.T, worktree string) worktreeSnapshot {
	t.Helper()
	root, err := openWorktreeRoot(worktree, func(*os.File) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	snapshot, scanErr := scanWorktree(root)
	if err := errors.Join(scanErr, root.Close()); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func assertPublicCheckoutConverged(t *testing.T, environment libraryCLIEnvironment, clientDir, worktree string) clientBinding {
	t.Helper()
	binding := readTestBinding(t, clientDir, worktree)
	base := mustServerURL(t, environment.server.URL)
	head, err := getRemoteHead(t.Context(), base, testClientLibraryID, []byte(environment.token))
	if err != nil || head.CommitID == nil || *head.CommitID != binding.SyncBase {
		t.Fatalf("binding/Head mismatch binding=%+v head=%+v err=%v", binding, head, err)
	}
	commit, err := getRemoteCommit(t.Context(), base, testClientLibraryID, []byte(environment.token), *head.CommitID)
	if err != nil {
		t.Fatalf("get Head commit: %v", err)
	}
	first, second := scanPublicCheckout(t, worktree), scanPublicCheckout(t, worktree)
	if binding.SyncBaseRoot != commit.Root || first.root != commit.Root || second.root != commit.Root {
		t.Fatalf("Base/Commit/scan roots differ binding=%q commit=%q first=%q second=%q",
			binding.SyncBaseRoot, commit.Root, first.root, second.root)
	}

	db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT path, type, object_id, canonical_mtime, actual_mtime, size
		FROM path_index WHERE worktree=? ORDER BY path`, worktree)
	if err != nil {
		t.Fatal(err)
	}
	indexed := make(map[string]checkoutPath)
	for rows.Next() {
		var path checkoutPath
		var actualMtime string
		if err := rows.Scan(&path.path, &path.kind, &path.id, &path.mtime, &actualMtime, &path.size); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if actualMtime != path.mtime {
			rows.Close()
			t.Fatalf("path index mtime differs at %q: canonical=%q actual=%q", path.path, path.mtime, actualMtime)
		}
		indexed[path.path] = path
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(indexed) != len(second.paths) {
		t.Fatalf("path index count=%d scanned paths=%d", len(indexed), len(second.paths))
	}
	for _, path := range second.paths {
		indexedPath, ok := indexed[path.path]
		if !ok || indexedPath.kind != path.kind || indexedPath.id != path.id || indexedPath.mtime != path.mtime || indexedPath.size != path.size {
			t.Fatalf("path index mismatch at %q: indexed=%+v scanned=%+v", path.path, indexedPath, path)
		}
	}
	for _, table := range []string{"pending_checkouts", "checkout_paths", "fs_actions", "sync_recoveries"} {
		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM "+table+" WHERE worktree=?", worktree).Scan(&count); err != nil || count != 0 {
			t.Fatalf("%s rows=%d err=%v", table, count, err)
		}
	}
	assertNoSyncInternalPaths(t, worktree)
	return binding
}

func assertInitialCheckoutCrashState(t *testing.T, clientDir, worktree, target, targetRoot, point string) {
	t.Helper()
	db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if point == "before" {
		var bindings int
		if err := db.QueryRow("SELECT COUNT(*) FROM bindings WHERE worktree=?", worktree).Scan(&bindings); err != nil || bindings != 0 {
			t.Fatalf("pre-commit bindings=%d err=%v", bindings, err)
		}
		var pendingCommit, pendingRoot, applyState string
		if err := db.QueryRow(`SELECT target_commit, target_root, apply_state FROM pending_checkouts WHERE worktree=?`, worktree).
			Scan(&pendingCommit, &pendingRoot, &applyState); err != nil || pendingCommit != target || pendingRoot != targetRoot || applyState != "pending" {
			t.Fatalf("pre-commit pending commit=%q root=%q state=%q err=%v", pendingCommit, pendingRoot, applyState, err)
		}
		var checkoutCount, incompleteCheckout int
		if err := db.QueryRow(`SELECT COUNT(*), COUNT(*) FILTER (WHERE completed<>1 OR temp_device=0 OR temp_inode=0 OR
			actual_mtime='' OR (type='Directory' AND (target_device=0 OR target_inode=0)) OR
			(type='File' AND (target_device<>0 OR target_inode<>0))) FROM checkout_paths WHERE worktree=?`, worktree).
			Scan(&checkoutCount, &incompleteCheckout); err != nil || checkoutCount != 5 || incompleteCheckout != 0 {
			t.Fatalf("pre-commit checkout count=%d incomplete=%d err=%v", checkoutCount, incompleteCheckout, err)
		}
		var actionCount, invalidActions int
		if err := db.QueryRow(`SELECT COUNT(*), COUNT(*) FILTER (WHERE phase<>? OR state<>?)
			FROM fs_actions WHERE worktree=?`, fsPhasePreBase, fsStateCompleted, worktree).Scan(&actionCount, &invalidActions); err != nil || actionCount != 15 || invalidActions != 0 {
			t.Fatalf("pre-commit actions=%d invalid=%d err=%v", actionCount, invalidActions, err)
		}
		return
	}
	var commit, root string
	if err := db.QueryRow("SELECT sync_base_commit, sync_base_root FROM bindings WHERE worktree=?", worktree).Scan(&commit, &root); err != nil || commit != target || root != targetRoot {
		t.Fatalf("post-commit binding commit=%q root=%q err=%v", commit, root, err)
	}
	for _, table := range []string{"pending_checkouts", "checkout_paths", "fs_actions", "sync_recoveries"} {
		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM "+table+" WHERE worktree=?", worktree).Scan(&count); err != nil || count != 0 {
			t.Fatalf("post-commit %s rows=%d err=%v", table, count, err)
		}
	}
	var indexed int
	if err := db.QueryRow("SELECT COUNT(*) FROM path_index WHERE worktree=?", worktree).Scan(&indexed); err != nil || indexed != 5 {
		t.Fatalf("post-commit path index=%d err=%v", indexed, err)
	}
}

func TestPublicInitialCheckoutBaseCommitCrashMatrix(t *testing.T) {
	for _, point := range []string{"before", "after"} {
		t.Run(point, func(t *testing.T) {
			environment, target, targetRoot, files := importedRemoteCheckout(t)
			clientDir, worktree := newClientPaths(t)
			command := exec.Command(os.Args[0], "-test.run=^TestPublicInitialCheckoutBaseCommitCrashHelper$")
			command.Env = append(os.Environ(), "FILECLOUD_INITIAL_COMMIT_HELPER=1", "FILECLOUD_INITIAL_COMMIT_POINT="+point,
				"FILECLOUD_PUBLIC_CRASH_CLIENT="+clientDir, "FILECLOUD_PUBLIC_CRASH_WORKTREE="+worktree,
				"FILECLOUD_PUBLIC_CRASH_SERVER="+environment.server.URL, "FILECLOUD_PUBLIC_CRASH_TOKEN="+environment.token)
			assertProcessSIGKILL(t, command.Run())
			assertInitialCheckoutCrashState(t, clientDir, worktree, target, targetRoot, point)
			args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
			if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
				t.Fatalf("public bind restart: %v", err)
			}
			binding := assertPublicCheckoutConverged(t, environment, clientDir, worktree)
			if binding.SyncBase != target || binding.SyncBaseRoot != targetRoot {
				t.Fatalf("restart Base=%+v target=%s root=%s", binding, target, targetRoot)
			}
			assertPlatformConverged(t, "initial checkout crash "+point, environment, clientDir, worktree,
				platformConfirmedFiles(files))
		})
	}
}

func TestPublicInitialCheckoutBaseCommitCrashHelper(t *testing.T) {
	if os.Getenv("FILECLOUD_INITIAL_COMMIT_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	kill := func() error { return killTestProcess() }
	config := libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}
	if os.Getenv("FILECLOUD_INITIAL_COMMIT_POINT") == "before" {
		config.beforeCheckoutBaseCommit = kill
	} else {
		config.afterCheckoutBaseCommit = kill
	}
	args := bindArgs(os.Getenv("FILECLOUD_PUBLIC_CRASH_CLIENT"), os.Getenv("FILECLOUD_PUBLIC_CRASH_SERVER"),
		testClientLibraryID, os.Getenv("FILECLOUD_PUBLIC_CRASH_WORKTREE"), testOtherDeviceID)
	_ = runLibraryWithConfig(context.Background(), args[1:], strings.NewReader(os.Getenv("FILECLOUD_PUBLIC_CRASH_TOKEN")+"\n"),
		io.Discard, io.Discard, config)
	os.Exit(98)
}

func platformBindCrashScenario(op, kind, point string) string {
	return "bind checkout fs crash " + op + " " + kind + " " + point
}

func TestPublicBindSubprocessCrashMatrix(t *testing.T) {
	categories := []struct{ op, kind string }{
		{fsOpCreateFile, "File"}, {fsOpCreateDirectory, "Directory"},
		{fsOpMtime, "File"}, {fsOpMtime, "Directory"},
		{fsOpRename, "File"}, {fsOpRename, "Directory"},
	}
	points := []string{"before_intent_commit", "after_intent_commit", "after_action", "after_parent_sync", "after_completed"}
	for _, category := range categories {
		for _, point := range points {
			test := struct{ op, kind, point string }{category.op, category.kind, point}
			t.Run(test.op+"/"+test.kind+"/"+test.point, func(t *testing.T) {
				environment, target, targetRoot, files := importedRemoteCheckout(t)
				clientDir, worktree := newClientPaths(t)
				command := exec.Command(os.Args[0], "-test.run=^TestPublicFSActionCrashHelper$")
				command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_CRASH_HELPER=1", "FILECLOUD_PUBLIC_CRASH_CLIENT="+clientDir,
					"FILECLOUD_PUBLIC_CRASH_WORKTREE="+worktree, "FILECLOUD_PUBLIC_CRASH_SERVER="+environment.server.URL,
					"FILECLOUD_PUBLIC_CRASH_TOKEN="+environment.token, "FILECLOUD_PUBLIC_CRASH_OP="+test.op,
					"FILECLOUD_PUBLIC_CRASH_KIND="+test.kind, "FILECLOUD_PUBLIC_CRASH_POINT="+test.point)
				assertProcessSIGKILL(t, command.Run())
				args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
				if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
					t.Fatalf("public bind restart: %v", err)
				}
				binding := assertPublicCheckoutConverged(t, environment, clientDir, worktree)
				if binding.SyncBase != target || binding.SyncBaseRoot != targetRoot {
					t.Fatalf("restart Base=%+v target=%s root=%s", binding, target, targetRoot)
				}
				assertPlatformConverged(t, platformBindCrashScenario(test.op, test.kind, test.point),
					environment, clientDir, worktree, platformConfirmedFiles(files))
			})
		}
	}
}

func TestPublicCreateZeroIdentityReplacementPreserved(t *testing.T) {
	for _, kind := range []string{"File", "Directory"} {
		t.Run(kind, func(t *testing.T) {
			environment, _, _, _ := importedRemoteCheckout(t)
			clientDir, worktree := newClientPaths(t)
			beforeHead := mustAcceptanceHead(t, environment)
			command := exec.Command(os.Args[0], "-test.run=^TestPublicFSActionCrashHelper$")
			command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_CRASH_HELPER=1", "FILECLOUD_PUBLIC_CRASH_CLIENT="+clientDir,
				"FILECLOUD_PUBLIC_CRASH_WORKTREE="+worktree, "FILECLOUD_PUBLIC_CRASH_SERVER="+environment.server.URL,
				"FILECLOUD_PUBLIC_CRASH_TOKEN="+environment.token, "FILECLOUD_PUBLIC_CRASH_OP="+map[string]string{"File": fsOpCreateFile, "Directory": fsOpCreateDirectory}[kind],
				"FILECLOUD_PUBLIC_CRASH_KIND="+kind, "FILECLOUD_PUBLIC_CRASH_POINT=between_create_identity")
			assertProcessSIGKILL(t, command.Run())
			db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			var path, name, actionID string
			err = db.QueryRow(`SELECT checkout_paths.path, checkout_paths.temp_name, fs_actions.action_id
				FROM checkout_paths JOIN fs_actions ON fs_actions.worktree = checkout_paths.worktree
					AND fs_actions.action_id = checkout_paths.create_action_id
				WHERE checkout_paths.worktree = ? AND checkout_paths.type = ? AND checkout_paths.temp_name <> ''
					AND checkout_paths.temp_device = 0 AND checkout_paths.temp_inode = 0
					AND fs_actions.state = 'intent' AND fs_actions.expected_device = 0 AND fs_actions.expected_inode = 0
				ORDER BY checkout_paths.path LIMIT 1`, worktree, kind).Scan(&path, &name, &actionID)
			if closeErr := db.Close(); err != nil || closeErr != nil {
				t.Fatal(errors.Join(err, closeErr))
			}
			replacement := filepath.Join(worktree, filepath.Dir(filepath.FromSlash(path)), name)
			recovered := filepath.Join(filepath.Dir(replacement), "Filecloud recovered "+actionID[:12])
			mtime := time.Date(2025, 6, 7, 8, 9, 10, 0, time.UTC)
			if kind == "File" {
				if err := os.WriteFile(replacement, []byte("replacement"), 0o600); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.WriteFile(filepath.Join(replacement, "user"), []byte("user"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Chtimes(replacement, mtime, mtime); err != nil {
				t.Fatal(err)
			}
			args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
			err = runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard)
			if err == nil || !strings.Contains(err.Error(), filepath.Base(recovered)) {
				t.Fatalf("public bind rollback error=%v", err)
			}
			assertNoBinding(t, clientDir, worktree)
			assertCreateRolledBackState(t, clientDir, worktree)
			if _, err := os.Lstat(replacement); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("internal path remains after rollback: %v", err)
			}
			if err := runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", clientDir, "--worktree", worktree},
				strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}); err != nil {
				t.Fatalf("public unbind after safe rollback: %v", err)
			}
			for _, table := range []string{"fs_actions", "pending_checkouts"} {
				if count := countClientRows(t, clientDir, table, worktree); count != 0 {
					t.Fatalf("%s rows=%d", table, count)
				}
			}
			assertNoSyncInternalPaths(t, worktree)
			info, err := os.Stat(recovered)
			if err != nil || !info.ModTime().UTC().Equal(mtime) || (kind == "Directory") != info.IsDir() {
				t.Fatalf("replacement info=%v err=%v", info, err)
			}
			var preserved []byte
			if kind == "File" {
				preserved, err = os.ReadFile(recovered)
				if err != nil || string(preserved) != "replacement" {
					t.Fatalf("replacement=%q err=%v", preserved, err)
				}
			} else {
				preserved, err = os.ReadFile(filepath.Join(recovered, "user"))
				if err != nil || string(preserved) != "user" {
					t.Fatalf("directory replacement child=%q err=%v", preserved, err)
				}
			}
			expected := []byte("replacement")
			if kind == "Directory" {
				expected = []byte("user")
			}
			emitRecoveryAttestation(t, "bind checkout create "+kind+" between identity", beforeHead,
				mustAcceptanceHead(t, environment), "", "", expected, preserved)
		})
	}
}

func TestPublicDeepCreateZeroIdentityPreserved(t *testing.T) {
	for _, kind := range []string{"File", "Directory"} {
		t.Run(kind, func(t *testing.T) {
			environment := newLibraryCLIEnvironment(t, libraryapi.Config{})
			publisherDir, publisherTree := newClientPaths(t)
			parts := []string{strings.Repeat("a", 240), strings.Repeat("b", 240), strings.Repeat("c", 240), strings.Repeat("d", 240), strings.Repeat("e", 16)}
			parent := filepath.Join(append([]string{publisherTree}, parts...)...)
			if err := os.MkdirAll(parent, 0o700); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(parent, "final")
			if kind == "File" {
				if err := os.WriteFile(target, []byte("remote"), 0o600); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Mkdir(target, 0o700); err != nil {
				t.Fatal(err)
			}
			args := append(bindArgs(publisherDir, environment.server.URL, testClientLibraryID, publisherTree, testClientDeviceID), "--import-local")
			if err := runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard); err != nil {
				t.Fatal(err)
			}

			clientDir, worktree := newClientPaths(t)
			op := map[string]string{"File": fsOpCreateFile, "Directory": fsOpCreateDirectory}[kind]
			command := exec.Command(os.Args[0], "-test.run=^TestPublicFSActionCrashHelper$")
			command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_CRASH_HELPER=1", "FILECLOUD_PUBLIC_CRASH_CLIENT="+clientDir,
				"FILECLOUD_PUBLIC_CRASH_WORKTREE="+worktree, "FILECLOUD_PUBLIC_CRASH_SERVER="+environment.server.URL,
				"FILECLOUD_PUBLIC_CRASH_TOKEN="+environment.token, "FILECLOUD_PUBLIC_CRASH_OP="+op,
				"FILECLOUD_PUBLIC_CRASH_KIND="+kind, "FILECLOUD_PUBLIC_CRASH_POINT=between_create_identity")
			assertProcessSIGKILL(t, command.Run())
			db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			var path, name, actionID string
			err = db.QueryRow(`SELECT checkout_paths.path, checkout_paths.temp_name, fs_actions.action_id
				FROM checkout_paths JOIN fs_actions ON fs_actions.worktree=checkout_paths.worktree
					AND fs_actions.action_id=checkout_paths.create_action_id
				WHERE checkout_paths.worktree=? AND checkout_paths.type=? AND fs_actions.op=?
					AND fs_actions.state='intent' AND fs_actions.expected_device=0 ORDER BY length(checkout_paths.path) DESC LIMIT 1`,
				worktree, kind, op).Scan(&path, &name, &actionID)
			if closeErr := db.Close(); err != nil || closeErr != nil {
				t.Fatal(errors.Join(err, closeErr))
			}
			relativeParent, _ := splitFSActionPath(path)
			root, err := openWorktreeRoot(worktree, func(*os.File) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			actionParent, err := openFSActionParent(root, relativeParent, 0, 0)
			if err != nil {
				t.Fatalf("open deep action parent %q for %q: %v", relativeParent, name, errors.Join(err, root.Close()))
			}
			flags := fscompat.O_RDONLY | fscompat.O_CLOEXEC | fscompat.O_NOFOLLOW
			if kind == "Directory" {
				flags |= fscompat.O_DIRECTORY
			} else {
				flags = fscompat.O_RDWR | fscompat.O_CLOEXEC | fscompat.O_NOFOLLOW
			}
			fd, err := fscompat.Openat(int(actionParent.Fd()), name, flags, 0)
			if err != nil {
				t.Fatal(errors.Join(err, actionParent.Close(), root.Close()))
			}
			file := os.NewFile(uintptr(fd), name)
			var writeErr error
			if kind == "File" {
				_, writeErr = file.Write([]byte("unknown"))
			}
			mtime := time.Date(2025, 8, 9, 10, 11, 12, 0, time.UTC)
			mtimeErr := setFileMtime(file, mtime)
			before, statErr := file.Stat()
			if err := errors.Join(writeErr, mtimeErr, statErr, file.Close(), actionParent.Close(), root.Close()); err != nil {
				t.Fatal(err)
			}
			leaf, err := recoveryVisibleLeaf(relativeParent, actionID, 1)
			if err != nil || len(relativeParent)+1+len(leaf) > 1024 {
				t.Fatalf("recovery leaf=%q parent=%d err=%v", leaf, len(relativeParent), err)
			}
			err = runTest(t.Context(), bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID),
				strings.NewReader(environment.token+"\n"), io.Discard, io.Discard)
			if err == nil || !strings.Contains(err.Error(), leaf) {
				t.Fatalf("deep rollback error=%v leaf=%q", err, leaf)
			}
			root, err = openWorktreeRoot(worktree, func(*os.File) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			actionParent, err = openFSActionParent(root, relativeParent, 0, 0)
			if err != nil {
				t.Fatalf("open deep recovery parent %q: %v", relativeParent, errors.Join(err, root.Close()))
			}
			fd, err = fscompat.Openat(int(actionParent.Fd()), leaf, flags, 0)
			if err != nil {
				t.Fatal(errors.Join(err, actionParent.Close(), root.Close()))
			}
			file = os.NewFile(uintptr(fd), leaf)
			after, statErr := file.Stat()
			var data []byte
			var readErr error
			if kind == "File" {
				data, readErr = io.ReadAll(file)
			}
			if closeErr := errors.Join(file.Close(), actionParent.Close()); statErr != nil || readErr != nil || closeErr != nil ||
				!os.SameFile(before, after) || !after.ModTime().UTC().Equal(mtime) || (kind == "Directory") != after.IsDir() {
				t.Fatalf("preserved identity before=%v after=%v data=%q stat=%v read=%v close=%v",
					before, after, data, statErr, readErr, closeErr)
			}
			if kind == "File" && string(data) != "unknown" {
				t.Fatalf("preserved data=%q, want unknown", data)
			}
			_, scanErr := scanWorktree(root)
			if err := errors.Join(scanErr, root.Close()); err != nil {
				t.Fatalf("scan preserved deep recovery: %v", err)
			}
		})
	}
}

type zeroIdentityReplacementCase struct {
	name, originalKind, replacement string
}

var zeroIdentityReplacementCases = []zeroIdentityReplacementCase{
	{"file-to-directory", "File", "directory"},
	{"directory-to-file", "Directory", "file"},
	{"file-to-symlink", "File", "symlink"},
	{"file-to-multilink", "File", "multilink"},
}

func zeroIdentityIntent(t *testing.T, clientDir, worktree, kind string) (string, string, string) {
	t.Helper()
	db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var path, name, actionID string
	err = db.QueryRow(`SELECT checkout_paths.path, checkout_paths.temp_name, fs_actions.action_id
		FROM checkout_paths JOIN fs_actions ON fs_actions.worktree=checkout_paths.worktree
			AND fs_actions.action_id=checkout_paths.create_action_id
		WHERE checkout_paths.worktree=? AND checkout_paths.type=? AND fs_actions.state='intent'
			AND fs_actions.expected_device=0 AND fs_actions.expected_inode=0
		ORDER BY checkout_paths.path LIMIT 1`, worktree, kind).Scan(&path, &name, &actionID)
	if err != nil {
		t.Fatal(err)
	}
	return path, name, actionID
}

func installZeroIdentityReplacement(t *testing.T, internal, replacement string) (map[string]exactPathState, string, map[string]exactPathState) {
	t.Helper()
	if err := os.RemoveAll(internal); err != nil {
		t.Fatal(err)
	}
	outside := ""
	switch replacement {
	case "directory":
		if err := os.MkdirAll(filepath.Join(internal, "nested"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(internal, "nested", "child"), []byte("directory replacement"), 0o600); err != nil {
			t.Fatal(err)
		}
	case "file":
		if err := os.WriteFile(internal, []byte("file replacement"), 0o600); err != nil {
			t.Fatal(err)
		}
	case "symlink", "multilink":
		outside = filepath.Join(t.TempDir(), "outside")
		if err := os.WriteFile(outside, []byte("outside unchanged"), 0o600); err != nil {
			t.Fatal(err)
		}
		if replacement == "symlink" {
			if err := createTestSymlink(outside, internal); err != nil {
				t.Fatal(err)
			}
		} else if err := os.Link(outside, internal); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unknown replacement %q", replacement)
	}
	before := captureExactTree(t, internal)
	if outside == "" {
		return before, "", nil
	}
	return before, outside, captureExactTree(t, outside)
}

func assertZeroIdentityReplacementPreserved(t *testing.T, internal, recovered, outside string,
	before, outsideBefore map[string]exactPathState) {
	t.Helper()
	if _, err := os.Lstat(internal); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("internal path remains: %v", err)
	}
	assertExactTree(t, recovered, before)
	if outside != "" {
		assertExactTree(t, outside, outsideBefore)
	}
}

func assertZeroIdentityJournalsCleared(t *testing.T, clientDir, worktree string) {
	t.Helper()
	for _, table := range []string{"pending_checkouts", "checkout_paths", "fs_actions", "sync_recoveries", "fs_journal_bindings"} {
		if count := countClientRows(t, clientDir, table, worktree); count != 0 {
			t.Fatalf("%s rows=%d", table, count)
		}
	}
}

func TestPublicCreateZeroIdentityTypeChangedEntryPreserved(t *testing.T) {
	for _, test := range zeroIdentityReplacementCases {
		if test.replacement == "symlink" && !testCanRenameReparsePoint() {
			continue
		}
		t.Run(test.name, func(t *testing.T) {
			environment, _, _, _ := importedRemoteCheckout(t)
			clientDir, worktree := newClientPaths(t)
			op := map[string]string{"File": fsOpCreateFile, "Directory": fsOpCreateDirectory}[test.originalKind]
			command := exec.Command(os.Args[0], "-test.run=^TestPublicFSActionCrashHelper$")
			command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_CRASH_HELPER=1", "FILECLOUD_PUBLIC_CRASH_CLIENT="+clientDir,
				"FILECLOUD_PUBLIC_CRASH_WORKTREE="+worktree, "FILECLOUD_PUBLIC_CRASH_SERVER="+environment.server.URL,
				"FILECLOUD_PUBLIC_CRASH_TOKEN="+environment.token, "FILECLOUD_PUBLIC_CRASH_OP="+op,
				"FILECLOUD_PUBLIC_CRASH_KIND="+test.originalKind, "FILECLOUD_PUBLIC_CRASH_POINT=between_create_identity")
			assertProcessSIGKILL(t, command.Run())
			path, name, actionID := zeroIdentityIntent(t, clientDir, worktree, test.originalKind)
			relativeParent, _ := splitFSActionPath(path)
			internal := filepath.Join(worktree, filepath.FromSlash(relativeParent), name)
			leaf, err := recoveryVisibleLeaf(relativeParent, actionID, 1)
			if err != nil {
				t.Fatal(err)
			}
			recovered := filepath.Join(filepath.Dir(internal), leaf)
			before, outside, outsideBefore := installZeroIdentityReplacement(t, internal, test.replacement)
			err = runTest(t.Context(), bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID),
				strings.NewReader(environment.token+"\n"), io.Discard, io.Discard)
			if err == nil || !strings.Contains(err.Error(), leaf) {
				t.Fatalf("bind recovery error=%v", err)
			}
			assertNoBinding(t, clientDir, worktree)
			assertZeroIdentityReplacementPreserved(t, internal, recovered, outside, before, outsideBefore)
			if err := runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", clientDir, "--worktree", worktree},
				strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}); err != nil {
				t.Fatal(err)
			}
			assertZeroIdentityJournalsCleared(t, clientDir, worktree)
			assertZeroIdentityReplacementPreserved(t, internal, recovered, outside, before, outsideBefore)
		})
	}
}

func TestPublicCreateRecoveryCollisionChain(t *testing.T) {
	for _, kind := range []string{"File", "Directory"} {
		t.Run(kind, func(t *testing.T) {
			environment, _, _, _ := importedRemoteCheckout(t)
			clientDir, worktree := newClientPaths(t)
			op := map[string]string{"File": fsOpCreateFile, "Directory": fsOpCreateDirectory}[kind]
			crash := func(op, point, target string) {
				command := exec.Command(os.Args[0], "-test.run=^TestPublicFSActionCrashHelper$")
				command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_CRASH_HELPER=1", "FILECLOUD_PUBLIC_CRASH_CLIENT="+clientDir,
					"FILECLOUD_PUBLIC_CRASH_WORKTREE="+worktree, "FILECLOUD_PUBLIC_CRASH_SERVER="+environment.server.URL,
					"FILECLOUD_PUBLIC_CRASH_TOKEN="+environment.token, "FILECLOUD_PUBLIC_CRASH_OP="+op,
					"FILECLOUD_PUBLIC_CRASH_KIND="+kind, "FILECLOUD_PUBLIC_CRASH_POINT="+point,
					"FILECLOUD_PUBLIC_CRASH_TARGET="+target)
				assertProcessSIGKILL(t, command.Run())
			}
			crash(op, "between_create_identity", "")
			db, err := openClientDB(filepath.Join(clientDir, _clientDatabaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			var path, name, actionID string
			err = db.QueryRow(`SELECT checkout_paths.path, checkout_paths.temp_name, fs_actions.action_id
				FROM checkout_paths JOIN fs_actions ON fs_actions.worktree=checkout_paths.worktree
					AND fs_actions.action_id=checkout_paths.create_action_id
				WHERE checkout_paths.worktree=? AND checkout_paths.type=? AND fs_actions.op=?
					AND fs_actions.state='intent' AND fs_actions.expected_device=0 ORDER BY checkout_paths.path LIMIT 1`,
				worktree, kind, op).Scan(&path, &name, &actionID)
			if closeErr := db.Close(); err != nil || closeErr != nil {
				t.Fatal(errors.Join(err, closeErr))
			}
			internal := filepath.Join(worktree, filepath.Dir(filepath.FromSlash(path)), name)
			base := filepath.Join(filepath.Dir(internal), "Filecloud recovered "+actionID[:12])
			second, third := base+" 2", base+" 3"
			if kind == "File" {
				if err := os.WriteFile(internal, []byte("unknown"), 0o600); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Mkdir(filepath.Join(internal, "nested"), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(internal, "nested", "unknown"), []byte("unknown"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(base, []byte("collision-one"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Join(second, "nested"), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(second, "nested", "marker"), []byte("collision-two"), 0o600); err != nil {
				t.Fatal(err)
			}
			mtime := time.Date(2025, 9, 10, 11, 12, 13, 0, time.UTC)
			for _, path := range []string{internal, base, filepath.Join(second, "nested", "marker"), filepath.Join(second, "nested"), second} {
				if err := os.Chtimes(path, mtime, mtime); err != nil {
					t.Fatal(err)
				}
			}
			unknownBefore, baseBefore, secondBefore := captureExactTree(t, internal), captureExactTree(t, base), captureExactTree(t, second)
			crash(fsOpRename, "after_intent_commit", filepath.Base(second))
			crash(fsOpRename, "after_intent_commit", filepath.Base(third))
			args := bindArgs(clientDir, environment.server.URL, testClientLibraryID, worktree, testOtherDeviceID)
			err = runTest(t.Context(), args, strings.NewReader(environment.token+"\n"), io.Discard, io.Discard)
			if err == nil || !strings.Contains(err.Error(), filepath.Base(third)) {
				t.Fatalf("collision recovery error=%v", err)
			}
			assertExactTree(t, base, baseBefore)
			assertExactTree(t, second, secondBefore)
			assertRecoveryCollisionState(t, clientDir, worktree)
			if err := runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", clientDir, "--worktree", worktree},
				strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}); err != nil {
				t.Fatal(err)
			}
			assertExactTree(t, third, unknownBefore)
			assertNoSyncInternalPaths(t, worktree)
		})
	}
}

func mustAcceptanceHead(t *testing.T, environment libraryCLIEnvironment) string {
	t.Helper()
	head, err := getRemoteHead(t.Context(), mustServerURL(t, environment.server.URL), testClientLibraryID, []byte(environment.token))
	if err != nil || head.CommitID == nil {
		t.Fatalf("read acceptance Head: head=%+v err=%v", head, err)
	}
	return *head.CommitID
}

func emitRecoveryAttestation(t *testing.T, scenario, oldHead, currentHead, previousBase, currentBase string,
	confirmed, preserved []byte,
) {
	t.Helper()
	if _, _, enabled := acceptance.ActivePlatform(); !enabled {
		return
	}
	platform, filesystem, _ := acceptance.ActivePlatform()
	emitPlatformAttestation(t, platformAttestation{
		Kind: "recovery", Scenario: scenario, Platform: platform, Filesystem: filesystem,
		FailurePoint: "between_create_identity", OldHead: oldHead, CurrentHead: currentHead,
		PreviousSyncBase: previousBase, SyncBase: currentBase,
		ConfirmedInputDigests: []string{object.ID(confirmed)}, PreservedInputDigests: []string{object.ID(preserved)},
	})
}

func TestPublicSyncZeroIdentityCreateAutoRollback(t *testing.T) {
	for _, kind := range []string{"File", "Directory"} {
		t.Run(kind, func(t *testing.T) {
			environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
			before := readTestBinding(t, subscriberDir, subscriberTree)
			if kind == "File" {
				if err := os.WriteFile(filepath.Join(publisherTree, "remote"), []byte("remote"), 0o600); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Mkdir(filepath.Join(publisherTree, "remote-dir"), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			beforeHead := mustAcceptanceHead(t, environment)
			op := map[string]string{"File": fsOpCreateFile, "Directory": fsOpCreateDirectory}[kind]
			command := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
			command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1", "FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir,
				"FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree, "FILECLOUD_PUBLIC_CRASH_PHASE="+fsPhasePreBase,
				"FILECLOUD_PUBLIC_CRASH_OP="+op, "FILECLOUD_PUBLIC_CRASH_KIND="+kind,
				"FILECLOUD_PUBLIC_CRASH_POINT=between_create_identity", "FILECLOUD_PUBLIC_CRASH_ROLE=zero-identity")
			assertProcessSIGKILL(t, command.Run())
			db, err := openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			var path, name, actionID string
			err = db.QueryRow(`SELECT checkout_paths.path, checkout_paths.temp_name, fs_actions.action_id
				FROM checkout_paths JOIN fs_actions ON fs_actions.worktree = checkout_paths.worktree
					AND fs_actions.action_id = checkout_paths.create_action_id
				WHERE checkout_paths.worktree = ? AND checkout_paths.type = ? AND fs_actions.op = ?
					AND fs_actions.state = 'intent' AND fs_actions.expected_device = 0 AND fs_actions.expected_inode = 0
				ORDER BY checkout_paths.path LIMIT 1`, subscriberTree, kind, op).Scan(&path, &name, &actionID)
			if closeErr := db.Close(); err != nil || closeErr != nil {
				t.Fatal(errors.Join(err, closeErr))
			}
			internal := filepath.Join(subscriberTree, filepath.Dir(filepath.FromSlash(path)), name)
			recovered := filepath.Join(filepath.Dir(internal), "Filecloud recovered "+actionID[:12])
			mtime := time.Date(2025, 7, 8, 9, 10, 11, 0, time.UTC)
			if kind == "File" {
				if err := os.WriteFile(internal, []byte("unknown"), 0o600); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(filepath.Join(internal, "unknown"), []byte("unknown"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chtimes(internal, mtime, mtime); err != nil {
				t.Fatal(err)
			}
			err = syncTestWorktree(t, subscriberDir, subscriberTree)
			if err == nil || !strings.Contains(err.Error(), filepath.Base(recovered)) {
				t.Fatalf("public sync rollback error=%v", err)
			}
			if after := readTestBinding(t, subscriberDir, subscriberTree); after.SyncBase != before.SyncBase {
				t.Fatalf("Base advanced: before=%+v after=%+v", before, after)
			}
			assertCreateRolledBackState(t, subscriberDir, subscriberTree)
			if _, err := os.Lstat(internal); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("internal path remains: %v", err)
			}
			if err := runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", subscriberDir, "--worktree", subscriberTree},
				strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}); err != nil {
				t.Fatal(err)
			}
			assertNoBinding(t, subscriberDir, subscriberTree)
			for _, table := range []string{"fs_actions", "pending_checkouts"} {
				if count := countClientRows(t, subscriberDir, table, subscriberTree); count != 0 {
					t.Fatalf("%s rows=%d", table, count)
				}
			}
			assertNoSyncInternalPaths(t, subscriberTree)
			info, err := os.Stat(recovered)
			if err != nil || !info.ModTime().UTC().Equal(mtime) || (kind == "Directory") != info.IsDir() {
				t.Fatalf("recovered info=%v err=%v", info, err)
			}
			content := recovered
			if kind == "Directory" {
				content = filepath.Join(recovered, "unknown")
			}
			preserved, err := os.ReadFile(content)
			if err != nil || string(preserved) != "unknown" {
				t.Fatalf("recovered content=%q err=%v", preserved, err)
			}
			emitRecoveryAttestation(t, "sync checkout create "+kind+" between identity", beforeHead,
				mustAcceptanceHead(t, environment), before.SyncBase, before.SyncBase, []byte("unknown"), preserved)
		})
	}
}

func TestPublicSyncZeroIdentityTypeChangedEntryPreserved(t *testing.T) {
	for _, test := range zeroIdentityReplacementCases {
		if test.replacement == "symlink" && !testCanRenameReparsePoint() {
			continue
		}
		t.Run(test.name, func(t *testing.T) {
			_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
			beforeBase := readTestBinding(t, subscriberDir, subscriberTree)
			if test.originalKind == "File" {
				if err := os.WriteFile(filepath.Join(publisherTree, "remote"), []byte("remote"), 0o600); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Mkdir(filepath.Join(publisherTree, "remote-dir"), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			op := map[string]string{"File": fsOpCreateFile, "Directory": fsOpCreateDirectory}[test.originalKind]
			command := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
			command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1", "FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir,
				"FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree, "FILECLOUD_PUBLIC_CRASH_PHASE="+fsPhasePreBase,
				"FILECLOUD_PUBLIC_CRASH_OP="+op, "FILECLOUD_PUBLIC_CRASH_KIND="+test.originalKind,
				"FILECLOUD_PUBLIC_CRASH_POINT=between_create_identity", "FILECLOUD_PUBLIC_CRASH_ROLE=zero-identity-type-change")
			assertProcessSIGKILL(t, command.Run())
			path, name, actionID := zeroIdentityIntent(t, subscriberDir, subscriberTree, test.originalKind)
			relativeParent, _ := splitFSActionPath(path)
			internal := filepath.Join(subscriberTree, filepath.FromSlash(relativeParent), name)
			leaf, err := recoveryVisibleLeaf(relativeParent, actionID, 1)
			if err != nil {
				t.Fatal(err)
			}
			recovered := filepath.Join(filepath.Dir(internal), leaf)
			entryBefore, outside, outsideBefore := installZeroIdentityReplacement(t, internal, test.replacement)
			err = syncTestWorktree(t, subscriberDir, subscriberTree)
			if err == nil || !strings.Contains(err.Error(), leaf) {
				t.Fatalf("sync recovery error=%v", err)
			}
			if afterBase := readTestBinding(t, subscriberDir, subscriberTree); afterBase != beforeBase {
				t.Fatalf("Base changed: before=%+v after=%+v", beforeBase, afterBase)
			}
			assertZeroIdentityReplacementPreserved(t, internal, recovered, outside, entryBefore, outsideBefore)
			if err := runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", subscriberDir, "--worktree", subscriberTree},
				strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}); err != nil {
				t.Fatal(err)
			}
			assertZeroIdentityJournalsCleared(t, subscriberDir, subscriberTree)
			assertZeroIdentityReplacementPreserved(t, internal, recovered, outside, entryBefore, outsideBefore)
		})
	}
}

func TestPublicSyncCreateRecoveryCollisionChain(t *testing.T) {
	for _, kind := range []string{"File", "Directory"} {
		t.Run(kind, func(t *testing.T) {
			_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
			before := readTestBinding(t, subscriberDir, subscriberTree)
			if kind == "File" {
				if err := os.WriteFile(filepath.Join(publisherTree, "remote"), []byte("remote"), 0o600); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Mkdir(filepath.Join(publisherTree, "remote-dir"), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			op := map[string]string{"File": fsOpCreateFile, "Directory": fsOpCreateDirectory}[kind]
			crash := func(op, phase, point, target string) {
				command := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
				command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1", "FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir,
					"FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree, "FILECLOUD_PUBLIC_CRASH_PHASE="+phase,
					"FILECLOUD_PUBLIC_CRASH_OP="+op, "FILECLOUD_PUBLIC_CRASH_KIND="+kind,
					"FILECLOUD_PUBLIC_CRASH_POINT="+point, "FILECLOUD_PUBLIC_CRASH_ROLE=collision",
					"FILECLOUD_PUBLIC_CRASH_TARGET="+target)
				assertProcessSIGKILL(t, command.Run())
			}
			crash(op, fsPhasePreBase, "between_create_identity", "")
			db, err := openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			var path, name, actionID string
			err = db.QueryRow(`SELECT checkout_paths.path, checkout_paths.temp_name, fs_actions.action_id
				FROM checkout_paths JOIN fs_actions ON fs_actions.worktree=checkout_paths.worktree
					AND fs_actions.action_id=checkout_paths.create_action_id
				WHERE checkout_paths.worktree=? AND checkout_paths.type=? AND fs_actions.op=?
					AND fs_actions.state='intent' AND fs_actions.expected_device=0 ORDER BY checkout_paths.path LIMIT 1`,
				subscriberTree, kind, op).Scan(&path, &name, &actionID)
			if closeErr := db.Close(); err != nil || closeErr != nil {
				t.Fatal(errors.Join(err, closeErr))
			}
			internal := filepath.Join(subscriberTree, filepath.Dir(filepath.FromSlash(path)), name)
			base := filepath.Join(filepath.Dir(internal), "Filecloud recovered "+actionID[:12])
			second, third := base+" 2", base+" 3"
			if kind == "File" {
				if err := os.WriteFile(internal, []byte("unknown"), 0o600); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Mkdir(filepath.Join(internal, "nested"), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(internal, "nested", "unknown"), []byte("unknown"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(base, []byte("collision-one"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Join(second, "nested"), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(second, "nested", "marker"), []byte("collision-two"), 0o600); err != nil {
				t.Fatal(err)
			}
			mtime := time.Date(2025, 9, 10, 11, 12, 13, 0, time.UTC)
			for _, path := range []string{internal, base, filepath.Join(second, "nested", "marker"), filepath.Join(second, "nested"), second} {
				if err := os.Chtimes(path, mtime, mtime); err != nil {
					t.Fatal(err)
				}
			}
			unknownBefore, baseBefore, secondBefore := captureExactTree(t, internal), captureExactTree(t, base), captureExactTree(t, second)
			crash(fsOpRename, fsPhaseRollback, "after_intent_commit", filepath.Base(second))
			crash(fsOpRename, fsPhaseRollback, "after_intent_commit", filepath.Base(third))
			err = syncTestWorktree(t, subscriberDir, subscriberTree)
			if err == nil || !strings.Contains(err.Error(), filepath.Base(third)) {
				t.Fatalf("sync collision recovery error=%v", err)
			}
			if after := readTestBinding(t, subscriberDir, subscriberTree); after.SyncBase != before.SyncBase {
				t.Fatalf("Base advanced: before=%+v after=%+v", before, after)
			}
			assertExactTree(t, base, baseBefore)
			assertExactTree(t, second, secondBefore)
			assertRecoveryCollisionState(t, subscriberDir, subscriberTree)
			if err := runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", subscriberDir, "--worktree", subscriberTree},
				strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}); err != nil {
				t.Fatal(err)
			}
			assertExactTree(t, third, unknownBefore)
			assertNoSyncInternalPaths(t, subscriberTree)
		})
	}
}

func platformSyncCrashScenario(name, point string) string {
	return "sync checkout fs crash " + name + " " + point
}

func TestPublicSyncSubprocessCrashMatrix(t *testing.T) {
	categories := []struct{ name, phase, op, kind string }{
		{"capture-file", fsPhasePreBase, fsOpRename, "File"},
		{"capture-directory", fsPhasePreBase, fsOpRename, "Directory"},
		{"post-base-file", fsPhasePostBase, fsOpUnlink, "File"},
		{"post-base-directory", fsPhasePostBase, fsOpRmdir, "Directory"},
	}
	points := []string{"before_intent_commit", "after_intent_commit", "after_action", "after_parent_sync", "after_completed"}
	for _, category := range categories {
		for _, point := range points {
			test := struct{ name, phase, op, kind, point string }{category.name, category.phase, category.op, category.kind, point}
			t.Run(test.name+"/"+test.point, func(t *testing.T) {
				environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
				if test.kind == "Directory" {
					if err := os.MkdirAll(filepath.Join(publisherTree, "old", "nested"), 0o700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(publisherTree, "old", "nested", "file"), []byte("old"), 0o600); err != nil {
						t.Fatal(err)
					}
					if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
						t.Fatal(err)
					}
					if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
						t.Fatal(err)
					}
					if err := os.RemoveAll(filepath.Join(publisherTree, "old")); err != nil {
						t.Fatal(err)
					}
				} else if err := os.WriteFile(filepath.Join(publisherTree, "remote"), []byte("remote"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := syncTestWorktreeConfirmingDeletes(t, publisherDir, publisherTree); err != nil {
					t.Fatal(err)
				}
				command := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
				command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1", "FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir,
					"FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree, "FILECLOUD_PUBLIC_CRASH_PHASE="+test.phase,
					"FILECLOUD_PUBLIC_CRASH_OP="+test.op, "FILECLOUD_PUBLIC_CRASH_KIND="+test.kind,
					"FILECLOUD_PUBLIC_CRASH_POINT="+test.point, "FILECLOUD_PUBLIC_CRASH_ROLE="+test.name)
				assertProcessSIGKILL(t, command.Run())
				if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
					t.Fatalf("public sync restart: %v", err)
				}
				assertTestConverged(t, environment, subscriberDir, subscriberTree)
				for _, table := range []string{"fs_actions", "pending_checkouts"} {
					if count := countClientRows(t, subscriberDir, table, subscriberTree); count != 0 {
						t.Fatalf("%s rows=%d", table, count)
					}
				}
				assertNoSyncInternalPaths(t, subscriberTree)
				confirmed := platformConfirmedInputs("base")
				if test.kind == "File" {
					confirmed = platformConfirmedInputs("base", "remote")
				}
				assertPlatformConverged(t, platformSyncCrashScenario(test.name, test.point),
					environment, subscriberDir, subscriberTree, confirmed)
			})
		}
	}
}

type transactionBindingState struct {
	serverURL, libraryID, worktree, userID, deviceID string
	syncBase, syncBaseRoot, headETag                 string
	accessToken                                      string
}

type transactionPendingState struct {
	targetCommit, targetRoot, headETag, applyState string
}

type transactionCheckoutState struct {
	path, kind, objectID, canonicalMtime, actualMtime, tempName, rollbackName string
	size                                                                      int64
	tempDevice, tempInode, targetDevice, targetInode                          uint64
	completed                                                                 int
}

type transactionActionState struct {
	phase, op, state, outcome, source, target string
}

type transactionRecoveryState struct {
	path, recoveryName, tombstoneName, kind, objectID, mtime string
	size                                                     int64
	device, inode                                            uint64
	completed                                                int
}

type transactionIndexState struct {
	path, kind, objectID, canonicalMtime, actualMtime string
	size                                              int64
}

func readTransactionBindingState(t *testing.T, db *sql.DB, worktree string) transactionBindingState {
	t.Helper()
	var value transactionBindingState
	if err := db.QueryRow(`SELECT server_url, library_id, worktree, user_id, device_id, sync_base_commit,
		sync_base_root, head_etag, access_token FROM bindings WHERE worktree=?`, worktree).Scan(&value.serverURL,
		&value.libraryID, &value.worktree, &value.userID, &value.deviceID, &value.syncBase, &value.syncBaseRoot,
		&value.headETag, &value.accessToken); err != nil {
		t.Fatal(err)
	}
	return value
}

func readTransactionPendingStates(t *testing.T, db *sql.DB, worktree string) []transactionPendingState {
	t.Helper()
	rows, err := db.Query(`SELECT target_commit, target_root, head_etag, apply_state
		FROM pending_checkouts WHERE worktree=? ORDER BY target_commit, target_root, head_etag, apply_state`, worktree)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var values []transactionPendingState
	for rows.Next() {
		var value transactionPendingState
		if err := rows.Scan(&value.targetCommit, &value.targetRoot, &value.headETag, &value.applyState); err != nil {
			t.Fatal(err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return values
}

func readTransactionCheckoutStates(t *testing.T, db *sql.DB, worktree string) []transactionCheckoutState {
	t.Helper()
	rows, err := db.Query(`SELECT path, type, object_id, canonical_mtime, actual_mtime, size, temp_name,
		temp_device, temp_inode, target_device, target_inode, completed, rollback_name
		FROM checkout_paths WHERE worktree=? ORDER BY path`, worktree)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var values []transactionCheckoutState
	for rows.Next() {
		var value transactionCheckoutState
		if err := rows.Scan(&value.path, &value.kind, &value.objectID, &value.canonicalMtime, &value.actualMtime,
			&value.size, &value.tempName, &value.tempDevice, &value.tempInode, &value.targetDevice,
			&value.targetInode, &value.completed, &value.rollbackName); err != nil {
			t.Fatal(err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return values
}

func readTransactionActionStates(t *testing.T, db *sql.DB, worktree string) []transactionActionState {
	t.Helper()
	rows, err := db.Query(`SELECT phase, op, state, action_outcome, source_name, target_name
		FROM fs_actions WHERE worktree=? ORDER BY action_order`, worktree)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var values []transactionActionState
	for rows.Next() {
		var value transactionActionState
		if err := rows.Scan(&value.phase, &value.op, &value.state, &value.outcome, &value.source, &value.target); err != nil {
			t.Fatal(err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return values
}

func readTransactionRecoveryStates(t *testing.T, db *sql.DB, worktree string) []transactionRecoveryState {
	t.Helper()
	rows, err := db.Query(`SELECT path, recovery_name, tombstone_name, type, object_id, canonical_mtime,
		size, device, inode, completed FROM sync_recoveries WHERE worktree=?
		ORDER BY path, recovery_name, tombstone_name, type, object_id, canonical_mtime, size, device, inode, completed`, worktree)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var values []transactionRecoveryState
	for rows.Next() {
		var value transactionRecoveryState
		if err := rows.Scan(&value.path, &value.recoveryName, &value.tombstoneName, &value.kind, &value.objectID,
			&value.mtime, &value.size, &value.device, &value.inode, &value.completed); err != nil {
			t.Fatal(err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return values
}

func readTransactionIndexStates(t *testing.T, db *sql.DB, worktree string) []transactionIndexState {
	t.Helper()
	rows, err := db.Query(`SELECT path, type, object_id, canonical_mtime, actual_mtime, size
		FROM path_index WHERE worktree=? ORDER BY path`, worktree)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var values []transactionIndexState
	for rows.Next() {
		var value transactionIndexState
		if err := rows.Scan(&value.path, &value.kind, &value.objectID, &value.canonicalMtime, &value.actualMtime, &value.size); err != nil {
			t.Fatal(err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return values
}

func assertTransactionStateEqual[T any](t *testing.T, label string, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s mismatch:\n got: %#v\nwant: %#v", label, got, want)
	}
}

func TestPublicSyncTransactionCrashMatrix(t *testing.T) {
	for _, point := range []string{"before_base_commit", "after_base_commit", "before_cleanup_commit", "after_cleanup_commit"} {
		t.Run(point, func(t *testing.T) {
			environment, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
			subscriberDB, err := openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), true)
			if err != nil {
				t.Fatal(err)
			}
			beforeBinding := readTransactionBindingState(t, subscriberDB, subscriberTree)
			oldIndex := readTransactionIndexStates(t, subscriberDB, subscriberTree)
			if err := subscriberDB.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(publisherTree, "remote"), []byte("remote"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
				t.Fatal(err)
			}
			publisherDB, err := openClientDB(filepath.Join(publisherDir, _clientDatabaseName), true)
			if err != nil {
				t.Fatal(err)
			}
			targetBinding := readTransactionBindingState(t, publisherDB, publisherTree)
			newIndex := readTransactionIndexStates(t, publisherDB, publisherTree)
			if err := publisherDB.Close(); err != nil {
				t.Fatal(err)
			}
			command := exec.Command(os.Args[0], "-test.run=^TestPublicSyncTransactionCrashHelper$")
			command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_TRANSACTION_HELPER=1", "FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir,
				"FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree, "FILECLOUD_PUBLIC_TRANSACTION_POINT="+point)
			assertProcessSIGKILL(t, command.Run())

			crashedDB, err := openClientDB(filepath.Join(subscriberDir, _clientDatabaseName), true)
			if err != nil {
				t.Fatal(err)
			}
			wantBinding := beforeBinding
			wantIndex := oldIndex
			wantPending := []transactionPendingState{{targetBinding.syncBase, targetBinding.syncBaseRoot, targetBinding.headETag, "applying"}}
			checkout := readTransactionCheckoutStates(t, crashedDB, subscriberTree)
			actions := readTransactionActionStates(t, crashedDB, subscriberTree)
			recoveries := readTransactionRecoveryStates(t, crashedDB, subscriberTree)
			if point != "before_base_commit" {
				wantBinding.syncBase, wantBinding.syncBaseRoot, wantBinding.headETag = targetBinding.syncBase, targetBinding.syncBaseRoot, targetBinding.headETag
				wantIndex = newIndex
				wantPending[0].applyState = "finalized"
			}
			if point == "after_cleanup_commit" {
				wantPending = nil
				assertTransactionStateEqual(t, "checkout_paths", checkout, []transactionCheckoutState(nil))
				assertTransactionStateEqual(t, "fs_actions", actions, []transactionActionState(nil))
				assertTransactionStateEqual(t, "sync_recoveries", recoveries, []transactionRecoveryState(nil))
			} else {
				if len(checkout) != 2 || checkout[0].path != "base" || checkout[1].path != "remote" {
					t.Fatalf("checkout_paths fixture mismatch: %#v", checkout)
				}
				wantCheckout := make([]transactionCheckoutState, len(checkout))
				if len(actions) < 1 {
					t.Fatal("fs_actions fixture is empty")
				}
				recoveryName := actions[0].target
				wantActions := []transactionActionState{{fsPhasePreBase, fsOpRename, fsStateCompleted, "", "base", recoveryName}}
				wantRecoveries := []transactionRecoveryState(nil)
				if point == "before_base_commit" || point == "after_base_commit" {
					var base transactionIndexState
					for _, indexed := range oldIndex {
						if indexed.path == "base" {
							base = indexed
						}
					}
					stat, err := testPathStat(filepath.Join(subscriberTree, recoveryName))
					if err != nil {
						t.Fatal(err)
					}
					wantRecoveries = []transactionRecoveryState{{path: base.path, recoveryName: recoveryName,
						kind: base.kind, objectID: base.objectID, mtime: base.canonicalMtime, size: base.size,
						device: uint64(stat.Dev), inode: stat.Ino, completed: 1}}
				}
				assertTransactionStateEqual(t, "sync_recoveries", recoveries, wantRecoveries)
				for index, path := range newIndex {
					stat, err := testPathStat(filepath.Join(subscriberTree, path.path))
					if err != nil {
						t.Fatal(err)
					}
					wantCheckout[index] = transactionCheckoutState{path: path.path, kind: path.kind, objectID: path.objectID,
						canonicalMtime: path.canonicalMtime, actualMtime: path.actualMtime, size: path.size,
						tempName: checkout[index].tempName, tempDevice: uint64(stat.Dev), tempInode: stat.Ino, completed: 1}
					wantActions = append(wantActions,
						transactionActionState{fsPhasePreBase, fsOpCreateFile, fsStateCompleted, "", checkout[index].tempName, ""},
						transactionActionState{fsPhasePreBase, fsOpMtime, fsStateCompleted, "", checkout[index].tempName, ""},
						transactionActionState{fsPhasePreBase, fsOpRename, fsStateCompleted, "", checkout[index].tempName, path.path})
				}
				if point == "before_cleanup_commit" {
					if len(actions) != len(wantActions)+2 {
						t.Fatalf("cleanup fs_actions fixture mismatch: %#v", actions)
					}
					trashName := actions[len(wantActions)].target
					wantActions = append(wantActions,
						transactionActionState{fsPhasePostBase, fsOpRename, fsStateCompleted, "", recoveryName, trashName},
						transactionActionState{fsPhasePostBase, fsOpUnlink, fsStateCompleted, "", trashName, ""})
				}
				assertTransactionStateEqual(t, "checkout_paths", checkout, wantCheckout)
				assertTransactionStateEqual(t, "fs_actions", actions, wantActions)
			}
			assertTransactionStateEqual(t, "binding", readTransactionBindingState(t, crashedDB, subscriberTree), wantBinding)
			assertTransactionStateEqual(t, "pending_checkouts", readTransactionPendingStates(t, crashedDB, subscriberTree), wantPending)
			assertTransactionStateEqual(t, "path_index", readTransactionIndexStates(t, crashedDB, subscriberTree), wantIndex)
			if err := crashedDB.Close(); err != nil {
				t.Fatal(err)
			}

			if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
				t.Fatalf("public transaction restart: %v", err)
			}
			assertPublicCheckoutConverged(t, environment, subscriberDir, subscriberTree)
			assertPlatformConverged(t, "sync transaction crash "+point, environment, subscriberDir, subscriberTree,
				platformConfirmedInputs("base", "remote"))
			for _, table := range []string{"fs_actions", "pending_checkouts", "checkout_paths"} {
				if count := countClientRows(t, subscriberDir, table, subscriberTree); count != 0 {
					t.Fatalf("%s rows=%d", table, count)
				}
			}
			assertNoSyncInternalPaths(t, subscriberTree)
		})
	}
}

func TestPublicSyncTransactionCrashHelper(t *testing.T) {
	if os.Getenv("FILECLOUD_PUBLIC_TRANSACTION_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	fault := func(point string) error {
		if point == os.Getenv("FILECLOUD_PUBLIC_TRANSACTION_POINT") {
			return killTestProcess()
		}
		return nil
	}
	_ = runLibraryWithConfig(context.Background(), []string{"sync", "--client-dir", os.Getenv("FILECLOUD_PUBLIC_CRASH_CLIENT"),
		"--worktree", os.Getenv("FILECLOUD_PUBLIC_CRASH_WORKTREE")}, strings.NewReader(""), io.Discard, io.Discard,
		libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, fsTransactionFault: fault})
	os.Exit(98)
}

func TestPublicUnbindTempCleanupCrashMatrix(t *testing.T) {
	points := []string{"before_intent_commit", "after_intent_commit", "after_action", "after_parent_sync", "after_completed"}
	for _, kind := range []string{"File", "Directory"} {
		for _, point := range points {
			t.Run(kind+"/"+point, func(t *testing.T) {
				environment, _, _, _ := importedRemoteCheckout(t)
				clientDir, worktree := newClientPaths(t)
				setup := exec.Command(os.Args[0], "-test.run=^TestPublicFSActionCrashHelper$")
				setup.Env = append(os.Environ(), "FILECLOUD_PUBLIC_CRASH_HELPER=1", "FILECLOUD_PUBLIC_CRASH_CLIENT="+clientDir,
					"FILECLOUD_PUBLIC_CRASH_WORKTREE="+worktree, "FILECLOUD_PUBLIC_CRASH_SERVER="+environment.server.URL,
					"FILECLOUD_PUBLIC_CRASH_TOKEN="+environment.token, "FILECLOUD_PUBLIC_CRASH_OP="+map[string]string{"File": fsOpCreateFile, "Directory": fsOpCreateDirectory}[kind],
					"FILECLOUD_PUBLIC_CRASH_KIND="+kind, "FILECLOUD_PUBLIC_CRASH_POINT=after_action")
				assertProcessSIGKILL(t, setup.Run())
				if err := os.WriteFile(filepath.Join(worktree, "user"), []byte("user"), 0o600); err != nil {
					t.Fatal(err)
				}
				command := exec.Command(os.Args[0], "-test.run=^TestPublicUnbindFSActionCrashHelper$")
				command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_UNBIND_HELPER=1", "FILECLOUD_PUBLIC_CRASH_CLIENT="+clientDir,
					"FILECLOUD_PUBLIC_CRASH_WORKTREE="+worktree, "FILECLOUD_PUBLIC_CRASH_POINT="+point,
					"FILECLOUD_PUBLIC_CRASH_KIND="+kind, "FILECLOUD_PUBLIC_CRASH_OP="+map[string]string{"File": fsOpUnlink, "Directory": fsOpRmdir}[kind])
				assertProcessSIGKILL(t, command.Run())
				if err := runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", clientDir, "--worktree", worktree},
					strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}); err != nil {
					t.Fatalf("public temp cleanup unbind restart: %v", err)
				}
				if data, err := os.ReadFile(filepath.Join(worktree, "user")); err != nil || string(data) != "user" {
					t.Fatalf("user content=%q err=%v", data, err)
				}
				assertNoBinding(t, clientDir, worktree)
				for _, table := range []string{"fs_actions", "pending_checkouts"} {
					if count := countClientRows(t, clientDir, table, worktree); count != 0 {
						t.Fatalf("%s rows=%d", table, count)
					}
				}
				assertNoSyncInternalPaths(t, worktree)
			})
		}
	}
}

func TestPublicUnbindRollbackSubprocessCrash(t *testing.T) {
	points := []string{"before_intent_commit", "after_intent_commit", "after_action", "after_parent_sync", "after_completed"}
	for _, kind := range []string{"File", "Directory"} {
		for _, point := range points {
			t.Run(kind+"/"+point, func(t *testing.T) {
				_, publisherDir, publisherTree, subscriberDir, subscriberTree, _, _ := newSyncPair(t)
				if kind == "Directory" {
					if err := os.MkdirAll(filepath.Join(publisherTree, "old", "nested"), 0o700); err != nil {
						t.Fatal(err)
					}
					if err := syncTestWorktree(t, publisherDir, publisherTree); err != nil {
						t.Fatal(err)
					}
					if err := syncTestWorktree(t, subscriberDir, subscriberTree); err != nil {
						t.Fatal(err)
					}
					if err := os.RemoveAll(filepath.Join(publisherTree, "old")); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.WriteFile(filepath.Join(publisherTree, "remote"), []byte("remote"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := syncTestWorktreeConfirmingDeletes(t, publisherDir, publisherTree); err != nil {
					t.Fatal(err)
				}
				setup := exec.Command(os.Args[0], "-test.run=^TestPublicSyncFSActionCrashHelper$")
				setup.Env = append(os.Environ(), "FILECLOUD_PUBLIC_SYNC_HELPER=1", "FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir,
					"FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree, "FILECLOUD_PUBLIC_CRASH_PHASE="+fsPhasePreBase,
					"FILECLOUD_PUBLIC_CRASH_OP="+fsOpCreateFile, "FILECLOUD_PUBLIC_CRASH_KIND=File",
					"FILECLOUD_PUBLIC_CRASH_POINT=after_intent_commit", "FILECLOUD_PUBLIC_CRASH_ROLE=rollback-setup")
				assertProcessSIGKILL(t, setup.Run())
				command := exec.Command(os.Args[0], "-test.run=^TestPublicUnbindFSActionCrashHelper$")
				command.Env = append(os.Environ(), "FILECLOUD_PUBLIC_UNBIND_HELPER=1", "FILECLOUD_PUBLIC_CRASH_CLIENT="+subscriberDir,
					"FILECLOUD_PUBLIC_CRASH_WORKTREE="+subscriberTree, "FILECLOUD_PUBLIC_CRASH_POINT="+point,
					"FILECLOUD_PUBLIC_CRASH_KIND="+kind, "FILECLOUD_PUBLIC_CRASH_OP="+fsOpRename)
				assertProcessSIGKILL(t, command.Run())
				if err := runLibraryWithConfig(t.Context(), []string{"unbind", "--client-dir", subscriberDir, "--worktree", subscriberTree},
					strings.NewReader(""), io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }}); err != nil {
					t.Fatalf("public unbind restart: %v", err)
				}
				assertNoBinding(t, subscriberDir, subscriberTree)
				for _, table := range []string{"fs_actions", "pending_checkouts"} {
					if count := countClientRows(t, subscriberDir, table, subscriberTree); count != 0 {
						t.Fatalf("%s rows=%d", table, count)
					}
				}
				assertNoSyncInternalPaths(t, subscriberTree)
			})
		}
	}
}

func TestPublicUnbindFSActionCrashHelper(t *testing.T) {
	if os.Getenv("FILECLOUD_PUBLIC_UNBIND_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	fault := func(point string, action fsAction) error {
		if point == os.Getenv("FILECLOUD_PUBLIC_CRASH_POINT") && action.Phase == fsPhaseRollback &&
			action.Op == os.Getenv("FILECLOUD_PUBLIC_CRASH_OP") && action.ExpectedKind == os.Getenv("FILECLOUD_PUBLIC_CRASH_KIND") {
			return killTestProcess()
		}
		return nil
	}
	_ = runLibraryWithConfig(context.Background(), []string{"unbind", "--client-dir", os.Getenv("FILECLOUD_PUBLIC_CRASH_CLIENT"),
		"--worktree", os.Getenv("FILECLOUD_PUBLIC_CRASH_WORKTREE")}, strings.NewReader(""), io.Discard, io.Discard,
		libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, fsActionFault: fault})
	os.Exit(98)
}

func TestPublicSyncFSActionCrashHelper(t *testing.T) {
	if os.Getenv("FILECLOUD_PUBLIC_SYNC_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	collisionInjected := false
	casefoldAliasInjected := false
	mutationInjected := false
	mixedPromotionDisturbed := false
	mixedRecoveryNames := make(map[string]string)
	var mixedOriginalMtime time.Time
	matchedFaults := 0
	fault := func(point string, action fsAction) error {
		role := os.Getenv("FILECLOUD_PUBLIC_CRASH_ROLE")
		if role == "promotion-collision" && point == "before_action" && action.ExpectedObject != "" &&
			action.OriginActionID == "" && !collisionInjected {
			collisionInjected = true
			if err := os.WriteFile(filepath.Join(os.Getenv("FILECLOUD_PUBLIC_CRASH_WORKTREE"), filepath.FromSlash(action.Target)),
				[]byte("racing"), 0o600); err != nil {
				return err
			}
		}
		if role == "casefold-alias-relocation" && point == "before_action" && action.ExpectedObject != "" &&
			action.OriginActionID == "" && !casefoldAliasInjected {
			casefoldAliasInjected = true
			parent, leaf := filepath.Split(filepath.Join(os.Getenv("FILECLOUD_PUBLIC_CRASH_WORKTREE"), filepath.FromSlash(action.Target)))
			if err := os.WriteFile(filepath.Join(parent, strings.ToUpper(leaf)), []byte("alias"), 0o600); err != nil {
				return err
			}
		}
		if role == "mixed-restore-promotion" && point == "after_completed" && action.Phase == fsPhasePreBase &&
			action.Op == fsOpRename && action.ExpectedObject != "" && action.OriginActionID == "" && !mixedPromotionDisturbed {
			second := mixedRecoveryNames[os.Getenv("FILECLOUD_PUBLIC_SECOND_PATH")]
			info, err := os.Stat(filepath.Join(os.Getenv("FILECLOUD_PUBLIC_CRASH_WORKTREE"), second))
			if err != nil {
				return err
			}
			mixedOriginalMtime = info.ModTime()
			mixedPromotionDisturbed = true
			changed := mixedOriginalMtime.Add(time.Hour)
			if err := os.Chtimes(filepath.Join(os.Getenv("FILECLOUD_PUBLIC_CRASH_WORKTREE"), second), changed, changed); err != nil {
				return err
			}
		}
		if role == "mixed-restore-promotion" && point == "before_intent_commit" && action.Op == fsOpRestorePromotion &&
			mixedPromotionDisturbed {
			second := mixedRecoveryNames[os.Getenv("FILECLOUD_PUBLIC_SECOND_PATH")]
			if err := os.Chtimes(filepath.Join(os.Getenv("FILECLOUD_PUBLIC_CRASH_WORKTREE"), second),
				mixedOriginalMtime, mixedOriginalMtime); err != nil {
				return err
			}
		}
		matches := point == os.Getenv("FILECLOUD_PUBLIC_CRASH_POINT") && action.Phase == os.Getenv("FILECLOUD_PUBLIC_CRASH_PHASE") &&
			action.Op == os.Getenv("FILECLOUD_PUBLIC_CRASH_OP") && action.ExpectedKind == os.Getenv("FILECLOUD_PUBLIC_CRASH_KIND")
		if matches && strings.HasPrefix(role, "capture-") {
			matches = strings.HasPrefix(action.InternalTarget, syncRecoveryPrefix)
		}
		if matches && (role == "promotion" || role == "pre-promotion-mutation") {
			matches = action.ExpectedObject != "" && action.OriginActionID == ""
		}
		if matches && (role == "promotion-collision" || role == "late-promotion" || role == "casefold-alias-relocation") {
			matches = action.ExpectedObject != "" && action.OriginActionID != ""
		}
		if matches && role == "fallback-root-create" {
			matches = strings.HasPrefix(action.InternalTarget, fsPromotionFallbackOwnerPrefix)
		}
		if target := os.Getenv("FILECLOUD_PUBLIC_CRASH_TARGET"); target != "" {
			matches = matches && action.Target == target
		}
		if matches {
			matchedFaults++
			matchIndex, _ := strconv.Atoi(os.Getenv("FILECLOUD_PUBLIC_CRASH_MATCH_INDEX"))
			if matchIndex <= 1 || matchedFaults == matchIndex {
				return killTestProcess()
			}
		}
		return nil
	}
	config := libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, fsActionFault: fault}
	if os.Getenv("FILECLOUD_PUBLIC_CRASH_ROLE") == "mixed-restore-promotion" {
		config.afterSyncRecoveryRename = func(path, recoveryName string) error {
			mixedRecoveryNames[path] = recoveryName
			return nil
		}
	}
	if os.Getenv("FILECLOUD_PUBLIC_CRASH_ROLE") == "casefold-alias-relocation" {
		config.afterSyncRecoveryRename = func(string, string) error {
			if mutationInjected {
				return nil
			}
			mutationInjected = true
			held, err := openTestHeldConflictFile()
			if err != nil {
				return err
			}
			_, err = held.WriteAt([]byte("late!"), 0)
			return errors.Join(err, held.Sync())
		}
	}
	if os.Getenv("FILECLOUD_PUBLIC_CRASH_ROLE") == "pre-promotion-mutation" {
		config.afterSyncRecoveryRename = func(path, _ string) error {
			mutationPath := os.Getenv("FILECLOUD_PUBLIC_MUTATION_PATH")
			if mutationInjected || (mutationPath != path && !strings.HasPrefix(mutationPath, path+"/")) {
				return nil
			}
			mutationInjected = true
			held, err := openTestHeldConflictFile()
			if err != nil {
				return err
			}
			if _, err := held.WriteAt([]byte("changed"), 0); err != nil {
				return err
			}
			if err := held.Sync(); err != nil {
				return err
			}
			if os.Getenv("FILECLOUD_PUBLIC_CRASH_COLLISION") != "1" {
				return nil
			}
			db, err := openClientDB(filepath.Join(os.Getenv("FILECLOUD_PUBLIC_CRASH_CLIENT"), _clientDatabaseName), true)
			if err != nil {
				return err
			}
			promotions, loadErr := loadConflictPromotions(context.Background(), db, os.Getenv("FILECLOUD_PUBLIC_CRASH_WORKTREE"))
			closeErr := db.Close()
			if loadErr != nil || closeErr != nil {
				return errors.Join(loadErr, closeErr)
			}
			for _, promotion := range promotions {
				if promotion.source != mutationPath {
					continue
				}
				suffix, err := nextConflictChainPath(promotion.target)
				if err != nil {
					return err
				}
				parent, leaf := splitFSActionPath(suffix)
				return os.WriteFile(filepath.Join(os.Getenv("FILECLOUD_PUBLIC_CRASH_WORKTREE"), filepath.FromSlash(parent), strings.ToUpper(leaf)), []byte("occupied"), 0o600)
			}
			return errors.New("conflict provenance for inherited descriptor is absent")
		}
	}
	_ = runLibraryWithConfig(context.Background(), []string{"sync", "--client-dir", os.Getenv("FILECLOUD_PUBLIC_CRASH_CLIENT"),
		"--worktree", os.Getenv("FILECLOUD_PUBLIC_CRASH_WORKTREE")}, strings.NewReader(""), io.Discard, io.Discard, config)
	os.Exit(98)
}

func TestPublicFSActionCrashHelper(t *testing.T) {
	if os.Getenv("FILECLOUD_PUBLIC_CRASH_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	fault := func(point string, action fsAction) error {
		matches := point == os.Getenv("FILECLOUD_PUBLIC_CRASH_POINT") && action.Op == os.Getenv("FILECLOUD_PUBLIC_CRASH_OP") &&
			action.ExpectedKind == os.Getenv("FILECLOUD_PUBLIC_CRASH_KIND")
		if target := os.Getenv("FILECLOUD_PUBLIC_CRASH_TARGET"); target != "" {
			matches = matches && action.Target == target
		}
		if matches {
			return killTestProcess()
		}
		return nil
	}
	args := bindArgs(os.Getenv("FILECLOUD_PUBLIC_CRASH_CLIENT"), os.Getenv("FILECLOUD_PUBLIC_CRASH_SERVER"),
		testClientLibraryID, os.Getenv("FILECLOUD_PUBLIC_CRASH_WORKTREE"), testOtherDeviceID)
	_ = runLibraryWithConfig(context.Background(), args[1:], strings.NewReader(os.Getenv("FILECLOUD_PUBLIC_CRASH_TOKEN")+"\n"),
		io.Discard, io.Discard, libraryClientConfig{checkFilesystem: func(*os.File) error { return nil }, fsActionFault: fault})
	os.Exit(98)
}

func TestFSActionCrashHelper(t *testing.T) {
	if os.Getenv("FILECLOUD_FS_CRASH_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	ctx := context.Background()
	clientDir, rootPath := os.Getenv("FILECLOUD_FS_CRASH_CLIENT"), os.Getenv("FILECLOUD_FS_CRASH_ROOT")
	db, err := initializeClientDB(ctx, clientDir, syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	root, err := openWorktreeRoot(rootPath, func(*os.File) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	scenario := os.Getenv("FILECLOUD_FS_CRASH_SCENARIO")
	action := fsAction{Worktree: rootPath, ActionID: "00112233445566778899aabbccddeeff", Order: 1,
		Phase: fsPhasePreBase, Parent: "", ParentDevice: root.device, ParentInode: root.inode, State: fsStateIntent}
	switch scenario {
	case "create-file", "create-directory":
		action.Source = "created"
		action.InternalSource = fsActionInternalPrefix + action.ActionID
		action.Source = action.InternalSource
		action.ExpectedKind, action.Op = "File", fsOpCreateFile
		if scenario == "create-directory" {
			action.ExpectedKind, action.Op = "Directory", fsOpCreateDirectory
		}
	case "install-file-rename", "capture-rename", "rollback-rename":
		if err := os.WriteFile(filepath.Join(rootPath, "source"), []byte("durable"), 0o600); err != nil {
			t.Fatal(err)
		}
		action.Source, action.Target, action.ExpectedKind, action.Op = "source", "target", "File", fsOpRename
		if scenario == "capture-rename" {
			action.Phase = fsPhasePreBase
		} else if scenario == "rollback-rename" {
			action.Phase = fsPhaseRollback
		}
	case "install-directory-rename":
		if err := os.Mkdir(filepath.Join(rootPath, "source"), 0o700); err != nil {
			t.Fatal(err)
		}
		action.Source, action.Target, action.ExpectedKind, action.Op = "source", "target", "Directory", fsOpRename
	case "mtime":
		if err := os.WriteFile(filepath.Join(rootPath, "source"), []byte("durable"), 0o600); err != nil {
			t.Fatal(err)
		}
		action.Source, action.ExpectedKind, action.Op = "source", "File", fsOpMtime
		action.ExpectedMtime = "2000-01-02T03:04:05Z"
	case "post-base-file-cleanup":
		if err := os.WriteFile(filepath.Join(rootPath, "trash"), []byte("durable"), 0o600); err != nil {
			t.Fatal(err)
		}
		action.Source, action.ExpectedKind, action.Op, action.Phase = "trash", "File", fsOpUnlink, fsPhasePostBase
	case "post-base-directory-cleanup":
		if err := os.Mkdir(filepath.Join(rootPath, "trash"), 0o700); err != nil {
			t.Fatal(err)
		}
		action.Source, action.ExpectedKind, action.Op, action.Phase = "trash", "Directory", fsOpRmdir, fsPhasePostBase
	default:
		t.Fatalf("unknown scenario %q", scenario)
	}
	if action.Op != fsOpCreateFile && action.Op != fsOpCreateDirectory {
		var stat fscompat.Stat_t
		if err := fscompat.Fstatat(int(root.directory.Fd()), action.Source, &stat, fscompat.AT_SYMLINK_NOFOLLOW); err != nil {
			t.Fatal(err)
		}
		action.ExpectedDevice, action.ExpectedInode = uint64(stat.Dev), stat.Ino
		if action.Op == fsOpUnlink {
			file, info, err := openScannableAt(root.directory, action.Source, action.Source)
			if err != nil {
				t.Fatal(err)
			}
			snapshot := worktreeSnapshot{blocks: make(map[string]blockSource)}
			action.ExpectedObject, err = scanRegularFile(file, action.Source, info, &snapshot)
			action.ExpectedSize = info.Size()
			action.ExpectedMtime = info.ModTime().UTC().Truncate(time.Second).Format("2006-01-02T15:04:05Z")
			if closeErr := file.Close(); err != nil || closeErr != nil {
				t.Fatal(errors.Join(err, closeErr))
			}
		}
	}
	point := os.Getenv("FILECLOUD_FS_CRASH_POINT")
	fault := func(at string, _ fsAction) error {
		if at != point {
			return nil
		}
		return killTestProcess()
	}
	_ = executeFSAction(ctx, db, root, action, fault)
	os.Exit(98)
}

func TestFSActionRemoveIntentPreservesOldFDModification(t *testing.T) {
	clientDir, rootPath := t.TempDir(), t.TempDir()
	db, err := initializeClientDB(t.Context(), clientDir, syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	root, err := openWorktreeRoot(rootPath, func(*os.File) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	path := filepath.Join(rootPath, "remove")
	if err := os.WriteFile(path, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	held, err := openTestHeldFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	file, info, err := openScannableAt(root.directory, "remove", "remove")
	if err != nil {
		t.Fatal(err)
	}
	snapshot := worktreeSnapshot{blocks: make(map[string]blockSource)}
	id, err := scanRegularFile(file, "remove", info, &snapshot)
	if closeErr := file.Close(); err != nil || closeErr != nil {
		t.Fatal(errors.Join(err, closeErr))
	}
	mtime := info.ModTime().UTC().Truncate(time.Second).Format("2006-01-02T15:04:05Z")
	var stat fscompat.Stat_t
	if err := fscompat.Fstat(int(held.Fd()), &stat); err != nil {
		t.Fatal(err)
	}
	modified := false
	fault := func(point string, _ fsAction) error {
		if point != "after_intent_commit" {
			return nil
		}
		modified = true
		if _, err := held.WriteAt([]byte("changed!"), 0); err != nil {
			t.Fatal(err)
		}
		if err := held.Sync(); err != nil {
			t.Fatal(err)
		}
		return errors.New("stop after remove Intent")
	}
	if err := journalRemove(t.Context(), db, root, rootPath, fsPhasePostBase, "", "remove", "File", "",
		id, mtime, info.Size(), uint64(stat.Dev), stat.Ino, fault); err == nil || !modified {
		t.Fatalf("remove error=%v modified=%v", err, modified)
	}
	if err := recoverFSActions(t.Context(), db, rootPath, root, nil); err == nil {
		t.Fatal("modified inode was removed")
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "changed!" {
		t.Fatalf("preserved data=%q err=%v", data, err)
	}
}

func TestFSActionRejectsSyntacticallyValidOwnershipSwap(t *testing.T) {
	first := fsActionInternalPrefix + "00112233445566778899aabbccddeeff"
	second := fsActionInternalPrefix + "ffeeddccbbaa99887766554433221100"
	action := fsAction{Worktree: "/work", ActionID: "abcdefabcdefabcdefabcdefabcdefab", Order: 1,
		Phase: fsPhasePreBase, Op: fsOpRename, Parent: "", ParentDevice: 1, ParentInode: 2,
		Source: first, Target: "visible", ExpectedKind: "File", ExpectedDevice: 1, ExpectedInode: 3,
		InternalSource: second, State: fsStateIntent}
	if err := validateFSAction(action); err == nil {
		t.Fatal("syntactically valid but unowned internal source was accepted")
	}
}

func setupRecoveryDirectory(t *testing.T) (*sql.DB, *openedWorktree, string, syncRecovery) {
	t.Helper()
	clientDir, rootPath := t.TempDir(), t.TempDir()
	db, err := initializeClientDB(t.Context(), clientDir, syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	root, err := openWorktreeRoot(rootPath, func(*os.File) error { return nil })
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(rootPath, "captured", "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootPath, "captured", "nested", "file"), []byte("captured"), 0o600); err != nil {
		t.Fatal(err)
	}
	file, info, err := openScannableAt(root.directory, "captured", "captured")
	if err != nil {
		t.Fatal(err)
	}
	snapshot := worktreeSnapshot{blocks: make(map[string]blockSource)}
	id, err := scanDirectory(file, "captured", &snapshot)
	if closeErr := file.Close(); err != nil || closeErr != nil {
		t.Fatal(errors.Join(err, closeErr))
	}
	stat, err := testPathStat(filepath.Join(rootPath, "captured"))
	if err != nil {
		t.Fatal(err)
	}
	tombstone := syncTombstonePrefix + "11223344556677889900aabbccddeeff"
	if err := os.Rename(filepath.Join(rootPath, "captured"), filepath.Join(rootPath, tombstone)); err != nil {
		t.Fatal(err)
	}
	value := syncRecovery{path: "captured", tombstone: tombstone, kind: "Directory", id: id,
		mtime:  info.ModTime().UTC().Truncate(time.Second).Format("2006-01-02T15:04:05Z"),
		device: uint64(stat.Dev), inode: stat.Ino}
	t.Cleanup(func() { root.Close(); db.Close() })
	return db, root, rootPath, value
}

func TestRecoveryDirectoryRemovalPersistsPostorderPlanBeforeDelete(t *testing.T) {
	db, root, rootPath, value := setupRecoveryDirectory(t)
	checked := false
	fault := func(point string, _ fsAction) error {
		if point != "before_action" || checked {
			return nil
		}
		checked = true
		var total, intents int
		if err := db.QueryRow("SELECT COUNT(*), SUM(state = 'intent') FROM fs_actions WHERE worktree = ?", rootPath).Scan(&total, &intents); err != nil {
			t.Fatal(err)
		}
		if total != 3 || intents != total {
			t.Fatalf("actions total=%d intents=%d", total, intents)
		}
		return errors.New("stop after durable plan")
	}
	if err := removeRecoveryDirectory(t.Context(), db, root, rootPath, value, fault); err == nil {
		t.Fatal("removal unexpectedly completed")
	}
	if !checked {
		t.Fatal("first removal was not reached")
	}
	if err := recoverFSActions(t.Context(), db, rootPath, root, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(rootPath, value.tombstone)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recovery directory remains: %v", err)
	}
	var incomplete int
	if err := db.QueryRow("SELECT COUNT(*) FROM fs_actions WHERE worktree = ? AND state <> 'completed'", rootPath).Scan(&incomplete); err != nil || incomplete != 0 {
		t.Fatalf("incomplete=%d err=%v", incomplete, err)
	}
}

func TestRecoveryDirectoryRemovalRejectsUnexpectedChildWithoutDeleting(t *testing.T) {
	db, root, rootPath, value := setupRecoveryDirectory(t)
	extra := filepath.Join(rootPath, value.tombstone, "user")
	if err := os.WriteFile(extra, []byte("user"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := removeRecoveryDirectory(t.Context(), db, root, rootPath, value, nil); err == nil {
		t.Fatal("unexpected child was accepted")
	}
	for _, path := range []string{extra, filepath.Join(rootPath, value.tombstone, "nested", "file")} {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("captured tree was changed at %q: %v", path, err)
		}
	}
	var actions int
	if err := db.QueryRow("SELECT COUNT(*) FROM fs_actions WHERE worktree = ?", rootPath).Scan(&actions); err != nil || actions != 0 {
		t.Fatalf("actions=%d err=%v", actions, err)
	}
}

func TestRecoveryDirectoryRemovalPreservesReplacementAfterPlanning(t *testing.T) {
	db, root, rootPath, value := setupRecoveryDirectory(t)
	paths, err := snapshotRecoveryRemoval(root.directory, value.tombstone, value)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := persistRecoveryRemovalPlan(t.Context(), db, root, rootPath, value, paths, nil); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(rootPath, value.tombstone, "nested", "file")
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := recoverFSActions(t.Context(), db, rootPath, root, nil); err == nil {
		t.Fatal("replacement was accepted")
	}
	if data, err := os.ReadFile(file); err != nil || string(data) != "replacement" {
		t.Fatalf("replacement=%q err=%v", data, err)
	}
}

func insertRawFSAction(t *testing.T, db *sql.DB, values ...any) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO fs_actions(worktree, action_id, action_order, phase, op, parent_path,
		parent_device, parent_inode, source_name, target_name, expected_kind, expected_device, expected_inode,
		expected_object, expected_size, expected_mtime, internal_name, state) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, values...)
	if err != nil {
		t.Fatal(err)
	}
}
