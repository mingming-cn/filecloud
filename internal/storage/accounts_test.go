package storage

import (
	"crypto/sha256"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

func TestCanonicalUsernamesAreUnique(t *testing.T) {
	store := openAccountStore(t)
	defer closeAccountStore(t, store)
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := store.CreateUser(t.Context(), User{ID: "first", Username: "A\u030AngstroM", PasswordHash: "hash-one"}, now); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if err := store.CreateUser(t.Context(), User{ID: "second", Username: "ÅNGSTROM", PasswordHash: "hash-two"}, now); !errors.Is(err, ErrUsernameExists) {
		t.Fatalf("canonical duplicate error = %v, want ErrUsernameExists", err)
	}
	user, err := store.FindUserByUsername(t.Context(), "ångstrom")
	if err != nil {
		t.Fatalf("FindUserByUsername: %v", err)
	}
	if user.ID != "first" || user.Username != "ÅngstroM" || user.PasswordHash != "hash-one" {
		t.Fatalf("user = %+v, want normalized first user", user)
	}
}

func TestCreateUserDoesNotReportPrimaryKeyCollisionAsUsernameCollision(t *testing.T) {
	store := openAccountStore(t)
	defer closeAccountStore(t, store)
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := store.CreateUser(t.Context(), User{ID: "same-id", Username: "alice", PasswordHash: "hash-one"}, now); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	err := store.CreateUser(t.Context(), User{ID: "same-id", Username: "bob", PasswordHash: "hash-two"}, now)
	if err == nil || errors.Is(err, ErrUsernameExists) {
		t.Fatalf("primary-key collision error = %v, want non-username storage error", err)
	}
	sqliteErr, ok := errors.AsType[*sqlite.Error](err)
	if !ok || sqliteErr.Code() != sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY {
		t.Fatalf("primary-key collision error = %v, code = %v; want SQLITE_CONSTRAINT_PRIMARYKEY", err, sqliteErr)
	}
}

func TestSessionPersistenceExpiryRevocationAndPasswordReset(t *testing.T) {
	store := openAccountStore(t)
	defer closeAccountStore(t, store)
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := store.CreateUser(t.Context(), User{ID: "user-id", Username: "alice", PasswordHash: "old-hash"}, now); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	var isAdmin int
	if err := store.DB().QueryRow("SELECT is_admin FROM users WHERE id = 'user-id'").Scan(&isAdmin); err != nil {
		t.Fatalf("read is_admin: %v", err)
	}
	if isAdmin != 0 {
		t.Fatalf("locally created user is_admin = %d, want 0", isAdmin)
	}

	rawToken := "secret-access-token-that-must-not-be-stored"
	tokenHash := sha256.Sum256([]byte(rawToken))
	if err := store.CreateSession(t.Context(), "user-id", tokenHash, "laptop", now, now.Add(time.Hour)); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	var stored []byte
	if err := store.DB().QueryRow("SELECT token_hash FROM access_tokens").Scan(&stored); err != nil {
		t.Fatalf("read token hash: %v", err)
	}
	if string(stored) != string(tokenHash[:]) || string(stored) == rawToken || len(stored) != sha256.Size {
		t.Fatalf("stored token is not the expected binary hash")
	}

	revoked, err := store.RevokeSession(t.Context(), tokenHash, now.Add(time.Minute))
	if err != nil || !revoked {
		t.Fatalf("first RevokeSession = %v, %v; want true, nil", revoked, err)
	}
	revoked, err = store.RevokeSession(t.Context(), tokenHash, now.Add(2*time.Minute))
	if err != nil || revoked {
		t.Fatalf("second RevokeSession = %v, %v; want false, nil", revoked, err)
	}

	expiredDigest := sha256.Sum256([]byte("expired"))
	if err := store.CreateSession(t.Context(), "user-id", expiredDigest, "old", now, now.Add(time.Minute)); err != nil {
		t.Fatalf("CreateSession expired: %v", err)
	}
	revoked, err = store.RevokeSession(t.Context(), expiredDigest, now.Add(2*time.Minute))
	if err != nil || revoked {
		t.Fatalf("expired RevokeSession = %v, %v; want false, nil", revoked, err)
	}

	activeDigest := sha256.Sum256([]byte("active"))
	if err := store.CreateSession(t.Context(), "user-id", activeDigest, "phone", now, now.Add(time.Hour)); err != nil {
		t.Fatalf("CreateSession active: %v", err)
	}
	if err := store.ResetPassword(t.Context(), "ALICE", "new-hash", now.Add(3*time.Minute)); err != nil {
		t.Fatalf("ResetPassword: %v", err)
	}
	user, err := store.FindUserByUsername(t.Context(), "alice")
	if err != nil || user.PasswordHash != "new-hash" {
		t.Fatalf("reset user = %+v, %v", user, err)
	}
	revoked, err = store.RevokeSession(t.Context(), activeDigest, now.Add(4*time.Minute))
	if err != nil || revoked {
		t.Fatalf("reset token RevokeSession = %v, %v; want false, nil", revoked, err)
	}
}

func TestSessionRevocationPersistsAcrossRestart(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := Init(t.Context(), dataDir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	store, err := OpenForServe(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenForServe: %v", err)
	}
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	if err := store.CreateUser(t.Context(), User{ID: "owner", Username: "alice", PasswordHash: "hash"}, now); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	tokenHash := sha256.Sum256([]byte("restart-token"))
	if err := store.CreateSession(t.Context(), "owner", tokenHash, "device", now, now.Add(time.Hour)); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if revoked, err := store.RevokeSession(t.Context(), tokenHash, now.Add(time.Minute)); err != nil || !revoked {
		t.Fatalf("RevokeSession = %v, %v; want true, nil", revoked, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	store, err = OpenForServe(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer closeAccountStore(t, store)
	if _, err := store.FindActiveSession(t.Context(), tokenHash, now.Add(2*time.Minute)); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("FindActiveSession after restart error = %v, want ErrSessionNotFound", err)
	}
}

func openAccountStore(t *testing.T) *Store {
	t.Helper()
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := Init(t.Context(), dataDir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	store, err := OpenForServe(t.Context(), dataDir)
	if err != nil {
		t.Fatalf("OpenForServe: %v", err)
	}
	return store
}

func closeAccountStore(t *testing.T, store *Store) {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
