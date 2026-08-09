package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

var (
	// ErrUsernameExists reports a canonical username uniqueness conflict.
	ErrUsernameExists = errors.New("username already exists")
	// ErrUserNotFound reports that no canonical username matches.
	ErrUserNotFound = errors.New("user not found")
)

// User is the authentication data for one canonical username.
type User struct {
	ID           string
	Username     string
	PasswordHash string
}

// CanonicalUsername returns the NFC display form and its NFC case-fold key.
func CanonicalUsername(username string) (display, key string) {
	display = norm.NFC.String(username)
	return display, norm.NFC.String(cases.Fold().String(display))
}

// CreateUser inserts a locally managed user.
func (s *Store) CreateUser(ctx context.Context, user User, now time.Time) error {
	display, key := CanonicalUsername(user.Username)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO users(id, username, username_key, password_hash, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		user.ID, display, key, user.PasswordHash, formatTime(now), formatTime(now))
	if err != nil {
		if sqliteErr, ok := errors.AsType[*sqlite.Error](err); ok && sqliteErr.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE {
			return ErrUsernameExists
		}
		return fmt.Errorf("create user: %w", err)
	}
	return nil
}

// ResetPassword replaces a password hash and revokes every active token atomically.
func (s *Store) ResetPassword(ctx context.Context, username, passwordHash string, now time.Time) (retErr error) {
	_, key := CanonicalUsername(username)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin password reset: %w", err)
	}
	defer func() {
		if retErr != nil {
			rollbackErr := tx.Rollback()
			if !errors.Is(rollbackErr, sql.ErrTxDone) {
				retErr = errors.Join(retErr, rollbackErr)
			}
		}
	}()
	result, err := tx.ExecContext(ctx,
		"UPDATE users SET password_hash = ?, updated_at = ? WHERE username_key = ?",
		passwordHash, formatTime(now), key)
	if err != nil {
		return fmt.Errorf("reset password: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read password reset result: %w", err)
	}
	if changed == 0 {
		return ErrUserNotFound
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE access_tokens SET revoked_at = ?
		WHERE user_id = (SELECT id FROM users WHERE username_key = ?) AND revoked_at IS NULL`,
		formatTime(now), key); err != nil {
		return fmt.Errorf("revoke sessions after password reset: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit password reset: %w", err)
	}
	return nil
}

// FindUserByUsername finds a user by its NFC case-fold key.
func (s *Store) FindUserByUsername(ctx context.Context, username string) (User, error) {
	_, key := CanonicalUsername(username)
	var user User
	err := s.db.QueryRowContext(ctx,
		"SELECT id, username, password_hash FROM users WHERE username_key = ?", key,
	).Scan(&user.ID, &user.Username, &user.PasswordHash)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, ErrUserNotFound
	}
	if err != nil {
		return User{}, fmt.Errorf("find user: %w", err)
	}
	return user, nil
}

// CreateSession persists only the access token hash.
func (s *Store) CreateSession(ctx context.Context, userID string, tokenHash [sha256.Size]byte, deviceName string, now, expiresAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO access_tokens(user_id, token_hash, device_name, created_at, expires_at)
		VALUES (?, ?, ?, ?, ?)`, userID, tokenHash[:], deviceName, formatTime(now), formatTime(expiresAt))
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	return nil
}

// RevokeSession revokes an active, unexpired token. It returns false uniformly otherwise.
func (s *Store) RevokeSession(ctx context.Context, tokenHash [sha256.Size]byte, now time.Time) (bool, error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE access_tokens SET revoked_at = ?
		WHERE token_hash = ? AND revoked_at IS NULL AND expires_at > ?`,
		formatTime(now), tokenHash[:], formatTime(now))
	if err != nil {
		return false, fmt.Errorf("revoke session: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read revoke result: %w", err)
	}
	return changed == 1, nil
}

func formatTime(value time.Time) string {
	return value.UTC().Format("2006-01-02T15:04:05.000000000Z")
}
