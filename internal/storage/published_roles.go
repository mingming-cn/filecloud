package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/mingming-cn/filecloud/internal/object"
)

const (
	_publishedRoleMainline    = "mainline"
	_publishedRoleMergeSource = "merge-source"
	_maxRoleMigrationSources  = 1024
)

var errPublishedRoleMigration = errors.New("published history roles cannot be proven; run integrity check")

// PublishedCommitRole identifies how a published Commit entered library history.
type PublishedCommitRole struct {
	CommitID         string
	Role             string
	MainlineCommitID string
}

func backfillPublishedCommitRoles(ctx context.Context, tx *sql.Tx, environment migrationEnvironment) error {
	libraries, err := migrationLibraries(ctx, tx)
	if err != nil {
		return err
	}
	for _, library := range libraries {
		if err := ctx.Err(); err != nil {
			return err
		}
		published, err := migrationPublishedCommits(ctx, tx, library.owner, library.id)
		if err != nil {
			return err
		}
		roles, err := provePublishedCommitRoles(ctx, environment, library.owner, library.id, library.head, published)
		if err != nil {
			return err
		}
		if err := insertPublishedCommitRoles(ctx, tx, library.owner, library.id, roles); err != nil {
			return err
		}
	}
	return nil
}

type migrationLibrary struct {
	owner string
	id    string
	head  string
}

func migrationLibraries(ctx context.Context, tx *sql.Tx) (libraries []migrationLibrary, retErr error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT owner_user_id, id, COALESCE(head_commit_id, '')
		FROM libraries ORDER BY owner_user_id, id`)
	if err != nil {
		return nil, fmt.Errorf("read libraries for history role migration: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, rows.Close()) }()
	libraries = make([]migrationLibrary, 0)
	for rows.Next() {
		var library migrationLibrary
		if err := rows.Scan(&library.owner, &library.id, &library.head); err != nil {
			return nil, fmt.Errorf("scan library for history role migration: %w", err)
		}
		libraries = append(libraries, library)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate libraries for history role migration: %w", err)
	}
	return libraries, nil
}

func migrationPublishedCommits(ctx context.Context, tx *sql.Tx, owner, library string) (published map[string]struct{}, retErr error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT commit_id FROM published_commits
		WHERE owner_user_id = ? AND library_id = ? ORDER BY commit_id`, owner, library)
	if err != nil {
		return nil, fmt.Errorf("read published history during role migration: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, rows.Close()) }()
	published = make(map[string]struct{})
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan published history during role migration: %w", err)
		}
		if !object.ValidID(id) {
			return nil, errPublishedRoleMigration
		}
		published[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate published history during role migration: %w", err)
	}
	return published, nil
}

func provePublishedCommitRoles(ctx context.Context, environment migrationEnvironment, owner, library, head string, published map[string]struct{}) (map[string]PublishedCommitRole, error) {
	roles := make(map[string]PublishedCommitRole, len(published))
	if head == "" {
		if len(published) != 0 {
			return nil, errPublishedRoleMigration
		}
		return roles, nil
	}
	if !object.ValidID(head) {
		return nil, errPublishedRoleMigration
	}

	mainline := make([]object.Commit, 0)
	mainlineIDs := make([]string, 0)
	mainlineIndex := make(map[string]int)
	for current := head; current != ""; {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, exists := mainlineIndex[current]; exists {
			return nil, errPublishedRoleMigration
		}
		commit, err := readMigrationCommit(environment, owner, library, current)
		if err != nil || commit.AuthorUserID != owner || len(commit.Parents) > 2 {
			return nil, errPublishedRoleMigration
		}
		mainlineIndex[current] = len(mainlineIDs)
		mainlineIDs = append(mainlineIDs, current)
		mainline = append(mainline, commit)
		roles[current] = PublishedCommitRole{CommitID: current, Role: _publishedRoleMainline, MainlineCommitID: current}
		if len(commit.Parents) == 0 {
			break
		}
		current = commit.Parents[0]
	}

	for index, commit := range mainline {
		if len(commit.Parents) != 2 {
			continue
		}
		current := commit.Parents[1]
		seen := make(map[string]struct{})
		introducedSources := 0
		for current != "" {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if _, exists := mainlineIndex[current]; exists {
				return nil, errPublishedRoleMigration
			}
			if _, exists := seen[current]; exists {
				return nil, errPublishedRoleMigration
			}
			seen[current] = struct{}{}
			if _, exists := roles[current]; exists {
				return nil, errPublishedRoleMigration
			}
			if introducedSources == _maxRoleMigrationSources {
				return nil, errPublishedRoleMigration
			}
			value, err := readMigrationCommit(environment, owner, library, current)
			if err != nil || value.AuthorUserID != owner || len(value.Parents) == 0 || len(value.Parents) > 2 {
				return nil, errPublishedRoleMigration
			}
			parentIndex, ok := mainlineIndex[value.Parents[0]]
			if !ok || parentIndex <= index {
				return nil, errPublishedRoleMigration
			}
			roles[current] = PublishedCommitRole{CommitID: current, Role: _publishedRoleMergeSource, MainlineCommitID: mainlineIDs[index]}
			introducedSources++
			if len(value.Parents) == 1 {
				break
			}
			current = value.Parents[1]
		}
	}

	if len(roles) != len(published) {
		return nil, errPublishedRoleMigration
	}
	for id := range published {
		if _, exists := roles[id]; !exists {
			return nil, errPublishedRoleMigration
		}
	}
	return roles, nil
}

func readMigrationCommit(environment migrationEnvironment, owner, library, id string) (object.Commit, error) {
	if environment.objectsDir == "" || !object.ValidID(id) {
		return object.Commit{}, errPublishedRoleMigration
	}
	if err := validateObjectLocation(environment.objectsDir, owner, library, "commits", id); err != nil {
		return object.Commit{}, errPublishedRoleMigration
	}
	file, err := os.Open(filepath.Join(environment.objectsDir, owner, library, "commits", id[:2], id[2:]))
	if err != nil {
		return object.Commit{}, errPublishedRoleMigration
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return object.Commit{}, errPublishedRoleMigration
	}
	if !info.Mode().IsRegular() || info.Size() > object.MaxCommitSize {
		_ = file.Close()
		return object.Commit{}, errPublishedRoleMigration
	}
	data, readErr := io.ReadAll(io.LimitReader(file, object.MaxCommitSize+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || int64(len(data)) != info.Size() {
		return object.Commit{}, errPublishedRoleMigration
	}
	commit, err := object.VerifyCommit(data, id)
	if err != nil {
		return object.Commit{}, errPublishedRoleMigration
	}
	return commit, nil
}

func insertPublishedCommitRoles(ctx context.Context, tx *sql.Tx, owner, library string, roles map[string]PublishedCommitRole) error {
	for _, role := range roles {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO published_commit_roles(owner_user_id, library_id, commit_id, role, mainline_commit_id)
			VALUES (?, ?, ?, ?, ?)`, owner, library, role.CommitID, role.Role, role.MainlineCommitID); err != nil {
			return fmt.Errorf("record published history role: %w", err)
		}
	}
	return nil
}
