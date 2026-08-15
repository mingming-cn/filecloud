package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/mingming-cn/filecloud/internal/fscompat"
	"github.com/mingming-cn/filecloud/internal/object"
)

const _integrityObjectIDPrefix = 12

// IntegrityChecker holds the exclusive data-directory lock for one read-only check.
type IntegrityChecker struct {
	db         *sql.DB
	objectsDir string
	lock       *dataLock
	stateMu    sync.RWMutex
	closed     bool
}

// IntegrityIssue safely identifies one missing, unreadable, or corrupt published object.
type IntegrityIssue struct {
	OwnerUserID string
	LibraryID   string
	ObjectType  string
	ObjectID    string
	State       string
}

// IntegrityReport summarizes one complete check of all library Heads.
type IntegrityReport struct {
	Libraries int
	Objects   int
	Issues    []IntegrityIssue
}

// OpenIntegrityChecker acquires the exclusive data-directory lock before opening metadata or objects.
func OpenIntegrityChecker(ctx context.Context, dataDir string) (*IntegrityChecker, error) {
	lock, err := openDataLock(dataDir, false)
	if err != nil {
		return nil, err
	}
	if err := lock.exclusive(); err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	info, err := os.Stat(dataDir)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("open data directory: %w", err), lock.Close())
	}
	if !info.IsDir() {
		return nil, errors.Join(errors.New("data directory is not a directory"), lock.Close())
	}
	db, err := openReadOnlyDB(ctx, filepath.Join(dataDir, _databaseName))
	if err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	return &IntegrityChecker{
		db:         db,
		objectsDir: filepath.Join(dataDir, _objectsName),
		lock:       lock,
	}, nil
}

// Close closes metadata and releases the exclusive data-directory lock.
func (c *IntegrityChecker) Close() error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.closed {
		return os.ErrClosed
	}
	c.closed = true
	return errors.Join(c.db.Close(), c.lock.Close())
}

// Check verifies every object reachable through each library Head and both commit parents.
func (c *IntegrityChecker) Check(ctx context.Context) (IntegrityReport, error) {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	if c.closed {
		return IntegrityReport{}, os.ErrClosed
	}

	libraries, err := c.libraryHeads(ctx)
	if err != nil {
		return IntegrityReport{}, err
	}
	report := IntegrityReport{Libraries: len(libraries)}
	for _, library := range libraries {
		if library.head == "" {
			continue
		}
		state := integrityState{
			checker: c,
			ctx:     ctx,
			owner:   library.owner,
			library: library.id,
			seen:    make(map[integrityObjectKey]struct{}),
			issues:  make(map[integrityFinding]struct{}),
			blocks:  make(map[string]integrityBlock),
		}
		state.checkCommitGraph(library.head)
		if err := ctx.Err(); err != nil {
			return IntegrityReport{}, err
		}
		report.Objects += len(state.seen)
		for finding := range state.issues {
			report.Issues = append(report.Issues, IntegrityIssue{
				OwnerUserID: safeIntegrityScopeID(state.owner),
				LibraryID:   safeIntegrityScopeID(state.library),
				ObjectType:  finding.objectType,
				ObjectID:    maskIntegrityObjectID(finding.id),
				State:       finding.state,
			})
		}
	}
	sort.Slice(report.Issues, func(i, j int) bool {
		a, b := report.Issues[i], report.Issues[j]
		if a.OwnerUserID != b.OwnerUserID {
			return a.OwnerUserID < b.OwnerUserID
		}
		if a.LibraryID != b.LibraryID {
			return a.LibraryID < b.LibraryID
		}
		if a.ObjectType != b.ObjectType {
			return a.ObjectType < b.ObjectType
		}
		if a.ObjectID != b.ObjectID {
			return a.ObjectID < b.ObjectID
		}
		return a.State < b.State
	})
	return report, nil
}

type integrityLibraryHead struct {
	owner string
	id    string
	head  string
}

func (c *IntegrityChecker) libraryHeads(ctx context.Context) (heads []integrityLibraryHead, retErr error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT owner_user_id, id, head_commit_id
		FROM libraries
		ORDER BY owner_user_id, id`)
	if err != nil {
		return nil, fmt.Errorf("read library Heads: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, rows.Close()) }()
	for rows.Next() {
		var head integrityLibraryHead
		var commitID sql.NullString
		if err := rows.Scan(&head.owner, &head.id, &commitID); err != nil {
			return nil, fmt.Errorf("scan library Head: %w", err)
		}
		if commitID.Valid {
			head.head = commitID.String
		}
		heads = append(heads, head)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate library Heads: %w", err)
	}
	return heads, nil
}

type integrityObjectKey struct {
	kind string
	id   string
}

type integrityBlock struct {
	size  int64
	valid bool
}

type integrityFinding struct {
	objectType string
	id         string
	state      string
}

type integrityState struct {
	checker *IntegrityChecker
	ctx     context.Context
	owner   string
	library string
	seen    map[integrityObjectKey]struct{}
	issues  map[integrityFinding]struct{}
	blocks  map[string]integrityBlock
}

func (s *integrityState) checkCommitGraph(head string) {
	commits := []string{head}
	for len(commits) > 0 {
		if s.ctx.Err() != nil {
			return
		}
		id := commits[len(commits)-1]
		commits = commits[:len(commits)-1]
		data, ok := s.readObject("commits", "Commit", id, object.MaxCommitSize)
		if !ok {
			continue
		}
		commit, err := object.VerifyCommit(data, id)
		if err != nil {
			s.addIssue("Commit", id, "corrupt")
			continue
		}
		if commit.AuthorUserID != s.owner {
			s.addIssue("Commit", id, "corrupt")
		}
		commits = append(commits, commit.Parents...)
		s.checkSnapshot(commit.Root)
	}
}

func (s *integrityState) checkSnapshot(root string) {
	directories := []string{root}
	for len(directories) > 0 {
		if s.ctx.Err() != nil {
			return
		}
		id := directories[len(directories)-1]
		directories = directories[:len(directories)-1]
		data, ok := s.readObject("directories", "Directory", id, object.MaxDirectoryObjectSize)
		if !ok {
			continue
		}
		directory, err := object.VerifyDirectory(data, id)
		if err != nil {
			s.addIssue("Directory", id, "corrupt")
			continue
		}
		for _, entry := range directory.Entries {
			switch entry.Type {
			case "Directory":
				directories = append(directories, entry.ID)
			case "File":
				s.checkFile(entry.ID)
			default:
				s.addIssue("Directory", id, "corrupt")
			}
		}
	}
}

func (s *integrityState) checkFile(id string) {
	data, ok := s.readObject("files", "File", id, object.MaxFileObjectSize)
	if !ok {
		return
	}
	file, err := object.VerifyFile(data, id)
	if err != nil {
		s.addIssue("File", id, "corrupt")
		return
	}
	var total int64
	complete := true
	for index, blockID := range file.Blocks {
		block := s.checkBlock(blockID)
		if !block.valid {
			complete = false
			continue
		}
		total += block.size
		if index < len(file.Blocks)-1 && block.size != object.MaxBlockSize {
			complete = false
		}
	}
	if !complete || total != file.Size {
		s.addIssue("File", id, "corrupt")
	}
}

func (s *integrityState) checkBlock(id string) integrityBlock {
	if block, exists := s.blocks[id]; exists {
		return block
	}
	file, size, ok := s.openObject("blocks", "Block", id, object.MaxBlockSize)
	if !ok {
		block := integrityBlock{}
		s.blocks[id] = block
		return block
	}
	hash := sha256.New()
	written, readErr := io.Copy(hash, io.LimitReader(integrityContextReader{ctx: s.ctx, reader: file}, object.MaxBlockSize+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		s.addIssue("Block", id, "unreadable")
		block := integrityBlock{}
		s.blocks[id] = block
		return block
	}
	block := integrityBlock{
		size:  written,
		valid: written == size && written > 0 && written <= object.MaxBlockSize && hex.EncodeToString(hash.Sum(nil)) == id,
	}
	if !block.valid {
		s.addIssue("Block", id, "corrupt")
	}
	s.blocks[id] = block
	return block
}

func (s *integrityState) readObject(kind, objectType, id string, maximum int64) ([]byte, bool) {
	file, size, ok := s.openObject(kind, objectType, id, maximum)
	if !ok {
		return nil, false
	}
	data, readErr := io.ReadAll(io.LimitReader(integrityContextReader{ctx: s.ctx, reader: file}, maximum+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		s.addIssue(objectType, id, "unreadable")
		return nil, false
	}
	if int64(len(data)) != size {
		s.addIssue(objectType, id, "corrupt")
		return nil, false
	}
	return data, true
}

func (s *integrityState) openObject(kind, objectType, id string, maximum int64) (*os.File, int64, bool) {
	key := integrityObjectKey{kind: kind, id: id}
	if _, exists := s.seen[key]; exists {
		return nil, 0, false
	}
	s.seen[key] = struct{}{}
	if err := validateObjectLocation(s.checker.objectsDir, s.owner, s.library, kind, id); err != nil {
		s.addIssue(objectType, id, "corrupt")
		return nil, 0, false
	}
	file, err := openIntegrityObjectFile(s.checker.objectsDir, s.owner, s.library, kind, id)
	if os.IsNotExist(err) || errors.Is(err, fscompat.ENOENT) {
		s.addIssue(objectType, id, "missing")
		return nil, 0, false
	}
	if err != nil {
		s.addIssue(objectType, id, "unreadable")
		return nil, 0, false
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		s.addIssue(objectType, id, "unreadable")
		return nil, 0, false
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > maximum {
		_ = file.Close()
		s.addIssue(objectType, id, "corrupt")
		return nil, 0, false
	}
	return file, info.Size(), true
}

func openIntegrityObjectFile(objectsDir, owner, library, kind, id string) (*os.File, error) {
	fd, err := fscompat.Open(objectsDir, fscompat.O_RDONLY|fscompat.O_DIRECTORY|fscompat.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	for _, segment := range []string{owner, library, kind, id[:2]} {
		next, openErr := fscompat.Openat(fd, segment, fscompat.O_RDONLY|fscompat.O_DIRECTORY|fscompat.O_NOFOLLOW, 0)
		closeErr := fscompat.Close(fd)
		if openErr != nil || closeErr != nil {
			if openErr == nil {
				_ = fscompat.Close(next)
			}
			return nil, errors.Join(openErr, closeErr)
		}
		fd = next
	}
	leaf, openErr := fscompat.Openat(fd, id[2:], fscompat.O_RDONLY|fscompat.O_NOFOLLOW, 0)
	closeErr := fscompat.Close(fd)
	if openErr != nil || closeErr != nil {
		if openErr == nil {
			_ = fscompat.Close(leaf)
		}
		return nil, errors.Join(openErr, closeErr)
	}
	return os.NewFile(uintptr(leaf), "integrity-object"), nil
}

func (s *integrityState) addIssue(objectType, id, state string) {
	s.issues[integrityFinding{objectType: objectType, id: id, state: state}] = struct{}{}
}

func safeIntegrityScopeID(id string) string {
	if !validObjectScopeID(id) || len(id) > 128 {
		return "invalid"
	}
	for _, value := range []byte(id) {
		if value < 0x21 || value > 0x7e {
			return "invalid"
		}
	}
	return id
}

func maskIntegrityObjectID(id string) string {
	if !object.ValidID(id) {
		return "invalid"
	}
	return id[:_integrityObjectIDPrefix] + "..."
}

type integrityContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r integrityContextReader) Read(buffer []byte) (int, error) {
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
