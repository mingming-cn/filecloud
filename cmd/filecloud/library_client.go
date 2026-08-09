package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/mingming-cn/filecloud/internal/object"
	_ "modernc.org/sqlite"
)

const (
	_clientDatabaseName = "client.db"
	_ext4Magic          = 0xef53
)

type libraryClientConfig struct {
	checkFilesystem                 func(*os.File) error
	now                             func() time.Time
	syncFile                        func(*os.File) error
	syncDirectory                   func(string) error
	beforeHeadCAS                   func() error
	beforeCheckoutMaterialize       func() error
	beforeCheckoutTempIdentity      func() error
	beforeCheckoutFileWrite         func(string, string) error
	beforeCheckoutFileRename        func(string, string) error
	beforeCheckoutDirectoryIdentity func() error
	beforeCheckoutDirectoryRename   func(string, string) error
	afterCheckoutInstall            func(string, string) error
	beforeFinalize                  func() error
	beforeFlock                     func()
	afterLock                       func()
}

func normalizeLibraryClientConfig(config libraryClientConfig) libraryClientConfig {
	if config.checkFilesystem == nil {
		config.checkFilesystem = requireExt4
	}
	if config.now == nil {
		config.now = time.Now
	}
	if config.syncFile == nil {
		config.syncFile = func(file *os.File) error { return file.Sync() }
	}
	if config.syncDirectory == nil {
		config.syncDirectory = syncDirectory
	}
	return config
}

type bindOptions struct {
	clientDir, serverURL, libraryID, worktree, deviceID string
	base                                                *url.URL
	token                                               []byte
	worktreeRoot                                        *openedWorktree
	cacheRoot                                           *os.File
	importLocal                                         bool
}

type clientBinding struct {
	ServerURL, LibraryID, Worktree, UserID, DeviceID string
	SyncBase, SyncBaseRoot, HeadETag                 string
}

type openedWorktree struct {
	path      string
	directory *os.File
	device    uint64
	inode     uint64
}

type bindIntent struct {
	ServerURL, LibraryID, Worktree, UserID, DeviceID string
	ExpectedETag                                     string
	CandidateCommit, CandidateRoot                   string
	CandidateData                                    []byte
	ImportLocal                                      bool
}

type pendingCheckout struct {
	ServerURL, LibraryID, Worktree, UserID, DeviceID string
	TargetCommit, TargetRoot, HeadETag               string
}

type remoteHead struct {
	CommitID *string `json:"CommitId"`
	ETag     string
}

func runLibrary(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	return runLibraryWithConfig(ctx, args, stdin, stdout, stderr, libraryClientConfig{})
}

func runLibraryWithConfig(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, config libraryClientConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	config = normalizeLibraryClientConfig(config)
	if len(args) == 0 {
		return errors.New("usage: filecloud library <bind|sync|unbind> [options]")
	}
	switch args[0] {
	case "bind":
		options, err := parseLibraryBind(ctx, args[1:], stdin, stderr, config)
		if err != nil {
			return err
		}
		defer clear(options.token)
		return bindLibrary(ctx, options, stdout, config)
	case "sync":
		return runLibrarySync(ctx, args[1:], stdout, stderr, config)
	case "unbind":
		return runLibraryUnbind(ctx, args[1:], stdout, stderr, config)
	default:
		return fmt.Errorf("unknown library command %q", args[0])
	}
}

func parseLibraryBind(ctx context.Context, args []string, stdin io.Reader, stderr io.Writer, config libraryClientConfig) (bindOptions, error) {
	flags := newFlagSet("library bind", stderr)
	clientDir := flags.String("client-dir", "", "Filecloud client state directory")
	server := flags.String("server", "", "Filecloud server URL")
	libraryID := flags.String("library-id", "", "Library ID")
	worktree := flags.String("worktree", "", "Worktree directory")
	deviceID := flags.String("device-id", "", "Device ID")
	tokenStdin := flags.Bool("token-stdin", false, "Read token from standard input")
	importLocal := flags.Bool("import-local", false, "Import an existing local worktree into an empty library")
	if err := flags.Parse(args); err != nil {
		return bindOptions{}, err
	}
	const usage = "usage: filecloud library bind --client-dir path --server url --library-id uuid --worktree path --device-id uuid --token-stdin"
	if *clientDir == "" || *server == "" || *libraryID == "" || *worktree == "" || *deviceID == "" || !*tokenStdin || flags.NArg() != 0 {
		return bindOptions{}, errors.New(usage)
	}
	base, err := validateServerURL(*server)
	if err != nil {
		return bindOptions{}, err
	}
	if !validClientUUID(*libraryID) || !validClientUUID(*deviceID) {
		return bindOptions{}, errors.New("library-id and device-id must be canonical UUIDs")
	}
	if err := ctx.Err(); err != nil {
		return bindOptions{}, err
	}
	token, err := readLineSecret(stdin, 4096, "token")
	if err != nil {
		return bindOptions{}, err
	}
	worktreeRoot, err := openWorktreeRoot(*worktree, config.checkFilesystem)
	if err != nil {
		clear(token)
		return bindOptions{}, err
	}
	canonicalWorktree := worktreeRoot.path
	canonicalClientDir, err := canonicalStateDir(*clientDir)
	if err != nil {
		clear(token)
		return bindOptions{}, errors.Join(err, worktreeRoot.Close())
	}
	if pathWithin(canonicalWorktree, canonicalClientDir) {
		clear(token)
		return bindOptions{}, errors.Join(errors.New("client state directory must be outside the worktree"), worktreeRoot.Close())
	}
	if !*importLocal {
		if emptyErr := worktreeRoot.validateEmpty(); emptyErr != nil {
			pending, pendingErr := pendingCheckoutExists(ctx, canonicalClientDir, canonicalWorktree)
			if pendingErr != nil || !pending {
				clear(token)
				return bindOptions{}, errors.Join(emptyErr, pendingErr, worktreeRoot.Close())
			}
		}
	} else if _, err := scanWorktree(worktreeRoot); err != nil {
		clear(token)
		return bindOptions{}, errors.Join(err, worktreeRoot.Close())
	}
	base.Scheme = strings.ToLower(base.Scheme)
	base.Host = strings.ToLower(base.Host)
	return bindOptions{clientDir: canonicalClientDir, serverURL: strings.TrimSuffix(base.String(), "/"), libraryID: *libraryID,
		worktree: canonicalWorktree, deviceID: *deviceID, base: base, token: token, worktreeRoot: worktreeRoot, importLocal: *importLocal}, nil
}

func bindLibrary(ctx context.Context, options bindOptions, stdout io.Writer, config libraryClientConfig) (retErr error) {
	defer func() { retErr = errors.Join(retErr, options.worktreeRoot.Close()) }()
	emptyDirectory, emptyRoot, err := canonicalEmptyDirectory()
	if err != nil {
		return err
	}

	owner, err := getLibraryOwner(ctx, options.base, options.libraryID, options.token)
	if err != nil {
		return err
	}
	head, err := getRemoteHead(ctx, options.base, options.libraryID, options.token)
	if err != nil {
		return err
	}
	if err := preflightClientState(ctx, options, owner, emptyRoot, head); err != nil {
		return err
	}
	locks, err := lockBinding(ctx, options.clientDir, options.worktree, options.serverURL, options.libraryID, config)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, locks.Close()) }()
	if config.afterLock != nil {
		config.afterLock()
	}
	recoveringCheckout, err := pendingCheckoutExists(ctx, options.clientDir, options.worktree)
	if err != nil {
		return err
	}
	snapshot := worktreeSnapshot{root: emptyRoot}
	if !recoveringCheckout {
		snapshot, err = scanWorktree(options.worktreeRoot)
		if err != nil {
			return fmt.Errorf("scan worktree: %w", err)
		}
		if !options.importLocal && snapshot.root != emptyRoot {
			return errors.New("local worktree is non-empty; rerun with --import-local to confirm import")
		}
	}
	owner, err = getLibraryOwner(ctx, options.base, options.libraryID, options.token)
	if err != nil {
		return err
	}
	head, err = getRemoteHead(ctx, options.base, options.libraryID, options.token)
	if err != nil {
		return err
	}
	db, err := initializeClientDB(ctx, options.clientDir, config.syncDirectory)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, db.Close()) }()
	existing, intent, err := inspectBinding(ctx, db, options.serverURL, options.libraryID, options.worktree)
	if err != nil {
		return err
	}
	if existing != nil {
		return confirmExistingBinding(ctx, db, options, owner, existing, stdout, config)
	}
	pending, err := loadPendingCheckout(ctx, db, options.serverURL, options.libraryID, options.worktree)
	if err != nil {
		return err
	}
	if pending != nil {
		return runInitialCheckout(ctx, db, options, owner, pending, stdout, config)
	}
	if head.CommitID != nil && intent == nil {
		if snapshot.root != emptyRoot {
			return errors.New("local and remote libraries are both non-empty; bind requires an empty worktree or empty remote library")
		}
		pending = &pendingCheckout{ServerURL: options.serverURL, LibraryID: options.libraryID, Worktree: options.worktree,
			UserID: owner, DeviceID: options.deviceID, TargetCommit: *head.CommitID, HeadETag: head.ETag}
		if err := savePendingCheckout(ctx, db, *pending); err != nil {
			return err
		}
		return runInitialCheckout(ctx, db, options, owner, pending, stdout, config)
	}
	if intent == nil {
		intent, err = newBindIntent(options, owner, head, emptyRoot, config.now)
		if err != nil {
			return err
		}
		if err := saveBindIntent(ctx, db, *intent); err != nil {
			return err
		}
	}
	if err := verifyBindIntent(*intent, options, owner, emptyRoot); err != nil {
		return err
	}

	commit, _ := object.VerifyCommit(intent.CandidateData, intent.CandidateCommit)
	var initializationError error
	if len(commit.Parents) == 0 {
		if head.CommitID == nil {
			if head.ETag != intent.ExpectedETag {
				return errors.New("library Head conflicts with pending bind intent")
			}
			if err := putMetadata(ctx, options.base, options.libraryID, options.token, "directories", emptyRoot, emptyDirectory); err != nil {
				return err
			}
			if err := putMetadata(ctx, options.base, options.libraryID, options.token, "commits", intent.CandidateCommit, intent.CandidateData); err != nil {
				return err
			}
			if config.beforeHeadCAS != nil {
				if err := config.beforeHeadCAS(); err != nil {
					return fmt.Errorf("prepare Head publication: %w", err)
				}
			}
			if _, err := rescanRoot(options, snapshot.root); err != nil {
				return err
			}
			_, _, initializationError = updateRemoteHead(ctx, options.base, options.libraryID, options.token, intent.ExpectedETag, intent.CandidateCommit)
			head, err = getRemoteHead(ctx, options.base, options.libraryID, options.token)
			if err != nil {
				return errors.Join(initializationError, fmt.Errorf("resolve library Head after publish: %w", err))
			}
		}
		if head.CommitID == nil {
			return errors.Join(initializationError, errors.New("library Head remained empty after initialization"))
		}
		if err := verifyInitialCommit(ctx, options.base, options.libraryID, options.token, *head.CommitID, emptyRoot, owner); err != nil {
			return fmt.Errorf("library Head conflicts with pending bind intent: %w", err)
		}
		if snapshot.root == emptyRoot {
			return finalizeInitialWinner(ctx, db, options, *intent, head, stdout, config)
		}
		candidate, err := newImportIntent(options, owner, head, snapshot.root, config.now)
		if err != nil {
			return err
		}
		if err := replaceBindIntent(ctx, db, *candidate); err != nil {
			return err
		}
		intent = candidate
		commit, _ = object.VerifyCommit(intent.CandidateData, intent.CandidateCommit)
	}

	if head.CommitID != nil && *head.CommitID == intent.CandidateCommit {
		return finalizeImportedBinding(ctx, db, options, *intent, head, stdout, config)
	}
	if head.CommitID == nil || len(commit.Parents) != 1 || *head.CommitID != commit.Parents[0] || head.ETag != intent.ExpectedETag {
		return errors.New("library Head changed during local import; merge requires issue #10")
	}
	snapshot, err = rescanRoot(options, intent.CandidateRoot)
	if err != nil {
		return err
	}
	if err := uploadSnapshot(ctx, options, snapshot); err != nil {
		return err
	}
	if err := putMetadata(ctx, options.base, options.libraryID, options.token, "commits", intent.CandidateCommit, intent.CandidateData); err != nil {
		return err
	}
	if config.beforeHeadCAS != nil {
		if err := config.beforeHeadCAS(); err != nil {
			return fmt.Errorf("prepare Head publication: %w", err)
		}
	}
	if _, err := rescanRoot(options, intent.CandidateRoot); err != nil {
		return err
	}
	_, _, publishErr := updateRemoteHead(ctx, options.base, options.libraryID, options.token, intent.ExpectedETag, intent.CandidateCommit)
	published, getErr := getRemoteHead(ctx, options.base, options.libraryID, options.token)
	if getErr != nil {
		return errors.Join(publishErr, fmt.Errorf("resolve library Head after publish: %w", getErr))
	}
	if published.CommitID == nil || *published.CommitID != intent.CandidateCommit {
		return errors.Join(publishErr, errors.New("library Head changed during local import; merge requires issue #10"))
	}
	return finalizeImportedBinding(ctx, db, options, *intent, published, stdout, config)
}

func canonicalEmptyDirectory() ([]byte, string, error) {
	data, root, err := object.Canonicalize("directories", []byte(`{"Entries":[],"Type":"Directory","Version":1}`))
	if err != nil {
		return nil, "", fmt.Errorf("construct empty snapshot: %w", err)
	}
	return data, root, nil
}

func rescanRoot(options bindOptions, expected string) (worktreeSnapshot, error) {
	snapshot, err := scanWorktree(options.worktreeRoot)
	if err != nil {
		return worktreeSnapshot{}, fmt.Errorf("worktree changed during bind: %w", err)
	}
	if snapshot.root != expected {
		return worktreeSnapshot{}, errors.New("worktree changed during bind")
	}
	return snapshot, nil
}

func uploadSnapshot(ctx context.Context, options bindOptions, snapshot worktreeSnapshot) error {
	ids := make([]string, 0, len(snapshot.blocks))
	for id := range snapshot.blocks {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		data, err := options.worktreeRoot.readBlock(snapshot.blocks[id], id)
		if err != nil {
			return err
		}
		if err := putBlock(ctx, options.base, options.libraryID, options.token, id, data); err != nil {
			return err
		}
	}
	for _, value := range snapshot.objects {
		if err := putMetadata(ctx, options.base, options.libraryID, options.token, value.kind, value.id, value.data); err != nil {
			return err
		}
	}
	return nil
}

func newImportIntent(options bindOptions, owner string, head remoteHead, root string, now func() time.Time) (*bindIntent, error) {
	if head.CommitID == nil {
		return nil, errors.New("cannot construct local import without initial Head")
	}
	data, id, err := canonicalCommit(owner, options.deviceID, root, []string{*head.CommitID}, now)
	if err != nil {
		return nil, err
	}
	return &bindIntent{ServerURL: options.serverURL, LibraryID: options.libraryID, Worktree: options.worktree, UserID: owner,
		DeviceID: options.deviceID, ExpectedETag: head.ETag, CandidateCommit: id, CandidateRoot: root,
		CandidateData: data, ImportLocal: true}, nil
}

func canonicalCommit(owner, deviceID, root string, parents []string, now func() time.Time) ([]byte, string, error) {
	if now == nil {
		now = time.Now
	}
	input, err := json.Marshal(struct {
		AuthorUserID string   `json:"AuthorUserId"`
		CreatedAt    string   `json:"CreatedAt"`
		DeviceID     string   `json:"DeviceId"`
		Message      string   `json:"Message"`
		Parents      []string `json:"Parents"`
		Root         string   `json:"Root"`
		Type         string   `json:"Type"`
		Version      int      `json:"Version"`
	}{owner, now().UTC().Truncate(time.Second).Format("2006-01-02T15:04:05Z"), deviceID, "sync", parents, root, "Commit", 1})
	if err != nil {
		return nil, "", fmt.Errorf("construct commit: %w", err)
	}
	data, id, err := object.Canonicalize("commits", input)
	if err != nil {
		return nil, "", fmt.Errorf("construct commit: %w", err)
	}
	return data, id, nil
}

func preflightClientState(ctx context.Context, options bindOptions, owner, emptyRoot string, head remoteHead) (retErr error) {
	databasePath := filepath.Join(options.clientDir, _clientDatabaseName)
	if _, err := os.Stat(databasePath); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("inspect client state: %w", err)
	}
	db, err := openClientDB(databasePath, true)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, db.Close()) }()
	existing, intent, err := inspectBinding(ctx, db, options.serverURL, options.libraryID, options.worktree)
	if err != nil {
		return err
	}
	if existing != nil {
		if existing.UserID != owner || existing.DeviceID != options.deviceID {
			return errors.New("existing binding uses a different owner or device identity")
		}
		return nil
	}
	if intent != nil {
		return verifyBindIntent(*intent, options, owner, emptyRoot)
	}
	return nil
}

func newBindIntent(options bindOptions, owner string, head remoteHead, emptyRoot string, now func() time.Time) (*bindIntent, error) {
	commitBytes, commitID, err := canonicalCommit(owner, options.deviceID, emptyRoot, []string{}, now)
	if err != nil {
		return nil, fmt.Errorf("construct initial commit: %w", err)
	}
	return &bindIntent{ServerURL: options.serverURL, LibraryID: options.libraryID, Worktree: options.worktree, UserID: owner,
		DeviceID: options.deviceID, ExpectedETag: head.ETag, CandidateCommit: commitID, CandidateRoot: emptyRoot,
		CandidateData: commitBytes, ImportLocal: options.importLocal}, nil
}

func finalizeInitialWinner(ctx context.Context, db *sql.DB, options bindOptions, intent bindIntent, head remoteHead, stdout io.Writer, config libraryClientConfig) error {
	if head.CommitID == nil {
		return errors.New("library Head remained empty after initialization")
	}
	if _, err := rescanRoot(options, intent.CandidateRoot); err != nil {
		return err
	}
	if config.beforeFinalize != nil {
		if err := config.beforeFinalize(); err != nil {
			return fmt.Errorf("finalize binding: %w", err)
		}
	}
	if _, err := rescanRoot(options, intent.CandidateRoot); err != nil {
		return err
	}
	binding := clientBinding{ServerURL: options.serverURL, LibraryID: options.libraryID, Worktree: options.worktree,
		UserID: intent.UserID, DeviceID: options.deviceID, SyncBase: *head.CommitID, SyncBaseRoot: intent.CandidateRoot, HeadETag: head.ETag}
	if err := finalizeBinding(ctx, db, binding, options.token); err != nil {
		return err
	}
	_, err := fmt.Fprintf(stdout, "library bound: %s\n", options.worktree)
	return err
}

func finalizeImportedBinding(ctx context.Context, db *sql.DB, options bindOptions, intent bindIntent, head remoteHead, stdout io.Writer, config libraryClientConfig) error {
	if head.CommitID == nil || *head.CommitID != intent.CandidateCommit {
		return errors.New("library Head does not match pending bind intent")
	}
	commit, err := object.VerifyCommit(intent.CandidateData, intent.CandidateCommit)
	if err != nil || commit.Root != intent.CandidateRoot || commit.AuthorUserID != intent.UserID || commit.DeviceID != intent.DeviceID {
		return errors.New("pending bind intent is corrupt")
	}
	if _, err := rescanRoot(options, intent.CandidateRoot); err != nil {
		return err
	}
	if config.beforeFinalize != nil {
		if err := config.beforeFinalize(); err != nil {
			return fmt.Errorf("finalize binding: %w", err)
		}
	}
	if _, err := rescanRoot(options, intent.CandidateRoot); err != nil {
		return err
	}
	binding := clientBinding{ServerURL: options.serverURL, LibraryID: options.libraryID, Worktree: options.worktree,
		UserID: intent.UserID, DeviceID: options.deviceID, SyncBase: *head.CommitID, SyncBaseRoot: intent.CandidateRoot, HeadETag: head.ETag}
	if err := finalizeBinding(ctx, db, binding, options.token); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "library bound: %s\n", options.worktree)
	return err
}

func confirmExistingBinding(ctx context.Context, db *sql.DB, options bindOptions, owner string, existing *clientBinding, stdout io.Writer, config libraryClientConfig) error {
	if existing.UserID != owner || existing.DeviceID != options.deviceID {
		return errors.New("existing binding uses a different owner or device identity")
	}
	head, err := getRemoteHead(ctx, options.base, options.libraryID, options.token)
	if err != nil {
		return err
	}
	if head.CommitID == nil || *head.CommitID != existing.SyncBase {
		return errors.New("existing binding is no longer converged with the library Head")
	}
	commit, err := getRemoteCommit(ctx, options.base, options.libraryID, options.token, *head.CommitID)
	if err != nil {
		return fmt.Errorf("verify existing binding base commit: %w", err)
	}
	if commit.AuthorUserID != existing.UserID || commit.Root != existing.SyncBaseRoot {
		return errors.New("existing binding base commit does not match its owner and snapshot")
	}
	if _, err := rescanRoot(options, existing.SyncBaseRoot); err != nil {
		return errors.New("existing binding worktree has changes; sync requires issue #10")
	}
	if _, err := db.ExecContext(ctx, "UPDATE bindings SET access_token = ?, head_etag = ? WHERE worktree = ?", options.token, head.ETag, options.worktree); err != nil {
		return fmt.Errorf("update binding token: %w", err)
	}
	_, err = fmt.Fprintf(stdout, "library already bound: %s\n", options.worktree)
	return err
}

func runLibrarySync(ctx context.Context, args []string, stdout, stderr io.Writer, config libraryClientConfig) (retErr error) {
	flags := newFlagSet("library sync", stderr)
	clientDir := flags.String("client-dir", "", "Filecloud client state directory")
	worktree := flags.String("worktree", "", "Worktree directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *clientDir == "" || *worktree == "" || flags.NArg() != 0 {
		return errors.New("usage: filecloud library sync --client-dir path --worktree path")
	}
	canonicalClientDir, err := canonicalStateDir(*clientDir)
	if err != nil {
		return err
	}
	canonicalWorktree, err := canonicalExistingPath(*worktree)
	if err != nil {
		return err
	}
	databasePath := filepath.Join(canonicalClientDir, _clientDatabaseName)
	locks, err := lockUnbind(ctx, canonicalClientDir, databasePath, canonicalWorktree, config)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, locks.Close()) }()
	db, err := openClientDB(databasePath, false)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, db.Close()) }()
	var binding clientBinding
	var token []byte
	if err := db.QueryRowContext(ctx, `SELECT server_url, library_id, worktree, user_id, device_id, sync_base_commit,
		sync_base_root, head_etag, access_token FROM bindings WHERE worktree = ?`, canonicalWorktree).Scan(&binding.ServerURL,
		&binding.LibraryID, &binding.Worktree, &binding.UserID, &binding.DeviceID, &binding.SyncBase, &binding.SyncBaseRoot,
		&binding.HeadETag, &token); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errors.New("worktree is not bound")
		}
		return fmt.Errorf("read client binding: %w", err)
	}
	defer clear(token)
	base, err := validateServerURL(binding.ServerURL)
	if err != nil {
		return err
	}
	root, err := openWorktreeRoot(canonicalWorktree, config.checkFilesystem)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, root.Close()) }()
	snapshot, err := scanWorktree(root)
	if err != nil {
		return err
	}
	head, err := getRemoteHead(ctx, base, binding.LibraryID, token)
	if err != nil {
		return err
	}
	if snapshot.root == binding.SyncBaseRoot && head.CommitID != nil && *head.CommitID == binding.SyncBase {
		_, err = fmt.Fprintln(stdout, "library already synchronized")
		return err
	}
	if snapshot.root != binding.SyncBaseRoot {
		return errors.New("local worktree has changes; general synchronization requires issue #10")
	}
	return errors.New("remote library has changes; checkout requires issue #9")
}

func runLibraryUnbind(ctx context.Context, args []string, stdout, stderr io.Writer, config libraryClientConfig) (retErr error) {
	flags := newFlagSet("library unbind", stderr)
	clientDir := flags.String("client-dir", "", "Filecloud client state directory")
	worktree := flags.String("worktree", "", "Worktree directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *clientDir == "" || *worktree == "" || flags.NArg() != 0 {
		return errors.New("usage: filecloud library unbind --client-dir path --worktree path")
	}
	canonicalClientDir, err := canonicalStateDir(*clientDir)
	if err != nil {
		return err
	}
	canonicalWorktree, err := canonicalUnbindPath(*worktree)
	if err != nil {
		return err
	}
	databasePath := filepath.Join(canonicalClientDir, _clientDatabaseName)
	if _, err := os.Stat(databasePath); errors.Is(err, os.ErrNotExist) {
		_, err = fmt.Fprintf(stdout, "library not bound: %s\n", canonicalWorktree)
		return err
	} else if err != nil {
		return fmt.Errorf("inspect client state: %w", err)
	}
	locks, err := lockUnbind(ctx, canonicalClientDir, databasePath, canonicalWorktree, config)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, locks.Close()) }()
	if config.afterLock != nil {
		config.afterLock()
	}
	db, err := openClientDB(databasePath, false)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, db.Close()) }()
	temps, err := checkoutTempNames(ctx, db, canonicalWorktree)
	if err != nil {
		return fmt.Errorf("read registered checkout temporary files: %w", err)
	}
	if len(temps) != 0 {
		root, err := openWorktreeRoot(canonicalWorktree, config.checkFilesystem)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err == nil {
			cleanupErr := cleanupCheckoutTemps(root, temps)
			if closeErr := root.Close(); cleanupErr != nil || closeErr != nil {
				return errors.Join(cleanupErr, closeErr)
			}
		}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin unbind transaction: %w", err)
	}
	result, err := tx.ExecContext(ctx, "DELETE FROM bindings WHERE worktree = ?", canonicalWorktree)
	if err != nil {
		return errors.Join(fmt.Errorf("remove client binding: %w", err), tx.Rollback())
	}
	bindingsChanged, err := result.RowsAffected()
	if err != nil {
		return errors.Join(fmt.Errorf("count removed client bindings: %w", err), tx.Rollback())
	}
	result, err = tx.ExecContext(ctx, "DELETE FROM bind_intents WHERE worktree = ?", canonicalWorktree)
	if err != nil {
		return errors.Join(fmt.Errorf("remove pending bind intent: %w", err), tx.Rollback())
	}
	intentsChanged, err := result.RowsAffected()
	if err != nil {
		return errors.Join(fmt.Errorf("count removed bind intents: %w", err), tx.Rollback())
	}
	result, err = tx.ExecContext(ctx, "DELETE FROM pending_checkouts WHERE worktree = ?", canonicalWorktree)
	if err != nil {
		return errors.Join(fmt.Errorf("remove pending checkout: %w", err), tx.Rollback())
	}
	checkoutsChanged, err := result.RowsAffected()
	if err != nil {
		return errors.Join(fmt.Errorf("count removed pending checkouts: %w", err), tx.Rollback())
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM checkout_paths WHERE worktree = ?", canonicalWorktree); err != nil {
		return errors.Join(fmt.Errorf("remove checkout paths: %w", err), tx.Rollback())
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM path_index WHERE worktree = ?", canonicalWorktree); err != nil {
		return errors.Join(fmt.Errorf("remove path index: %w", err), tx.Rollback())
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit unbind: %w", err)
	}
	if bindingsChanged+intentsChanged+checkoutsChanged > 0 {
		_, err = fmt.Fprintf(stdout, "library unbound: %s\n", canonicalWorktree)
	} else {
		_, err = fmt.Fprintf(stdout, "library not bound: %s\n", canonicalWorktree)
	}
	return err
}

func lockUnbind(ctx context.Context, clientDir, databasePath, worktree string, config libraryClientConfig) (*clientLocks, error) {
	for {
		serverURL, libraryID, err := readWorktreeScope(ctx, databasePath, worktree)
		if err != nil {
			return nil, err
		}
		names := []string{lockName("worktree\x00" + worktree)}
		if serverURL != "" {
			names = append(names, lockName("library\x00"+serverURL+"\x00"+libraryID))
			sort.Strings(names)
		}
		locks, err := lockClientKeys(ctx, clientDir, names, config)
		if err != nil {
			return nil, err
		}
		lockedServerURL, lockedLibraryID, err := readWorktreeScope(ctx, databasePath, worktree)
		if err != nil {
			return nil, errors.Join(err, locks.Close())
		}
		if serverURL == lockedServerURL && libraryID == lockedLibraryID {
			return locks, nil
		}
		if err := locks.Close(); err != nil {
			return nil, err
		}
	}
}

func readWorktreeScope(ctx context.Context, databasePath, worktree string) (serverURL, libraryID string, retErr error) {
	db, err := openClientDB(databasePath, true)
	if err != nil {
		return "", "", err
	}
	defer func() { retErr = errors.Join(retErr, db.Close()) }()
	err = db.QueryRowContext(ctx, `SELECT server_url, library_id FROM bindings WHERE worktree = ?
		UNION ALL SELECT server_url, library_id FROM bind_intents WHERE worktree = ?
		UNION ALL SELECT server_url, library_id FROM pending_checkouts WHERE worktree = ? LIMIT 1`, worktree, worktree, worktree).Scan(&serverURL, &libraryID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", nil
	}
	if err != nil {
		return "", "", fmt.Errorf("inspect worktree client state: %w", err)
	}
	return serverURL, libraryID, nil
}

type clientLocks struct{ files []*os.File }

func lockName(key string) string {
	return fmt.Sprintf("lock-%x", sha256.Sum256([]byte(key)))
}

func bindingLockNames(worktree, serverURL, libraryID string) []string {
	names := []string{lockName("worktree\x00" + worktree), lockName("library\x00" + serverURL + "\x00" + libraryID)}
	sort.Strings(names)
	return names
}

func lockBinding(ctx context.Context, clientDir, worktree, serverURL, libraryID string, config libraryClientConfig) (*clientLocks, error) {
	return lockClientKeys(ctx, clientDir, bindingLockNames(worktree, serverURL, libraryID), config)
}

func lockClientKeys(ctx context.Context, clientDir string, names []string, config libraryClientConfig) (_ *clientLocks, retErr error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("lock client state: %w", err)
	}
	config = normalizeLibraryClientConfig(config)
	if err := os.MkdirAll(clientDir, 0o700); err != nil {
		return nil, fmt.Errorf("create client directory: %w", err)
	}
	if err := os.Chmod(clientDir, 0o700); err != nil {
		return nil, fmt.Errorf("secure client directory: %w", err)
	}
	if err := config.syncDirectory(filepath.Dir(clientDir)); err != nil {
		return nil, err
	}
	locks := &clientLocks{}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, locks.Close())
		}
	}()
	for index, name := range names {
		file, err := os.OpenFile(filepath.Join(clientDir, name), os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			return nil, fmt.Errorf("open client lock: %w", err)
		}
		locks.files = append(locks.files, file)
		if err := file.Chmod(0o600); err != nil {
			return nil, fmt.Errorf("secure client lock: %w", err)
		}
		if err := config.syncFile(file); err != nil {
			return nil, fmt.Errorf("sync client lock: %w", err)
		}
		if err := config.syncDirectory(clientDir); err != nil {
			return nil, fmt.Errorf("sync client lock: %w", err)
		}
		if index == 0 && config.beforeFlock != nil {
			config.beforeFlock()
		}
		if err := flockContext(ctx, file); err != nil {
			return nil, err
		}
	}
	return locks, nil
}

func flockContext(ctx context.Context, file *os.File) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("lock client state: %w", err)
		}
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			return fmt.Errorf("lock client state: %w", err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("lock client state: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (locks *clientLocks) Close() (retErr error) {
	for index := len(locks.files) - 1; index >= 0; index-- {
		retErr = errors.Join(retErr, syscall.Flock(int(locks.files[index].Fd()), syscall.LOCK_UN), locks.files[index].Close())
	}
	locks.files = nil
	return retErr
}

func initializeClientDB(ctx context.Context, clientDir string, syncDir func(string) error) (*sql.DB, error) {
	path := filepath.Join(clientDir, _clientDatabaseName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create client database: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close new client database: %w", err)
	}
	db, err := openClientDB(path, false)
	if err != nil {
		return nil, err
	}
	if err := initializeClientSchema(ctx, db); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	if err := syncFile(path); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	if err := syncDir(clientDir); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return db, nil
}

func initializeClientSchema(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("initialize client state: %w", err)
	}
	fail := func(err error) error {
		return errors.Join(fmt.Errorf("initialize client state: %w", err), tx.Rollback())
	}
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS bindings (
		server_url TEXT NOT NULL, library_id TEXT NOT NULL,
		worktree TEXT PRIMARY KEY NOT NULL, user_id TEXT NOT NULL, device_id TEXT NOT NULL,
		sync_base_commit TEXT NOT NULL, sync_base_root TEXT NOT NULL, head_etag TEXT NOT NULL,
		access_token BLOB NOT NULL, UNIQUE(server_url, library_id))`); err != nil {
		return fail(err)
	}
	var oldSchema int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_table_info('bind_intents') WHERE name = 'id'").Scan(&oldSchema); err != nil {
		return fail(err)
	}
	if oldSchema != 0 {
		if _, err := tx.ExecContext(ctx, "ALTER TABLE bind_intents RENAME TO old_bind_intents"); err != nil {
			return fail(err)
		}
	}
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS bind_intents (
		server_url TEXT NOT NULL, library_id TEXT NOT NULL,
		worktree TEXT PRIMARY KEY NOT NULL, user_id TEXT NOT NULL, device_id TEXT NOT NULL,
		expected_etag TEXT NOT NULL, candidate_commit TEXT NOT NULL, candidate_root TEXT NOT NULL,
		candidate_data BLOB NOT NULL, import_local INTEGER NOT NULL DEFAULT 0, UNIQUE(server_url, library_id))`); err != nil {
		return fail(err)
	}
	var hasImportLocal int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_table_info('bind_intents') WHERE name = 'import_local'").Scan(&hasImportLocal); err != nil {
		return fail(err)
	}
	if hasImportLocal == 0 {
		if _, err := tx.ExecContext(ctx, "ALTER TABLE bind_intents ADD COLUMN import_local INTEGER NOT NULL DEFAULT 0"); err != nil {
			return fail(err)
		}
	}
	if oldSchema != 0 {
		if _, err := tx.ExecContext(ctx, `INSERT INTO bind_intents(server_url, library_id, worktree, user_id, device_id,
			expected_etag, candidate_commit, candidate_root, candidate_data)
			SELECT server_url, library_id, worktree, user_id, device_id, expected_etag, candidate_commit, candidate_root, candidate_data FROM old_bind_intents;
			DROP TABLE old_bind_intents`); err != nil {
			return fail(err)
		}
	}
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS pending_checkouts (
		server_url TEXT NOT NULL, library_id TEXT NOT NULL,
		worktree TEXT PRIMARY KEY NOT NULL, user_id TEXT NOT NULL, device_id TEXT NOT NULL,
		target_commit TEXT NOT NULL, target_root TEXT NOT NULL, head_etag TEXT NOT NULL,
		UNIQUE(server_url, library_id));
		CREATE TABLE IF NOT EXISTS checkout_paths (
		worktree TEXT NOT NULL, path TEXT NOT NULL, type TEXT NOT NULL, object_id TEXT NOT NULL,
		canonical_mtime TEXT NOT NULL, actual_mtime TEXT NOT NULL DEFAULT '', size INTEGER NOT NULL DEFAULT 0,
		temp_name TEXT NOT NULL DEFAULT '', temp_device INTEGER NOT NULL DEFAULT 0, temp_inode INTEGER NOT NULL DEFAULT 0,
		target_device INTEGER NOT NULL DEFAULT 0, target_inode INTEGER NOT NULL DEFAULT 0,
		completed INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY(worktree, path));
		CREATE TABLE IF NOT EXISTS path_index (
		worktree TEXT NOT NULL, path TEXT NOT NULL, type TEXT NOT NULL, object_id TEXT NOT NULL,
		canonical_mtime TEXT NOT NULL, actual_mtime TEXT NOT NULL, size INTEGER NOT NULL,
		PRIMARY KEY(worktree, path));`); err != nil {
		return fail(err)
	}
	for _, column := range []string{"target_device", "target_inode"} {
		var present int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_table_info('checkout_paths') WHERE name = ?", column).Scan(&present); err != nil {
			return fail(err)
		}
		if present == 0 {
			if _, err := tx.ExecContext(ctx, "ALTER TABLE checkout_paths ADD COLUMN "+column+" INTEGER NOT NULL DEFAULT 0"); err != nil {
				return fail(err)
			}
		}
	}
	var checkoutTokenColumn int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_table_info('pending_checkouts') WHERE name = 'access_token'").Scan(&checkoutTokenColumn); err != nil {
		return fail(err)
	}
	if checkoutTokenColumn != 0 {
		if _, err := tx.ExecContext(ctx, "ALTER TABLE pending_checkouts DROP COLUMN access_token"); err != nil {
			return fail(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("initialize client state: %w", err)
	}
	return nil
}

func inspectBinding(ctx context.Context, db *sql.DB, serverURL, libraryID, worktree string) (exact *clientBinding, intent *bindIntent, retErr error) {
	rows, err := db.QueryContext(ctx, `SELECT server_url, library_id, worktree, user_id, device_id, sync_base_commit, sync_base_root, head_etag
		FROM bindings WHERE worktree = ? OR (server_url = ? AND library_id = ?)`, worktree, serverURL, libraryID)
	if err != nil {
		return nil, nil, fmt.Errorf("read client bindings: %w", err)
	}
	for rows.Next() {
		var binding clientBinding
		if err := rows.Scan(&binding.ServerURL, &binding.LibraryID, &binding.Worktree, &binding.UserID, &binding.DeviceID,
			&binding.SyncBase, &binding.SyncBaseRoot, &binding.HeadETag); err != nil {
			return nil, nil, errors.Join(fmt.Errorf("scan client binding: %w", err), rows.Close())
		}
		if binding.ServerURL == serverURL && binding.LibraryID == libraryID && binding.Worktree == worktree {
			exact = &binding
		} else if binding.Worktree == worktree {
			return nil, nil, errors.Join(errors.New("worktree is already bound to another library"), rows.Close())
		} else {
			return nil, nil, errors.Join(errors.New("library is already bound to another worktree; unbind it first"), rows.Close())
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, nil, fmt.Errorf("iterate client bindings: %w", err)
	}
	importLocalColumn := "0"
	var hasImportLocal int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_table_info('bind_intents') WHERE name = 'import_local'").Scan(&hasImportLocal); err != nil {
		return nil, nil, fmt.Errorf("inspect pending bind intent schema: %w", err)
	}
	if hasImportLocal != 0 {
		importLocalColumn = "import_local"
	}
	intentRows, err := db.QueryContext(ctx, `SELECT server_url, library_id, worktree, user_id, device_id, expected_etag,
		candidate_commit, candidate_root, candidate_data, `+importLocalColumn+` FROM bind_intents
		WHERE worktree = ? OR (server_url = ? AND library_id = ?)`, worktree, serverURL, libraryID)
	if err != nil {
		return nil, nil, fmt.Errorf("read pending bind intents: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, intentRows.Close()) }()
	for intentRows.Next() {
		var value bindIntent
		if err := intentRows.Scan(&value.ServerURL, &value.LibraryID, &value.Worktree, &value.UserID, &value.DeviceID,
			&value.ExpectedETag, &value.CandidateCommit, &value.CandidateRoot, &value.CandidateData, &value.ImportLocal); err != nil {
			return nil, nil, fmt.Errorf("scan pending bind intent: %w", err)
		}
		if value.ServerURL == serverURL && value.LibraryID == libraryID && value.Worktree == worktree {
			copy := value
			intent = &copy
		} else if value.Worktree == worktree {
			return nil, nil, errors.New("pending bind intent uses another library for this worktree")
		} else {
			return nil, nil, errors.New("pending bind intent uses another worktree for this library")
		}
	}
	if err := intentRows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate pending bind intents: %w", err)
	}
	return exact, intent, nil
}

func verifyBindIntent(intent bindIntent, options bindOptions, owner, emptyRoot string) error {
	if intent.ServerURL != options.serverURL || intent.LibraryID != options.libraryID || intent.Worktree != options.worktree || intent.ImportLocal != options.importLocal {
		return errors.New("pending bind intent uses different bind parameters")
	}
	commit, err := object.VerifyCommit(intent.CandidateData, intent.CandidateCommit)
	validShape := (len(commit.Parents) == 0 && commit.Root == emptyRoot && intent.CandidateRoot == emptyRoot) ||
		(intent.ImportLocal && len(commit.Parents) == 1 && commit.Root == intent.CandidateRoot)
	if err != nil || commit.AuthorUserID != owner || commit.DeviceID != options.deviceID || !validShape ||
		commit.Message != "sync" || intent.UserID != owner || intent.DeviceID != options.deviceID {
		return errors.New("pending bind intent is corrupt or uses different bind parameters")
	}
	return nil
}

func saveBindIntent(ctx context.Context, db *sql.DB, intent bindIntent) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin bind intent transaction: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO bind_intents(server_url, library_id, worktree, user_id, device_id,
		expected_etag, candidate_commit, candidate_root, candidate_data, import_local) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		intent.ServerURL, intent.LibraryID, intent.Worktree, intent.UserID, intent.DeviceID, intent.ExpectedETag,
		intent.CandidateCommit, intent.CandidateRoot, intent.CandidateData, intent.ImportLocal); err != nil {
		return errors.Join(fmt.Errorf("save pending bind intent: %w", err), tx.Rollback())
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit bind intent: %w", err)
	}
	return nil
}

func replaceBindIntent(ctx context.Context, db *sql.DB, intent bindIntent) error {
	result, err := db.ExecContext(ctx, `UPDATE bind_intents SET expected_etag = ?, candidate_commit = ?, candidate_root = ?, candidate_data = ?, import_local = ?
		WHERE worktree = ? AND server_url = ? AND library_id = ?`, intent.ExpectedETag, intent.CandidateCommit, intent.CandidateRoot,
		intent.CandidateData, intent.ImportLocal, intent.Worktree, intent.ServerURL, intent.LibraryID)
	if err != nil {
		return fmt.Errorf("replace pending bind intent: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return errors.New("replace pending bind intent did not update one row")
	}
	return nil
}

func finalizeBinding(ctx context.Context, db *sql.DB, binding clientBinding, accessToken []byte) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin binding transaction: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO bindings(server_url, library_id, worktree, user_id, device_id,
		sync_base_commit, sync_base_root, head_etag, access_token) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		binding.ServerURL, binding.LibraryID, binding.Worktree, binding.UserID, binding.DeviceID, binding.SyncBase, binding.SyncBaseRoot,
		binding.HeadETag, accessToken); err != nil {
		return errors.Join(fmt.Errorf("save client binding: %w", err), tx.Rollback())
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM bind_intents WHERE worktree = ? AND server_url = ? AND library_id = ?",
		binding.Worktree, binding.ServerURL, binding.LibraryID); err != nil {
		return errors.Join(fmt.Errorf("complete bind intent: %w", err), tx.Rollback())
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit binding: %w", err)
	}
	return nil
}

func getLibraryOwner(ctx context.Context, base *url.URL, libraryID string, token []byte) (string, error) {
	request, err := authenticatedRequest(ctx, http.MethodGet, base.JoinPath("v1/libraries", libraryID).String(), token, nil)
	if err != nil {
		return "", err
	}
	status, data, _, err := doClientRequest(request)
	if err != nil {
		return "", fmt.Errorf("get library: %w", err)
	}
	if status != http.StatusOK {
		return "", fmt.Errorf("get library failed: server returned %s", http.StatusText(status))
	}
	var envelope struct {
		Library struct {
			OwnerUserID string `json:"OwnerUserId"`
		}
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return "", fmt.Errorf("decode library: %w", err)
	}
	if !validClientUUID(envelope.Library.OwnerUserID) {
		return "", errors.New("incompatible server: library response has no valid OwnerUserId")
	}
	return envelope.Library.OwnerUserID, nil
}

func getRemoteHead(ctx context.Context, base *url.URL, libraryID string, token []byte) (remoteHead, error) {
	request, err := authenticatedRequest(ctx, http.MethodGet, base.JoinPath("v1/libraries", libraryID, "head").String(), token, nil)
	if err != nil {
		return remoteHead{}, err
	}
	status, data, etag, err := doClientRequest(request)
	if err != nil {
		return remoteHead{}, fmt.Errorf("get library Head: %w", err)
	}
	if status != http.StatusOK {
		return remoteHead{}, fmt.Errorf("get library Head failed: server returned %s", http.StatusText(status))
	}
	var envelope struct{ Head remoteHead }
	if err := json.Unmarshal(data, &envelope); err != nil {
		return remoteHead{}, fmt.Errorf("decode library Head: %w", err)
	}
	if envelope.Head.ETag == "" {
		envelope.Head.ETag = etag
	}
	if envelope.Head.ETag == "" {
		return remoteHead{}, errors.New("library Head response has no ETag")
	}
	return envelope.Head, nil
}

func putMetadata(ctx context.Context, base *url.URL, libraryID string, token []byte, kind, id string, data []byte) error {
	request, err := authenticatedRequest(ctx, http.MethodPut, base.JoinPath("v1/libraries", libraryID, "objects", kind, id).String(), token, data)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	status, _, _, err := doClientRequest(request)
	if err != nil {
		return fmt.Errorf("put %s object: %w", kind, err)
	}
	if status != http.StatusOK && status != http.StatusCreated {
		return fmt.Errorf("put %s object failed: server returned %s", kind, http.StatusText(status))
	}
	return nil
}

func putBlock(ctx context.Context, base *url.URL, libraryID string, token []byte, id string, data []byte) error {
	request, err := authenticatedRequest(ctx, http.MethodPut, base.JoinPath("v1/libraries", libraryID, "blocks", id).String(), token, data)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	status, _, _, err := doClientRequest(request)
	if err != nil {
		return fmt.Errorf("put block object: %w", err)
	}
	if status != http.StatusOK && status != http.StatusCreated {
		return fmt.Errorf("put block object failed: server returned %s", http.StatusText(status))
	}
	return nil
}

func updateRemoteHead(ctx context.Context, base *url.URL, libraryID string, token []byte, etag, commitID string) (remoteHead, bool, error) {
	body, err := json.Marshal(struct {
		CommitID string `json:"CommitId"`
	}{commitID})
	if err != nil {
		return remoteHead{}, false, err
	}
	request, err := authenticatedRequest(ctx, http.MethodPut, base.JoinPath("v1/libraries", libraryID, "head").String(), token, body)
	if err != nil {
		return remoteHead{}, false, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("If-Match", etag)
	status, data, _, err := doClientRequest(request)
	if err != nil {
		return remoteHead{}, false, fmt.Errorf("publish library Head: %w", err)
	}
	if status != http.StatusOK && status != http.StatusPreconditionFailed {
		return remoteHead{}, false, fmt.Errorf("publish library Head failed: server returned %s", http.StatusText(status))
	}
	var envelope struct{ Head remoteHead }
	if err := json.Unmarshal(data, &envelope); err != nil {
		return remoteHead{}, false, fmt.Errorf("decode published library Head: %w", err)
	}
	return envelope.Head, status == http.StatusPreconditionFailed, nil
}

func getRemoteCommit(ctx context.Context, base *url.URL, libraryID string, token []byte, commitID string) (object.Commit, error) {
	request, err := authenticatedRequest(ctx, http.MethodGet, base.JoinPath("v1/libraries", libraryID, "objects", "commits", commitID).String(), token, nil)
	if err != nil {
		return object.Commit{}, err
	}
	status, data, _, err := doClientRequest(request)
	if err != nil {
		return object.Commit{}, fmt.Errorf("get commit: %w", err)
	}
	if status != http.StatusOK {
		return object.Commit{}, fmt.Errorf("get commit failed: server returned %s", http.StatusText(status))
	}
	commit, err := object.VerifyCommit(data, commitID)
	if err != nil {
		return object.Commit{}, errors.New("commit is not valid canonical content")
	}
	return commit, nil
}

func verifyInitialCommit(ctx context.Context, base *url.URL, libraryID string, token []byte, commitID, emptyRoot, userID string) error {
	commit, err := getRemoteCommit(ctx, base, libraryID, token, commitID)
	if err != nil {
		return err
	}
	if commit.AuthorUserID != userID || commit.Root != emptyRoot || commit.Message != "sync" || len(commit.Parents) != 0 {
		return errors.New("library Head is not a canonical empty initialization commit")
	}
	return nil
}

func authenticatedRequest(ctx context.Context, method, target string, token, body []byte) (*http.Request, error) {
	request, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create client request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+string(token))
	return request, nil
}

func doClientRequest(request *http.Request) (int, []byte, string, error) {
	response, err := noRedirectClient().Do(request)
	if err != nil {
		return 0, nil, "", err
	}
	data, readErr := io.ReadAll(io.LimitReader(response.Body, 33<<20))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		return 0, nil, "", errors.Join(readErr, closeErr)
	}
	return response.StatusCode, data, response.Header.Get("ETag"), nil
}

func newFlagSet(name string, output io.Writer) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(output)
	return flags
}

func requireExt4(directory *os.File) error {
	var info syscall.Statfs_t
	if err := syscall.Fstatfs(int(directory.Fd()), &info); err != nil {
		return fmt.Errorf("inspect worktree filesystem: %w", err)
	}
	if uint64(info.Type) != _ext4Magic {
		return fmt.Errorf("unsupported worktree filesystem type 0x%x; Linux ext4 is required", uint64(info.Type))
	}
	return nil
}

func openWorktree(path string, checkFilesystem func(*os.File) error, allowNonEmpty bool) (*openedWorktree, error) {
	root, err := openWorktreeRoot(path, checkFilesystem)
	if err != nil {
		return nil, err
	}
	if !allowNonEmpty {
		if err := root.validateEmpty(); err != nil {
			return nil, errors.Join(err, root.Close())
		}
	} else if _, err := scanWorktree(root); err != nil {
		return nil, errors.Join(err, root.Close())
	}
	return root, nil
}

func openWorktreeRoot(path string, checkFilesystem func(*os.File) error) (*openedWorktree, error) {
	canonical, err := canonicalExistingPath(path)
	if err != nil {
		return nil, err
	}
	fd, err := syscall.Open(canonical, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open worktree: %w", err)
	}
	root := &openedWorktree{path: canonical, directory: os.NewFile(uintptr(fd), canonical)}
	var stat syscall.Stat_t
	if err := syscall.Fstat(fd, &stat); err != nil {
		return nil, errors.Join(fmt.Errorf("inspect opened worktree: %w", err), root.Close())
	}
	root.device, root.inode = uint64(stat.Dev), stat.Ino
	if err := checkFilesystem(root.directory); err != nil {
		return nil, errors.Join(err, root.Close())
	}
	return root, nil
}

func (root *openedWorktree) validateIdentity() error {
	current := string(filepath.Separator)
	for _, component := range strings.Split(strings.TrimPrefix(root.path, string(filepath.Separator)), string(filepath.Separator)) {
		if component == "" {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			return fmt.Errorf("inspect worktree path: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return errors.New("worktree path changed to a symlink during bind")
		}
	}
	var pathStat, openedStat syscall.Stat_t
	if err := syscall.Lstat(root.path, &pathStat); err != nil {
		return fmt.Errorf("inspect worktree: %w", err)
	}
	if pathStat.Mode&syscall.S_IFMT != syscall.S_IFDIR || uint64(pathStat.Dev) != root.device || pathStat.Ino != root.inode {
		return errors.New("worktree identity changed during bind")
	}
	if err := syscall.Fstat(int(root.directory.Fd()), &openedStat); err != nil {
		return fmt.Errorf("inspect opened worktree: %w", err)
	}
	if uint64(openedStat.Dev) != root.device || openedStat.Ino != root.inode {
		return errors.New("opened worktree identity changed during bind")
	}
	return nil
}

func (root *openedWorktree) validateEmpty() error {
	if err := root.validateIdentity(); err != nil {
		return err
	}
	if _, err := root.directory.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind worktree: %w", err)
	}
	_, err := root.directory.Readdirnames(1)
	if err == nil {
		return errors.New("local worktree is non-empty; local import requires --import-local (issue #8)")
	}
	if !errors.Is(err, io.EOF) {
		return fmt.Errorf("read worktree: %w", err)
	}
	return root.validateIdentity()
}

func (root *openedWorktree) Close() error {
	return root.directory.Close()
}

func canonicalExistingPath(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("make path absolute: %w", err)
	}
	canonical, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", fmt.Errorf("resolve path: %w", err)
	}
	return filepath.Clean(canonical), nil
}

func canonicalUnbindPath(path string) (string, error) {
	for _, component := range strings.Split(filepath.ToSlash(path), "/") {
		if component == ".." {
			return "", errors.New("unbind worktree path must not contain '..'")
		}
	}
	canonical, err := canonicalExistingPath(path)
	if err == nil {
		return canonical, nil
	}
	absolute, absoluteErr := filepath.Abs(path)
	if absoluteErr != nil {
		return "", fmt.Errorf("make worktree path absolute: %w", absoluteErr)
	}
	absolute = filepath.Clean(absolute)
	ancestor := absolute
	var suffix []string
	for {
		if _, statErr := os.Lstat(ancestor); statErr == nil {
			break
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return "", fmt.Errorf("inspect unbind worktree ancestor: %w", statErr)
		}
		parent := filepath.Dir(ancestor)
		if parent == ancestor {
			return "", err
		}
		component := filepath.Base(ancestor)
		if component == "" || component == "." || component == ".." || filepath.IsAbs(component) {
			return "", errors.New("invalid missing worktree path suffix")
		}
		suffix = append([]string{component}, suffix...)
		ancestor = parent
	}
	resolved, resolveErr := filepath.EvalSymlinks(ancestor)
	if resolveErr != nil {
		return "", fmt.Errorf("resolve unbind worktree ancestor: %w", resolveErr)
	}
	for _, component := range suffix {
		resolved = filepath.Join(resolved, component)
	}
	return filepath.Clean(resolved), nil
}

func canonicalStateDir(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("make client directory absolute: %w", err)
	}
	absolute = filepath.Clean(absolute)
	if _, err := os.Lstat(absolute); err == nil {
		return canonicalExistingPath(absolute)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("inspect client directory: %w", err)
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return "", fmt.Errorf("resolve client directory parent: %w", err)
	}
	return filepath.Join(parent, filepath.Base(absolute)), nil
}

func pathWithin(parent, child string) bool {
	relative, err := filepath.Rel(parent, child)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func openClientDB(path string, readOnly bool) (*sql.DB, error) {
	u := &url.URL{Scheme: "file", Path: path}
	query := u.Query()
	query.Set("_pragma", "busy_timeout(5000)")
	query.Add("_pragma", "synchronous(FULL)")
	query.Add("_pragma", "secure_delete(ON)")
	if readOnly {
		query.Set("mode", "ro")
	}
	u.RawQuery = query.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, fmt.Errorf("open client database: %w", err)
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		return nil, errors.Join(fmt.Errorf("ping client database: %w", err), db.Close())
	}
	return db, nil
}

func syncFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open client state for sync: %w", err)
	}
	return errors.Join(file.Sync(), file.Close())
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open client state parent: %w", err)
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func validClientUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	if err != nil || len(decoded) != 16 {
		return false
	}
	version := decoded[6] >> 4
	return version >= 1 && version <= 8 && decoded[8]&0xc0 == 0x80
}
