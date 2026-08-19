package main

import (
	"bufio"
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
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	fscompat "github.com/mingming-cn/filecloud/internal/fscompat"
	"github.com/mingming-cn/filecloud/internal/object"
	_ "modernc.org/sqlite"
)

const (
	_clientDatabaseName         = "client.db"
	_headUpdateAttempts         = 3
	_defaultTransientRetryDelay = time.Second
	_maximumTransientRetryDelay = 30 * time.Second
)

type libraryClientConfig struct {
	checkFilesystem                 func(*os.File) error
	now                             func() time.Time
	syncFile                        func(*os.File) error
	syncDirectory                   func(string) error
	beforeHeadCAS                   func() error
	beforeBindingRefresh            func() error
	afterPendingReplacement         func() error
	afterSyncCheckoutTransition     func() error
	beforeCheckoutMaterialize       func() error
	beforeCheckoutBaseCommit        func() error
	afterCheckoutBaseCommit         func() error
	beforeCheckoutTempIdentity      func() error
	beforeCheckoutFileWrite         func(string, string) error
	beforeCheckoutFileRename        func(string, string) error
	beforeCheckoutDirectoryIdentity func() error
	beforeCheckoutDirectoryRename   func(string, string) error
	afterCheckoutInstall            func(string, string) error
	beforeFinalize                  func() error
	beforeSyncRecoveryPrepare       func() error
	afterSyncRecoveryRegistered     func(string) error
	beforeSyncRecoveryRename        func(string, string) error
	afterSyncRecoveryRename         func(string, string) error
	beforeSyncRecoveryCompleted     func(string) error
	beforeSyncRecoveryCleanup       func(string, string) error
	afterSyncRecoveryCleanupRename  func(string, string) error
	afterSyncRecoveryRestore        func(string) error
	beforeFlock                     func()
	afterLock                       func()
	scanFault                       func(scanFault) error
	fsActionFault                   fsActionFault
	fsTransactionFault              func(string) error
	fallbackOccupied                func(string) bool
	bindingLockHeld                 bool
}

func normalizeLibraryClientConfig(config libraryClientConfig) libraryClientConfig {
	if config.checkFilesystem == nil {
		config.checkFilesystem = requireSupportedFilesystem
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
	fallbackOccupied                                    func(string) bool
	importLocal                                         bool
	confirmDelete                                       string
	confirmDeleteSet                                    bool
	scanConfig                                          worktreeScanConfig
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
	TargetCommit, TargetRoot, HeadETag, ApplyState   string
	ConflictPromotions                               []byte
	RollbackRootMtimeNS                              int64
	RollbackRootMtimeValid                           bool
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
		return errors.New("usage: filecloud library <create|list|inspect|history|bind|sync|watch|restore|unbind> [options]")
	}
	switch args[0] {
	case "create":
		return runLibraryCreate(ctx, args[1:], stdin, stdout, stderr)
	case "list":
		return runLibraryList(ctx, args[1:], stdin, stdout, stderr)
	case "inspect":
		return runLibraryInspect(ctx, args[1:], stdin, stdout, stderr)
	case "history":
		if len(args) < 2 {
			return errors.New("usage: filecloud library history <list|inspect> [options]")
		}
		switch args[1] {
		case "list":
			return runLibraryHistoryList(ctx, args[2:], stdout, stderr)
		case "inspect":
			return runLibraryHistoryInspect(ctx, args[2:], stdout, stderr)
		default:
			return errors.New("usage: filecloud library history <list|inspect> [options]")
		}
	case "bind":
		options, err := parseLibraryBind(ctx, args[1:], stdin, stderr, config)
		if err != nil {
			return err
		}
		defer clear(options.token)
		return bindLibrary(ctx, options, stdout, config)
	case "sync":
		return runLibrarySync(ctx, args[1:], stdout, stderr, config)
	case "watch":
		return runLibraryWatch(ctx, args[1:], stdout, stderr, config)
	case "restore":
		return runLibraryRestore(ctx, args[1:], stdout, stderr, config)
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
	if err := checkStateDirFilesystem(canonicalClientDir, config.checkFilesystem); err != nil {
		clear(token)
		return bindOptions{}, errors.Join(err, worktreeRoot.Close())
	}
	if pathWithin(canonicalWorktree, canonicalClientDir) {
		clear(token)
		return bindOptions{}, errors.Join(errors.New("client state directory must be outside the worktree"), worktreeRoot.Close())
	}
	if !*importLocal {
		if emptyErr := worktreeRoot.validateEmpty(); emptyErr != nil {
			resumable, stateErr := resumableBindExists(ctx, canonicalClientDir, canonicalWorktree)
			if stateErr != nil || !resumable {
				clear(token)
				return bindOptions{}, errors.Join(emptyErr, stateErr, worktreeRoot.Close())
			}
		}
	} else if _, err := scanWorktreeWithConfig(worktreeRoot, worktreeScanConfig{fault: config.scanFault}); err != nil {
		clear(token)
		return bindOptions{}, errors.Join(err, worktreeRoot.Close())
	}
	base.Scheme = strings.ToLower(base.Scheme)
	base.Host = strings.ToLower(base.Host)
	return bindOptions{clientDir: canonicalClientDir, serverURL: strings.TrimSuffix(base.String(), "/"), libraryID: *libraryID,
		worktree: canonicalWorktree, deviceID: *deviceID, base: base, token: token, worktreeRoot: worktreeRoot, importLocal: *importLocal,
		scanConfig: worktreeScanConfig{fault: config.scanFault}}, nil
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
	recoveringBind, err := resumableBindExists(ctx, options.clientDir, options.worktree)
	if err != nil {
		return err
	}
	snapshot := worktreeSnapshot{root: emptyRoot}
	if !recoveringBind {
		snapshot, err = scanWorktreeWithConfig(options.worktreeRoot, options.scanConfig)
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
	if err := validatePendingPromotionTargets(ctx, db, options.worktree); err != nil {
		return fmt.Errorf("validate checkout promotion targets: %w", err)
	}
	if err := recoverFSActions(ctx, db, options.worktree, options.worktreeRoot, config.fsActionFault); err != nil {
		return fmt.Errorf("recover checkout filesystem actions: %w", err)
	}
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
		return finalizeImportedBinding(ctx, db, options, *intent, head, snapshot, stdout, config)
	}
	if head.CommitID == nil || len(commit.Parents) != 1 || *head.CommitID != commit.Parents[0] || head.ETag != intent.ExpectedETag {
		return errors.New("library Head changed during local import; merge is not supported yet")
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
		return errors.Join(publishErr, errors.New("library Head changed during local import; merge is not supported yet"))
	}
	return finalizeImportedBinding(ctx, db, options, *intent, published, snapshot, stdout, config)
}

func canonicalEmptyDirectory() ([]byte, string, error) {
	data, root, err := object.Canonicalize("directories", []byte(`{"Entries":[],"Type":"Directory","Version":1}`))
	if err != nil {
		return nil, "", fmt.Errorf("construct empty snapshot: %w", err)
	}
	return data, root, nil
}

func rescanRoot(options bindOptions, expected string) (worktreeSnapshot, error) {
	snapshot, err := scanWorktreeWithConfig(options.worktreeRoot, options.scanConfig)
	if err != nil {
		return worktreeSnapshot{}, fmt.Errorf("worktree changed during bind: %w", err)
	}
	if snapshot.root != expected {
		return worktreeSnapshot{}, errors.New("worktree changed during bind")
	}
	return snapshot, nil
}

type clientObjectReference struct {
	ObjectID   string `json:"ObjectId"`
	ObjectType string `json:"ObjectType"`
}

func uploadSnapshot(ctx context.Context, options bindOptions, snapshot worktreeSnapshot) error {
	references := make([]clientObjectReference, 0, len(snapshot.blocks)+len(snapshot.objects))
	seen := make(map[string]bool, cap(references))
	add := func(kind, id string) {
		key := kind + "\x00" + id
		if !seen[key] {
			seen[key] = true
			references = append(references, clientObjectReference{ObjectID: id, ObjectType: clientObjectType(kind)})
		}
	}
	for id := range snapshot.blocks {
		add("blocks", id)
	}
	for _, value := range snapshot.objects {
		add(value.kind, value.id)
	}
	sort.Slice(references, func(i, j int) bool {
		if references[i].ObjectType != references[j].ObjectType {
			return references[i].ObjectType < references[j].ObjectType
		}
		return references[i].ObjectID < references[j].ObjectID
	})
	missing, err := checkRemoteObjects(ctx, options, references)
	if err != nil {
		return err
	}
	ids := make([]string, 0, len(snapshot.blocks))
	for id := range snapshot.blocks {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if !missing["blocks\x00"+id] {
			continue
		}
		data, err := options.worktreeRoot.readBlock(snapshot.blocks[id], id)
		if err != nil {
			return err
		}
		if err := putBlock(ctx, options.base, options.libraryID, options.token, id, data); err != nil {
			return err
		}
	}
	for _, value := range snapshot.objects {
		key := value.kind + "\x00" + value.id
		if !missing[key] {
			continue
		}
		if err := putMetadata(ctx, options.base, options.libraryID, options.token, value.kind, value.id, value.data); err != nil {
			return err
		}
		delete(missing, key)
	}
	return nil
}

func clientObjectType(kind string) string {
	switch kind {
	case "blocks":
		return "Block"
	case "files":
		return "File"
	case "directories":
		return "Directory"
	case "commits":
		return "Commit"
	default:
		return ""
	}
}

func clientObjectKind(typeName string) string {
	switch typeName {
	case "Block":
		return "blocks"
	case "File":
		return "files"
	case "Directory":
		return "directories"
	case "Commit":
		return "commits"
	default:
		return ""
	}
}

func checkRemoteObjects(ctx context.Context, options bindOptions, references []clientObjectReference) (map[string]bool, error) {
	missing := make(map[string]bool)
	for start := 0; start < len(references); start += 1000 {
		end := min(start+1000, len(references))
		body, err := json.Marshal(struct {
			Objects []clientObjectReference
		}{Objects: references[start:end]})
		if err != nil {
			return nil, fmt.Errorf("encode object checks: %w", err)
		}
		request, err := authenticatedRequest(ctx, http.MethodPost, options.base.JoinPath("v1/libraries", options.libraryID, "object-checks").String(), options.token, body)
		if err != nil {
			return nil, err
		}
		request.Header.Set("Content-Type", "application/json")
		status, data, _, err := doClientRequest(request)
		if err != nil {
			return nil, fmt.Errorf("check remote objects: %w", err)
		}
		if status != http.StatusOK {
			return nil, fmt.Errorf("check remote objects failed: server returned %s", http.StatusText(status))
		}
		var response struct {
			RetCode        *int
			Message        *string
			MissingObjects *[]clientObjectReference
		}
		if err := json.Unmarshal(data, &response); err != nil || response.RetCode == nil || *response.RetCode != 0 ||
			response.Message == nil || response.MissingObjects == nil {
			return nil, errors.New("invalid object checks response")
		}
		requested := make(map[string]bool, end-start)
		for _, reference := range references[start:end] {
			requested[clientObjectKind(reference.ObjectType)+"\x00"+reference.ObjectID] = true
		}
		for _, reference := range *response.MissingObjects {
			kind := clientObjectKind(reference.ObjectType)
			key := kind + "\x00" + reference.ObjectID
			if kind == "" || !object.ValidID(reference.ObjectID) || !requested[key] || missing[key] {
				return nil, errors.New("invalid object checks response")
			}
			missing[key] = true
		}
	}
	return missing, nil
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
	}{owner, canonicalProtocolMtime(now()), deviceID, "sync", parents, root, "Commit", 1})
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
	snapshot, err := rescanRoot(options, intent.CandidateRoot)
	if err != nil {
		return err
	}
	binding := clientBinding{ServerURL: options.serverURL, LibraryID: options.libraryID, Worktree: options.worktree,
		UserID: intent.UserID, DeviceID: options.deviceID, SyncBase: *head.CommitID, SyncBaseRoot: intent.CandidateRoot, HeadETag: head.ETag}
	if err := finalizeBinding(ctx, db, binding, options.token, snapshot.paths); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "library bound: %s\n", options.worktree)
	return err
}

func finalizeImportedBinding(ctx context.Context, db *sql.DB, options bindOptions, intent bindIntent, head remoteHead,
	snapshot worktreeSnapshot, stdout io.Writer, config libraryClientConfig) error {
	if head.CommitID == nil || *head.CommitID != intent.CandidateCommit {
		return errors.New("library Head does not match pending bind intent")
	}
	commit, err := object.VerifyCommit(intent.CandidateData, intent.CandidateCommit)
	if err != nil || commit.Root != intent.CandidateRoot || commit.AuthorUserID != intent.UserID || commit.DeviceID != intent.DeviceID {
		return errors.New("pending bind intent is corrupt")
	}
	if snapshot, err = rescanRoot(options, intent.CandidateRoot); err != nil {
		return err
	}
	if config.beforeFinalize != nil {
		if err := config.beforeFinalize(); err != nil {
			return fmt.Errorf("finalize binding: %w", err)
		}
	}
	if snapshot, err = rescanRoot(options, intent.CandidateRoot); err != nil {
		return err
	}
	binding := clientBinding{ServerURL: options.serverURL, LibraryID: options.libraryID, Worktree: options.worktree,
		UserID: intent.UserID, DeviceID: options.deviceID, SyncBase: *head.CommitID, SyncBaseRoot: intent.CandidateRoot, HeadETag: head.ETag}
	if err := finalizeBinding(ctx, db, binding, options.token, snapshot.paths); err != nil {
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
		return errors.New("existing binding worktree has changes; run library sync")
	}
	if config.beforeBindingRefresh != nil {
		if err := config.beforeBindingRefresh(); err != nil {
			return fmt.Errorf("prepare binding credential refresh: %w", err)
		}
	}
	result, err := db.ExecContext(ctx, `UPDATE bindings SET access_token = ?, head_etag = ?
		WHERE server_url = ? AND library_id = ? AND worktree = ? AND user_id = ? AND device_id = ?
		AND sync_base_commit = ? AND sync_base_root = ? AND head_etag = ?`, options.token, head.ETag,
		existing.ServerURL, existing.LibraryID, existing.Worktree, existing.UserID, existing.DeviceID,
		existing.SyncBase, existing.SyncBaseRoot, existing.HeadETag)
	if err != nil {
		return fmt.Errorf("update binding token: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return errors.Join(errors.New("existing binding changed before credential refresh"), err)
	}
	_, err = fmt.Fprintf(stdout, "library already bound: %s\n", options.worktree)
	return err
}

func runLibraryWatch(ctx context.Context, args []string, stdout, stderr io.Writer, config libraryClientConfig) (retErr error) {
	flags := newFlagSet("library watch", stderr)
	clientDir := flags.String("client-dir", "", "Filecloud client state directory")
	worktree := flags.String("worktree", "", "Worktree directory")
	interval := flags.Duration("interval", 0, "Synchronization interval")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *clientDir == "" || *worktree == "" || *interval <= 0 || flags.NArg() != 0 {
		return errors.New("usage: filecloud library watch --client-dir path --worktree path --interval duration")
	}
	canonicalClientDir, err := canonicalStateDir(*clientDir)
	if err != nil {
		return err
	}
	if err := checkStateDirFilesystem(canonicalClientDir, config.checkFilesystem); err != nil {
		return err
	}
	canonicalWorktree, err := canonicalExistingPath(*worktree)
	if err != nil {
		return err
	}
	databasePath := filepath.Join(canonicalClientDir, _clientDatabaseName)
	locks, err := tryLockUnbind(ctx, canonicalClientDir, databasePath, canonicalWorktree, config)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, locks.Close()) }()
	config.bindingLockHeld = true
	syncArgs := []string{"--client-dir", canonicalClientDir, "--worktree", canonicalWorktree}
	for {
		started := time.Now()
		roundCtx := context.WithoutCancel(ctx)
		stopRound := func() {}
		if deadline, ok := ctx.Deadline(); ok {
			roundCtx, stopRound = context.WithDeadline(roundCtx, deadline)
		}
		err := runLibrarySync(roundCtx, syncArgs, stdout, stderr, config)
		stopRound()
		if err != nil {
			if ctx.Err() != nil && errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		remaining := *interval - time.Since(started)
		if remaining <= 0 {
			if _, err := fmt.Fprintf(stderr, "warning: synchronization exceeded watch interval by %s; next round postponed\n", -remaining); err != nil {
				return fmt.Errorf("write watch delay warning: %w", err)
			}
			remaining = *interval
		}
		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
	}
}

func runLibrarySync(ctx context.Context, args []string, stdout, stderr io.Writer, config libraryClientConfig) (retErr error) {
	flags := newFlagSet("library sync", stderr)
	clientDir := flags.String("client-dir", "", "Filecloud client state directory")
	worktree := flags.String("worktree", "", "Worktree directory")
	confirmDelete := flags.String("confirm-delete", "", "Confirm a protected deletion candidate")
	if err := flags.Parse(args); err != nil {
		return err
	}
	confirmDeleteSet := false
	flags.Visit(func(value *flag.Flag) { confirmDeleteSet = confirmDeleteSet || value.Name == "confirm-delete" })
	if *clientDir == "" || *worktree == "" || flags.NArg() != 0 {
		return errors.New("usage: filecloud library sync --client-dir path --worktree path [--confirm-delete 12-hex-prefix]")
	}
	canonicalClientDir, err := canonicalStateDir(*clientDir)
	if err != nil {
		return err
	}
	if err := checkStateDirFilesystem(canonicalClientDir, config.checkFilesystem); err != nil {
		return err
	}
	canonicalWorktree, err := canonicalExistingPath(*worktree)
	if err != nil {
		return err
	}
	databasePath := filepath.Join(canonicalClientDir, _clientDatabaseName)
	if !config.bindingLockHeld {
		locks, err := tryLockUnbind(ctx, canonicalClientDir, databasePath, canonicalWorktree, config)
		if err != nil {
			return err
		}
		defer func() { retErr = errors.Join(retErr, locks.Close()) }()
	}
	db, err := openClientDB(databasePath, false)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, db.Close()) }()
	if err := initializeClientSchema(ctx, db); err != nil {
		return err
	}
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
	authorityOptions := bindOptions{clientDir: canonicalClientDir, serverURL: binding.ServerURL,
		libraryID: binding.LibraryID, worktree: binding.Worktree, deviceID: binding.DeviceID, base: base, token: token}
	if err := rehydratePendingPromotionSeedAuthority(ctx, db, authorityOptions, binding); err != nil {
		return fmt.Errorf("rehydrate checkout promotion authority: %w", err)
	}
	root, err := openWorktreeRoot(canonicalWorktree, config.checkFilesystem)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, root.Close()) }()
	if err := validatePendingPromotionTargets(ctx, db, binding.Worktree); err != nil {
		return fmt.Errorf("validate checkout promotion targets: %w", err)
	}
	if err := recoverFSActions(ctx, db, binding.Worktree, root, config.fsActionFault); err != nil {
		return fmt.Errorf("recover checkout filesystem actions: %w", err)
	}
	tracked, err := loadTrackedPaths(ctx, db, binding.Worktree)
	if err != nil {
		return err
	}
	options := bindOptions{clientDir: canonicalClientDir, serverURL: binding.ServerURL, libraryID: binding.LibraryID,
		worktree: binding.Worktree, deviceID: binding.DeviceID, base: base, token: token, worktreeRoot: root,
		confirmDelete: *confirmDelete, confirmDeleteSet: confirmDeleteSet, fallbackOccupied: config.fallbackOccupied,
		scanConfig: worktreeScanConfig{trackedPaths: tracked, warning: stderr, fault: config.scanFault, ignoreUntrackedUnsupported: true}}
	if len(tracked) == 0 {
		tracked, err = loadRemoteTrackedPaths(ctx, options, binding)
		if err != nil {
			return err
		}
		options.scanConfig.trackedPaths = tracked
	}
	return syncLibrary(ctx, db, options, binding, stdout, config)
}

func loadTrackedPaths(ctx context.Context, db *sql.DB, worktree string) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, "SELECT path FROM path_index WHERE worktree = ?", worktree)
	if err != nil {
		return nil, fmt.Errorf("read tracked worktree paths: %w", err)
	}
	defer rows.Close()
	tracked := make(map[string]bool)
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, fmt.Errorf("read tracked worktree path: %w", err)
		}
		tracked[path] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read tracked worktree paths: %w", err)
	}
	return tracked, nil
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
	if err := checkStateDirFilesystem(canonicalClientDir, config.checkFilesystem); err != nil {
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
	if err := initializeClientSchema(ctx, db); err != nil {
		return err
	}
	if err := validatePendingPromotionTargets(ctx, db, canonicalWorktree); err != nil {
		return fmt.Errorf("validate checkout promotion targets: %w", err)
	}
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
			cleanupErr := recoverFSActions(ctx, db, canonicalWorktree, root, config.fsActionFault)
			if cleanupErr == nil {
				cleanupErr = cleanupCheckoutTemps(ctx, db, root, canonicalWorktree, fsPhaseRollback, temps, config.fsActionFault)
			}
			if closeErr := root.Close(); cleanupErr != nil || closeErr != nil {
				return errors.Join(cleanupErr, closeErr)
			}
		}
	}
	var applyState string
	stateErr := db.QueryRowContext(ctx, "SELECT apply_state FROM pending_checkouts WHERE worktree = ?", canonicalWorktree).Scan(&applyState)
	if stateErr != nil && !errors.Is(stateErr, sql.ErrNoRows) {
		return stateErr
	}
	if stateErr == nil && (applyState == "pending" || applyState == "applying" || applyState == "rolling_back" || applyState == "finalized") {
		_, pathErr := os.Lstat(canonicalWorktree)
		if pathErr != nil && !errors.Is(pathErr, os.ErrNotExist) {
			return pathErr
		}
		if pathErr == nil {
			root, err := openWorktreeRoot(canonicalWorktree, config.checkFilesystem)
			if err != nil {
				return err
			}
			var recoveryErr error
			if err := recoverFSActions(ctx, db, canonicalWorktree, root, config.fsActionFault); err != nil {
				recoveryErr = err
			} else if applyState == "finalized" {
				recoveryErr = cleanupSyncRecoveries(ctx, db, root, canonicalWorktree, config)
			} else {
				if applyState == "applying" || applyState == "rolling_back" {
					recoveryErr = beginSyncRollback(ctx, db, canonicalWorktree)
				}
				if recoveryErr == nil {
					recoveryErr = rollbackSyncApply(ctx, db, root, canonicalClientDir, canonicalWorktree, config)
				}
			}
			if closeErr := root.Close(); recoveryErr != nil || closeErr != nil {
				return errors.Join(recoveryErr, closeErr)
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
	if _, err := tx.ExecContext(ctx, "DELETE FROM pending_publications WHERE worktree = ?", canonicalWorktree); err != nil {
		return errors.Join(fmt.Errorf("remove pending publication: %w", err), tx.Rollback())
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM checkout_paths WHERE worktree = ?", canonicalWorktree); err != nil {
		return errors.Join(fmt.Errorf("remove checkout paths: %w", err), tx.Rollback())
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM sync_recovery_promotions WHERE worktree = ?", canonicalWorktree); err != nil {
		return errors.Join(fmt.Errorf("remove sync recovery promotions: %w", err), tx.Rollback())
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM sync_recoveries WHERE worktree = ?", canonicalWorktree); err != nil {
		return errors.Join(fmt.Errorf("remove sync recoveries: %w", err), tx.Rollback())
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM path_index WHERE worktree = ?", canonicalWorktree); err != nil {
		return errors.Join(fmt.Errorf("remove path index: %w", err), tx.Rollback())
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM fs_actions WHERE worktree = ? AND state = 'completed' AND origin_action_id IS NOT NULL", canonicalWorktree); err != nil {
		return errors.Join(fmt.Errorf("remove completed filesystem preserve journal: %w", err), tx.Rollback())
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM fs_actions WHERE worktree = ? AND state = 'completed'", canonicalWorktree); err != nil {
		return errors.Join(fmt.Errorf("remove completed filesystem journal: %w", err), tx.Rollback())
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM fs_journal_bindings WHERE worktree = ?
		AND NOT EXISTS (SELECT 1 FROM fs_actions WHERE fs_actions.worktree = fs_journal_bindings.worktree)`, canonicalWorktree); err != nil {
		return errors.Join(fmt.Errorf("remove filesystem journal root: %w", err), tx.Rollback())
	}
	var incompleteActions int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM fs_actions WHERE worktree = ?", canonicalWorktree).Scan(&incompleteActions); err != nil || incompleteActions != 0 {
		return errors.Join(errors.New("cannot unbind with incomplete filesystem actions"), err, tx.Rollback())
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
	return lockUnbindMode(ctx, clientDir, databasePath, worktree, config, true)
}

func tryLockUnbind(ctx context.Context, clientDir, databasePath, worktree string, config libraryClientConfig) (*clientLocks, error) {
	return lockUnbindMode(ctx, clientDir, databasePath, worktree, config, false)
}

func lockUnbindMode(ctx context.Context, clientDir, databasePath, worktree string, config libraryClientConfig, wait bool) (*clientLocks, error) {
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
		locks, err := lockClientKeysMode(ctx, clientDir, names, config, wait)
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

func lockClientKeys(ctx context.Context, clientDir string, names []string, config libraryClientConfig) (*clientLocks, error) {
	return lockClientKeysMode(ctx, clientDir, names, config, true)
}

func lockClientKeysMode(ctx context.Context, clientDir string, names []string, config libraryClientConfig, wait bool) (_ *clientLocks, retErr error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("lock client state: %w", err)
	}
	checkFilesystem := config.checkFilesystem
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
	if err := checkStateDirFilesystem(clientDir, checkFilesystem); err != nil {
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
		if wait {
			if err := flockContext(ctx, file); err != nil {
				return nil, err
			}
			continue
		}
		if err := fscompat.Flock(int(file.Fd()), fscompat.LOCK_EX|fscompat.LOCK_NB); err != nil {
			if errors.Is(err, fscompat.EWOULDBLOCK) || errors.Is(err, fscompat.EAGAIN) {
				return nil, errors.New("binding synchronization is already running")
			}
			return nil, fmt.Errorf("lock client state: %w", err)
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
		err := fscompat.Flock(int(file.Fd()), fscompat.LOCK_EX|fscompat.LOCK_NB)
		if err == nil {
			return nil
		}
		if !errors.Is(err, fscompat.EWOULDBLOCK) && !errors.Is(err, fscompat.EAGAIN) {
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
		retErr = errors.Join(retErr, fscompat.Flock(int(locks.files[index].Fd()), fscompat.LOCK_UN), locks.files[index].Close())
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
	locks, err := lockClientKeys(ctx, clientDir, []string{lockName("client-database")}, libraryClientConfig{
		syncDirectory: func(string) error { return nil },
	})
	if err != nil {
		return nil, err
	}
	defer locks.Close()
	db, err := openClientDB(path, false)
	if err != nil {
		return nil, err
	}
	if err := enableClientDBWAL(ctx, db); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	if err := initializeClientSchema(ctx, db); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	if err := assertClientDBDurability(ctx, db); err != nil {
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

const _clientSchemaVersion = 24

var legacyClientV12Columns = map[string][]string{
	"bindings":             {"server_url|TEXT|1||0", "library_id|TEXT|1||0", "worktree|TEXT|1||1", "user_id|TEXT|1||0", "device_id|TEXT|1||0", "sync_base_commit|TEXT|1||0", "sync_base_root|TEXT|1||0", "head_etag|TEXT|1||0", "access_token|BLOB|1||0"},
	"bind_intents":         {"server_url|TEXT|1||0", "library_id|TEXT|1||0", "worktree|TEXT|1||1", "user_id|TEXT|1||0", "device_id|TEXT|1||0", "expected_etag|TEXT|1||0", "candidate_commit|TEXT|1||0", "candidate_root|TEXT|1||0", "candidate_data|BLOB|1||0", "import_local|INTEGER|1|0|0"},
	"pending_checkouts":    {"server_url|TEXT|1||0", "library_id|TEXT|1||0", "worktree|TEXT|1||1", "user_id|TEXT|1||0", "device_id|TEXT|1||0", "target_commit|TEXT|1||0", "target_root|TEXT|1||0", "head_etag|TEXT|1||0", "apply_state|TEXT|1|'pending'|0"},
	"pending_publications": {"worktree|TEXT|1||1", "base_commit|TEXT|1||0", "base_root|TEXT|1||0", "expected_etag|TEXT|1||0", "candidate_commit|TEXT|1||0", "candidate_root|TEXT|1||0", "candidate_data|BLOB|1||0"},
	"sync_recoveries":      {"worktree|TEXT|1||1", "path|TEXT|1||2", "recovery_name|TEXT|1||0", "type|TEXT|1||0", "object_id|TEXT|1|''|0", "canonical_mtime|TEXT|1|''|0", "size|INTEGER|1|0|0", "device|INTEGER|1|0|0", "inode|INTEGER|1|0|0", "completed|INTEGER|1|0|0", "tombstone_name|TEXT|1|''|0"},
	"checkout_paths":       {"worktree|TEXT|1||1", "path|TEXT|1||2", "type|TEXT|1||0", "object_id|TEXT|1||0", "canonical_mtime|TEXT|1||0", "actual_mtime|TEXT|1|''|0", "size|INTEGER|1|0|0", "temp_name|TEXT|1|''|0", "temp_device|INTEGER|1|0|0", "temp_inode|INTEGER|1|0|0", "target_device|INTEGER|1|0|0", "target_inode|INTEGER|1|0|0", "completed|INTEGER|1|0|0", "rollback_name|TEXT|1|''|0"},
	"path_index":           {"worktree|TEXT|1||1", "path|TEXT|1||2", "type|TEXT|1||0", "object_id|TEXT|1||0", "canonical_mtime|TEXT|1||0", "actual_mtime|TEXT|1||0", "size|INTEGER|1||0"},
}

var legacyClientV12SQL = map[string]string{
	"bindings":             `CREATE TABLE bindings (server_url TEXT NOT NULL, library_id TEXT NOT NULL, worktree TEXT PRIMARY KEY NOT NULL, user_id TEXT NOT NULL, device_id TEXT NOT NULL, sync_base_commit TEXT NOT NULL, sync_base_root TEXT NOT NULL, head_etag TEXT NOT NULL, access_token BLOB NOT NULL, UNIQUE(server_url, library_id))`,
	"bind_intents":         `CREATE TABLE bind_intents (server_url TEXT NOT NULL, library_id TEXT NOT NULL, worktree TEXT PRIMARY KEY NOT NULL, user_id TEXT NOT NULL, device_id TEXT NOT NULL, expected_etag TEXT NOT NULL, candidate_commit TEXT NOT NULL, candidate_root TEXT NOT NULL, candidate_data BLOB NOT NULL, import_local INTEGER NOT NULL DEFAULT 0, UNIQUE(server_url, library_id))`,
	"pending_checkouts":    `CREATE TABLE pending_checkouts (server_url TEXT NOT NULL, library_id TEXT NOT NULL, worktree TEXT PRIMARY KEY NOT NULL, user_id TEXT NOT NULL, device_id TEXT NOT NULL, target_commit TEXT NOT NULL, target_root TEXT NOT NULL, head_etag TEXT NOT NULL, apply_state TEXT NOT NULL DEFAULT 'pending', UNIQUE(server_url, library_id))`,
	"pending_publications": `CREATE TABLE pending_publications (worktree TEXT PRIMARY KEY NOT NULL, base_commit TEXT NOT NULL, base_root TEXT NOT NULL, expected_etag TEXT NOT NULL, candidate_commit TEXT NOT NULL, candidate_root TEXT NOT NULL, candidate_data BLOB NOT NULL)`,
	"sync_recoveries":      `CREATE TABLE sync_recoveries (worktree TEXT NOT NULL, path TEXT NOT NULL, recovery_name TEXT NOT NULL, type TEXT NOT NULL, object_id TEXT NOT NULL DEFAULT '', canonical_mtime TEXT NOT NULL DEFAULT '', size INTEGER NOT NULL DEFAULT 0, device INTEGER NOT NULL DEFAULT 0, inode INTEGER NOT NULL DEFAULT 0, completed INTEGER NOT NULL DEFAULT 0, tombstone_name TEXT NOT NULL DEFAULT '', PRIMARY KEY(worktree, path))`,
	"checkout_paths":       `CREATE TABLE checkout_paths (worktree TEXT NOT NULL, path TEXT NOT NULL, type TEXT NOT NULL, object_id TEXT NOT NULL, canonical_mtime TEXT NOT NULL, actual_mtime TEXT NOT NULL DEFAULT '', size INTEGER NOT NULL DEFAULT 0, temp_name TEXT NOT NULL DEFAULT '', temp_device INTEGER NOT NULL DEFAULT 0, temp_inode INTEGER NOT NULL DEFAULT 0, target_device INTEGER NOT NULL DEFAULT 0, target_inode INTEGER NOT NULL DEFAULT 0, completed INTEGER NOT NULL DEFAULT 0, rollback_name TEXT NOT NULL DEFAULT '', PRIMARY KEY(worktree, path))`,
	"path_index":           `CREATE TABLE path_index (worktree TEXT NOT NULL, path TEXT NOT NULL, type TEXT NOT NULL, object_id TEXT NOT NULL, canonical_mtime TEXT NOT NULL, actual_mtime TEXT NOT NULL, size INTEGER NOT NULL, PRIMARY KEY(worktree, path))`,
}

const legacyClientV18PendingSQL = `CREATE TABLE pending_publications (
	worktree TEXT PRIMARY KEY NOT NULL, base_commit TEXT NOT NULL, base_root TEXT NOT NULL,
	expected_etag TEXT NOT NULL, candidate_commit TEXT NOT NULL, candidate_root TEXT NOT NULL,
	candidate_data BLOB NOT NULL, deletion_count INTEGER NOT NULL DEFAULT 0,
	tracked_count INTEGER NOT NULL DEFAULT 0, requires_delete_confirmation INTEGER NOT NULL DEFAULT 0,
	delete_confirmed INTEGER NOT NULL DEFAULT 0,
	CHECK(deletion_count >= 0 AND tracked_count >= deletion_count),
	CHECK(requires_delete_confirmation IN (0, 1) AND delete_confirmed IN (0, 1)),
	CHECK(delete_confirmed = 0 OR requires_delete_confirmation = 1),
	CHECK(requires_delete_confirmation = (deletion_count > 100 OR (tracked_count > 0 AND
		deletion_count >= tracked_count / 10 + CASE WHEN tracked_count % 10 = 0 THEN 0 ELSE 1 END))))`

const clientV19PendingSQL = `CREATE TABLE pending_publications (
	worktree TEXT PRIMARY KEY NOT NULL, base_commit TEXT NOT NULL, base_root TEXT NOT NULL,
	expected_etag TEXT NOT NULL, candidate_commit TEXT NOT NULL, candidate_root TEXT NOT NULL,
	candidate_data BLOB NOT NULL, deletion_count INTEGER NOT NULL DEFAULT 0,
	tracked_count INTEGER NOT NULL DEFAULT 0, requires_delete_confirmation INTEGER NOT NULL DEFAULT 0,
	delete_confirmed INTEGER NOT NULL DEFAULT 0, legacy_revalidation_required INTEGER NOT NULL DEFAULT 0,
	CHECK(deletion_count >= 0 AND tracked_count >= deletion_count),
	CHECK(requires_delete_confirmation IN (0, 1) AND delete_confirmed IN (0, 1)
		AND legacy_revalidation_required IN (0, 1)),
	CHECK((legacy_revalidation_required = 1 AND deletion_count = 0 AND tracked_count = 0
		AND requires_delete_confirmation = 0 AND delete_confirmed = 0) OR
		(legacy_revalidation_required = 0 AND delete_confirmed <= requires_delete_confirmation AND
		requires_delete_confirmation = (deletion_count > 100 OR (tracked_count > 0 AND
		deletion_count >= tracked_count / 10 + CASE WHEN tracked_count % 10 = 0 THEN 0 ELSE 1 END)))))`

const clientV20PendingSQL = `CREATE TABLE pending_publications (
	worktree TEXT PRIMARY KEY NOT NULL, base_commit TEXT NOT NULL, base_root TEXT NOT NULL,
	expected_head TEXT NOT NULL, expected_etag TEXT NOT NULL, candidate_commit TEXT NOT NULL,
	candidate_root TEXT NOT NULL, candidate_data BLOB NOT NULL, deletion_count INTEGER NOT NULL DEFAULT 0,
	tracked_count INTEGER NOT NULL DEFAULT 0, requires_delete_confirmation INTEGER NOT NULL DEFAULT 0,
	delete_confirmed INTEGER NOT NULL DEFAULT 0, legacy_revalidation_required INTEGER NOT NULL DEFAULT 0,
	CHECK(deletion_count >= 0 AND tracked_count >= deletion_count),
	CHECK(requires_delete_confirmation IN (0, 1) AND delete_confirmed IN (0, 1)
		AND legacy_revalidation_required IN (0, 1)),
	CHECK((legacy_revalidation_required = 1 AND deletion_count = 0 AND tracked_count = 0
		AND requires_delete_confirmation = 0 AND delete_confirmed = 0) OR
		(legacy_revalidation_required = 0 AND delete_confirmed <= requires_delete_confirmation AND
		requires_delete_confirmation = (deletion_count > 100 OR (tracked_count > 0 AND
		deletion_count >= tracked_count / 10 + CASE WHEN tracked_count % 10 = 0 THEN 0 ELSE 1 END)))))`

const _clientV21PendingSQL = `CREATE TABLE pending_publications (
	worktree TEXT PRIMARY KEY NOT NULL, base_commit TEXT NOT NULL, base_root TEXT NOT NULL,
	expected_head TEXT NOT NULL, expected_etag TEXT NOT NULL, candidate_commit TEXT NOT NULL,
	candidate_root TEXT NOT NULL, candidate_data BLOB NOT NULL, captured_commit TEXT NOT NULL,
	captured_root TEXT NOT NULL, captured_data BLOB NOT NULL, candidate_history BLOB NOT NULL,
	deletion_count INTEGER NOT NULL DEFAULT 0,
	tracked_count INTEGER NOT NULL DEFAULT 0, requires_delete_confirmation INTEGER NOT NULL DEFAULT 0,
	delete_confirmed INTEGER NOT NULL DEFAULT 0, legacy_revalidation_required INTEGER NOT NULL DEFAULT 0,
	CHECK(length(candidate_data) BETWEEN 1 AND 65536),
	CHECK(length(captured_data) BETWEEN 1 AND 65536),
	CHECK(length(candidate_history) BETWEEN 8 AND 67112968),
	CHECK(deletion_count >= 0 AND tracked_count >= deletion_count),
	CHECK(requires_delete_confirmation IN (0, 1) AND delete_confirmed IN (0, 1)
		AND legacy_revalidation_required IN (0, 1)),
	CHECK((legacy_revalidation_required = 1 AND deletion_count = 0 AND tracked_count = 0
		AND requires_delete_confirmation = 0 AND delete_confirmed = 0) OR
		(legacy_revalidation_required = 0 AND delete_confirmed <= requires_delete_confirmation AND
		requires_delete_confirmation = (deletion_count > 100 OR (tracked_count > 0 AND
		deletion_count >= tracked_count / 10 + CASE WHEN tracked_count % 10 = 0 THEN 0 ELSE 1 END)))))`

const _clientV23PendingSQL = `CREATE TABLE pending_publications (
	worktree TEXT PRIMARY KEY NOT NULL, publication_kind TEXT NOT NULL, base_commit TEXT NOT NULL, base_root TEXT NOT NULL,
	expected_head TEXT NOT NULL, expected_etag TEXT NOT NULL, candidate_commit TEXT NOT NULL,
	candidate_root TEXT NOT NULL, candidate_data BLOB NOT NULL, captured_commit TEXT NOT NULL,
	captured_root TEXT NOT NULL, captured_data BLOB NOT NULL, candidate_history BLOB NOT NULL,
	deletion_count INTEGER NOT NULL DEFAULT 0,
	tracked_count INTEGER NOT NULL DEFAULT 0, requires_delete_confirmation INTEGER NOT NULL DEFAULT 0,
	delete_confirmed INTEGER NOT NULL DEFAULT 0, legacy_revalidation_required INTEGER NOT NULL DEFAULT 0,
	CHECK(publication_kind = 'sync'),
	CHECK(length(candidate_data) BETWEEN 1 AND 65536),
	CHECK(length(captured_data) BETWEEN 1 AND 65536),
	CHECK(length(candidate_history) BETWEEN 8 AND 67112968),
	CHECK(deletion_count >= 0 AND tracked_count >= deletion_count),
	CHECK(requires_delete_confirmation IN (0, 1) AND delete_confirmed IN (0, 1)
		AND legacy_revalidation_required IN (0, 1)),
	CHECK((legacy_revalidation_required = 1 AND deletion_count = 0 AND tracked_count = 0
		AND requires_delete_confirmation = 0 AND delete_confirmed = 0) OR
		(legacy_revalidation_required = 0 AND delete_confirmed <= requires_delete_confirmation AND
		requires_delete_confirmation = (deletion_count > 100 OR (tracked_count > 0 AND
		deletion_count >= tracked_count / 10 + CASE WHEN tracked_count % 10 = 0 THEN 0 ELSE 1 END)))))`

const _clientV24PendingSQL = `CREATE TABLE pending_publications (
	worktree TEXT PRIMARY KEY NOT NULL, publication_kind TEXT NOT NULL, base_commit TEXT NOT NULL, base_root TEXT NOT NULL,
	expected_head TEXT NOT NULL, expected_etag TEXT NOT NULL, candidate_commit TEXT NOT NULL,
	candidate_root TEXT NOT NULL, candidate_data BLOB NOT NULL, captured_commit TEXT NOT NULL,
	captured_root TEXT NOT NULL, captured_data BLOB NOT NULL, candidate_history BLOB NOT NULL,
	deletion_count INTEGER NOT NULL DEFAULT 0,
	tracked_count INTEGER NOT NULL DEFAULT 0, requires_delete_confirmation INTEGER NOT NULL DEFAULT 0,
	delete_confirmed INTEGER NOT NULL DEFAULT 0, legacy_revalidation_required INTEGER NOT NULL DEFAULT 0,
	source_commit TEXT NOT NULL DEFAULT '', source_path TEXT NOT NULL DEFAULT '', source_root TEXT NOT NULL DEFAULT '',
	created_count INTEGER NOT NULL DEFAULT 0, updated_count INTEGER NOT NULL DEFAULT 0,
	type_replacement_count INTEGER NOT NULL DEFAULT 0, removed_descendant_count INTEGER NOT NULL DEFAULT 0,
	preserved_current_only_count INTEGER NOT NULL DEFAULT 0,
	changed_path_preview BLOB NOT NULL DEFAULT X'4652503100000000', changed_path_count INTEGER NOT NULL DEFAULT 0,
	preview_truncated INTEGER NOT NULL DEFAULT 0, restore_confirmed INTEGER NOT NULL DEFAULT 0,
	CHECK(publication_kind IN ('sync', 'restore')),
	CHECK(length(candidate_data) BETWEEN 1 AND 65536),
	CHECK(length(captured_data) BETWEEN 1 AND 65536),
	CHECK(length(candidate_history) = 0 OR length(candidate_history) BETWEEN 8 AND 67112968),
	CHECK(deletion_count >= 0 AND tracked_count >= deletion_count),
	CHECK(requires_delete_confirmation IN (0, 1) AND delete_confirmed IN (0, 1)
		AND legacy_revalidation_required IN (0, 1) AND preview_truncated IN (0, 1)
		AND restore_confirmed IN (0, 1)),
	CHECK((publication_kind = 'sync' AND source_commit = '' AND source_path = '' AND source_root = ''
		AND created_count = 0 AND updated_count = 0 AND type_replacement_count = 0
		AND removed_descendant_count = 0 AND preserved_current_only_count = 0
		AND changed_path_preview = X'4652503100000000' AND changed_path_count = 0
		AND preview_truncated = 0 AND restore_confirmed = 0) OR
		(publication_kind = 'restore' AND length(candidate_history) = 0 AND deletion_count = 0
		AND tracked_count = 0 AND requires_delete_confirmation = 0 AND delete_confirmed = 0
		AND legacy_revalidation_required = 0 AND length(source_commit) = 64
		AND source_commit NOT GLOB '*[^0-9a-f]*' AND length(source_root) = 64
		AND source_root NOT GLOB '*[^0-9a-f]*' AND length(source_path) BETWEEN 1 AND 1024
		AND created_count >= 0 AND updated_count >= 0 AND type_replacement_count >= 0
		AND removed_descendant_count >= 0 AND preserved_current_only_count >= 0
		AND changed_path_count > 0 AND changed_path_count <= 2000000
		AND changed_path_count = created_count + updated_count + type_replacement_count
		AND candidate_root <> captured_root
		AND length(changed_path_preview) BETWEEN 8 AND 102808)))`

var clientV19PendingColumns = []string{
	"0|worktree|TEXT|1||1", "1|base_commit|TEXT|1||0", "2|base_root|TEXT|1||0",
	"3|expected_etag|TEXT|1||0", "4|candidate_commit|TEXT|1||0", "5|candidate_root|TEXT|1||0",
	"6|candidate_data|BLOB|1||0", "7|deletion_count|INTEGER|1|0|0", "8|tracked_count|INTEGER|1|0|0",
	"9|requires_delete_confirmation|INTEGER|1|0|0", "10|delete_confirmed|INTEGER|1|0|0",
	"11|legacy_revalidation_required|INTEGER|1|0|0",
}

var clientV20PendingColumns = []string{
	"0|worktree|TEXT|1||1", "1|base_commit|TEXT|1||0", "2|base_root|TEXT|1||0",
	"3|expected_head|TEXT|1||0", "4|expected_etag|TEXT|1||0", "5|candidate_commit|TEXT|1||0",
	"6|candidate_root|TEXT|1||0", "7|candidate_data|BLOB|1||0", "8|deletion_count|INTEGER|1|0|0",
	"9|tracked_count|INTEGER|1|0|0", "10|requires_delete_confirmation|INTEGER|1|0|0",
	"11|delete_confirmed|INTEGER|1|0|0", "12|legacy_revalidation_required|INTEGER|1|0|0",
}

var _clientV21PendingColumns = []string{
	"0|worktree|TEXT|1||1", "1|base_commit|TEXT|1||0", "2|base_root|TEXT|1||0",
	"3|expected_head|TEXT|1||0", "4|expected_etag|TEXT|1||0", "5|candidate_commit|TEXT|1||0",
	"6|candidate_root|TEXT|1||0", "7|candidate_data|BLOB|1||0", "8|captured_commit|TEXT|1||0",
	"9|captured_root|TEXT|1||0", "10|captured_data|BLOB|1||0", "11|candidate_history|BLOB|1||0",
	"12|deletion_count|INTEGER|1|0|0", "13|tracked_count|INTEGER|1|0|0",
	"14|requires_delete_confirmation|INTEGER|1|0|0", "15|delete_confirmed|INTEGER|1|0|0",
	"16|legacy_revalidation_required|INTEGER|1|0|0",
}

var _clientV23PendingColumns = []string{
	"0|worktree|TEXT|1||1", "1|publication_kind|TEXT|1||0", "2|base_commit|TEXT|1||0", "3|base_root|TEXT|1||0",
	"4|expected_head|TEXT|1||0", "5|expected_etag|TEXT|1||0", "6|candidate_commit|TEXT|1||0",
	"7|candidate_root|TEXT|1||0", "8|candidate_data|BLOB|1||0", "9|captured_commit|TEXT|1||0",
	"10|captured_root|TEXT|1||0", "11|captured_data|BLOB|1||0", "12|candidate_history|BLOB|1||0",
	"13|deletion_count|INTEGER|1|0|0", "14|tracked_count|INTEGER|1|0|0",
	"15|requires_delete_confirmation|INTEGER|1|0|0", "16|delete_confirmed|INTEGER|1|0|0",
	"17|legacy_revalidation_required|INTEGER|1|0|0",
}

var _clientV24PendingColumns = []string{
	"0|worktree|TEXT|1||1", "1|publication_kind|TEXT|1||0", "2|base_commit|TEXT|1||0", "3|base_root|TEXT|1||0",
	"4|expected_head|TEXT|1||0", "5|expected_etag|TEXT|1||0", "6|candidate_commit|TEXT|1||0",
	"7|candidate_root|TEXT|1||0", "8|candidate_data|BLOB|1||0", "9|captured_commit|TEXT|1||0",
	"10|captured_root|TEXT|1||0", "11|captured_data|BLOB|1||0", "12|candidate_history|BLOB|1||0",
	"13|deletion_count|INTEGER|1|0|0", "14|tracked_count|INTEGER|1|0|0",
	"15|requires_delete_confirmation|INTEGER|1|0|0", "16|delete_confirmed|INTEGER|1|0|0",
	"17|legacy_revalidation_required|INTEGER|1|0|0", "18|source_commit|TEXT|1|''|0",
	"19|source_path|TEXT|1|''|0", "20|source_root|TEXT|1|''|0", "21|created_count|INTEGER|1|0|0",
	"22|updated_count|INTEGER|1|0|0", "23|type_replacement_count|INTEGER|1|0|0",
	"24|removed_descendant_count|INTEGER|1|0|0", "25|preserved_current_only_count|INTEGER|1|0|0",
	"26|changed_path_preview|BLOB|1|X'4652503100000000'|0", "27|changed_path_count|INTEGER|1|0|0",
	"28|preview_truncated|INTEGER|1|0|0", "29|restore_confirmed|INTEGER|1|0|0",
}

var legacyClientV12Indexes = map[string][]string{
	"bindings": {"server_url,library_id", "worktree"}, "bind_intents": {"server_url,library_id", "worktree"},
	"pending_checkouts": {"server_url,library_id", "worktree"}, "pending_publications": {"worktree"},
	"sync_recoveries": {"worktree,path"}, "checkout_paths": {"worktree,path"}, "path_index": {"worktree,path"},
}

func canonicalSQLiteSQL(value string) string {
	var result strings.Builder
	var quote byte
	space := false
	for index := 0; index < len(value); index++ {
		char := value[index]
		if quote != 0 {
			result.WriteByte(char)
			if (quote == '[' && char == ']') || (quote != '[' && char == quote) {
				quote = 0
			}
			continue
		}
		if char == ' ' || char == '\n' || char == '\r' || char == '\t' {
			space = result.Len() != 0
			continue
		}
		if space {
			result.WriteByte(' ')
			space = false
		}
		if char == '\'' || char == '"' || char == '`' || char == '[' {
			quote = char
		} else if char >= 'a' && char <= 'z' {
			char -= 'a' - 'A'
		}
		result.WriteByte(char)
	}
	return result.String()
}

type clientSchemaQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func validateClientV19Schema(ctx context.Context, db clientSchemaQuerier) error {
	return validatePendingPublicationSchema(ctx, db, 19, clientV19PendingSQL, clientV19PendingColumns)
}

func validateClientV20Schema(ctx context.Context, db clientSchemaQuerier) error {
	return validatePendingPublicationSchema(ctx, db, 20, clientV20PendingSQL, clientV20PendingColumns)
}

func _validateClientV21Schema(ctx context.Context, db clientSchemaQuerier) error {
	return validatePendingPublicationSchema(ctx, db, 21, _clientV21PendingSQL, _clientV21PendingColumns)
}

func _validateClientV23Schema(ctx context.Context, db clientSchemaQuerier) error {
	return validatePendingPublicationSchema(ctx, db, 23, _clientV23PendingSQL, _clientV23PendingColumns)
}

func _validateClientV24Schema(ctx context.Context, db clientSchemaQuerier) error {
	return validatePendingPublicationSchema(ctx, db, 24, _clientV24PendingSQL, _clientV24PendingColumns)
}

const _clientV21CheckoutSQL = `CREATE TABLE pending_checkouts (server_url TEXT NOT NULL, library_id TEXT NOT NULL,
	worktree TEXT PRIMARY KEY NOT NULL, user_id TEXT NOT NULL, device_id TEXT NOT NULL,
	target_commit TEXT NOT NULL, target_root TEXT NOT NULL, head_etag TEXT NOT NULL,
	apply_state TEXT NOT NULL DEFAULT 'pending', UNIQUE(server_url, library_id))`

const _clientV22CheckoutSQL = `CREATE TABLE pending_checkouts (
	server_url TEXT NOT NULL, library_id TEXT NOT NULL, worktree TEXT PRIMARY KEY NOT NULL,
	user_id TEXT NOT NULL, device_id TEXT NOT NULL, target_commit TEXT NOT NULL, target_root TEXT NOT NULL,
	head_etag TEXT NOT NULL, apply_state TEXT NOT NULL DEFAULT 'pending',
	conflict_promotions BLOB NOT NULL DEFAULT X'4643503100000000', rollback_root_mtime_ns INTEGER NOT NULL DEFAULT 0,
	rollback_root_mtime_valid INTEGER NOT NULL DEFAULT 0,
	CHECK(length(conflict_promotions) BETWEEN 8 AND 33554432), CHECK(rollback_root_mtime_valid IN (0, 1)),
	CHECK(apply_state NOT IN ('applying', 'rolling_back') OR rollback_root_mtime_valid = 1), UNIQUE(server_url, library_id))`

const _clientV21SyncRecoverySQL = `CREATE TABLE sync_recoveries (
	worktree TEXT NOT NULL, path TEXT NOT NULL, recovery_name TEXT NOT NULL,
	type TEXT NOT NULL, object_id TEXT NOT NULL DEFAULT '', canonical_mtime TEXT NOT NULL DEFAULT '',
	size INTEGER NOT NULL DEFAULT 0, device INTEGER NOT NULL DEFAULT 0, inode INTEGER NOT NULL DEFAULT 0,
	completed INTEGER NOT NULL DEFAULT 0, tombstone_name TEXT NOT NULL DEFAULT '', PRIMARY KEY(worktree, path))`

const _clientV22SyncRecoverySQL = _clientV21SyncRecoverySQL

const _clientV22SyncRecoveryPromotionSQL = `CREATE TABLE sync_recovery_promotions (
	worktree TEXT NOT NULL, recovery_path TEXT NOT NULL, source_path TEXT NOT NULL, current_action_id TEXT NOT NULL,
	rollback_action_id TEXT NOT NULL DEFAULT '', PRIMARY KEY(worktree, source_path), UNIQUE(worktree, current_action_id),
	CHECK(length(worktree) BETWEEN 1 AND 4096),
	CHECK(length(recovery_path) BETWEEN 1 AND 4096), CHECK(length(source_path) BETWEEN 1 AND 4096),
	CHECK(length(current_action_id) = 32 AND current_action_id NOT GLOB '*[^0-9a-f]*'),
	CHECK(rollback_action_id = '' OR (length(rollback_action_id) = 32 AND rollback_action_id NOT GLOB '*[^0-9a-f]*')))`

const _clientV22SyncRecoveryPromotionRollbackIndexSQL = `CREATE UNIQUE INDEX sync_recovery_promotions_rollback_action
	ON sync_recovery_promotions(worktree, rollback_action_id) WHERE rollback_action_id <> ''`

var _clientV21SyncRecoveryColumns = []string{
	"0|worktree|TEXT|1||1", "1|path|TEXT|1||2", "2|recovery_name|TEXT|1||0", "3|type|TEXT|1||0",
	"4|object_id|TEXT|1|''|0", "5|canonical_mtime|TEXT|1|''|0", "6|size|INTEGER|1|0|0",
	"7|device|INTEGER|1|0|0", "8|inode|INTEGER|1|0|0", "9|completed|INTEGER|1|0|0",
	"10|tombstone_name|TEXT|1|''|0",
}

var _clientV22SyncRecoveryColumns = _clientV21SyncRecoveryColumns

var _clientV22SyncRecoveryPromotionColumns = []string{
	"0|worktree|TEXT|1||1", "1|recovery_path|TEXT|1||0", "2|source_path|TEXT|1||2",
	"3|current_action_id|TEXT|1||0", "4|rollback_action_id|TEXT|1|''|0",
}

func _validateClientSyncRecoverySchema(ctx context.Context, db clientSchemaQuerier, version int, expectedSQL string, expectedColumns []string) error {
	var createSQL string
	if err := db.QueryRowContext(ctx, `SELECT sql FROM sqlite_schema WHERE type='table' AND name='sync_recoveries'`).Scan(&createSQL); err != nil {
		return fmt.Errorf("read v%d sync recovery schema: %w", version, err)
	}
	if canonicalSQLiteSQL(createSQL) != canonicalSQLiteSQL(expectedSQL) {
		return fmt.Errorf("v%d sync recovery canonical SQL changed", version)
	}
	rows, err := db.QueryContext(ctx, `SELECT cid,name,type,[notnull],COALESCE(dflt_value,''),pk
		FROM pragma_table_info('sync_recoveries') ORDER BY cid`)
	if err != nil {
		return err
	}
	var columns []string
	for rows.Next() {
		var cid, notNull, pk int
		var name, kind, defaultValue string
		if err := rows.Scan(&cid, &name, &kind, &notNull, &defaultValue, &pk); err != nil {
			rows.Close()
			return err
		}
		columns = append(columns, fmt.Sprintf("%d|%s|%s|%d|%s|%d", cid, name, kind, notNull, defaultValue, pk))
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	if !slices.Equal(columns, expectedColumns) {
		return fmt.Errorf("v%d sync recovery column fingerprint changed", version)
	}
	var indexes, primary, triggers, views int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_index_list('sync_recoveries')").Scan(&indexes); err != nil {
		return err
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_index_list('sync_recoveries') AS i
		JOIN pragma_index_info(i.name) AS c ON c.seqno=0 WHERE i.[unique]=1 AND i.partial=0 AND i.origin='pk'
		AND c.name='worktree'`).Scan(&primary); err != nil {
		return err
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_schema WHERE type='trigger' AND tbl_name='sync_recoveries'`).Scan(&triggers); err != nil {
		return err
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_schema WHERE type='view' AND instr(upper(sql),'SYNC_RECOVERIES')<>0`).Scan(&views); err != nil {
		return err
	}
	if indexes != 1 || primary != 1 || triggers != 0 || views != 0 {
		return fmt.Errorf("v%d sync recovery index/trigger/view fingerprint changed", version)
	}
	return nil
}

func _validateClientSyncRecoveryPromotionSchema(ctx context.Context, db clientSchemaQuerier) error {
	var createSQL string
	if err := db.QueryRowContext(ctx, `SELECT sql FROM sqlite_schema WHERE type='table' AND name='sync_recovery_promotions'`).Scan(&createSQL); err != nil {
		return fmt.Errorf("read v22 sync recovery promotion schema: %w", err)
	}
	if canonicalSQLiteSQL(createSQL) != canonicalSQLiteSQL(_clientV22SyncRecoveryPromotionSQL) {
		return errors.New("v22 sync recovery promotion canonical SQL changed")
	}
	rows, err := db.QueryContext(ctx, `SELECT cid,name,type,[notnull],COALESCE(dflt_value,''),pk
		FROM pragma_table_info('sync_recovery_promotions') ORDER BY cid`)
	if err != nil {
		return err
	}
	var columns []string
	for rows.Next() {
		var cid, notNull, pk int
		var name, kind, defaultValue string
		if err := rows.Scan(&cid, &name, &kind, &notNull, &defaultValue, &pk); err != nil {
			rows.Close()
			return err
		}
		columns = append(columns, fmt.Sprintf("%d|%s|%s|%d|%s|%d", cid, name, kind, notNull, defaultValue, pk))
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	if !slices.Equal(columns, _clientV22SyncRecoveryPromotionColumns) {
		return errors.New("v22 sync recovery promotion column fingerprint changed")
	}
	var indexes, exactIndexes, triggers, views int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_index_list('sync_recovery_promotions')").Scan(&indexes); err != nil {
		return err
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_index_list('sync_recovery_promotions') AS i
		WHERE i.[unique]=1 AND ((i.partial=0 AND i.origin='pk' AND
		(SELECT group_concat(name, ',') FROM pragma_index_info(i.name))='worktree,source_path') OR
		(i.partial=0 AND i.origin='u' AND
		(SELECT group_concat(name, ',') FROM pragma_index_info(i.name))='worktree,current_action_id') OR
		(i.partial=1 AND i.origin='c' AND i.name='sync_recovery_promotions_rollback_action' AND
		(SELECT group_concat(name, ',') FROM pragma_index_info(i.name))='worktree,rollback_action_id'))`).Scan(&exactIndexes); err != nil {
		return err
	}
	var rollbackIndexSQL string
	if err := db.QueryRowContext(ctx, `SELECT sql FROM sqlite_schema WHERE type='index'
		AND name='sync_recovery_promotions_rollback_action'`).Scan(&rollbackIndexSQL); err != nil ||
		canonicalSQLiteSQL(rollbackIndexSQL) != canonicalSQLiteSQL(_clientV22SyncRecoveryPromotionRollbackIndexSQL) {
		return errors.Join(errors.New("v22 sync recovery promotion rollback index changed"), err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_schema WHERE type='trigger' AND tbl_name='sync_recovery_promotions'`).Scan(&triggers); err != nil {
		return err
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_schema WHERE type='view' AND instr(upper(sql),'SYNC_RECOVERY_PROMOTIONS')<>0`).Scan(&views); err != nil {
		return err
	}
	if indexes != 3 || exactIndexes != 3 || triggers != 0 || views != 0 {
		return errors.New("v22 sync recovery promotion index/trigger/view fingerprint changed")
	}
	return nil
}

func _validateClientCheckoutSchema(ctx context.Context, db clientSchemaQuerier, version int, expectedSQL string,
	withPromotions bool) error {
	var createSQL string
	if err := db.QueryRowContext(ctx, `SELECT sql FROM sqlite_schema WHERE type='table' AND name='pending_checkouts'`).Scan(&createSQL); err != nil {
		return fmt.Errorf("read v%d pending checkout schema: %w", version, err)
	}
	if canonicalSQLiteSQL(createSQL) != canonicalSQLiteSQL(expectedSQL) {
		return fmt.Errorf("v%d pending checkout canonical SQL changed", version)
	}
	wantColumns := 9
	if withPromotions {
		wantColumns = 12
	}
	var columns int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_table_info('pending_checkouts')").Scan(&columns); err != nil || columns != wantColumns {
		return errors.Join(fmt.Errorf("v%d pending checkout column fingerprint changed", version), err)
	}
	var indexes, expectedIndexes int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_index_list('pending_checkouts')").Scan(&indexes); err != nil {
		return err
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_index_list('pending_checkouts') AS indexes
		JOIN pragma_index_info(indexes.name) AS columns ON columns.seqno=0
		WHERE indexes.[unique]=1 AND indexes.partial=0 AND columns.name IN ('worktree','server_url')`).Scan(&expectedIndexes); err != nil {
		return err
	}
	if indexes != 2 || expectedIndexes != 2 {
		return fmt.Errorf("v%d pending checkout index fingerprint changed", version)
	}
	return nil
}

func _validateClientV22CheckoutSchema(ctx context.Context, db clientSchemaQuerier) error {
	return _validateClientCheckoutSchema(ctx, db, 22, _clientV22CheckoutSQL, true)
}

func validatePendingPublicationSchema(ctx context.Context, db clientSchemaQuerier, version int, expectedSQL string, expectedColumns []string) error {
	var createSQL string
	if err := db.QueryRowContext(ctx, `SELECT sql FROM sqlite_schema
		WHERE type='table' AND name='pending_publications'`).Scan(&createSQL); err != nil {
		return fmt.Errorf("read v%d pending publication schema: %w", version, err)
	}
	if canonicalSQLiteSQL(createSQL) != canonicalSQLiteSQL(expectedSQL) {
		return fmt.Errorf("v%d pending publication canonical SQL changed", version)
	}
	rows, err := db.QueryContext(ctx, `SELECT cid,name,type,[notnull],COALESCE(dflt_value,''),pk
		FROM pragma_table_info('pending_publications') ORDER BY cid`)
	if err != nil {
		return err
	}
	var columns []string
	for rows.Next() {
		var cid, notnull, primary int
		var name, kind, defaultValue string
		if err := rows.Scan(&cid, &name, &kind, &notnull, &defaultValue, &primary); err != nil {
			rows.Close()
			return err
		}
		columns = append(columns, fmt.Sprintf("%d|%s|%s|%d|%s|%d", cid, name, kind, notnull, defaultValue, primary))
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	if strings.Join(columns, "\n") != strings.Join(expectedColumns, "\n") {
		return fmt.Errorf("v%d pending publication column fingerprint changed", version)
	}
	var indexes int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_index_list('pending_publications')
		WHERE [unique]=1 AND origin='pk' AND partial=0`).Scan(&indexes); err != nil || indexes != 1 {
		return errors.Join(fmt.Errorf("v%d pending publication primary index fingerprint changed", version), err)
	}
	var allIndexes, worktreeIndexes int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_index_list('pending_publications')`).Scan(&allIndexes); err != nil {
		return err
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_index_list('pending_publications') AS indexes
		JOIN pragma_index_info(indexes.name) AS columns
		WHERE indexes.[unique]=1 AND indexes.origin='pk' AND indexes.partial=0
		AND columns.seqno=0 AND columns.cid=0 AND columns.name='worktree'`).Scan(&worktreeIndexes); err != nil {
		return err
	}
	if allIndexes != 1 || worktreeIndexes != 1 {
		return fmt.Errorf("v%d pending publication index fingerprint changed", version)
	}
	var unsafeObjects int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_schema
		WHERE type IN ('trigger','view') AND name NOT LIKE 'sqlite_%'`).Scan(&unsafeObjects); err != nil {
		return err
	}
	if unsafeObjects != 0 {
		return errors.New("client schema has an unexpected trigger or view")
	}
	return nil
}

func validateLegacyClientV12(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `SELECT type, name, COALESCE(sql, '') FROM sqlite_schema
		WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name`)
	if err != nil {
		return err
	}
	var tables []string
	for rows.Next() {
		var objectType, table, createSQL string
		if err := rows.Scan(&objectType, &table, &createSQL); err != nil {
			rows.Close()
			return err
		}
		if objectType != "table" {
			rows.Close()
			return fmt.Errorf("legacy client schema has unexpected %s %q", objectType, table)
		}
		expectedSQL, ok := legacyClientV12SQL[table]
		if !ok || canonicalSQLiteSQL(createSQL) != canonicalSQLiteSQL(expectedSQL) {
			rows.Close()
			return fmt.Errorf("legacy client table %q canonical SQL changed", table)
		}
		tables = append(tables, table)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(tables) != len(legacyClientV12Columns) {
		return errors.New("legacy client schema has an unexpected table set")
	}
	for _, table := range tables {
		expected, ok := legacyClientV12Columns[table]
		if !ok {
			return errors.New("legacy client schema has an unknown table")
		}
		columnRows, err := db.QueryContext(ctx, "SELECT name, type, [notnull], COALESCE(dflt_value, ''), pk FROM pragma_table_info(?) ORDER BY cid", table)
		if err != nil {
			return err
		}
		var actual []string
		for columnRows.Next() {
			var name, kind, defaultValue string
			var notnull, primary int
			if err := columnRows.Scan(&name, &kind, &notnull, &defaultValue, &primary); err != nil {
				columnRows.Close()
				return err
			}
			actual = append(actual, fmt.Sprintf("%s|%s|%d|%s|%d", name, kind, notnull, defaultValue, primary))
		}
		if err := columnRows.Close(); err != nil {
			return err
		}
		if strings.Join(actual, "\n") != strings.Join(expected, "\n") {
			return fmt.Errorf("legacy client table %q column fingerprint changed", table)
		}
		var foreignKeys int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_foreign_key_list(?)", table).Scan(&foreignKeys); err != nil || foreignKeys != 0 {
			return errors.Join(fmt.Errorf("legacy client table %q foreign-key fingerprint changed", table), err)
		}
		indexRows, err := db.QueryContext(ctx, "SELECT name, [unique] FROM pragma_index_list(?) ORDER BY name", table)
		if err != nil {
			return err
		}
		var indexNames []string
		for indexRows.Next() {
			var index string
			var unique int
			if err := indexRows.Scan(&index, &unique); err != nil {
				indexRows.Close()
				return err
			}
			if unique != 1 {
				indexRows.Close()
				return fmt.Errorf("legacy client table %q has an unexpected non-unique index", table)
			}
			indexNames = append(indexNames, index)
		}
		if err := indexRows.Close(); err != nil {
			return err
		}
		var indexes []string
		for _, index := range indexNames {
			info, err := db.QueryContext(ctx, "SELECT name FROM pragma_index_info(?) ORDER BY seqno", index)
			if err != nil {
				return err
			}
			var columns []string
			for info.Next() {
				var column string
				if err := info.Scan(&column); err != nil {
					info.Close()
					return err
				}
				columns = append(columns, column)
			}
			if err := info.Close(); err != nil {
				return err
			}
			indexes = append(indexes, strings.Join(columns, ","))
		}
		sort.Strings(indexes)
		expectedIndexes := append([]string(nil), legacyClientV12Indexes[table]...)
		sort.Strings(expectedIndexes)
		if strings.Join(indexes, "\n") != strings.Join(expectedIndexes, "\n") {
			return fmt.Errorf("legacy client table %q index fingerprint changed", table)
		}
	}
	return nil
}

func validateClientSchemaVersion(ctx context.Context, db *sql.DB) error {
	var migrations int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master
		WHERE type = 'table' AND name = 'client_schema_migrations'`).Scan(&migrations); err != nil {
		return err
	}
	if migrations == 0 {
		var tables int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master
			WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`).Scan(&tables); err != nil {
			return err
		}
		if tables == 0 {
			return nil
		}
		return validateLegacyClientV12(ctx, db)
	}
	rows, err := db.QueryContext(ctx, "SELECT version FROM client_schema_migrations ORDER BY version")
	if err != nil {
		return err
	}
	defer rows.Close()
	var versions []int
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return err
		}
		versions = append(versions, version)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(versions) == 0 || versions[0] != 13 {
		return errors.New("client schema migration history has an unknown baseline or gap")
	}
	for index, version := range versions {
		if version != 13+index || version > _clientSchemaVersion {
			return errors.New("client schema migration history is unknown, gapped, or newer than this client")
		}
	}
	version := versions[len(versions)-1]
	if version == _clientSchemaVersion {
		if err := _validateClientV24Schema(ctx, db); err != nil {
			return err
		}
		if err := _validateClientV22CheckoutSchema(ctx, db); err != nil {
			return err
		}
		if err := _validateClientSyncRecoverySchema(ctx, db, 22, _clientV22SyncRecoverySQL, _clientV22SyncRecoveryColumns); err != nil {
			return err
		}
		return _validateClientSyncRecoveryPromotionSchema(ctx, db)
	}
	if version == 23 {
		if err := _validateClientV23Schema(ctx, db); err != nil {
			return err
		}
		if err := _validateClientV22CheckoutSchema(ctx, db); err != nil {
			return err
		}
		if err := _validateClientSyncRecoverySchema(ctx, db, 22, _clientV22SyncRecoverySQL, _clientV22SyncRecoveryColumns); err != nil {
			return err
		}
		return _validateClientSyncRecoveryPromotionSchema(ctx, db)
	}
	if version == 22 {
		if err := _validateClientV21Schema(ctx, db); err != nil {
			return err
		}
		if err := _validateClientV22CheckoutSchema(ctx, db); err != nil {
			return err
		}
		if err := _validateClientSyncRecoverySchema(ctx, db, 22, _clientV22SyncRecoverySQL, _clientV22SyncRecoveryColumns); err != nil {
			return err
		}
		return _validateClientSyncRecoveryPromotionSchema(ctx, db)
	}
	if version == 21 {
		if err := _validateClientV21Schema(ctx, db); err != nil {
			return err
		}
		if err := _validateClientCheckoutSchema(ctx, db, 21, _clientV21CheckoutSQL, false); err != nil {
			return err
		}
		return _validateClientSyncRecoverySchema(ctx, db, 21, _clientV21SyncRecoverySQL, _clientV21SyncRecoveryColumns)
	}
	if version == 20 {
		return validateClientV20Schema(ctx, db)
	}
	if version == 19 {
		return validateClientV19Schema(ctx, db)
	}
	if version == 17 || version == 18 {
		var createSQL string
		if err := db.QueryRowContext(ctx, `SELECT sql FROM sqlite_schema WHERE type='table' AND name='pending_publications'`).Scan(&createSQL); err != nil {
			return fmt.Errorf("read v%d pending publication schema: %w", version, err)
		}
		expected := legacyClientV18PendingSQL
		if version == 17 {
			expected = legacyClientV12SQL["pending_publications"]
		}
		if canonicalSQLiteSQL(createSQL) != canonicalSQLiteSQL(expected) {
			return fmt.Errorf("v%d pending publication schema fingerprint changed", version)
		}
	}
	return nil
}

func migrateFSActionProvenance(ctx context.Context, tx *sql.Tx) error {
	var hasOrigin int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_table_info('fs_actions') WHERE name='origin_action_id'").Scan(&hasOrigin); err != nil {
		return err
	}
	if hasOrigin == 0 {
		var ambiguous int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fs_actions
			WHERE action_outcome IN ('preserve_unknown', 'collision', 'rolled_back')`).Scan(&ambiguous); err != nil {
			return err
		}
		if ambiguous != 0 {
			return errors.New("legacy filesystem journal contains ambiguous preserve actions")
		}
		if _, err := tx.ExecContext(ctx, `DROP INDEX IF EXISTS fs_actions_pending;
			ALTER TABLE fs_actions RENAME TO old_fs_actions;
			CREATE TABLE fs_actions (
				worktree TEXT NOT NULL, action_id TEXT NOT NULL, origin_action_id TEXT, attempt INTEGER NOT NULL DEFAULT 0,
				action_order INTEGER NOT NULL, phase TEXT NOT NULL, op TEXT NOT NULL, parent_path TEXT NOT NULL,
				parent_device INTEGER NOT NULL, parent_inode INTEGER NOT NULL, source_name TEXT NOT NULL,
				target_name TEXT NOT NULL, expected_kind TEXT NOT NULL, expected_device INTEGER NOT NULL,
				expected_inode INTEGER NOT NULL, expected_object TEXT NOT NULL DEFAULT '', expected_size INTEGER NOT NULL DEFAULT 0,
				expected_mtime TEXT NOT NULL DEFAULT '', internal_name TEXT NOT NULL DEFAULT '',
				internal_source TEXT NOT NULL DEFAULT '', internal_target TEXT NOT NULL DEFAULT '',
				action_outcome TEXT NOT NULL DEFAULT '', state TEXT NOT NULL,
				PRIMARY KEY(worktree, action_id), FOREIGN KEY(worktree) REFERENCES fs_journal_bindings(worktree),
				FOREIGN KEY(worktree, origin_action_id) REFERENCES fs_actions(worktree, action_id),
				CHECK((origin_action_id IS NOT NULL AND attempt >= 1) OR (origin_action_id IS NULL AND attempt = 0)),
				CHECK(phase IN ('pre_base', 'rollback', 'post_base')),
				CHECK(op IN ('create_file', 'create_directory', 'rename', 'restore_promotion', 'unlink', 'rmdir', 'mtime')),
				CHECK(state IN ('intent', 'completed')), UNIQUE(worktree, action_order));
			INSERT INTO fs_actions(worktree, action_id, action_order, phase, op, parent_path, parent_device, parent_inode,
				source_name, target_name, expected_kind, expected_device, expected_inode, expected_object, expected_size,
				expected_mtime, internal_name, internal_source, internal_target, action_outcome, state)
			SELECT worktree, action_id, action_order, phase, op, parent_path, parent_device, parent_inode, source_name,
				target_name, expected_kind, expected_device, expected_inode, expected_object, expected_size, expected_mtime,
				internal_name, internal_source, internal_target, action_outcome, state FROM old_fs_actions;
			DROP TABLE old_fs_actions`); err != nil {
			return fmt.Errorf("migrate filesystem action provenance: %w", err)
		}
	}
	_, err := tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS fs_actions_pending ON fs_actions(worktree, phase, state, action_order);
		CREATE UNIQUE INDEX IF NOT EXISTS fs_actions_preserve_attempt ON fs_actions(worktree, origin_action_id, attempt)
			WHERE origin_action_id IS NOT NULL`)
	return err
}

func migrateFSActionRestorePromotionOp(ctx context.Context, tx *sql.Tx) error {
	var createSQL string
	if err := tx.QueryRowContext(ctx, `SELECT sql FROM sqlite_schema WHERE type='table' AND name='fs_actions'`).Scan(&createSQL); err != nil {
		return err
	}
	if strings.Contains(createSQL, "'restore_promotion'") {
		return nil
	}
	_, err := tx.ExecContext(ctx, `DROP INDEX IF EXISTS fs_actions_pending;
		DROP INDEX IF EXISTS fs_actions_preserve_attempt;
		ALTER TABLE fs_actions RENAME TO old_fs_actions_restore_op;
		CREATE TABLE fs_actions (
			worktree TEXT NOT NULL, action_id TEXT NOT NULL, origin_action_id TEXT, attempt INTEGER NOT NULL DEFAULT 0,
			action_order INTEGER NOT NULL, phase TEXT NOT NULL, op TEXT NOT NULL, parent_path TEXT NOT NULL,
			parent_device INTEGER NOT NULL, parent_inode INTEGER NOT NULL,
			source_name TEXT NOT NULL, target_name TEXT NOT NULL, expected_kind TEXT NOT NULL,
			expected_device INTEGER NOT NULL, expected_inode INTEGER NOT NULL,
			expected_object TEXT NOT NULL DEFAULT '', expected_size INTEGER NOT NULL DEFAULT 0,
			expected_mtime TEXT NOT NULL DEFAULT '', internal_name TEXT NOT NULL DEFAULT '',
			internal_source TEXT NOT NULL DEFAULT '', internal_target TEXT NOT NULL DEFAULT '',
			action_outcome TEXT NOT NULL DEFAULT '', state TEXT NOT NULL,
			PRIMARY KEY(worktree, action_id), FOREIGN KEY(worktree) REFERENCES fs_journal_bindings(worktree),
			FOREIGN KEY(worktree, origin_action_id) REFERENCES fs_actions(worktree, action_id),
			CHECK((origin_action_id IS NOT NULL AND attempt >= 1) OR (origin_action_id IS NULL AND attempt = 0)),
			CHECK(phase IN ('pre_base', 'rollback', 'post_base')),
			CHECK(op IN ('create_file', 'create_directory', 'rename', 'restore_promotion', 'unlink', 'rmdir', 'mtime')),
			CHECK(state IN ('intent', 'completed')), UNIQUE(worktree, action_order));
		INSERT INTO fs_actions(worktree,action_id,origin_action_id,attempt,action_order,phase,op,parent_path,
			parent_device,parent_inode,source_name,target_name,expected_kind,expected_device,expected_inode,
			expected_object,expected_size,expected_mtime,internal_name,internal_source,internal_target,action_outcome,state)
		SELECT worktree,action_id,origin_action_id,attempt,action_order,phase,op,parent_path,
			parent_device,parent_inode,source_name,target_name,expected_kind,expected_device,expected_inode,
			expected_object,expected_size,expected_mtime,internal_name,internal_source,internal_target,action_outcome,state
		FROM old_fs_actions_restore_op;
		DROP TABLE old_fs_actions_restore_op;
		CREATE INDEX fs_actions_pending ON fs_actions(worktree,phase,state,action_order);
		CREATE UNIQUE INDEX fs_actions_preserve_attempt ON fs_actions(worktree,origin_action_id,attempt)
			WHERE origin_action_id IS NOT NULL`)
	return err
}

func migrateCheckoutCreateOrigins(ctx context.Context, tx *sql.Tx) error {
	type checkoutOrigin struct{ worktree, path, kind, temp string }
	rows, err := tx.QueryContext(ctx, `SELECT worktree,path,type,temp_name FROM checkout_paths WHERE temp_name<>''`)
	if err != nil {
		return err
	}
	var paths []checkoutOrigin
	for rows.Next() {
		var value checkoutOrigin
		if err := rows.Scan(&value.worktree, &value.path, &value.kind, &value.temp); err != nil {
			rows.Close()
			return err
		}
		paths = append(paths, value)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	assigned := make(map[string]string)
	for _, value := range paths {
		parent, _ := splitFSActionPath(value.path)
		op := fsOpCreateFile
		if value.kind == "Directory" {
			op = fsOpCreateDirectory
		}
		candidates, err := tx.QueryContext(ctx, `SELECT action_id FROM fs_actions WHERE worktree=? AND origin_action_id IS NULL
			AND parent_path=? AND source_name=? AND expected_kind=? AND op=?`, value.worktree, parent, value.temp, value.kind, op)
		if err != nil {
			return err
		}
		var ids []string
		for candidates.Next() {
			var id string
			if err := candidates.Scan(&id); err != nil {
				candidates.Close()
				return err
			}
			ids = append(ids, id)
		}
		if err := errors.Join(candidates.Err(), candidates.Close()); err != nil {
			return err
		}
		if len(ids) > 1 {
			return errors.New("legacy checkout path has ambiguous filesystem creation origins")
		}
		if len(ids) == 0 {
			continue
		}
		key := value.worktree + "\x00" + ids[0]
		if previous, exists := assigned[key]; exists && previous != value.path {
			return errors.New("legacy filesystem creation origin maps to multiple checkout paths")
		}
		assigned[key] = value.path
		result, err := tx.ExecContext(ctx, `UPDATE checkout_paths SET create_action_id=?
			WHERE worktree=? AND path=? AND temp_name=? AND type=? AND create_action_id=''`,
			ids[0], value.worktree, value.path, value.temp, value.kind)
		if err != nil {
			return err
		}
		if changed, err := result.RowsAffected(); err != nil || changed != 1 {
			return errors.Join(errors.New("migrate checkout filesystem creation origin"), err)
		}
	}
	return nil
}

func initializeClientSchema(ctx context.Context, db *sql.DB) error {
	if err := validateClientSchemaVersion(ctx, db); err != nil {
		return fmt.Errorf("validate client schema before migration: %w", err)
	}
	var startedAtV21 bool
	var migrationTable int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name='client_schema_migrations'").Scan(&migrationTable); err != nil {
		return err
	}
	if migrationTable != 0 {
		var version int
		if err := db.QueryRowContext(ctx, "SELECT COALESCE(MAX(version),0) FROM client_schema_migrations").Scan(&version); err != nil {
			return err
		}
		startedAtV21 = version == 21
	}
	if startedAtV21 {
		// v21 pending rows have not mutated the worktree; finalized rows only need cleanup.
		// Every other state may require rollback, which v21 cannot perform with an exact root mtime.
		var unsafeCheckouts int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pending_checkouts
			WHERE apply_state NOT IN ('pending', 'finalized')`).Scan(&unsafeCheckouts); err != nil {
			return fmt.Errorf("preflight v21 pending checkout state: %w", err)
		}
		if unsafeCheckouts != 0 {
			return errors.New("v21 pending checkout in-flight state has no exact rollback root mtime")
		}
	}
	var pendingTableExists int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_schema
		WHERE type='table' AND name='pending_publications'`).Scan(&pendingTableExists); err != nil {
		return fmt.Errorf("preflight pending publication schema: %w", err)
	}
	if pendingTableExists != 0 {
		var pendingCandidateOversize int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pending_publications
			WHERE length(candidate_data) NOT BETWEEN 1 AND 65536`).Scan(&pendingCandidateOversize); err != nil {
			return fmt.Errorf("preflight pending publication metadata: %w", err)
		}
		if pendingCandidateOversize != 0 {
			return errors.New("pending publication candidate metadata exceeds synchronization budget")
		}
	}
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		return fmt.Errorf("enable client foreign keys: %w", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("initialize client state: %w", err)
	}
	fail := func(err error) error {
		return errors.Join(fmt.Errorf("initialize client state: %w", err), tx.Rollback())
	}
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS client_schema_migrations (
		version INTEGER PRIMARY KEY NOT NULL);
	CREATE TABLE IF NOT EXISTS bindings (
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
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS fs_journal_bindings (
		worktree TEXT PRIMARY KEY NOT NULL, root_device INTEGER NOT NULL, root_inode INTEGER NOT NULL,
		journal_format INTEGER NOT NULL);
	CREATE TABLE IF NOT EXISTS fs_actions (
		worktree TEXT NOT NULL, action_id TEXT NOT NULL, origin_action_id TEXT, attempt INTEGER NOT NULL DEFAULT 0,
		action_order INTEGER NOT NULL,
		phase TEXT NOT NULL, op TEXT NOT NULL, parent_path TEXT NOT NULL,
		parent_device INTEGER NOT NULL, parent_inode INTEGER NOT NULL,
		source_name TEXT NOT NULL, target_name TEXT NOT NULL, expected_kind TEXT NOT NULL,
		expected_device INTEGER NOT NULL, expected_inode INTEGER NOT NULL,
		expected_object TEXT NOT NULL DEFAULT '', expected_size INTEGER NOT NULL DEFAULT 0,
		expected_mtime TEXT NOT NULL DEFAULT '', internal_name TEXT NOT NULL DEFAULT '',
		internal_source TEXT NOT NULL DEFAULT '', internal_target TEXT NOT NULL DEFAULT '',
		action_outcome TEXT NOT NULL DEFAULT '', state TEXT NOT NULL,
		PRIMARY KEY(worktree, action_id), FOREIGN KEY(worktree) REFERENCES fs_journal_bindings(worktree),
		FOREIGN KEY(worktree, origin_action_id) REFERENCES fs_actions(worktree, action_id),
		CHECK((origin_action_id IS NOT NULL AND attempt >= 1) OR (origin_action_id IS NULL AND attempt = 0)),
		CHECK(phase IN ('pre_base', 'rollback', 'post_base')),
		CHECK(op IN ('create_file', 'create_directory', 'rename', 'restore_promotion', 'unlink', 'rmdir', 'mtime')),
		CHECK(state IN ('intent', 'completed')),
		UNIQUE(worktree, action_order));
	CREATE INDEX IF NOT EXISTS fs_actions_pending ON fs_actions(worktree, phase, state, action_order);
	CREATE TABLE IF NOT EXISTS pending_checkouts (
		server_url TEXT NOT NULL, library_id TEXT NOT NULL,
		worktree TEXT PRIMARY KEY NOT NULL, user_id TEXT NOT NULL, device_id TEXT NOT NULL,
		target_commit TEXT NOT NULL, target_root TEXT NOT NULL, head_etag TEXT NOT NULL,
		apply_state TEXT NOT NULL DEFAULT 'pending',
		conflict_promotions BLOB NOT NULL DEFAULT X'4643503100000000', rollback_root_mtime_ns INTEGER NOT NULL DEFAULT 0,
		rollback_root_mtime_valid INTEGER NOT NULL DEFAULT 0,
		CHECK(length(conflict_promotions) BETWEEN 8 AND 33554432), CHECK(rollback_root_mtime_valid IN (0, 1)),
		CHECK(apply_state NOT IN ('applying', 'rolling_back') OR rollback_root_mtime_valid = 1), UNIQUE(server_url, library_id));
		CREATE TABLE IF NOT EXISTS sync_recoveries (
		worktree TEXT NOT NULL, path TEXT NOT NULL, recovery_name TEXT NOT NULL,
		type TEXT NOT NULL, object_id TEXT NOT NULL DEFAULT '', canonical_mtime TEXT NOT NULL DEFAULT '',
		size INTEGER NOT NULL DEFAULT 0, device INTEGER NOT NULL DEFAULT 0, inode INTEGER NOT NULL DEFAULT 0,
		completed INTEGER NOT NULL DEFAULT 0, tombstone_name TEXT NOT NULL DEFAULT '', PRIMARY KEY(worktree, path));
		CREATE TABLE IF NOT EXISTS checkout_paths (
		worktree TEXT NOT NULL, path TEXT NOT NULL, type TEXT NOT NULL, object_id TEXT NOT NULL,
		canonical_mtime TEXT NOT NULL, actual_mtime TEXT NOT NULL DEFAULT '', size INTEGER NOT NULL DEFAULT 0,
		temp_name TEXT NOT NULL DEFAULT '', temp_device INTEGER NOT NULL DEFAULT 0, temp_inode INTEGER NOT NULL DEFAULT 0,
		target_device INTEGER NOT NULL DEFAULT 0, target_inode INTEGER NOT NULL DEFAULT 0,
		completed INTEGER NOT NULL DEFAULT 0, rollback_name TEXT NOT NULL DEFAULT '', create_action_id TEXT NOT NULL DEFAULT '',
		PRIMARY KEY(worktree, path));
		CREATE TABLE IF NOT EXISTS path_index (
		worktree TEXT NOT NULL, path TEXT NOT NULL, type TEXT NOT NULL, object_id TEXT NOT NULL,
		canonical_mtime TEXT NOT NULL, actual_mtime TEXT NOT NULL, size INTEGER NOT NULL,
		PRIMARY KEY(worktree, path));`); err != nil {
		return fail(err)
	}
	var pendingPublications int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_schema
		WHERE type='table' AND name='pending_publications'`).Scan(&pendingPublications); err != nil {
		return fail(err)
	}
	if pendingPublications == 0 {
		if _, err := tx.ExecContext(ctx, _clientV24PendingSQL); err != nil {
			return fail(err)
		}
	}
	for table, columns := range map[string]map[string]string{
		"pending_checkouts": {"apply_state": "TEXT NOT NULL DEFAULT 'pending'"},
		"fs_actions": {
			"internal_source": "TEXT NOT NULL DEFAULT ''", "internal_target": "TEXT NOT NULL DEFAULT ''",
			"action_outcome": "TEXT NOT NULL DEFAULT ''",
		},
		"sync_recoveries": {
			"object_id": "TEXT NOT NULL DEFAULT ''", "canonical_mtime": "TEXT NOT NULL DEFAULT ''",
			"size": "INTEGER NOT NULL DEFAULT 0", "tombstone_name": "TEXT NOT NULL DEFAULT ''",
		},
	} {
		for column, definition := range columns {
			var present int
			if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_table_info('"+table+"') WHERE name = ?", column).Scan(&present); err != nil {
				return fail(err)
			}
			if present == 0 {
				if _, err := tx.ExecContext(ctx, "ALTER TABLE "+table+" ADD COLUMN "+column+" "+definition); err != nil {
					return fail(err)
				}
			}
		}
	}
	if err := migrateFSActionProvenance(ctx, tx); err != nil {
		return fail(err)
	}
	if err := migrateFSActionRestorePromotionOp(ctx, tx); err != nil {
		return fail(fmt.Errorf("migrate filesystem restore promotion operation: %w", err))
	}
	if _, err := tx.ExecContext(ctx, `UPDATE fs_actions SET internal_source = internal_name
		WHERE internal_source = '' AND internal_name <> '' AND source_name = internal_name AND target_name <> internal_name;
		UPDATE fs_actions SET internal_target = internal_name
		WHERE internal_target = '' AND internal_name <> '' AND target_name = internal_name AND source_name <> internal_name`); err != nil {
		return fail(err)
	}
	for column, definition := range map[string]string{
		"target_device": "INTEGER NOT NULL DEFAULT 0", "target_inode": "INTEGER NOT NULL DEFAULT 0",
		"rollback_name": "TEXT NOT NULL DEFAULT ''",
	} {
		var present int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_table_info('checkout_paths') WHERE name = ?", column).Scan(&present); err != nil {
			return fail(err)
		}
		if present == 0 {
			if _, err := tx.ExecContext(ctx, "ALTER TABLE checkout_paths ADD COLUMN "+column+" "+definition); err != nil {
				return fail(err)
			}
		}
	}
	var hasCreateActionID int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_table_info('checkout_paths') WHERE name='create_action_id'").Scan(&hasCreateActionID); err != nil {
		return fail(err)
	}
	if hasCreateActionID == 0 {
		if _, err := tx.ExecContext(ctx, "ALTER TABLE checkout_paths ADD COLUMN create_action_id TEXT NOT NULL DEFAULT ''"); err != nil {
			return fail(err)
		}
		if err := migrateCheckoutCreateOrigins(ctx, tx); err != nil {
			return fail(err)
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
	var promotionColumn int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_table_info('pending_checkouts') WHERE name='conflict_promotions'").Scan(&promotionColumn); err != nil {
		return fail(err)
	}
	if promotionColumn == 0 {
		if err := _validateClientCheckoutSchema(ctx, tx, 21, _clientV21CheckoutSQL, false); err != nil {
			return fail(fmt.Errorf("validate pending checkout schema before provenance migration: %w", err))
		}
		if _, err := tx.ExecContext(ctx, `ALTER TABLE pending_checkouts RENAME TO old_pending_checkouts;
			CREATE TABLE pending_checkouts (
				server_url TEXT NOT NULL, library_id TEXT NOT NULL, worktree TEXT PRIMARY KEY NOT NULL,
				user_id TEXT NOT NULL, device_id TEXT NOT NULL, target_commit TEXT NOT NULL, target_root TEXT NOT NULL,
				head_etag TEXT NOT NULL, apply_state TEXT NOT NULL DEFAULT 'pending',
				conflict_promotions BLOB NOT NULL DEFAULT X'4643503100000000', rollback_root_mtime_ns INTEGER NOT NULL DEFAULT 0,
				rollback_root_mtime_valid INTEGER NOT NULL DEFAULT 0,
				CHECK(length(conflict_promotions) BETWEEN 8 AND 33554432), CHECK(rollback_root_mtime_valid IN (0, 1)),
				CHECK(apply_state NOT IN ('applying', 'rolling_back') OR rollback_root_mtime_valid = 1), UNIQUE(server_url, library_id));
			INSERT INTO pending_checkouts(server_url,library_id,worktree,user_id,device_id,target_commit,target_root,head_etag,apply_state,conflict_promotions,rollback_root_mtime_ns,rollback_root_mtime_valid)
			SELECT server_url,library_id,worktree,user_id,device_id,target_commit,target_root,head_etag,apply_state,X'4643503100000000',0,0 FROM old_pending_checkouts;
			DROP TABLE old_pending_checkouts`); err != nil {
			return fail(fmt.Errorf("migrate pending checkout conflict provenance: %w", err))
		}
	}
	var recoveryPromotionTable int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name='sync_recovery_promotions'").Scan(&recoveryPromotionTable); err != nil {
		return fail(err)
	}
	if recoveryPromotionTable == 0 {
		if startedAtV21 {
			if err := _validateClientSyncRecoverySchema(ctx, tx, 21, _clientV21SyncRecoverySQL, _clientV21SyncRecoveryColumns); err != nil {
				return fail(fmt.Errorf("validate v21 sync recovery schema before linkage migration: %w", err))
			}
		}
		var recoveries int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM sync_recoveries").Scan(&recoveries); err != nil {
			return fail(err)
		}
		if recoveries != 0 {
			return fail(errors.New("v21 sync recovery linkage migration requires an empty recovery table"))
		}
		if !startedAtV21 {
			if _, err := tx.ExecContext(ctx, `ALTER TABLE sync_recoveries RENAME TO old_sync_recoveries;
				CREATE TABLE sync_recoveries (
					worktree TEXT NOT NULL, path TEXT NOT NULL, recovery_name TEXT NOT NULL,
					type TEXT NOT NULL, object_id TEXT NOT NULL DEFAULT '', canonical_mtime TEXT NOT NULL DEFAULT '',
					size INTEGER NOT NULL DEFAULT 0, device INTEGER NOT NULL DEFAULT 0, inode INTEGER NOT NULL DEFAULT 0,
					completed INTEGER NOT NULL DEFAULT 0, tombstone_name TEXT NOT NULL DEFAULT '', PRIMARY KEY(worktree, path));
				DROP TABLE old_sync_recoveries`); err != nil {
				return fail(fmt.Errorf("normalize sync recovery schema: %w", err))
			}
		}
		if _, err := tx.ExecContext(ctx, _clientV22SyncRecoveryPromotionSQL); err != nil {
			return fail(fmt.Errorf("migrate sync recovery action linkage: %w", err))
		}
		if _, err := tx.ExecContext(ctx, _clientV22SyncRecoveryPromotionRollbackIndexSQL); err != nil {
			return fail(fmt.Errorf("index sync recovery rollback action linkage: %w", err))
		}
	}
	var pendingDeletionColumns int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('pending_publications')
		WHERE name IN ('deletion_count','tracked_count','requires_delete_confirmation','delete_confirmed',
		'legacy_revalidation_required')`).Scan(&pendingDeletionColumns); err != nil {
		return fail(err)
	}
	if pendingDeletionColumns == 0 || pendingDeletionColumns == 4 {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE pending_publications RENAME TO old_pending_publications`); err != nil {
			return fail(err)
		}
		if _, err := tx.ExecContext(ctx, clientV19PendingSQL); err != nil {
			return fail(err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO pending_publications(worktree,base_commit,base_root,expected_etag,candidate_commit,candidate_root,
			candidate_data,deletion_count,tracked_count,requires_delete_confirmation,delete_confirmed,legacy_revalidation_required)
			SELECT worktree,base_commit,base_root,expected_etag,candidate_commit,candidate_root,candidate_data,0,0,0,0,1
			FROM old_pending_publications;
			DROP TABLE old_pending_publications`); err != nil {
			return fail(err)
		}
	} else if pendingDeletionColumns != 5 {
		return fail(errors.New("pending publication deletion schema is incomplete"))
	}
	var expectedHeadColumn int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('pending_publications')
		WHERE name='expected_head'`).Scan(&expectedHeadColumn); err != nil {
		return fail(err)
	}
	if expectedHeadColumn == 0 {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE pending_publications RENAME TO old_pending_publications`); err != nil {
			return fail(err)
		}
		if _, err := tx.ExecContext(ctx, clientV20PendingSQL); err != nil {
			return fail(err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO pending_publications(worktree,base_commit,base_root,expected_head,
			expected_etag,candidate_commit,candidate_root,candidate_data,deletion_count,tracked_count,
			requires_delete_confirmation,delete_confirmed,legacy_revalidation_required)
			SELECT worktree,base_commit,base_root,base_commit,expected_etag,candidate_commit,candidate_root,candidate_data,
			deletion_count,tracked_count,requires_delete_confirmation,delete_confirmed,legacy_revalidation_required
			FROM old_pending_publications;
			DROP TABLE old_pending_publications`); err != nil {
			return fail(err)
		}
	}
	var capturedColumns int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('pending_publications')
		WHERE name IN ('captured_commit','captured_root','captured_data','candidate_history')`).Scan(&capturedColumns); err != nil {
		return fail(err)
	}
	if capturedColumns == 0 {
		if err := validateClientV20Schema(ctx, tx); err != nil {
			return fail(fmt.Errorf("validate v20 schema before migration: %w", err))
		}
		if _, err := tx.ExecContext(ctx, `ALTER TABLE pending_publications RENAME TO old_pending_publications`); err != nil {
			return fail(err)
		}
		if _, err := tx.ExecContext(ctx, _clientV21PendingSQL); err != nil {
			return fail(err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO pending_publications(worktree,base_commit,base_root,expected_head,
			expected_etag,candidate_commit,candidate_root,candidate_data,captured_commit,captured_root,captured_data,
			candidate_history,deletion_count,tracked_count,requires_delete_confirmation,delete_confirmed,legacy_revalidation_required)
			SELECT worktree,base_commit,base_root,expected_head,expected_etag,candidate_commit,candidate_root,candidate_data,
			candidate_commit,candidate_root,candidate_data,X'4643483100000000',deletion_count,tracked_count,
			requires_delete_confirmation,delete_confirmed,legacy_revalidation_required FROM old_pending_publications;
			DROP TABLE old_pending_publications`); err != nil {
			return fail(err)
		}
	} else if capturedColumns != 4 {
		return fail(errors.New("pending publication captured schema is incomplete"))
	}
	var publicationKindColumns int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('pending_publications')
		WHERE name='publication_kind'`).Scan(&publicationKindColumns); err != nil {
		return fail(err)
	}
	if publicationKindColumns == 0 {
		if err := _validateClientV21Schema(ctx, tx); err != nil {
			return fail(fmt.Errorf("validate v22 pending publication schema before kind migration: %w", err))
		}
		var invalidRows int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM pending_publications WHERE
			length(candidate_data) NOT BETWEEN 1 AND 65536 OR length(captured_data) NOT BETWEEN 1 AND 65536 OR
			length(candidate_history) NOT BETWEEN 8 AND 67112968 OR deletion_count < 0 OR tracked_count < deletion_count OR
			requires_delete_confirmation NOT IN (0,1) OR delete_confirmed NOT IN (0,1) OR
			legacy_revalidation_required NOT IN (0,1) OR
			(legacy_revalidation_required=1 AND (deletion_count<>0 OR tracked_count<>0 OR
				requires_delete_confirmation<>0 OR delete_confirmed<>0)) OR
			(legacy_revalidation_required=0 AND (delete_confirmed>requires_delete_confirmation OR
				requires_delete_confirmation<>(deletion_count>100 OR (tracked_count>0 AND deletion_count>=tracked_count/10+
					CASE WHEN tracked_count%10=0 THEN 0 ELSE 1 END))))`).Scan(&invalidRows); err != nil {
			return fail(fmt.Errorf("validate legacy pending publication rows: %w", err))
		}
		if invalidRows != 0 {
			return fail(errors.New("legacy pending publication is invalid"))
		}
		if _, err := tx.ExecContext(ctx, `ALTER TABLE pending_publications RENAME TO old_pending_publications;
			`+_clientV23PendingSQL+`;
			INSERT INTO pending_publications(worktree,publication_kind,base_commit,base_root,expected_head,expected_etag,
				candidate_commit,candidate_root,candidate_data,captured_commit,captured_root,captured_data,candidate_history,
				deletion_count,tracked_count,requires_delete_confirmation,delete_confirmed,legacy_revalidation_required)
			SELECT worktree,'sync',base_commit,base_root,expected_head,expected_etag,candidate_commit,candidate_root,candidate_data,
				captured_commit,captured_root,captured_data,candidate_history,deletion_count,tracked_count,
				requires_delete_confirmation,delete_confirmed,legacy_revalidation_required FROM old_pending_publications;
			DROP TABLE old_pending_publications`); err != nil {
			return fail(fmt.Errorf("migrate pending publication kind: %w", err))
		}
	} else if publicationKindColumns != 1 {
		return fail(errors.New("pending publication kind schema is incomplete"))
	}
	var restorePublicationColumns int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('pending_publications')
		WHERE name IN ('source_commit','source_path','source_root','created_count','updated_count',
		'type_replacement_count','removed_descendant_count','preserved_current_only_count',
		'changed_path_preview','changed_path_count','preview_truncated','restore_confirmed')`).Scan(&restorePublicationColumns); err != nil {
		return fail(err)
	}
	if restorePublicationColumns == 0 {
		if err := _validateClientV23Schema(ctx, tx); err != nil {
			return fail(fmt.Errorf("validate v23 pending publication schema before restore migration: %w", err))
		}
		if _, err := tx.ExecContext(ctx, `ALTER TABLE pending_publications RENAME TO old_pending_publications;
			`+_clientV24PendingSQL+`;
			INSERT INTO pending_publications(worktree,publication_kind,base_commit,base_root,expected_head,expected_etag,
				candidate_commit,candidate_root,candidate_data,captured_commit,captured_root,captured_data,candidate_history,
				deletion_count,tracked_count,requires_delete_confirmation,delete_confirmed,legacy_revalidation_required,
				source_commit,source_path,source_root,created_count,updated_count,type_replacement_count,
				removed_descendant_count,preserved_current_only_count,changed_path_preview,changed_path_count,
				preview_truncated,restore_confirmed)
			SELECT worktree,publication_kind,base_commit,base_root,expected_head,expected_etag,candidate_commit,candidate_root,
				candidate_data,captured_commit,captured_root,captured_data,candidate_history,deletion_count,tracked_count,
				requires_delete_confirmation,delete_confirmed,legacy_revalidation_required,'','','',0,0,0,0,0,
				X'4652503100000000',0,0,0 FROM old_pending_publications;
			DROP TABLE old_pending_publications`); err != nil {
			return fail(fmt.Errorf("migrate pending publication restore fields: %w", err))
		}
	} else if restorePublicationColumns != 12 {
		return fail(errors.New("pending publication restore schema is incomplete"))
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO client_schema_migrations(version) VALUES (13);
		INSERT OR IGNORE INTO client_schema_migrations(version) VALUES (14);
		INSERT OR IGNORE INTO client_schema_migrations(version) VALUES (15);
		INSERT OR IGNORE INTO client_schema_migrations(version) VALUES (16);
		INSERT OR IGNORE INTO client_schema_migrations(version) VALUES (17);
		INSERT OR IGNORE INTO client_schema_migrations(version) VALUES (18);
		INSERT OR IGNORE INTO client_schema_migrations(version) VALUES (19);
		INSERT OR IGNORE INTO client_schema_migrations(version) VALUES (20);
		INSERT OR IGNORE INTO client_schema_migrations(version) VALUES (21);
		INSERT OR IGNORE INTO client_schema_migrations(version) VALUES (22);
		INSERT OR IGNORE INTO client_schema_migrations(version) VALUES (23);
		INSERT OR IGNORE INTO client_schema_migrations(version) VALUES (24)`); err != nil {
		return fail(err)
	}
	if err := _validateClientV24Schema(ctx, tx); err != nil {
		return fail(fmt.Errorf("validate migrated client schema: %w", err))
	}
	if err := _validateClientV22CheckoutSchema(ctx, tx); err != nil {
		return fail(fmt.Errorf("validate migrated checkout provenance schema: %w", err))
	}
	if err := _validateClientSyncRecoverySchema(ctx, tx, 22, _clientV22SyncRecoverySQL, _clientV22SyncRecoveryColumns); err != nil {
		return fail(fmt.Errorf("validate migrated sync recovery linkage schema: %w", err))
	}
	if err := _validateClientSyncRecoveryPromotionSchema(ctx, tx); err != nil {
		return fail(fmt.Errorf("validate migrated sync recovery promotion schema: %w", err))
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

func finalizeBinding(ctx context.Context, db *sql.DB, binding clientBinding, accessToken []byte, paths []checkoutPath) error {
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
	for _, path := range paths {
		if _, err := tx.ExecContext(ctx, `INSERT INTO path_index(worktree, path, type, object_id, canonical_mtime, actual_mtime, size)
			VALUES (?, ?, ?, ?, ?, ?, ?)`, binding.Worktree, path.path, path.kind, path.id, path.mtime, path.mtime, path.size); err != nil {
			return errors.Join(fmt.Errorf("save imported path index: %w", err), tx.Rollback())
		}
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
	target := base.JoinPath("v1/libraries", libraryID, "head").String()
	for attempt := 0; attempt < _headUpdateAttempts; attempt++ {
		request, err := authenticatedRequest(ctx, http.MethodPut, target, token, body)
		if err != nil {
			return remoteHead{}, false, err
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("If-Match", etag)
		status, data, headers, err := doClientRequestWithHeaders(request)
		if err != nil {
			return remoteHead{}, false, fmt.Errorf("publish library Head: %w", err)
		}
		if status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable {
			if attempt+1 == _headUpdateAttempts {
				return remoteHead{}, false, fmt.Errorf("publish library Head failed after %d attempts: server returned %s",
					_headUpdateAttempts, http.StatusText(status))
			}
			if err := waitTransientRetry(ctx, headers.Get("Retry-After"), time.Now()); err != nil {
				return remoteHead{}, false, fmt.Errorf("wait to retry library Head: %w", err)
			}
			continue
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
	return remoteHead{}, false, errors.New("publish library Head exhausted retry attempts")
}

func waitTransientRetry(ctx context.Context, retryAfter string, now time.Time) error {
	timer := time.NewTimer(transientRetryDelay(retryAfter, now))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func transientRetryDelay(retryAfter string, now time.Time) time.Duration {
	delay := _defaultTransientRetryDelay
	if seconds, err := strconv.ParseUint(strings.TrimSpace(retryAfter), 10, 31); err == nil {
		delay = time.Duration(seconds) * time.Second
	} else if retryAt, err := http.ParseTime(retryAfter); err == nil {
		delay = max(time.Duration(0), retryAt.Sub(now))
	}
	return min(delay, _maximumTransientRetryDelay)
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
	status, data, headers, err := doClientRequestWithHeaders(request)
	return status, data, headers.Get("ETag"), err
}

func doClientRequestWithHeaders(request *http.Request) (int, []byte, http.Header, error) {
	response, err := noRedirectClient().Do(request)
	if err != nil {
		return 0, nil, nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(response.Body, 33<<20))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		return 0, nil, nil, errors.Join(readErr, closeErr)
	}
	return response.StatusCode, data, response.Header.Clone(), nil
}

func newFlagSet(name string, output io.Writer) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(output)
	return flags
}

func mountedFilesystem(source io.Reader, path string, major, minor uint32) (string, error) {
	longestMount := ""
	filesystem := ""
	scanner := bufio.NewScanner(source)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 7 {
			continue
		}
		deviceMajor, deviceMinor, ok := strings.Cut(fields[2], ":")
		if !ok {
			continue
		}
		entryMajor, majorErr := strconv.ParseUint(deviceMajor, 10, 32)
		entryMinor, minorErr := strconv.ParseUint(deviceMinor, 10, 32)
		if majorErr != nil || minorErr != nil || uint32(entryMajor) != major || uint32(entryMinor) != minor {
			continue
		}
		separator := -1
		for index := 6; index < len(fields); index++ {
			if fields[index] == "-" {
				separator = index
				break
			}
		}
		if separator < 0 || separator+1 >= len(fields) {
			continue
		}
		mount, err := unescapeMountinfoPath(fields[4])
		if err != nil {
			return "", err
		}
		if path != mount && !strings.HasPrefix(path, strings.TrimSuffix(mount, "/")+"/") {
			continue
		}
		if len(mount) > len(longestMount) {
			longestMount = mount
			filesystem = fields[separator+1]
		}
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("read mountinfo: %w", err)
	}
	if filesystem == "" {
		return "", errors.New("worktree mount is absent from mountinfo")
	}
	return filesystem, nil
}

func unescapeMountinfoPath(value string) (string, error) {
	var result strings.Builder
	result.Grow(len(value))
	for index := 0; index < len(value); index++ {
		if value[index] != '\\' {
			result.WriteByte(value[index])
			continue
		}
		if index+3 >= len(value) {
			return "", errors.New("mountinfo path has a truncated escape")
		}
		escape := value[index+1 : index+4]
		if escape != "040" && escape != "011" && escape != "012" && escape != "134" {
			return "", errors.New("mountinfo path has an invalid escape")
		}
		escaped, err := strconv.ParseUint(escape, 8, 8)
		if err != nil {
			return "", fmt.Errorf("parse mountinfo path escape: %w", err)
		}
		result.WriteByte(byte(escaped))
		index += 3
	}
	return result.String(), nil
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
	fd, err := fscompat.Open(canonical, fscompat.O_RDONLY|fscompat.O_DIRECTORY|fscompat.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open worktree: %w", err)
	}
	root := &openedWorktree{path: canonical, directory: os.NewFile(uintptr(fd), canonical)}
	var stat fscompat.Stat_t
	if err := fscompat.Fstat(fd, &stat); err != nil {
		return nil, errors.Join(fmt.Errorf("inspect opened worktree: %w", err), root.Close())
	}
	root.device, root.inode = uint64(stat.Dev), stat.Ino
	if err := checkFilesystem(root.directory); err != nil {
		return nil, errors.Join(err, root.Close())
	}
	return root, nil
}

func (root *openedWorktree) validateIdentity() error {
	current := filepath.VolumeName(root.path) + string(filepath.Separator)
	for _, component := range strings.Split(strings.TrimPrefix(root.path, current), string(filepath.Separator)) {
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
	var pathStat, openedStat fscompat.Stat_t
	if err := fscompat.Lstat(root.path, &pathStat); err != nil {
		return fmt.Errorf("inspect worktree: %w", err)
	}
	if pathStat.Mode&fscompat.S_IFMT != fscompat.S_IFDIR || uint64(pathStat.Dev) != root.device || pathStat.Ino != root.inode {
		return errors.New("worktree identity changed during bind")
	}
	if err := fscompat.Fstat(int(root.directory.Fd()), &openedStat); err != nil {
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
	_, err := readDirectoryNames(root.directory, 1)
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

func checkStateDirFilesystem(path string, check func(*os.File) error) error {
	if check == nil {
		return nil
	}
	probe := path
	if _, err := os.Lstat(probe); errors.Is(err, os.ErrNotExist) {
		probe = filepath.Dir(probe)
	} else if err != nil {
		return fmt.Errorf("inspect client filesystem path: %w", err)
	}
	fd, err := fscompat.Open(probe, fscompat.O_RDONLY|fscompat.O_DIRECTORY|fscompat.O_NOFOLLOW|fscompat.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open client filesystem path: %w", err)
	}
	directory := os.NewFile(uintptr(fd), probe)
	if err := check(directory); err != nil {
		return errors.Join(fmt.Errorf("check client filesystem: %w", err), directory.Close())
	}
	if err := directory.Close(); err != nil {
		return fmt.Errorf("close client filesystem path: %w", err)
	}
	return nil
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
	u := &url.URL{Scheme: "file", Path: sqliteClientURLPath(path)}
	query := u.Query()
	query.Set("_busy_timeout", "5000")
	query.Set("_synchronous", "full")
	query.Add("_pragma", "busy_timeout(5000)")
	query.Add("_pragma", "synchronous(FULL)")
	query.Add("_pragma", "secure_delete(ON)")
	query.Add("_pragma", "foreign_keys(ON)")
	if readOnly {
		query.Set("mode", "ro")
	} else {
		query.Set("_txlock", "immediate")
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

func sqliteClientURLPath(path string) string {
	value := filepath.ToSlash(path)
	if filepath.VolumeName(path) != "" && !strings.HasPrefix(value, "/") {
		return "/" + value
	}
	return value
}

type sqliteErrorCoder interface{ Code() int }

func enableClientDBWAL(ctx context.Context, db *sql.DB) error {
	const attempts = 100
	for attempt := 0; attempt < attempts; attempt++ {
		var mode string
		err := db.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&mode)
		if err == nil {
			if strings.ToLower(mode) != "wal" {
				return fmt.Errorf("enable client WAL: journal_mode=%s", mode)
			}
			return nil
		}
		var sqliteErr sqliteErrorCoder
		if !errors.As(err, &sqliteErr) || (sqliteErr.Code()&0xff != 5 && sqliteErr.Code()&0xff != 6) {
			return fmt.Errorf("enable client WAL: %w", err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("enable client WAL: %w", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	return errors.New("enable client WAL: database remained busy")
}

func assertClientDBDurability(ctx context.Context, db *sql.DB) error {
	var journal string
	var synchronous int
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal); err != nil {
		return fmt.Errorf("read client journal mode: %w", err)
	}
	if err := db.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous); err != nil {
		return fmt.Errorf("read client synchronous mode: %w", err)
	}
	if strings.ToLower(journal) != "wal" || synchronous != 2 {
		return fmt.Errorf("client database requires WAL/FULL durability, got journal_mode=%s synchronous=%d", journal, synchronous)
	}
	return nil
}

func syncFile(path string) error {
	fd, err := fscompat.Open(path, fscompat.O_RDWR|fscompat.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("open client state for sync: %w", err)
	}
	return errors.Join(fscompat.SyncFile(fd), fscompat.Close(fd))
}

func syncDirectory(path string) error {
	fd, err := fscompat.Open(path, fscompat.O_RDONLY|fscompat.O_DIRECTORY|fscompat.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("open client state parent: %w", err)
	}
	return errors.Join(fscompat.SyncDirectory(fd), fscompat.Close(fd))
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
