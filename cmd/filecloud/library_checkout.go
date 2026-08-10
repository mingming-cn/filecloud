package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/mingming-cn/filecloud/internal/object"
	"golang.org/x/sys/unix"
)

const (
	checkoutTempPrefix     = ".filecloud-internal-checkout-"
	checkoutTempRandomSize = 16
)

type checkoutPath struct {
	path, kind, id, mtime string
	size                  int64
	device, inode         uint64
}

func resumableBindExists(ctx context.Context, clientDir, worktree string) (bool, error) {
	path := filepath.Join(clientDir, _clientDatabaseName)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	db, err := openClientDB(path, true)
	if err != nil {
		return false, err
	}
	defer db.Close()
	var count int
	if err := db.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM pending_checkouts WHERE worktree = ?) +
		(SELECT COUNT(*) FROM bindings WHERE worktree = ?)`, worktree, worktree).Scan(&count); err != nil {
		return false, err
	}
	return count != 0, nil
}

func loadPendingCheckout(ctx context.Context, db *sql.DB, serverURL, libraryID, worktree string) (*pendingCheckout, error) {
	var value pendingCheckout
	err := db.QueryRowContext(ctx, `SELECT server_url, library_id, worktree, user_id, device_id,
		target_commit, target_root, head_etag, apply_state FROM pending_checkouts
		WHERE worktree = ? OR (server_url = ? AND library_id = ?) LIMIT 1`, worktree, serverURL, libraryID).Scan(
		&value.ServerURL, &value.LibraryID, &value.Worktree, &value.UserID, &value.DeviceID,
		&value.TargetCommit, &value.TargetRoot, &value.HeadETag, &value.ApplyState)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read pending checkout: %w", err)
	}
	if value.ServerURL != serverURL || value.LibraryID != libraryID || value.Worktree != worktree {
		return nil, errors.New("pending checkout conflicts with this worktree or library")
	}
	return &value, nil
}

func savePendingCheckout(ctx context.Context, db *sql.DB, value pendingCheckout) error {
	state := value.ApplyState
	if state == "" {
		state = "pending"
	}
	_, err := db.ExecContext(ctx, `INSERT INTO pending_checkouts(server_url, library_id, worktree, user_id, device_id,
		target_commit, target_root, head_etag, apply_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, value.ServerURL,
		value.LibraryID, value.Worktree, value.UserID, value.DeviceID, value.TargetCommit, value.TargetRoot, value.HeadETag, state)
	if err != nil {
		return fmt.Errorf("save pending checkout: %w", err)
	}
	return nil
}

func runInitialCheckout(ctx context.Context, db *sql.DB, options bindOptions, owner string, pending *pendingCheckout, stdout io.Writer, config libraryClientConfig) error {
	if pending.ServerURL != options.serverURL || pending.LibraryID != options.libraryID || pending.Worktree != options.worktree ||
		pending.UserID != owner || pending.DeviceID != options.deviceID {
		return errors.New("pending checkout uses different bind parameters")
	}
	cacheRoot, err := openVerifiedCacheRoot(options.clientDir)
	if err != nil {
		return err
	}
	defer cacheRoot.Close()
	options.cacheRoot = cacheRoot
	commit, err := downloadTargetCommit(ctx, options, pending.TargetCommit, owner)
	if err != nil {
		return err
	}
	if pending.TargetRoot == "" {
		pending.TargetRoot = commit.Root
		if _, err := db.ExecContext(ctx, "UPDATE pending_checkouts SET target_root = ? WHERE worktree = ?",
			pending.TargetRoot, options.worktree); err != nil {
			return fmt.Errorf("update pending checkout root: %w", err)
		}
	} else if pending.TargetRoot != commit.Root {
		return errors.New("pending checkout target commit changed root")
	}
	paths, err := downloadCheckoutTree(ctx, options, pending.TargetRoot)
	if err != nil {
		return err
	}
	if err := saveCheckoutPaths(ctx, db, options.worktree, paths); err != nil {
		return err
	}
	if config.beforeCheckoutMaterialize != nil {
		if err := config.beforeCheckoutMaterialize(); err != nil {
			return fmt.Errorf("prepare checkout materialization: %w", err)
		}
	}
	if err := materializeCheckout(ctx, db, options, paths, config); err != nil {
		return err
	}
	if _, err := rescanRoot(options, pending.TargetRoot); err != nil {
		return fmt.Errorf("verify checked out tree: %w", err)
	}
	if config.beforeFinalize != nil {
		if err := config.beforeFinalize(); err != nil {
			return fmt.Errorf("finalize checkout: %w", err)
		}
	}
	if _, err := rescanRoot(options, pending.TargetRoot); err != nil {
		return fmt.Errorf("verify checked out tree: %w", err)
	}
	if err := finalizeCheckout(ctx, db, options, *pending, config); err != nil {
		return err
	}
	latest, err := getRemoteHead(ctx, options.base, options.libraryID, options.token)
	if err != nil {
		return fmt.Errorf("verify library Head after checkout: %w", err)
	}
	if latest.CommitID == nil || *latest.CommitID != pending.TargetCommit {
		return errors.New("library Head advanced during checkout; rerun sync")
	}
	_, err = fmt.Fprintf(stdout, "library bound: %s\n", options.worktree)
	return err
}

func downloadTargetCommit(ctx context.Context, options bindOptions, target, owner string) (object.Commit, error) {
	data, err := cachedRemoteObject(ctx, options, "commits", target)
	if err != nil {
		return object.Commit{}, fmt.Errorf("download target commit %s: %w", target, err)
	}
	commit, err := object.VerifyCommit(data, target)
	if err != nil || commit.AuthorUserID != owner {
		return object.Commit{}, errors.New("remote target commit is not canonical for the library owner")
	}
	return commit, nil
}

func downloadCheckoutTree(ctx context.Context, options bindOptions, root string) ([]checkoutPath, error) {
	return deriveRemotePaths(ctx, options, root, true)
}

func deriveRemotePaths(ctx context.Context, options bindOptions, root string, downloadBlocks bool) ([]checkoutPath, error) {
	paths := make([]checkoutPath, 0)
	var walk func(string, string, int) error
	walk = func(id, prefix string, depth int) error {
		if depth > 256 {
			return errors.New("remote directory tree exceeds checkout depth limit")
		}
		data, err := cachedRemoteObject(ctx, options, "directories", id)
		if err != nil {
			return err
		}
		directory, err := object.VerifyDirectory(data, id)
		if err != nil {
			return errors.New("remote directory is not valid canonical content")
		}
		for _, entry := range directory.Entries {
			path := entry.Name
			if prefix != "" {
				path = prefix + "/" + entry.Name
			}
			if len(path) > 1024 {
				return errors.New("remote path exceeds checkout limit")
			}
			value := checkoutPath{path: path, kind: entry.Type, id: entry.ID, mtime: entry.ModifiedAt}
			if entry.Type == "Directory" {
				paths = append(paths, value)
				if err := walk(entry.ID, path, depth+1); err != nil {
					return err
				}
				continue
			}
			fileData, err := cachedRemoteObject(ctx, options, "files", entry.ID)
			if err != nil {
				return err
			}
			file, err := object.VerifyFile(fileData, entry.ID)
			if err != nil {
				return errors.New("remote file is not valid canonical content")
			}
			value.size = file.Size
			if downloadBlocks {
				for _, block := range file.Blocks {
					if _, err := cachedRemoteBlock(ctx, options, block); err != nil {
						return err
					}
				}
			}
			paths = append(paths, value)
		}
		return nil
	}
	if err := walk(root, "", 0); err != nil {
		return nil, fmt.Errorf("download checkout tree: %w", err)
	}
	return paths, nil
}

func loadRemoteTrackedPaths(ctx context.Context, options bindOptions, binding clientBinding) (map[string]bool, error) {
	cacheRoot, err := openVerifiedCacheRoot(options.clientDir)
	if err != nil {
		return nil, err
	}
	defer cacheRoot.Close()
	options.cacheRoot = cacheRoot
	commit, err := downloadTargetCommit(ctx, options, binding.SyncBase, binding.UserID)
	if err != nil {
		return nil, fmt.Errorf("load legacy tracked paths: %w", err)
	}
	if commit.Root != binding.SyncBaseRoot {
		return nil, errors.New("legacy binding Sync Base commit has a different root")
	}
	tracked := make(map[string]bool)
	var walk func(string, string, int) error
	walk = func(id, prefix string, depth int) error {
		if depth > 256 {
			return errors.New("Sync Base directory tree exceeds depth limit")
		}
		data, err := cachedRemoteObject(ctx, options, "directories", id)
		if err != nil {
			return err
		}
		directory, err := object.VerifyDirectory(data, id)
		if err != nil {
			return errors.New("Sync Base directory is not valid canonical content")
		}
		for _, entry := range directory.Entries {
			path := entry.Name
			if prefix != "" {
				path = prefix + "/" + entry.Name
			}
			if len(path) > 1024 {
				return errors.New("Sync Base path exceeds limit")
			}
			tracked[path] = true
			if entry.Type == "Directory" {
				if err := walk(entry.ID, path, depth+1); err != nil {
					return err
				}
				continue
			}
			fileData, err := cachedRemoteObject(ctx, options, "files", entry.ID)
			if err != nil {
				return err
			}
			if _, err := object.VerifyFile(fileData, entry.ID); err != nil {
				return errors.New("Sync Base file is not valid canonical content")
			}
		}
		return nil
	}
	if err := walk(commit.Root, "", 0); err != nil {
		return nil, fmt.Errorf("load legacy tracked paths: %w", err)
	}
	return tracked, nil
}

func cachedRemoteObject(ctx context.Context, options bindOptions, kind, id string) ([]byte, error) {
	validate := func(data []byte) error {
		var err error
		switch kind {
		case "commits":
			_, err = object.VerifyCommit(data, id)
		case "directories":
			_, err = object.VerifyDirectory(data, id)
		case "files":
			_, err = object.VerifyFile(data, id)
		default:
			err = errors.New("invalid cached object kind")
		}
		return err
	}
	return cachedDownload(ctx, options, kind, id, func() ([]byte, error) {
		request, err := authenticatedRequest(ctx, http.MethodGet, options.base.JoinPath("v1/libraries", options.libraryID, "objects", kind, id).String(), options.token, nil)
		if err != nil {
			return nil, err
		}
		status, data, _, err := doClientRequest(request)
		if err != nil {
			return nil, err
		}
		if status != http.StatusOK {
			return nil, fmt.Errorf("server returned %s", http.StatusText(status))
		}
		return data, nil
	}, validate)
}

func cachedRemoteBlock(ctx context.Context, options bindOptions, id string) ([]byte, error) {
	return cachedDownload(ctx, options, "blocks", id, func() ([]byte, error) {
		request, err := authenticatedRequest(ctx, http.MethodGet, options.base.JoinPath("v1/libraries", options.libraryID, "blocks", id).String(), options.token, nil)
		if err != nil {
			return nil, err
		}
		status, data, _, err := doClientRequest(request)
		if err != nil {
			return nil, err
		}
		if status != http.StatusOK {
			return nil, fmt.Errorf("server returned %s", http.StatusText(status))
		}
		return data, nil
	}, func(data []byte) error {
		if len(data) == 0 || len(data) > object.MaxBlockSize || object.ID(data) != id {
			return errors.New("block digest or size mismatch")
		}
		return nil
	})
}

func cachedDownload(ctx context.Context, options bindOptions, kind, id string, fetch func() ([]byte, error), validate func([]byte) error) ([]byte, error) {
	if !object.ValidID(id) || (kind != "blocks" && kind != "files" && kind != "directories" && kind != "commits") {
		return nil, errors.New("invalid remote cache object")
	}
	directory, err := openCacheDirectory(options.cacheRoot, kind, id[:2])
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	name := id[2:]
	if data, found, err := readCacheFile(directory, name); err != nil {
		return nil, err
	} else if found {
		if validate(data) != nil {
			return nil, errors.New("cached object failed verification")
		}
		return data, nil
	}
	data, err := fetch()
	if err != nil {
		return nil, err
	}
	if err := validate(data); err != nil {
		return nil, errors.New("downloaded object failed verification")
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, fmt.Errorf("name cached object temporary file: %w", err)
	}
	tempName := ".download-" + hex.EncodeToString(random[:])
	fd, err := unix.Openat(int(directory.Fd()), tempName, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create cached object: %w", err)
	}
	temp := os.NewFile(uintptr(fd), tempName)
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = unix.Unlinkat(int(directory.Fd()), tempName, 0)
		}
	}()
	if _, err := temp.Write(data); err != nil {
		return nil, errors.Join(err, temp.Close())
	}
	if err := temp.Sync(); err != nil {
		return nil, errors.Join(err, temp.Close())
	}
	if err := temp.Close(); err != nil {
		return nil, err
	}
	if err := unix.Renameat2(int(directory.Fd()), tempName, int(directory.Fd()), name, unix.RENAME_NOREPLACE); err != nil {
		if !errors.Is(err, syscall.EEXIST) {
			return nil, fmt.Errorf("publish cached object: %w", err)
		}
		existing, found, readErr := readCacheFile(directory, name)
		if readErr != nil || !found || validate(existing) != nil {
			return nil, errors.New("existing cached object failed verification")
		}
		if err := unix.Unlinkat(int(directory.Fd()), tempName, 0); err != nil {
			return nil, fmt.Errorf("remove duplicate cached object temporary file: %w", err)
		}
		removeTemp = false
	} else {
		removeTemp = false
	}
	if err := directory.Sync(); err != nil {
		return nil, fmt.Errorf("sync object cache: %w", err)
	}
	return data, nil
}

func openVerifiedCacheRoot(clientDir string) (*os.File, error) {
	canonical, err := canonicalExistingPath(clientDir)
	if err != nil || canonical != clientDir {
		return nil, errors.New("client cache root changed identity")
	}
	fd, err := unix.Open(clientDir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open client cache root: %w", err)
	}
	var opened, path unix.Stat_t
	if err := unix.Fstat(fd, &opened); err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("inspect client cache root: %w", err)
	}
	if err := unix.Lstat(clientDir, &path); err != nil || opened.Dev != path.Dev || opened.Ino != path.Ino || path.Mode&syscall.S_IFMT != syscall.S_IFDIR {
		unix.Close(fd)
		return nil, errors.New("client cache root changed identity")
	}
	return os.NewFile(uintptr(fd), clientDir), nil
}

func openCacheDirectory(root *os.File, kind, prefix string) (*os.File, error) {
	if root == nil {
		return nil, errors.New("client cache root is not open")
	}
	rootFD, err := unix.Dup(int(root.Fd()))
	if err != nil {
		return nil, fmt.Errorf("duplicate client cache root: %w", err)
	}
	current := os.NewFile(uintptr(rootFD), root.Name())
	for _, name := range []string{"objects", kind, prefix} {
		if err := unix.Mkdirat(int(current.Fd()), name, 0o700); err != nil && !errors.Is(err, syscall.EEXIST) {
			current.Close()
			return nil, fmt.Errorf("create object cache directory: %w", err)
		}
		nextFD, err := unix.Openat(int(current.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			current.Close()
			return nil, fmt.Errorf("open object cache directory without following links: %w", err)
		}
		if err := unix.Fchmod(nextFD, 0o700); err != nil {
			unix.Close(nextFD)
			current.Close()
			return nil, fmt.Errorf("secure object cache directory: %w", err)
		}
		if err := current.Sync(); err != nil {
			unix.Close(nextFD)
			current.Close()
			return nil, fmt.Errorf("sync object cache parent: %w", err)
		}
		current.Close()
		current = os.NewFile(uintptr(nextFD), name)
	}
	return current, nil
}

func readCacheFile(directory *os.File, name string) ([]byte, bool, error) {
	fd, err := unix.Openat(int(directory.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if errors.Is(err, syscall.ENOENT) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("open cached object without following links: %w", err)
	}
	file := os.NewFile(uintptr(fd), name)
	data, readErr := io.ReadAll(io.LimitReader(file, 33<<20))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return nil, false, errors.Join(readErr, closeErr)
	}
	return data, true, nil
}

func saveCheckoutPaths(ctx context.Context, db *sql.DB, worktree string, paths []checkoutPath) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin checkout path intents: %w", err)
	}
	for _, value := range paths {
		if _, err := tx.ExecContext(ctx, `INSERT INTO checkout_paths(worktree, path, type, object_id, canonical_mtime, size)
			VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(worktree, path) DO NOTHING`, worktree, value.path, value.kind, value.id, value.mtime, value.size); err != nil {
			return errors.Join(fmt.Errorf("save checkout path intent: %w", err), tx.Rollback())
		}
		var kind, id, mtime string
		var size int64
		if err := tx.QueryRowContext(ctx, `SELECT type, object_id, canonical_mtime, size FROM checkout_paths
			WHERE worktree = ? AND path = ?`, worktree, value.path).Scan(&kind, &id, &mtime, &size); err != nil {
			return errors.Join(fmt.Errorf("verify checkout path intent: %w", err), tx.Rollback())
		}
		if kind != value.kind || id != value.id || mtime != value.mtime || size != value.size {
			return errors.Join(errors.New("pending checkout path intent does not match fixed target"), tx.Rollback())
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit checkout path intents: %w", err)
	}
	return nil
}

func materializeCheckout(ctx context.Context, db *sql.DB, options bindOptions, paths []checkoutPath, config libraryClientConfig) error {
	for _, value := range paths {
		if value.kind == "Directory" {
			if err := installCheckoutDirectory(ctx, db, options, value, config); err != nil {
				return err
			}
		}
	}
	for _, value := range paths {
		if value.kind == "File" {
			if err := installCheckoutFile(ctx, db, options, value, config); err != nil {
				return err
			}
		}
	}
	for index := len(paths) - 1; index >= 0; index-- {
		if paths[index].kind == "Directory" {
			if err := setCheckoutMtime(ctx, db, options, paths[index], config); err != nil {
				return err
			}
			if err := recordCheckoutCompleted(ctx, db, options.worktree, paths[index]); err != nil {
				return err
			}
		}
	}
	return options.worktreeRoot.validateIdentity()
}

func installCheckoutDirectory(ctx context.Context, db *sql.DB, options bindOptions, value checkoutPath, config libraryClientConfig) error {
	completed, targetDevice, targetInode, err := checkoutDirectoryState(ctx, db, options.worktree, value.path)
	if err != nil {
		return err
	}
	tempName, tempDevice, tempInode, _, err := checkoutTempRecord(ctx, db, options.worktree, value.path)
	if err != nil {
		return err
	}
	if tempName == "" {
		tempName, err = newCheckoutTempName()
		if err != nil {
			return err
		}
		if err := registerCheckoutTemp(ctx, db, options.worktree, value, tempName); err != nil {
			return err
		}
	}
	parent, targetName, err := openCheckoutParent(options.worktreeRoot, value.path, checkoutParentVerifier(ctx, db, options.worktree))
	if err != nil {
		return err
	}
	defer parent.Close()

	targetFD, targetErr := syscall.Openat(int(parent.Fd()), targetName, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if targetErr == nil {
		target := os.NewFile(uintptr(targetFD), value.path)
		defer target.Close()
		var stat unix.Stat_t
		if err := unix.Fstat(targetFD, &stat); err != nil {
			return fmt.Errorf("inspect checkout target directory %q: %w", value.path, err)
		}
		matchesTarget := targetDevice != 0 && targetInode != 0 && uint64(stat.Dev) == targetDevice && stat.Ino == targetInode
		matchesTemp := tempDevice != 0 && tempInode != 0 && uint64(stat.Dev) == tempDevice && stat.Ino == tempInode
		if !matchesTarget && !matchesTemp {
			return fmt.Errorf("pending checkout target directory %q has no matching registered identity", value.path)
		}
		if completed && matchesTarget {
			return nil
		}
		if err := parent.Sync(); err != nil {
			return fmt.Errorf("sync installed checkout directory parent: %w", err)
		}
		return recordCheckoutDirectoryInstalled(ctx, db, options.worktree, value, stat, config)
	}
	if !errors.Is(targetErr, syscall.ENOENT) {
		return fmt.Errorf("open checkout target directory %q without following links: %w", value.path, targetErr)
	}

	parentPath, _ := splitFSActionPath(value.path)
	needsIdentity := tempDevice == 0 && tempInode == 0
	if needsIdentity {
		if err := journalCreate(ctx, db, options.worktreeRoot, options.worktree, value.path, parentPath, tempName, "Directory", config.fsActionFault); err != nil {
			return fmt.Errorf("create checkout temporary directory: %w", err)
		}
		var found bool
		tempDevice, tempInode, found, err = completedFSCreateIdentity(ctx, db, options.worktree, value.path)
		if err != nil || !found {
			return errors.Join(errors.New("created checkout directory has no durable identity"), err)
		}
	}
	tempFD, err := syscall.Openat(int(parent.Fd()), tempName, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open checkout temporary directory without following links: %w", err)
	}
	temp := os.NewFile(uintptr(tempFD), tempName)
	defer temp.Close()
	var opened unix.Stat_t
	if err := unix.Fstat(tempFD, &opened); err != nil {
		return fmt.Errorf("inspect checkout temporary directory: %w", err)
	}
	if tempDevice == 0 || tempInode == 0 || uint64(opened.Dev) != tempDevice || opened.Ino != tempInode {
		return fmt.Errorf("registered checkout temporary directory %q identity changed", tempName)
	}
	if needsIdentity {
		if identityErr := recordCheckoutDirectoryTempIdentity(ctx, db, options.worktree, value.path, tempName, opened, config); identityErr != nil {
			return fmt.Errorf("record checkout temporary directory identity: %w", identityErr)
		}
	}
	if config.beforeCheckoutDirectoryRename != nil {
		if err := config.beforeCheckoutDirectoryRename(value.path, tempName); err != nil {
			return fmt.Errorf("prepare checkout directory rename: %w", err)
		}
	}
	if err := verifyDirectoryPathIdentity(parent, tempName, opened); err != nil {
		return err
	}
	if err := journalRename(ctx, db, options.worktreeRoot, options.worktree, fsPhasePreBase, parentPath,
		tempName, targetName, "Directory", tempName, "", uint64(opened.Dev), opened.Ino, config.fsActionFault); err != nil {
		return fmt.Errorf("install checkout directory %q: %w", value.path, err)
	}
	if err := verifyDirectoryPathIdentity(parent, targetName, opened); err != nil {
		return err
	}
	return recordCheckoutDirectoryInstalled(ctx, db, options.worktree, value, opened, config)
}

func installCheckoutFile(ctx context.Context, db *sql.DB, options bindOptions, value checkoutPath, config libraryClientConfig) error {
	completed, err := checkoutPathCompleted(ctx, db, options.worktree, value.path)
	if err != nil {
		return err
	}
	tempName, tempDevice, tempInode, registered, err := checkoutTempRecord(ctx, db, options.worktree, value.path)
	if err != nil {
		return err
	}
	parent, name, err := openCheckoutParent(options.worktreeRoot, value.path, checkoutParentVerifier(ctx, db, options.worktree))
	if err != nil {
		return err
	}
	defer parent.Close()
	var stat unix.Stat_t
	if err := unix.Fstatat(int(parent.Fd()), name, &stat, unix.AT_SYMLINK_NOFOLLOW); err == nil {
		if tempDevice == 0 || tempInode == 0 {
			return fmt.Errorf("installed checkout file %q has no registered identity; pending checkout retained", value.path)
		}
		installed, err := verifyInstalledFile(ctx, db, options, value, parent, name, tempDevice, tempInode, config)
		if err != nil {
			if completed {
				return err
			}
			return fmt.Errorf("pending checkout target %q does not match: %w", value.path, err)
		}
		if completed {
			return installed.Close()
		}
		if config.afterCheckoutInstall != nil {
			if err := config.afterCheckoutInstall(value.path, value.kind); err != nil {
				return errors.Join(fmt.Errorf("record installed checkout file: %w", err), installed.Close())
			}
		}
		if err := verifyCheckoutFileIdentity(int(installed.Fd()), tempDevice, tempInode); err != nil {
			return errors.Join(fmt.Errorf("verify installed checkout file before completion: %w", err), installed.Close())
		}
		if err := verifyCheckoutFilePathIdentity(parent, name, tempDevice, tempInode); err != nil {
			return errors.Join(fmt.Errorf("verify installed checkout file path before completion: %w", err), installed.Close())
		}
		return errors.Join(recordCheckoutCompleted(ctx, db, options.worktree, value), installed.Close())
	} else if !errors.Is(err, syscall.ENOENT) {
		return err
	}
	if !registered || tempName == "" {
		tempName, err = newCheckoutTempName()
		if err != nil {
			return err
		}
		if err := registerCheckoutTemp(ctx, db, options.worktree, value, tempName); err != nil {
			return err
		}
	}
	parentPath, _ := splitFSActionPath(value.path)
	needsIdentity := tempDevice == 0 && tempInode == 0
	if needsIdentity {
		if err := journalCreate(ctx, db, options.worktreeRoot, options.worktree, value.path, parentPath, tempName, "File", config.fsActionFault); err != nil {
			return fmt.Errorf("create checkout temporary file: %w", err)
		}
		var found bool
		tempDevice, tempInode, found, err = completedFSCreateIdentity(ctx, db, options.worktree, value.path)
		if err != nil || !found {
			return errors.Join(errors.New("created checkout file has no durable identity"), err)
		}
	}
	fd, err := syscall.Openat(int(parent.Fd()), tempName, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0o600)
	created := err == nil
	var opened unix.Stat_t
	if errors.Is(err, syscall.EEXIST) {
		if tempDevice == 0 || tempInode == 0 {
			return errors.New("registered checkout temporary file has no identity; pending checkout retained")
		}
		var existing unix.Stat_t
		if statErr := unix.Fstatat(int(parent.Fd()), tempName, &existing, unix.AT_SYMLINK_NOFOLLOW); statErr != nil ||
			existing.Mode&syscall.S_IFMT != syscall.S_IFREG {
			return errors.New("registered checkout temporary file is not a regular file")
		}
		fd, err = syscall.Openat(int(parent.Fd()), tempName, syscall.O_WRONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
		if err == nil {
			err = unix.Fstat(fd, &opened)
		}
		if err == nil && (opened.Dev != existing.Dev || opened.Ino != existing.Ino || opened.Mode&syscall.S_IFMT != syscall.S_IFREG) {
			err = errors.New("registered checkout temporary file changed while opening")
		}
		if err == nil && (uint64(opened.Dev) != tempDevice || opened.Ino != tempInode) {
			err = errors.New("registered checkout temporary file identity changed")
		}
	} else if err == nil {
		err = unix.Fstat(fd, &opened)
	}
	if err != nil {
		if fd >= 0 {
			syscall.Close(fd)
		}
		return fmt.Errorf("open checkout temporary file: %w", err)
	}
	if created {
		tempDevice, tempInode = uint64(opened.Dev), opened.Ino
	}
	if err := verifyCheckoutFileIdentity(fd, tempDevice, tempInode); err != nil {
		return errors.Join(fmt.Errorf("verify checkout temporary file: %w", err), syscall.Close(fd))
	}
	if needsIdentity || created {
		identityErr := recordCheckoutTempIdentity(ctx, db, options.worktree, value.path, tempName, opened, config)
		if identityErr != nil {
			return errors.Join(fmt.Errorf("record checkout temporary identity: %w", identityErr), syscall.Close(fd))
		}
	}
	if config.beforeCheckoutFileWrite != nil {
		if err := config.beforeCheckoutFileWrite(value.path, tempName); err != nil {
			return errors.Join(fmt.Errorf("before checkout file write: %w", err), syscall.Close(fd))
		}
	}
	if err := verifyCheckoutFileIdentity(fd, tempDevice, tempInode); err != nil {
		return errors.Join(fmt.Errorf("verify checkout temporary file before write: %w", err), syscall.Close(fd))
	}
	if !created {
		if err := unix.Ftruncate(fd, 0); err != nil {
			syscall.Close(fd)
			return fmt.Errorf("truncate registered checkout temporary file: %w", err)
		}
	}
	temp := os.NewFile(uintptr(fd), tempName)
	fileData, err := cachedRemoteObject(ctx, options, "files", value.id)
	if err != nil {
		return errors.Join(err, temp.Close())
	}
	file, _ := object.VerifyFile(fileData, value.id)
	var written int64
	for _, blockID := range file.Blocks {
		block, err := cachedRemoteBlock(ctx, options, blockID)
		if err != nil {
			return errors.Join(err, temp.Close())
		}
		count, err := temp.Write(block)
		written += int64(count)
		if err != nil || count != len(block) {
			return errors.Join(errors.New("write checkout file failed"), err, temp.Close())
		}
	}
	if written != file.Size {
		return errors.Join(errors.New("checkout file size mismatch"), temp.Close())
	}
	if err := config.syncFile(temp); err != nil {
		return errors.Join(err, temp.Close())
	}
	if err := verifyCheckoutFileIdentity(int(temp.Fd()), tempDevice, tempInode); err != nil {
		return errors.Join(fmt.Errorf("verify checkout temporary file before rename: %w", err), temp.Close())
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := journalMtime(ctx, db, options.worktreeRoot, options.worktree, fsPhasePreBase, parentPath,
		tempName, "File", value.mtime, tempDevice, tempInode, config.fsActionFault); err != nil {
		return fmt.Errorf("set checkout file mtime %q: %w", value.path, err)
	}
	if config.beforeCheckoutFileRename != nil {
		if err := config.beforeCheckoutFileRename(value.path, tempName); err != nil {
			return fmt.Errorf("before checkout file rename: %w", err)
		}
	}
	if err := journalRename(ctx, db, options.worktreeRoot, options.worktree, fsPhasePreBase, parentPath,
		tempName, name, "File", tempName, "", tempDevice, tempInode, config.fsActionFault); err != nil {
		return fmt.Errorf("install checkout file %q: %w", value.path, err)
	}
	if config.afterCheckoutInstall != nil {
		if err := config.afterCheckoutInstall(value.path, value.kind); err != nil {
			return fmt.Errorf("record installed checkout file: %w", err)
		}
	}
	if err := verifyCheckoutFilePathIdentity(parent, name, tempDevice, tempInode); err != nil {
		return fmt.Errorf("verify installed checkout file before completion: %w", err)
	}
	return recordCheckoutCompleted(ctx, db, options.worktree, value)
}

type checkoutDirectoryVerifier func(string, unix.Stat_t) error

func openCheckoutParent(root *openedWorktree, path string, verify checkoutDirectoryVerifier) (*os.File, string, error) {
	components := strings.Split(path, "/")
	current, err := syscall.Dup(int(root.directory.Fd()))
	if err != nil {
		return nil, "", err
	}
	relative := ""
	for _, component := range components[:len(components)-1] {
		next, openErr := syscall.Openat(current, component, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
		syscall.Close(current)
		if openErr != nil {
			return nil, "", fmt.Errorf("open checkout parent for %q: %w", path, openErr)
		}
		current = next
		if relative == "" {
			relative = component
		} else {
			relative += "/" + component
		}
		if verify != nil {
			var stat unix.Stat_t
			if err := unix.Fstat(current, &stat); err != nil {
				syscall.Close(current)
				return nil, "", fmt.Errorf("inspect checkout parent %q: %w", relative, err)
			}
			if err := verify(relative, stat); err != nil {
				syscall.Close(current)
				return nil, "", err
			}
		}
	}
	return os.NewFile(uintptr(current), filepath.Dir(filepath.Join(root.path, filepath.FromSlash(path)))), components[len(components)-1], nil
}

func setCheckoutMtime(ctx context.Context, db *sql.DB, options bindOptions, value checkoutPath, config libraryClientConfig) error {
	verify := checkoutParentVerifier(ctx, db, options.worktree)
	parent, name, err := openCheckoutParent(options.worktreeRoot, value.path, verify)
	if err != nil {
		return err
	}
	defer parent.Close()
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open checkout directory for mtime %q: %w", value.path, err)
	}
	directory := os.NewFile(uintptr(fd), value.path)
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return errors.Join(err, directory.Close())
	}
	if err := verify(value.path, stat); err != nil {
		return errors.Join(err, directory.Close())
	}
	if err := directory.Close(); err != nil {
		return err
	}
	parentPath, leaf := splitFSActionPath(value.path)
	if err := journalMtime(ctx, db, options.worktreeRoot, options.worktree, fsPhasePreBase, parentPath,
		leaf, "Directory", value.mtime, uint64(stat.Dev), stat.Ino, config.fsActionFault); err != nil {
		return fmt.Errorf("set checkout directory mtime %q: %w", value.path, err)
	}
	return nil
}

func setOpenFileMtime(file *os.File, value string) error {
	parsed, err := time.Parse("2006-01-02T15:04:05Z", value)
	if err != nil {
		return err
	}
	timeval := unix.NsecToTimeval(parsed.UnixNano())
	if err := unix.Futimes(int(file.Fd()), []unix.Timeval{timeval, timeval}); err != nil {
		return fmt.Errorf("set checkout mtime: %w", err)
	}
	return nil
}

func checkoutPathCompleted(ctx context.Context, db *sql.DB, worktree, path string) (bool, error) {
	var completed int
	err := db.QueryRowContext(ctx, "SELECT completed FROM checkout_paths WHERE worktree = ? AND path = ?", worktree, path).Scan(&completed)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return completed != 0, err
}

func checkoutDirectoryState(ctx context.Context, db *sql.DB, worktree, path string) (bool, uint64, uint64, error) {
	var completed int
	var device, inode uint64
	err := db.QueryRowContext(ctx, `SELECT completed, target_device, target_inode FROM checkout_paths
		WHERE worktree = ? AND path = ? AND type = 'Directory'`, worktree, path).Scan(&completed, &device, &inode)
	if err != nil {
		return false, 0, 0, fmt.Errorf("read checkout directory identity: %w", err)
	}
	return completed != 0, device, inode, nil
}

func checkoutParentVerifier(ctx context.Context, db *sql.DB, worktree string) checkoutDirectoryVerifier {
	return func(path string, stat unix.Stat_t) error {
		_, device, inode, err := checkoutDirectoryState(ctx, db, worktree, path)
		if err != nil {
			return err
		}
		if device == 0 || inode == 0 || uint64(stat.Dev) != device || stat.Ino != inode || stat.Mode&syscall.S_IFMT != syscall.S_IFDIR {
			return fmt.Errorf("registered checkout parent directory %q identity changed", path)
		}
		return nil
	}
}

func recordCheckoutDirectoryTempIdentity(ctx context.Context, db *sql.DB, worktree, path, tempName string, stat unix.Stat_t, config libraryClientConfig) error {
	if config.beforeCheckoutDirectoryIdentity != nil {
		if err := config.beforeCheckoutDirectoryIdentity(); err != nil {
			return err
		}
	}
	result, err := db.ExecContext(ctx, `UPDATE checkout_paths SET temp_device = ?, temp_inode = ?
		WHERE worktree = ? AND path = ? AND type = 'Directory' AND temp_name = ? AND temp_device = 0 AND temp_inode = 0`,
		uint64(stat.Dev), stat.Ino, worktree, path, tempName)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return errors.New("record checkout temporary directory identity did not update fixed target path")
	}
	return nil
}

func recordCheckoutDirectoryInstalled(ctx context.Context, db *sql.DB, worktree string, value checkoutPath, stat unix.Stat_t, config libraryClientConfig) error {
	if config.afterCheckoutInstall != nil {
		if err := config.afterCheckoutInstall(value.path, value.kind); err != nil {
			return fmt.Errorf("record installed checkout directory: %w", err)
		}
	}
	result, err := db.ExecContext(ctx, `UPDATE checkout_paths SET target_device = ?, target_inode = ?, actual_mtime = ?, completed = 1
		WHERE worktree = ? AND path = ? AND type = 'Directory' AND
			((target_device = 0 AND target_inode = 0 AND temp_device = ? AND temp_inode = ?)
			 OR (target_device = ? AND target_inode = ?))`, uint64(stat.Dev), stat.Ino, value.mtime, worktree, value.path,
		uint64(stat.Dev), stat.Ino, uint64(stat.Dev), stat.Ino)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return errors.New("record installed checkout directory did not update fixed target path")
	}
	return nil
}

func verifyDirectoryPathIdentity(parent *os.File, name string, expected unix.Stat_t) error {
	var current unix.Stat_t
	if err := unix.Fstatat(int(parent.Fd()), name, &current, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return fmt.Errorf("inspect checkout directory path identity: %w", err)
	}
	if current.Dev != expected.Dev || current.Ino != expected.Ino || current.Mode&syscall.S_IFMT != syscall.S_IFDIR {
		return errors.New("checkout directory path identity changed")
	}
	return nil
}

func checkoutTempRecord(ctx context.Context, db *sql.DB, worktree, path string) (string, uint64, uint64, bool, error) {
	var name string
	var device, inode uint64
	err := db.QueryRowContext(ctx, "SELECT temp_name, temp_device, temp_inode FROM checkout_paths WHERE worktree = ? AND path = ?",
		worktree, path).Scan(&name, &device, &inode)
	if errors.Is(err, sql.ErrNoRows) {
		return "", 0, 0, false, nil
	}
	if err != nil {
		return "", 0, 0, false, err
	}
	if name != "" && !validCheckoutTempName(name) {
		return "", 0, 0, false, errors.New("pending checkout has invalid registered temporary name")
	}
	return name, device, inode, true, nil
}

func newCheckoutTempName() (string, error) {
	var capability [checkoutTempRandomSize]byte
	if _, err := rand.Read(capability[:]); err != nil {
		return "", fmt.Errorf("generate checkout temporary capability: %w", err)
	}
	return checkoutTempPrefix + hex.EncodeToString(capability[:]), nil
}

func validCheckoutTempName(name string) bool {
	if len(name) != len(checkoutTempPrefix)+checkoutTempRandomSize*2 || !strings.HasPrefix(name, checkoutTempPrefix) || filepath.Base(name) != name {
		return false
	}
	capability := name[len(checkoutTempPrefix):]
	decoded, err := hex.DecodeString(capability)
	return err == nil && len(decoded) == checkoutTempRandomSize && capability == strings.ToLower(capability)
}

func registerCheckoutTemp(ctx context.Context, db *sql.DB, worktree string, value checkoutPath, tempName string) error {
	result, err := db.ExecContext(ctx, `UPDATE checkout_paths SET temp_name = ? WHERE worktree = ? AND path = ?
		AND type = ? AND object_id = ? AND canonical_mtime = ? AND size = ?`, tempName, worktree, value.path,
		value.kind, value.id, value.mtime, value.size)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return errors.New("register checkout temporary file did not update fixed target path")
	}
	return nil
}

func recordCheckoutTempIdentity(ctx context.Context, db *sql.DB, worktree, path, tempName string, stat unix.Stat_t, config libraryClientConfig) error {
	if config.beforeCheckoutTempIdentity != nil {
		if err := config.beforeCheckoutTempIdentity(); err != nil {
			return err
		}
	}
	result, err := db.ExecContext(ctx, `UPDATE checkout_paths SET temp_device = ?, temp_inode = ?
		WHERE worktree = ? AND path = ? AND temp_name = ? AND temp_device = 0 AND temp_inode = 0`,
		uint64(stat.Dev), stat.Ino, worktree, path, tempName)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return errors.New("record checkout temporary identity did not update fixed target path")
	}
	return nil
}

func recordCheckoutCompleted(ctx context.Context, db *sql.DB, worktree string, value checkoutPath) error {
	actual := value.mtime
	_, err := db.ExecContext(ctx, `INSERT INTO checkout_paths(worktree, path, type, object_id, canonical_mtime, actual_mtime, size, completed)
		VALUES (?, ?, ?, ?, ?, ?, ?, 1) ON CONFLICT(worktree, path) DO UPDATE SET actual_mtime = excluded.actual_mtime, completed = 1`,
		worktree, value.path, value.kind, value.id, value.mtime, actual, value.size)
	return err
}

func verifyCheckoutFileIdentity(fd int, expectedDevice, expectedInode uint64) error {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return err
	}
	if stat.Mode&syscall.S_IFMT != syscall.S_IFREG || uint64(stat.Dev) != expectedDevice || stat.Ino != expectedInode || stat.Nlink != 1 {
		return errors.New("checkout file identity or link count changed")
	}
	return nil
}

func verifyCheckoutFilePathIdentity(parent *os.File, name string, expectedDevice, expectedInode uint64) error {
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	return errors.Join(verifyCheckoutFileIdentity(fd, expectedDevice, expectedInode), syscall.Close(fd))
}

func verifyInstalledFile(ctx context.Context, db *sql.DB, options bindOptions, value checkoutPath, parent *os.File,
	name string, expectedDevice, expectedInode uint64, config libraryClientConfig) (installed *os.File, resultErr error) {
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, errors.New("installed checkout file changed")
	}
	installed = os.NewFile(uintptr(fd), value.path)
	opened := installed
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, opened.Close())
			installed = nil
		}
	}()
	if err := verifyCheckoutFileIdentity(fd, expectedDevice, expectedInode); err != nil {
		return nil, fmt.Errorf("installed checkout file identity changed: %w", err)
	}
	info, err := installed.Stat()
	if err != nil || info.Size() != value.size {
		return nil, errors.New("installed checkout file changed")
	}
	metadata, err := cachedRemoteObject(ctx, options, "files", value.id)
	if err != nil {
		return nil, err
	}
	expected, err := object.VerifyFile(metadata, value.id)
	if err != nil || expected.Size != value.size {
		return nil, errors.New("cached checkout file metadata changed")
	}
	buffer := make([]byte, object.MaxBlockSize)
	for index, blockID := range expected.Blocks {
		size := object.MaxBlockSize
		if index == len(expected.Blocks)-1 {
			size = int(expected.Size - int64(index*object.MaxBlockSize))
		}
		count, readErr := io.ReadFull(installed, buffer[:size])
		if readErr != nil || count != size || object.ID(buffer[:count]) != blockID {
			return nil, errors.New("installed checkout file content changed")
		}
	}
	var extra [1]byte
	if count, readErr := installed.Read(extra[:]); count != 0 || !errors.Is(readErr, io.EOF) {
		return nil, errors.New("installed checkout file size changed")
	}
	if err := verifyCheckoutFileIdentity(fd, expectedDevice, expectedInode); err != nil {
		return nil, fmt.Errorf("installed checkout file identity changed before recovery: %w", err)
	}
	if info.ModTime().UTC().Format("2006-01-02T15:04:05Z") != value.mtime {
		parentPath, _ := splitFSActionPath(value.path)
		if err := journalMtime(ctx, db, options.worktreeRoot, options.worktree, fsPhasePreBase, parentPath,
			name, "File", value.mtime, expectedDevice, expectedInode, config.fsActionFault); err != nil {
			return nil, err
		}
		if err := verifyCheckoutFileIdentity(fd, expectedDevice, expectedInode); err != nil {
			return nil, fmt.Errorf("installed checkout file identity changed after recovery: %w", err)
		}
	}
	return installed, nil
}

func finalizeCheckout(ctx context.Context, db *sql.DB, options bindOptions, pending pendingCheckout, config libraryClientConfig) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin checkout finalization: %w", err)
	}
	fail := func(err error) error { return errors.Join(err, tx.Rollback()) }
	if err := assertNoIncompletePreBase(ctx, tx, options.worktree); err != nil {
		return fail(err)
	}
	var applyState string
	if err := tx.QueryRowContext(ctx, "SELECT apply_state FROM pending_checkouts WHERE worktree = ?", options.worktree).Scan(&applyState); err != nil || applyState != "pending" {
		return fail(errors.Join(errors.New("initial checkout pending apply state changed"), err))
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO bindings(server_url, library_id, worktree, user_id, device_id,
		sync_base_commit, sync_base_root, head_etag, access_token) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, pending.ServerURL,
		pending.LibraryID, pending.Worktree, pending.UserID, pending.DeviceID, pending.TargetCommit, pending.TargetRoot,
		pending.HeadETag, options.token); err != nil {
		return fail(fmt.Errorf("save checkout binding: %w", err))
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO path_index(worktree, path, type, object_id, canonical_mtime, actual_mtime, size)
		SELECT worktree, path, type, object_id, canonical_mtime, actual_mtime, size FROM checkout_paths
		WHERE worktree = ? AND completed = 1`, options.worktree); err != nil {
		return fail(fmt.Errorf("save checkout path index: %w", err))
	}
	var expected, completed int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM checkout_paths WHERE worktree = ?", options.worktree).Scan(&expected); err != nil {
		return fail(err)
	}
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM path_index WHERE worktree = ?", options.worktree).Scan(&completed); err != nil {
		return fail(err)
	}
	if expected != completed {
		return fail(errors.New("checkout path index is incomplete"))
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM pending_checkouts WHERE worktree = ?", options.worktree); err != nil {
		return fail(err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM checkout_paths WHERE worktree = ?", options.worktree); err != nil {
		return fail(err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM fs_actions WHERE worktree = ? AND state = 'completed' AND origin_action_id IS NOT NULL", options.worktree); err != nil {
		return fail(err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM fs_actions WHERE worktree = ? AND state = 'completed'", options.worktree); err != nil {
		return fail(err)
	}
	if config.beforeCheckoutBaseCommit != nil {
		if err := config.beforeCheckoutBaseCommit(); err != nil {
			return fail(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit checkout finalization: %w", err)
	}
	if config.afterCheckoutBaseCommit != nil {
		if err := config.afterCheckoutBaseCommit(); err != nil {
			return err
		}
	}
	return nil
}

type registeredCheckoutTemp struct {
	path, checkoutPath, kind string
	device, inode            uint64
}

func checkoutTempNames(ctx context.Context, db *sql.DB, worktree string) ([]registeredCheckoutTemp, error) {
	rows, err := db.QueryContext(ctx, `SELECT path, type, temp_name, temp_device, temp_inode FROM checkout_paths
		WHERE worktree = ? AND temp_name <> ''`, worktree)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []registeredCheckoutTemp
	for rows.Next() {
		var path, kind, temp string
		var device, inode uint64
		if err := rows.Scan(&path, &kind, &temp, &device, &inode); err != nil {
			return nil, err
		}
		if !validCheckoutTempName(temp) {
			return nil, errors.New("pending checkout has invalid registered temporary name")
		}
		directory := filepath.Dir(filepath.FromSlash(path))
		if directory == "." {
			directory = ""
		}
		names = append(names, registeredCheckoutTemp{path: filepath.Join(directory, temp), checkoutPath: path, kind: kind, device: device, inode: inode})
	}
	sort.Slice(names, func(i, j int) bool { return names[i].path < names[j].path })
	return names, rows.Err()
}

func cleanupCheckoutTemps(ctx context.Context, db *sql.DB, root *openedWorktree, worktree, phase string,
	temps []registeredCheckoutTemp, fault fsActionFault) error {
	for _, temp := range temps {
		parentPath, name := splitFSActionPath(filepath.ToSlash(temp.path))
		parent, name, err := openCheckoutParent(root, filepath.ToSlash(temp.path), nil)
		if err != nil {
			return err
		}
		var stat unix.Stat_t
		err = unix.Fstatat(int(parent.Fd()), name, &stat, unix.AT_SYMLINK_NOFOLLOW)
		if errors.Is(err, syscall.ENOENT) {
			parent.Close()
			continue
		}
		if err != nil {
			parent.Close()
			return fmt.Errorf("inspect registered checkout temporary file: %w", err)
		}
		if temp.device == 0 || temp.inode == 0 {
			var found bool
			temp.device, temp.inode, found, err = completedFSCreateIdentity(ctx, db, worktree, temp.checkoutPath)
			if err != nil || !found {
				parent.Close()
				return errors.Join(errors.New("registered checkout temporary path has no durable identity"), err)
			}
		}
		expectedMode := uint32(syscall.S_IFREG)
		if temp.kind == "Directory" {
			expectedMode = syscall.S_IFDIR
		} else if temp.kind != "File" {
			parent.Close()
			return errors.New("registered checkout temporary path has invalid type")
		}
		if uint64(stat.Dev) != temp.device || stat.Ino != temp.inode || stat.Mode&syscall.S_IFMT != expectedMode {
			parent.Close()
			return errors.New("registered checkout temporary path identity changed; pending checkout retained")
		}
		var expectedObject, expectedMtime string
		var expectedSize int64
		if temp.kind == "File" {
			file, info, err := openScannableAt(parent, name, temp.path)
			if err != nil {
				parent.Close()
				return err
			}
			snapshot := worktreeSnapshot{blocks: make(map[string]blockSource)}
			expectedObject, err = scanRegularFile(file, temp.path, info, &snapshot)
			expectedSize = info.Size()
			expectedMtime = info.ModTime().UTC().Truncate(time.Second).Format("2006-01-02T15:04:05Z")
			if closeErr := file.Close(); err != nil || closeErr != nil {
				parent.Close()
				return errors.Join(err, closeErr)
			}
		}
		if err := parent.Close(); err != nil {
			return err
		}
		if err := journalRemove(ctx, db, root, worktree, phase, parentPath, name, temp.kind, name,
			expectedObject, expectedMtime, expectedSize, temp.device, temp.inode, fault); err != nil {
			return fmt.Errorf("remove registered checkout temporary path: %w", err)
		}
	}
	return nil
}
