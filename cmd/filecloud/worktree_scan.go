package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	fscompat "github.com/mingming-cn/filecloud/internal/fscompat"
	"github.com/mingming-cn/filecloud/internal/object"
)

type scannedObject struct {
	kind, id string
	data     []byte
}

type blockSource struct {
	path         string
	offset, size int64
}

type worktreeSnapshot struct {
	root    string
	objects []scannedObject
	blocks  map[string]blockSource
	paths   []checkoutPath
}

type scanEntry struct {
	name, kind, id, modified string
}

type scanFault struct {
	phase, path string
	attempt     int
}

type worktreeScanConfig struct {
	ignoredRootNames           map[string]bool
	ignoredPaths               map[string]bool
	trackedPaths               map[string]bool
	warning                    io.Writer
	fault                      func(scanFault) error
	ignoreUntrackedUnsupported bool
}

type unstableWorktreeError struct {
	path, detail string
}

func (e *unstableWorktreeError) Error() string {
	if e.path == "" {
		return "unstable worktree: " + e.detail
	}
	return fmt.Sprintf("unstable worktree at %q: %s", e.path, e.detail)
}

type fileState struct {
	device, inode uint64
	mode, nlink   uint64
	size          int64
	mtime, ctime  fscompat.Timespec
}

type listedEntry struct {
	name, kind string
	state      fileState
}

type scannedPathState struct {
	kind  string
	state fileState
	hash  string
}

type scanSession struct {
	config      worktreeScanConfig
	snapshot    worktreeSnapshot
	paths       map[string]scannedPathState
	directories map[string][]listedEntry
	warned      map[string]bool
}

const scanRetryBudget = 3

func scanWorktree(root *openedWorktree) (worktreeSnapshot, error) {
	return scanWorktreeWithConfig(root, worktreeScanConfig{})
}

func scanWorktreeIgnoring(root *openedWorktree, ignoredRootNames map[string]bool) (worktreeSnapshot, error) {
	return scanWorktreeWithConfig(root, worktreeScanConfig{ignoredRootNames: ignoredRootNames})
}

func scanWorktreeWithConfig(root *openedWorktree, config worktreeScanConfig) (worktreeSnapshot, error) {
	if err := root.validateIdentity(); err != nil {
		return worktreeSnapshot{}, err
	}
	directory, err := duplicateDirectory(root.directory, root.path)
	if err != nil {
		return worktreeSnapshot{}, fmt.Errorf("duplicate worktree root: %w", err)
	}
	defer directory.Close()
	session := scanSession{
		config:   config,
		snapshot: worktreeSnapshot{blocks: make(map[string]blockSource)},
		paths:    make(map[string]scannedPathState), directories: make(map[string][]listedEntry), warned: make(map[string]bool),
	}
	rootID, err := session.scanDirectory(directory, "")
	if err != nil {
		return worktreeSnapshot{}, err
	}
	if config.fault != nil {
		if err := config.fault(scanFault{phase: "before-final-validation"}); err != nil {
			return worktreeSnapshot{}, err
		}
	}
	if err := session.validateTree(root); err != nil {
		return worktreeSnapshot{}, err
	}
	if err := root.validateIdentity(); err != nil {
		return worktreeSnapshot{}, err
	}
	session.snapshot.root = rootID
	return session.snapshot, nil
}

func duplicateDirectory(directory *os.File, name string) (*os.File, error) {
	return fscompat.OpenDirectoryEnumeration(int(directory.Fd()), name)
}

func readDirectoryNames(directory *os.File, count int) ([]string, error) {
	enumeration, err := fscompat.OpenDirectoryEnumeration(int(directory.Fd()), directory.Name())
	if err != nil {
		return nil, err
	}
	names, readErr := enumeration.Readdirnames(count)
	return names, errors.Join(readErr, enumeration.Close())
}

func (session *scanSession) scanDirectory(directory *os.File, relative string) (string, error) {
	if err := validateWorktreeDirectoryHandle(int(directory.Fd())); err != nil {
		return "", fmt.Errorf("inspect worktree directory %q: %w", relative, err)
	}
	before, err := stateOf(directory)
	if err != nil {
		return "", fmt.Errorf("inspect directory %q: %w", relative, err)
	}
	first, err := session.enumerateDirectory(directory, relative)
	if err != nil {
		return "", err
	}
	if err := session.runFault("after-directory-enumeration-1", relative, 1); err != nil {
		return "", err
	}
	second, err := session.enumerateDirectory(directory, relative)
	if err != nil {
		return "", err
	}
	if err := session.runFault("after-directory-enumeration-2", relative, 1); err != nil {
		return "", err
	}
	if !sameListing(first, second) {
		return "", &unstableWorktreeError{relative, "directory entries changed between enumerations"}
	}
	session.directories[relative] = cloneListing(second)
	session.paths[relative] = scannedPathState{kind: "Directory", state: before}

	values := make([]scanEntry, 0, len(second))
	for _, entry := range second {
		childPath := joinScanPath(relative, entry.name)
		if err := session.runFault("before-open", childPath, 1); err != nil {
			return "", fmt.Errorf("open worktree path %q: %w", childPath, err)
		}
		child, _, err := openListedAt(directory, entry, childPath)
		if err != nil {
			return "", err
		}
		var id string
		switch entry.kind {
		case "File":
			id, err = session.scanRegularFile(child, childPath)
		case "Directory":
			id, err = session.scanDirectory(child, childPath)
		default:
			err = fmt.Errorf("unsupported worktree path type at %q", childPath)
		}
		closeErr := child.Close()
		if err != nil || closeErr != nil {
			return "", errors.Join(err, closeErr)
		}
		stable := session.paths[childPath].state
		modified := formatScanTime(stable.mtime)
		values = append(values, scanEntry{entry.name, entry.kind, id, modified})
		size := int64(0)
		if entry.kind == "File" {
			size = stable.size
		}
		session.snapshot.paths = append(session.snapshot.paths, checkoutPath{path: childPath, kind: entry.kind, id: id,
			mtime: modified, size: size, device: stable.device, inode: stable.inode})
	}
	after, err := stateOf(directory)
	if err != nil || !sameState(before, after) {
		return "", &unstableWorktreeError{relative, "directory changed during scan"}
	}
	data, id, err := canonicalDirectory(relative, values)
	if err != nil {
		return "", err
	}
	session.snapshot.objects = append(session.snapshot.objects, scannedObject{"directories", id, data})
	return id, nil
}

func (session *scanSession) enumerateDirectory(directory *os.File, relative string) ([]listedEntry, error) {
	enumeration, err := fscompat.OpenDirectoryEnumeration(int(directory.Fd()), relative)
	if err != nil {
		return nil, fmt.Errorf("open directory enumeration %q: %w", relative, err)
	}
	entries, readErr := enumeration.Readdirnames(-1)
	closeErr := enumeration.Close()
	if readErr != nil || closeErr != nil {
		return nil, fmt.Errorf("read directory %q: %w", relative, errors.Join(readErr, closeErr))
	}
	result := make([]listedEntry, 0, len(entries))
	for _, name := range entries {
		if relative == "" && session.config.ignoredRootNames[name] {
			continue
		}
		path := joinScanPath(relative, name)
		if session.config.ignoredPaths[path] {
			continue
		}
		if !utf8.ValidString(name) || len(path) > 1024 {
			return nil, fmt.Errorf("invalid worktree path %q", path)
		}
		state, kind, err := inspectAt(directory, name, path)
		if err != nil {
			return nil, err
		}
		if kind == "" {
			if session.config.ignoreUntrackedUnsupported && !session.config.trackedPaths[path] {
				session.warnUnsupported(path)
				continue
			}
			return nil, fmt.Errorf("unsupported worktree path type at %q", path)
		}
		result = append(result, listedEntry{name: name, kind: kind, state: state})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].name != result[j].name {
			return result[i].name < result[j].name
		}
		if result[i].kind != result[j].kind {
			return result[i].kind < result[j].kind
		}
		if result[i].state.device != result[j].state.device {
			return result[i].state.device < result[j].state.device
		}
		return result[i].state.inode < result[j].state.inode
	})
	return result, nil
}

func inspectAt(directory *os.File, name, path string) (fileState, string, error) {
	var stat fscompat.Stat_t
	if err := fscompat.Fstatat(int(directory.Fd()), name, &stat, fscompat.AT_SYMLINK_NOFOLLOW); err != nil {
		return fileState{}, "", fmt.Errorf("inspect worktree path %q without following links: %w", path, err)
	}
	state := fileStateFromStat(stat)
	switch state.mode & fscompat.S_IFMT {
	case fscompat.S_IFREG:
		return state, "File", nil
	case fscompat.S_IFDIR:
		return state, "Directory", nil
	default:
		return state, "", nil
	}
}

func openListedAt(directory *os.File, listed listedEntry, path string) (*os.File, fileState, error) {
	var fd int
	var err error
	if listed.kind == "Directory" {
		fd, err = openWorktreeDirectoryAt(int(directory.Fd()), listed.name)
	} else {
		fd, err = fscompat.Openat(int(directory.Fd()), listed.name,
			fscompat.O_RDONLY|fscompat.O_NOFOLLOW|fscompat.O_CLOEXEC|fscompat.O_NONBLOCK, 0)
	}
	if err != nil {
		return nil, fileState{}, fmt.Errorf("open worktree path %q without following links: %w", path, err)
	}
	state, err := stateOfFD(fd)
	if err != nil || !sameIdentity(listed.state, state) || state.mode != listed.state.mode {
		fscompat.Close(fd)
		return nil, fileState{}, &unstableWorktreeError{path, "path changed while opening"}
	}
	return os.NewFile(uintptr(fd), path), state, nil
}

func (session *scanSession) scanRegularFile(file *os.File, path string) (string, error) {
	for attempt := 1; attempt <= scanRetryBudget; attempt++ {
		before, err := stateOf(file)
		if err != nil {
			return "", fmt.Errorf("inspect worktree file %q: %w", path, err)
		}
		first, _, err := readFileBlocks(file, false)
		if err != nil {
			return "", fmt.Errorf("read worktree file %q: %w", path, err)
		}
		if err := session.runFault("after-file-read-1", path, attempt); err != nil {
			return "", err
		}
		middle, err := stateOf(file)
		if err != nil {
			return "", fmt.Errorf("inspect worktree file %q: %w", path, err)
		}
		second, sources, err := readFileBlocks(file, true)
		if err != nil {
			return "", fmt.Errorf("reread worktree file %q: %w", path, err)
		}
		if err := session.runFault("after-file-read-2", path, attempt); err != nil {
			return "", err
		}
		after, err := stateOf(file)
		if err != nil {
			return "", fmt.Errorf("inspect worktree file %q: %w", path, err)
		}
		if sameState(before, middle) && sameState(middle, after) && equalStrings(first, second) {
			data, id, err := canonicalFile(path, after.size, second)
			if err != nil {
				return "", err
			}
			for index, blockID := range second {
				if _, exists := session.snapshot.blocks[blockID]; !exists {
					session.snapshot.blocks[blockID] = blockSource{path: path, offset: sources[index].offset, size: sources[index].size}
				}
			}
			session.snapshot.objects = append(session.snapshot.objects, scannedObject{"files", id, data})
			session.paths[path] = scannedPathState{kind: "File", state: after, hash: id}
			if err := session.runFault("after-file-scan", path, attempt); err != nil {
				return "", err
			}
			return id, nil
		}
	}
	return "", &unstableWorktreeError{path, fmt.Sprintf("file changed across %d scan attempts", scanRetryBudget)}
}

func readFileBlocks(file *os.File, keepSources bool) ([]string, []blockSource, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, nil, err
	}
	blocks := make([]string, 0)
	sources := make([]blockSource, 0)
	buffer := make([]byte, object.MaxBlockSize)
	for offset := int64(0); ; offset += object.MaxBlockSize {
		count, err := io.ReadFull(file, buffer)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, nil, err
		}
		blocks = append(blocks, object.ID(buffer[:count]))
		if keepSources {
			sources = append(sources, blockSource{offset: offset, size: int64(count)})
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
	}
	return blocks, sources, nil
}

func (session *scanSession) validateTree(root *openedWorktree) error {
	directory, err := duplicateDirectory(root.directory, root.path)
	if err != nil {
		return fmt.Errorf("duplicate worktree root for validation: %w", err)
	}
	defer directory.Close()
	return session.validateDirectory(directory, "")
}

func (session *scanSession) validateDirectory(directory *os.File, relative string) error {
	expected, ok := session.directories[relative]
	if !ok {
		return &unstableWorktreeError{relative, "directory was not recorded"}
	}
	state, err := stateOf(directory)
	if err != nil {
		return fmt.Errorf("validate directory %q: %w", relative, err)
	}
	if !sameState(session.paths[relative].state, state) {
		return &unstableWorktreeError{relative, "directory metadata changed before final validation"}
	}
	actual, err := session.enumerateDirectory(directory, relative)
	if err != nil {
		return err
	}
	if !sameListing(expected, actual) {
		return &unstableWorktreeError{relative, "directory entries changed before final validation"}
	}
	for _, entry := range actual {
		path := joinScanPath(relative, entry.name)
		if err := session.runFault("before-open", path, 1); err != nil {
			return fmt.Errorf("open worktree path %q during final validation: %w", path, err)
		}
		child, state, err := openListedAt(directory, entry, path)
		if err != nil {
			return err
		}
		recorded, ok := session.paths[path]
		if !ok || recorded.kind != entry.kind || !sameState(recorded.state, state) {
			child.Close()
			return &unstableWorktreeError{path, "path changed before final validation"}
		}
		if entry.kind == "Directory" {
			err = session.validateDirectory(child, path)
		} else if state.ctime == (fscompat.Timespec{}) {
			blocks, _, readErr := readFileBlocks(child, false)
			if readErr != nil {
				err = readErr
			} else {
				_, id, objectErr := canonicalFile(path, state.size, blocks)
				err = objectErr
				if err == nil && id != recorded.hash {
					err = &unstableWorktreeError{path, "file content changed before final validation"}
				}
			}
		}
		if closeErr := child.Close(); err != nil || closeErr != nil {
			return errors.Join(err, closeErr)
		}
	}
	return nil
}

func (session *scanSession) runFault(phase, path string, attempt int) error {
	if session.config.fault == nil {
		return nil
	}
	return session.config.fault(scanFault{phase: phase, path: path, attempt: attempt})
}

func (session *scanSession) warnUnsupported(path string) {
	if session.warned[path] || session.config.warning == nil {
		return
	}
	session.warned[path] = true
	fmt.Fprintf(session.config.warning, "warning: ignoring untracked unsupported worktree path %q\n", path)
}

func stateOf(file *os.File) (fileState, error) {
	return stateOfFD(int(file.Fd()))
}

func statOfOpenFile(file *os.File) (fscompat.Stat_t, error) {
	var stat fscompat.Stat_t
	return stat, fscompat.Fstat(int(file.Fd()), &stat)
}

func stateOfFD(fd int) (fileState, error) {
	var stat fscompat.Stat_t
	if err := fscompat.Fstat(fd, &stat); err != nil {
		return fileState{}, err
	}
	return fileStateFromStat(stat), nil
}

func fileStateFromStat(stat fscompat.Stat_t) fileState {
	return fileState{device: uint64(stat.Dev), inode: stat.Ino, mode: uint64(stat.Mode), nlink: uint64(stat.Nlink),
		size: stat.Size, mtime: stat.Mtim, ctime: stat.Ctim}
}

func sameIdentity(left, right fileState) bool {
	return left.device == right.device && left.inode == right.inode
}

func sameState(left, right fileState) bool {
	return sameIdentity(left, right) && left.mode == right.mode && left.nlink == right.nlink && left.size == right.size &&
		left.mtime == right.mtime && left.ctime == right.ctime
}

func sameListing(left, right []listedEntry) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].name != right[index].name || left[index].kind != right[index].kind ||
			!sameIdentity(left[index].state, right[index].state) {
			return false
		}
	}
	return true
}

func cloneListing(entries []listedEntry) []listedEntry {
	return append([]listedEntry(nil), entries...)
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func joinScanPath(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "/" + name
}

func formatScanTime(value fscompat.Timespec) string {
	return canonicalProtocolMtime(time.Unix(value.Sec, value.Nsec))
}

func canonicalFile(path string, size int64, blocks []string) ([]byte, string, error) {
	quotedBlocks := make([]string, len(blocks))
	for index, id := range blocks {
		quotedBlocks[index] = strconv.Quote(id)
	}
	input := []byte(`{"Blocks":[` + strings.Join(quotedBlocks, ",") + `],"Size":` + strconv.Quote(strconv.FormatInt(size, 10)) + `,"Type":"File","Version":1}`)
	data, id, err := object.Canonicalize("files", input)
	if err != nil {
		return nil, "", fmt.Errorf("construct file object for %q: %w", path, err)
	}
	return data, id, nil
}

func canonicalDirectory(relative string, values []scanEntry) ([]byte, string, error) {
	var input bytes.Buffer
	input.WriteString(`{"Entries":[`)
	for index, entry := range values {
		if index > 0 {
			input.WriteByte(',')
		}
		fmt.Fprintf(&input, `{"Id":%q,"ModifiedAt":%q,"Name":%q,"Type":%q}`, entry.id, entry.modified, entry.name, entry.kind)
	}
	input.WriteString(`],"Type":"Directory","Version":1}`)
	data, id, err := object.Canonicalize("directories", input.Bytes())
	if err != nil {
		return nil, "", fmt.Errorf("invalid worktree directory %q: %w", relative, err)
	}
	return data, id, nil
}

// These wrappers keep checkout recovery on the same stable scanner primitives.
func openScannableAt(directory *os.File, name, path string) (*os.File, os.FileInfo, error) {
	state, kind, err := inspectAt(directory, name, path)
	if err != nil {
		return nil, nil, err
	}
	if kind == "" {
		return nil, nil, fmt.Errorf("unsupported worktree path type at %q", path)
	}
	file, _, err := openListedAt(directory, listedEntry{name: name, kind: kind, state: state}, path)
	if err != nil {
		return nil, nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, nil, err
	}
	return file, info, nil
}

func scanRegularFile(file *os.File, path string, _ os.FileInfo, snapshot *worktreeSnapshot) (string, error) {
	session := scanSession{snapshot: *snapshot, paths: make(map[string]scannedPathState)}
	id, err := session.scanRegularFile(file, path)
	*snapshot = session.snapshot
	return id, err
}

func scanDirectory(directory *os.File, relative string, snapshot *worktreeSnapshot) (string, error) {
	session := scanSession{snapshot: *snapshot, paths: make(map[string]scannedPathState),
		directories: make(map[string][]listedEntry), warned: make(map[string]bool)}
	id, err := session.scanDirectory(directory, relative)
	*snapshot = session.snapshot
	return id, err
}

func (root *openedWorktree) readBlock(source blockSource, expectedID string) ([]byte, error) {
	file, err := root.openRelative(source.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data := make([]byte, source.size)
	if _, err := file.ReadAt(data, source.offset); err != nil {
		return nil, fmt.Errorf("reread block from %q: %w", source.path, err)
	}
	if object.ID(data) != expectedID {
		return nil, errors.New("worktree changed during upload")
	}
	return data, nil
}

func (root *openedWorktree) openRelative(relative string) (*os.File, error) {
	current, err := fscompat.Dup(int(root.directory.Fd()))
	if err != nil {
		return nil, err
	}
	components := strings.Split(relative, "/")
	for index, component := range components {
		var next int
		var openErr error
		if index < len(components)-1 {
			next, openErr = openWorktreeDirectoryAt(current, component)
		} else {
			next, openErr = fscompat.Openat(current, component,
				fscompat.O_RDONLY|fscompat.O_NOFOLLOW|fscompat.O_CLOEXEC|fscompat.O_NONBLOCK, 0)
		}
		fscompat.Close(current)
		if openErr != nil {
			return nil, fmt.Errorf("reopen worktree path %q: %w", relative, openErr)
		}
		current = next
	}
	return os.NewFile(uintptr(current), relative), nil
}
