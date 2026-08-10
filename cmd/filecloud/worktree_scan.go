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
	"syscall"
	"time"
	"unicode/utf8"

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

const openPath = 0x200000 // Linux O_PATH; inspect type without opening a device or blocking on a FIFO.

func scanWorktree(root *openedWorktree) (worktreeSnapshot, error) {
	return scanWorktreeIgnoring(root, nil)
}

func scanWorktreeIgnoring(root *openedWorktree, ignoredRootNames map[string]bool) (worktreeSnapshot, error) {
	if err := root.validateIdentity(); err != nil {
		return worktreeSnapshot{}, err
	}
	dup, err := syscall.Dup(int(root.directory.Fd()))
	if err != nil {
		return worktreeSnapshot{}, fmt.Errorf("duplicate worktree root: %w", err)
	}
	directory := os.NewFile(uintptr(dup), root.path)
	defer directory.Close()
	if _, err := directory.Seek(0, io.SeekStart); err != nil {
		return worktreeSnapshot{}, fmt.Errorf("rewind worktree: %w", err)
	}
	snapshot := worktreeSnapshot{blocks: make(map[string]blockSource)}
	rootID, err := scanDirectoryIgnoring(directory, "", &snapshot, ignoredRootNames)
	if err != nil {
		return worktreeSnapshot{}, err
	}
	if err := root.validateIdentity(); err != nil {
		return worktreeSnapshot{}, err
	}
	snapshot.root = rootID
	return snapshot, nil
}

func scanDirectory(directory *os.File, relative string, snapshot *worktreeSnapshot) (string, error) {
	return scanDirectoryIgnoring(directory, relative, snapshot, nil)
}

func scanDirectoryIgnoring(directory *os.File, relative string, snapshot *worktreeSnapshot, ignoredRootNames map[string]bool) (string, error) {
	before, err := directory.Stat()
	if err != nil {
		return "", fmt.Errorf("inspect directory %q: %w", relative, err)
	}
	entries, err := directory.Readdir(-1)
	if err != nil {
		return "", fmt.Errorf("read directory %q: %w", relative, err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	values := make([]scanEntry, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if relative == "" && ignoredRootNames[name] {
			continue
		}
		childPath := name
		if relative != "" {
			childPath = relative + "/" + name
		}
		if !utf8.ValidString(name) || len(childPath) > 1024 {
			return "", fmt.Errorf("invalid worktree path %q", childPath)
		}
		child, info, err := openScannableAt(directory, name, childPath)
		if err != nil {
			return "", err
		}
		modified := info.ModTime().UTC().Truncate(time.Second).Format("2006-01-02T15:04:05Z")
		var kind, id string
		switch {
		case info.Mode().IsRegular():
			kind = "File"
			id, err = scanRegularFile(child, childPath, info, snapshot)
		case info.IsDir():
			kind = "Directory"
			id, err = scanDirectoryIgnoring(child, childPath, snapshot, ignoredRootNames)
		default:
			err = fmt.Errorf("unsupported worktree path type at %q", childPath)
		}
		closeErr := child.Close()
		if err != nil || closeErr != nil {
			return "", errors.Join(err, closeErr)
		}
		values = append(values, scanEntry{name, kind, id, modified})
		size := int64(0)
		if kind == "File" {
			size = info.Size()
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return "", errors.New("worktree path has no stable identity")
		}
		snapshot.paths = append(snapshot.paths, checkoutPath{path: childPath, kind: kind, id: id, mtime: modified,
			size: size, device: uint64(stat.Dev), inode: stat.Ino})
	}
	after, err := directory.Stat()
	if err != nil || !sameFileState(before, after) {
		return "", errors.New("worktree changed during scan")
	}
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
		return "", fmt.Errorf("invalid worktree directory %q: %w", relative, err)
	}
	snapshot.objects = append(snapshot.objects, scannedObject{"directories", id, data})
	return id, nil
}

func openScannableAt(directory *os.File, name, path string) (*os.File, os.FileInfo, error) {
	inspectionFD, err := syscall.Openat(int(directory.Fd()), name, openPath|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("inspect worktree path %q without following links: %w", path, err)
	}
	var inspected syscall.Stat_t
	if err := syscall.Fstat(inspectionFD, &inspected); err != nil {
		syscall.Close(inspectionFD)
		return nil, nil, fmt.Errorf("inspect worktree path %q: %w", path, err)
	}
	kind := inspected.Mode & syscall.S_IFMT
	if kind != syscall.S_IFREG && kind != syscall.S_IFDIR {
		syscall.Close(inspectionFD)
		return nil, nil, fmt.Errorf("unsupported worktree path type at %q", path)
	}
	flags := syscall.O_RDONLY | syscall.O_NOFOLLOW | syscall.O_CLOEXEC
	if kind == syscall.S_IFDIR {
		flags |= syscall.O_DIRECTORY
	}
	fd, err := syscall.Openat(int(directory.Fd()), name, flags, 0)
	if err != nil {
		syscall.Close(inspectionFD)
		return nil, nil, fmt.Errorf("open worktree path %q without following links: %w", path, err)
	}
	var opened syscall.Stat_t
	if err := syscall.Fstat(fd, &opened); err != nil || opened.Dev != inspected.Dev || opened.Ino != inspected.Ino || opened.Mode != inspected.Mode {
		syscall.Close(inspectionFD)
		syscall.Close(fd)
		return nil, nil, fmt.Errorf("worktree path %q changed while opening", path)
	}
	syscall.Close(inspectionFD)
	file := os.NewFile(uintptr(fd), path)
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("inspect opened worktree path %q: %w", path, err)
	}
	return file, info, nil
}

func scanRegularFile(file *os.File, path string, before os.FileInfo, snapshot *worktreeSnapshot) (string, error) {
	blocks := make([]string, 0, (before.Size()+object.MaxBlockSize-1)/object.MaxBlockSize)
	buffer := make([]byte, object.MaxBlockSize)
	for offset := int64(0); ; offset += object.MaxBlockSize {
		count, err := io.ReadFull(file, buffer)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			return "", fmt.Errorf("read worktree file %q: %w", path, err)
		}
		id := object.ID(buffer[:count])
		blocks = append(blocks, id)
		if _, exists := snapshot.blocks[id]; !exists {
			snapshot.blocks[id] = blockSource{path: path, offset: offset, size: int64(count)}
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
	}
	after, err := file.Stat()
	if err != nil || !sameFileState(before, after) {
		return "", errors.New("worktree changed during scan")
	}
	quotedBlocks := make([]string, len(blocks))
	for index, id := range blocks {
		quotedBlocks[index] = strconv.Quote(id)
	}
	input := []byte(`{"Blocks":[` + strings.Join(quotedBlocks, ",") + `],"Size":` + strconv.Quote(strconv.FormatInt(before.Size(), 10)) + `,"Type":"File","Version":1}`)
	data, id, err := object.Canonicalize("files", input)
	if err != nil {
		return "", fmt.Errorf("construct file object for %q: %w", path, err)
	}
	snapshot.objects = append(snapshot.objects, scannedObject{"files", id, data})
	return id, nil
}

func sameFileState(left, right os.FileInfo) bool {
	l, lok := left.Sys().(*syscall.Stat_t)
	r, rok := right.Sys().(*syscall.Stat_t)
	return lok && rok && l.Dev == r.Dev && l.Ino == r.Ino && l.Mode == r.Mode && l.Size == r.Size && l.Mtim == r.Mtim
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
	current, err := syscall.Dup(int(root.directory.Fd()))
	if err != nil {
		return nil, err
	}
	components := strings.Split(relative, "/")
	for index, component := range components {
		flags := syscall.O_RDONLY | syscall.O_NOFOLLOW | syscall.O_CLOEXEC
		if index < len(components)-1 {
			flags |= syscall.O_DIRECTORY
		}
		next, openErr := syscall.Openat(current, component, flags, 0)
		syscall.Close(current)
		if openErr != nil {
			return nil, fmt.Errorf("reopen worktree path %q: %w", relative, openErr)
		}
		current = next
	}
	return os.NewFile(uintptr(current), relative), nil
}
