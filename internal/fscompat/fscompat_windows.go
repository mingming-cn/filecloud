//go:build windows

package fscompat

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// These values deliberately match the POSIX file-kind bits consumed by the
// shared journal. They are an internal representation, not Windows attributes.
const (
	AT_SYMLINK_NOFOLLOW = 1
	AT_REMOVEDIR        = 2
	O_RDONLY            = 0
	O_WRONLY            = 1
	O_RDWR              = 2
	O_CREAT             = 0x40
	O_EXCL              = 0x80
	O_DIRECTORY         = 0x10000
	O_NOFOLLOW          = 0x20000
	O_CLOEXEC           = 0
	O_NONBLOCK          = 0
	// O_DELETE requests DELETE access without changing POSIX callers on Unix.
	// NT rename and disposition operations require it on the source handle.
	O_DELETE    = 0x40000
	O_WRITEATTR = 0x80000
	S_IFMT      = 0xf000
	S_IFREG     = 0x8000
	S_IFDIR     = 0x4000
)

var (
	ENOENT      = windows.ERROR_FILE_NOT_FOUND
	EEXIST      = windows.ERROR_FILE_EXISTS
	ENOTEMPTY   = windows.ERROR_DIR_NOT_EMPTY
	EWOULDBLOCK = windows.ERROR_LOCK_VIOLATION
	EAGAIN      = windows.ERROR_LOCK_VIOLATION
)

const (
	LOCK_EX = 1
	LOCK_NB = 2
	LOCK_UN = 8
)

type Timespec struct{ Sec, Nsec int64 }
type Stat_t struct {
	Dev, Ino   uint64
	Mode       uint32
	Nlink      uint64
	Size       int64
	Mtim, Ctim Timespec
}

type _fileBasicInfo struct {
	CreationTime, LastAccessTime, LastWriteTime, ChangeTime int64
	FileAttributes                                          uint32
}

func Open(path string, flags int, mode uint32) (int, error) {
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return -1, err
	}
	access := uint32(windows.GENERIC_READ)
	if flags&O_WRONLY != 0 {
		access = windows.GENERIC_WRITE
	}
	if flags&O_RDWR != 0 {
		access |= windows.GENERIC_WRITE
	}
	if flags&O_DIRECTORY != 0 {
		access |= windows.GENERIC_WRITE
	}
	if flags&O_DELETE != 0 {
		access |= windows.DELETE
	}
	if flags&O_WRITEATTR != 0 {
		access |= windows.GENERIC_WRITE
	}
	disposition := uint32(windows.OPEN_EXISTING)
	if flags&O_CREAT != 0 && flags&O_EXCL != 0 {
		disposition = windows.CREATE_NEW
	}
	if flags&O_CREAT != 0 && flags&O_EXCL == 0 {
		disposition = windows.OPEN_ALWAYS
	}
	attributes := uint32(windows.FILE_ATTRIBUTE_NORMAL | windows.FILE_FLAG_BACKUP_SEMANTICS)
	if flags&O_NOFOLLOW != 0 {
		attributes |= windows.FILE_FLAG_OPEN_REPARSE_POINT
	}
	h, err := windows.CreateFile(name, access, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, disposition, attributes, 0)
	if err != nil {
		return -1, fmt.Errorf("CreateFile %q: %w", path, err)
	}
	if err := rejectReparse(h); err != nil {
		windows.CloseHandle(h)
		return -1, err
	}
	return int(h), nil
}

// Openat is the only primitive used for untrusted worktree children. NtCreateFile
// resolves name relative to RootDirectory and OBJ_DONT_REPARSE prevents an
// intermediate junction or symlink from escaping that verified parent.
func Openat(dirfd int, path string, flags int, mode uint32) (int, error) {
	name, err := windows.NewNTUnicodeString(filepath.FromSlash(path))
	if err != nil {
		return -1, err
	}
	access := uint32(windows.FILE_READ_ATTRIBUTES | windows.SYNCHRONIZE)
	if flags&(O_WRONLY|O_RDWR) != 0 {
		access |= windows.FILE_WRITE_DATA | windows.FILE_WRITE_ATTRIBUTES
	}
	if flags&O_DELETE != 0 {
		access |= windows.DELETE
	}
	if flags&O_WRITEATTR != 0 {
		access |= windows.GENERIC_WRITE
	}
	if flags&O_DIRECTORY != 0 {
		access |= windows.FILE_LIST_DIRECTORY | windows.GENERIC_WRITE
	} else {
		access |= windows.FILE_READ_DATA
	}
	options := uint32(windows.FILE_SYNCHRONOUS_IO_NONALERT | windows.FILE_OPEN_REPARSE_POINT)
	if flags&O_DIRECTORY != 0 {
		options |= windows.FILE_DIRECTORY_FILE
	}
	disposition := uint32(windows.FILE_OPEN)
	if flags&O_CREAT != 0 && flags&O_EXCL != 0 {
		disposition = windows.FILE_CREATE
	}
	if flags&O_CREAT != 0 && flags&O_EXCL == 0 {
		disposition = windows.FILE_OPEN_IF
	}
	oa := windows.OBJECT_ATTRIBUTES{Length: uint32(unsafe.Sizeof(windows.OBJECT_ATTRIBUTES{})), RootDirectory: windows.Handle(dirfd), ObjectName: name, Attributes: windows.OBJ_CASE_INSENSITIVE | windows.OBJ_DONT_REPARSE}
	var h windows.Handle
	var status windows.IO_STATUS_BLOCK
	if err := windows.NtCreateFile(&h, access, &oa, &status, nil, 0,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, disposition, options, 0, 0); err != nil {
		return -1, fmt.Errorf("NtCreateFile %q: %w", path, NormalizeError(err))
	}
	if err := rejectReparse(h); err != nil {
		windows.CloseHandle(h)
		return -1, err
	}
	return int(h), nil
}

func rejectReparse(h windows.Handle) error {
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(h, &info); err != nil {
		return err
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return errors.New("reparse point is unsupported in worktree")
	}
	return nil
}

func Fstat(fd int, stat *Stat_t) error {
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(fd), &info); err != nil {
		return err
	}
	kind := uint32(S_IFREG)
	if info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		kind = S_IFDIR
	}
	var basic _fileBasicInfo
	if err := windows.GetFileInformationByHandleEx(windows.Handle(fd), windows.FileBasicInfo,
		(*byte)(unsafe.Pointer(&basic)), uint32(unsafe.Sizeof(basic))); err != nil {
		return err
	}
	mtime := time.Unix(0, info.LastWriteTime.Nanoseconds())
	changeTime := windows.Filetime{LowDateTime: uint32(basic.ChangeTime), HighDateTime: uint32(uint64(basic.ChangeTime) >> 32)}
	change := time.Unix(0, changeTime.Nanoseconds())
	*stat = Stat_t{Dev: uint64(info.VolumeSerialNumber), Ino: uint64(info.FileIndexHigh)<<32 | uint64(info.FileIndexLow), Mode: kind, Nlink: uint64(info.NumberOfLinks), Size: int64(uint64(info.FileSizeHigh)<<32 | uint64(info.FileSizeLow)), Mtim: Timespec{mtime.Unix(), int64(mtime.Nanosecond())}, Ctim: Timespec{change.Unix(), int64(change.Nanosecond())}}
	if stat.Ino == 0 || stat.Ino > math.MaxInt64 {
		return errors.New("NTFS file identity is not safely representable")
	}
	return nil
}
func Fstatat(dirfd int, path string, stat *Stat_t, flags int) error {
	fd, err := Openat(dirfd, path, O_RDONLY|O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer Close(fd)
	return Fstat(fd, stat)
}
func Lstat(path string, stat *Stat_t) error {
	fd, err := Open(path, O_RDONLY|O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer Close(fd)
	return Fstat(fd, stat)
}
func Dup(fd int) (int, error) {
	var duplicate windows.Handle
	if err := windows.DuplicateHandle(windows.CurrentProcess(), windows.Handle(fd), windows.CurrentProcess(), &duplicate, 0, false, windows.DUPLICATE_SAME_ACCESS); err != nil {
		return -1, err
	}
	return int(duplicate), nil
}

// OpenDirectoryEnumeration duplicates the verified directory handle. Windows
// starts a fresh directory query for a newly wrapped duplicate and does not
// support the Unix Seek(0) reset used by the Unix implementation.
func OpenDirectoryEnumeration(fd int, name string) (*os.File, error) {
	directory, err := Dup(fd)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(directory), name), nil
}
func Close(fd int) error { return windows.CloseHandle(windows.Handle(fd)) }
func Mkdirat(dirfd int, path string, mode uint32) error {
	fd, err := Openat(dirfd, path, O_CREAT|O_EXCL|O_DIRECTORY|O_NOFOLLOW, mode)
	if err != nil {
		return err
	}
	return Close(fd)
}
func Unlinkat(dirfd int, path string, flags int) error {
	openFlags := O_WRONLY | O_NOFOLLOW | O_DELETE
	if flags&AT_REMOVEDIR != 0 {
		openFlags |= O_DIRECTORY
	}
	fd, err := Openat(dirfd, path, openFlags, 0)
	if err != nil {
		return err
	}
	defer Close(fd)
	deleteFile := byte(1)
	var status windows.IO_STATUS_BLOCK
	if err := windows.NtSetInformationFile(windows.Handle(fd), &status, &deleteFile, 1, windows.FileDispositionInformation); err != nil {
		return NormalizeError(err)
	}
	return nil
}
func Ftruncate(fd int, size int64) error {
	if size < 0 {
		return errors.New("negative truncate size")
	}
	high := int32(size >> 32)
	if _, err := windows.SetFilePointer(windows.Handle(fd), int32(size), &high, windows.FILE_BEGIN); err != nil {
		return err
	}
	return windows.SetEndOfFile(windows.Handle(fd))
}

// Windows mode bits do not map to POSIX permissions. The cache root is created
// by the current user and its ACL remains authoritative; silently claiming a
// chmod succeeded would weaken that boundary.
func Fchmod(fd int, mode uint32) error {
	if mode != 0o700 {
		return fmt.Errorf("Windows cannot apply POSIX mode %#o", mode)
	}
	return nil
}
func SyncFile(fd int) error      { return windows.FlushFileBuffers(windows.Handle(fd)) }
func SyncDirectory(fd int) error { return windows.FlushFileBuffers(windows.Handle(fd)) }

func Flock(fd int, operation int) error {
	if operation&LOCK_UN != 0 {
		return windows.UnlockFileEx(windows.Handle(fd), 0, 1, 0, &windows.Overlapped{})
	}
	flags := uint32(windows.LOCKFILE_FAIL_IMMEDIATELY)
	if operation&LOCK_EX != 0 {
		flags |= windows.LOCKFILE_EXCLUSIVE_LOCK
	}
	return windows.LockFileEx(windows.Handle(fd), flags, 0, 1, 0, &windows.Overlapped{})
}

// NormalizeError turns NT native statuses into the Win32 errors shared code
// compares with errors.Is (for example ERROR_FILE_NOT_FOUND and ERROR_SHARING_VIOLATION).
func NormalizeError(err error) error {
	var status windows.NTStatus
	if !errors.As(err, &status) {
		return err
	}
	if status == windows.STATUS_OBJECT_NAME_COLLISION || status == windows.STATUS_OBJECT_NAME_EXISTS {
		return EEXIST
	}
	return status.Errno()
}
