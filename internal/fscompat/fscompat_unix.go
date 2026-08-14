//go:build linux || darwin

// Package fscompat isolates handle-relative filesystem operations used by the client.
package fscompat

import "golang.org/x/sys/unix"

type Stat_t = unix.Stat_t
type Timespec = unix.Timespec

const (
	AT_SYMLINK_NOFOLLOW = unix.AT_SYMLINK_NOFOLLOW
	AT_REMOVEDIR        = unix.AT_REMOVEDIR
	O_RDONLY            = unix.O_RDONLY
	O_WRONLY            = unix.O_WRONLY
	O_CREAT             = unix.O_CREAT
	O_EXCL              = unix.O_EXCL
	O_DIRECTORY         = unix.O_DIRECTORY
	O_NOFOLLOW          = unix.O_NOFOLLOW
	O_CLOEXEC           = unix.O_CLOEXEC
	O_NONBLOCK          = unix.O_NONBLOCK
	S_IFMT              = unix.S_IFMT
	S_IFREG             = unix.S_IFREG
	S_IFDIR             = unix.S_IFDIR
	ENOENT              = unix.ENOENT
	EEXIST              = unix.EEXIST
	ENOTEMPTY           = unix.ENOTEMPTY
	EWOULDBLOCK         = unix.EWOULDBLOCK
	EAGAIN              = unix.EAGAIN
	LOCK_EX             = unix.LOCK_EX
	LOCK_NB             = unix.LOCK_NB
	LOCK_UN             = unix.LOCK_UN
)

func Open(path string, flags int, mode uint32) (int, error) { return unix.Open(path, flags, mode) }
func Openat(dirfd int, path string, flags int, mode uint32) (int, error) {
	return unix.Openat(dirfd, path, flags, mode)
}
func Fstat(fd int, stat *Stat_t) error { return unix.Fstat(fd, stat) }
func Fstatat(dirfd int, path string, stat *Stat_t, flags int) error {
	return unix.Fstatat(dirfd, path, stat, flags)
}
func Lstat(path string, stat *Stat_t) error             { return unix.Lstat(path, stat) }
func Dup(fd int) (int, error)                           { return unix.Dup(fd) }
func Close(fd int) error                                { return unix.Close(fd) }
func Mkdirat(dirfd int, path string, mode uint32) error { return unix.Mkdirat(dirfd, path, mode) }
func Unlinkat(dirfd int, path string, flags int) error  { return unix.Unlinkat(dirfd, path, flags) }
func Ftruncate(fd int, size int64) error                { return unix.Ftruncate(fd, size) }
func Fchmod(fd int, mode uint32) error                  { return unix.Fchmod(fd, mode) }
func Flock(fd int, operation int) error                 { return unix.Flock(fd, operation) }
