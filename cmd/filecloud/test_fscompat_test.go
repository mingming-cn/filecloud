package main

import (
	"os"

	"github.com/mingming-cn/filecloud/internal/fscompat"
)

func testOpenFileStat(file *os.File) (fscompat.Stat_t, error) {
	var stat fscompat.Stat_t
	return stat, fscompat.Fstat(int(file.Fd()), &stat)
}

func testPathStat(path string) (fscompat.Stat_t, error) {
	var stat fscompat.Stat_t
	return stat, fscompat.Lstat(path, &stat)
}
