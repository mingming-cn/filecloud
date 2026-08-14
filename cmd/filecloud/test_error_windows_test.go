//go:build windows

package main

import "golang.org/x/sys/windows"

var testAccessDenied = error(windows.ERROR_ACCESS_DENIED)
