//go:build linux || darwin

package main

import "syscall"

var testAccessDenied = error(syscall.EACCES)
