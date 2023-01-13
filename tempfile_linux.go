//go:build linux

package extsort

import (
	"os"
	"strconv"
	"sync/atomic"
	"syscall"

	"golang.org/x/sys/unix"
)

var noOTmpFileSupport uint32

// Copyright 2017 Tom Thorogood. All rights reserved.
// Use of this source code is governed by a Modified
// BSD License that can be found in the LICENSE file.
func createTempFile(dir string) (f *os.File, remove bool, err error) {
	if dir == "" {
		dir = os.TempDir()
	}

	if atomic.LoadUint32(&noOTmpFileSupport) != 0 {
		f, err := os.CreateTemp(dir, "extsort")
		return f, err == nil, err
	}

	fd, err := unix.Open(dir, unix.O_RDWR|unix.O_TMPFILE|unix.O_CLOEXEC, 0600)
	switch err {
	case nil:
	case syscall.EISDIR:
		atomic.StoreUint32(&noOTmpFileSupport, 1)
		fallthrough
	case syscall.EOPNOTSUPP:
		f, err := os.CreateTemp(dir, "extsort")
		return f, err == nil, err
	default:
		return nil, false, &os.PathError{
			Op:   "open",
			Path: dir,
			Err:  err,
		}
	}

	path := "/proc/self/fd/" + strconv.FormatUint(uint64(fd), 10)
	return os.NewFile(uintptr(fd), path), false, nil
}
