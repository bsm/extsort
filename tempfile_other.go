//go:build !linux

package extsort

import "os"

// Copyright 2017 Tom Thorogood. All rights reserved.
// Use of this source code is governed by a Modified
// BSD License that can be found in the LICENSE file.
func createTempFile(dir string) (f *os.File, remove bool, err error) {
	f, err := os.CreateTemp(dir, "extsort")
	return f, err == nil, err
}
