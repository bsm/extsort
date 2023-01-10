//go:build !unix
// +build !unix

package extsort

import "os"

func newTempFile(dir, pattern string, keepFile bool) (*os.File, error) {
	f, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func closeTempFile(f *os.File, keepFile bool) (err error) {
	if e := f.Close(); e != nil {
		err = e
	}
	if e := os.Remove(f.Name()); e != nil {
		err = e
	}
	return
}
