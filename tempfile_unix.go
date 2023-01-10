//go:build unix
// +build unix

package extsort

import "os"

func newTempFile(dir, pattern string, keepFile bool) (*os.File, error) {
	f, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return nil, err
	}
	if keepFile {
		return f, nil
	}
	// immediately remove for less chance of orphaning.
	if err := os.Remove(f.Name()); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func closeTempFile(f *os.File, keepFile bool) (err error) {
	if e := f.Close(); e != nil {
		err = e
	}
	if !keepFile {
		return
	}
	if e := os.Remove(f.Name()); e != nil {
		err = e
	}
	return
}
