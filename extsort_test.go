package extsort_test

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"testing"

	"github.com/bsm/extsort"
)

func newTestSorter(t *testing.T) (*extsort.Sorter, string) {
	t.Helper()

	workDir, err := os.MkdirTemp("", "extsort-test")
	if err != nil {
		t.Fatal(err)
	}

	subject := extsort.New(&extsort.Options{
		BufferSize: 1024 * 1024,
		WorkDir:    workDir,
	})

	t.Cleanup(func() {
		if err := subject.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
		if entries, err := filepath.Glob(workDir + "/*"); err != nil {
			t.Errorf("Glob: %v", err)
		} else if len(entries) != 0 {
			t.Errorf("expected no leftover files, got %v", entries)
		}
		if err := os.RemoveAll(workDir); err != nil {
			t.Errorf("RemoveAll: %v", err)
		}
	})

	return subject, workDir
}

func memUsed() uint64 {
	var ms runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&ms)
	return ms.Alloc / 1024
}

func drain(s *extsort.Sorter) ([][2]string, error) {
	iter, err := s.Sort()
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	read := make([][2]string, 0, 4)
	for iter.Next() {
		read = append(read, [2]string{string(iter.Key()), string(iter.Value())})
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return read, iter.Close()
}

func sortedKeys(s *extsort.Sorter) ([]string, error) {
	pairs, err := drain(s)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		keys = append(keys, pair[0])
	}
	return keys, nil
}

func soleFileSize(workDir string) (int64, error) {
	entries, err := filepath.Glob(workDir + "/*")
	if err != nil {
		return 0, err
	} else if len(entries) != 1 {
		return 0, fmt.Errorf("expected one file: %v", entries)
	}

	info, err := os.Stat(entries[0])
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func mustPut(t *testing.T, s *extsort.Sorter, key, value string) {
	t.Helper()
	if err := s.Put([]byte(key), []byte(value)); err != nil {
		t.Fatalf("Put(%q, %q): %v", key, value, err)
	}
}

func mustAppend(t *testing.T, s *extsort.Sorter, data string) {
	t.Helper()
	if err := s.Append([]byte(data)); err != nil {
		t.Fatalf("Append(%q): %v", data, err)
	}
}

func mustDrain(t *testing.T, s *extsort.Sorter) [][2]string {
	t.Helper()
	pairs, err := drain(s)
	if err != nil {
		t.Fatal(err)
	}
	return pairs
}

func mustKeys(t *testing.T, s *extsort.Sorter) []string {
	t.Helper()
	keys, err := sortedKeys(s)
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func TestSorter_PutSort(t *testing.T) {
	subject, _ := newTestSorter(t)

	mustPut(t, subject, "foo", "v1")
	mustPut(t, subject, "bar", "v2")
	mustPut(t, subject, "baz", "v3")
	mustPut(t, subject, "foo", "v4")
	mustPut(t, subject, "dau", "v5")
	mustPut(t, subject, "bar", "v6")

	if got, exp := mustDrain(t, subject), [][2]string{
		{"bar", "v6"},
		{"bar", "v2"},
		{"baz", "v3"},
		{"dau", "v5"},
		{"foo", "v4"},
		{"foo", "v1"},
	}; !reflect.DeepEqual(got, exp) {
		t.Errorf("got %v, want %v", got, exp)
	}
}

func TestSorter_AppendSort(t *testing.T) {
	subject, _ := newTestSorter(t)

	mustAppend(t, subject, "foo")
	mustAppend(t, subject, "bar")
	mustAppend(t, subject, "baz")
	mustAppend(t, subject, "foo")
	mustAppend(t, subject, "dau")
	mustAppend(t, subject, "bar")

	if got, exp := mustDrain(t, subject), [][2]string{
		{"bar", ""},
		{"bar", ""},
		{"baz", ""},
		{"dau", ""},
		{"foo", ""},
		{"foo", ""},
	}; !reflect.DeepEqual(got, exp) {
		t.Errorf("got %v, want %v", got, exp)
	}
}

func TestSorter_Dedupe(t *testing.T) {
	_, workDir := newTestSorter(t)

	deduped := extsort.New(&extsort.Options{
		BufferSize: 64 * 1024,
		Dedupe:     bytes.Equal,
		WorkDir:    workDir,
		Sort:       sort.Stable,
	})
	defer deduped.Close()

	for i := range 100_000 {
		val := fmt.Appendf(nil, "x%d", i)
		if err := deduped.Put([]byte("foo"), val); err != nil {
			t.Fatal(err)
		}
		if err := deduped.Put([]byte("baz"), val); err != nil {
			t.Fatal(err)
		}
	}
	mustPut(t, deduped, "bar", "v1")
	mustPut(t, deduped, "dau", "v2")

	if got, exp := mustDrain(t, deduped), [][2]string{
		{"bar", "v1"},
		{"baz", "x99999"},
		{"dau", "v2"},
		{"foo", "x99999"},
	}; !reflect.DeepEqual(got, exp) {
		t.Errorf("got %v, want %v", got, exp)
	}
}

func TestSorter_CustomSort(t *testing.T) {
	_, workDir := newTestSorter(t)

	reverse := extsort.New(&extsort.Options{
		BufferSize: 1024 * 1024,
		WorkDir:    workDir,
		Sort:       func(v sort.Interface) { sort.Sort(sort.Reverse(v)) },
	})
	defer reverse.Close()

	mustAppend(t, reverse, "foo")
	mustAppend(t, reverse, "bar")
	mustAppend(t, reverse, "baz")
	mustAppend(t, reverse, "dau")

	if got, exp := mustKeys(t, reverse), []string{"foo", "dau", "baz", "bar"}; !reflect.DeepEqual(got, exp) {
		t.Errorf("got %v, want %v", got, exp)
	}
}

func TestSorter_Compression(t *testing.T) {
	for _, tc := range []struct {
		name string
		comp extsort.Compression
	}{
		{"gzip", extsort.CompressionGzip},
		{"snappy", extsort.CompressionSnappy},
		{"zstd", extsort.CompressionZstd},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, workDir := newTestSorter(t)

			compressed := extsort.New(&extsort.Options{
				BufferSize:  1024 * 1024,
				Dedupe:      bytes.Equal,
				WorkDir:     workDir,
				Compression: tc.comp,
			})
			defer compressed.Close()

			for range 100 {
				mustAppend(t, compressed, "foo")
				mustAppend(t, compressed, "bar")
				mustAppend(t, compressed, "baz")
				mustAppend(t, compressed, "dau")
			}

			if got, exp := mustKeys(t, compressed), []string{"bar", "baz", "dau", "foo"}; !reflect.DeepEqual(got, exp) {
				t.Errorf("got %v, want %v", got, exp)
			}
		})
	}
}

func TestSorter_CompressTempFiles(t *testing.T) {
	for _, tc := range []struct {
		name    string
		comp    extsort.Compression
		expSize int64
	}{
		{"gzip", extsort.CompressionGzip, 400},
		{"snappy", extsort.CompressionSnappy, 10400},
		{"zstd", extsort.CompressionZstd, 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, workDir := newTestSorter(t)

			compressed := extsort.New(&extsort.Options{
				BufferSize:  1024 * 1024,
				WorkDir:     workDir,
				Compression: tc.comp,
				KeepFiles:   true,
			})
			defer compressed.Close()

			val := bytes.Repeat([]byte{'x'}, 4096)
			for range 50 {
				if err := compressed.Put([]byte("foo"), val); err != nil {
					t.Fatal(err)
				}
			}
			if got := mustDrain(t, compressed); len(got) != 50 {
				t.Fatalf("expected 50 entries, got %d", len(got))
			}

			size, err := soleFileSize(workDir)
			if err != nil {
				t.Fatal(err)
			}
			if diff := size - tc.expSize; diff < -100 || diff > 100 {
				t.Errorf("got file size %d, want ~%d", size, tc.expSize)
			}
		})
	}
}

func TestSorter_CopiesValues(t *testing.T) {
	subject, _ := newTestSorter(t)

	var val []byte
	for _, s := range []string{"foo", "bar", "baz", "dau"} {
		if err := subject.Append(append(val[:0], s...)); err != nil {
			t.Fatal(err)
		}
	}

	if got, exp := mustKeys(t, subject), []string{"bar", "baz", "dau", "foo"}; !reflect.DeepEqual(got, exp) {
		t.Errorf("got %v, want %v", got, exp)
	}
}

func TestSorter_Blank(t *testing.T) {
	subject, _ := newTestSorter(t)

	if got := mustDrain(t, subject); len(got) != 0 {
		t.Errorf("expected no entries, got %v", got)
	}
}

func TestSorter_ConstantMemory(t *testing.T) {
	subject, _ := newTestSorter(t)

	val := bytes.Repeat([]byte{'x'}, 1024)

	fix, err := seedFixture()
	if err != nil {
		t.Fatal(err)
	}
	defer fix.Close()

	for fix.Scan() {
		if err := subject.Put(fix.Bytes(), val); err != nil {
			t.Fatal(err)
		}
	}
	if err := fix.Err(); err != nil {
		t.Fatal(err)
	}
	if used := memUsed(); used >= 4096 {
		t.Errorf("expected memory usage < 4096, got %d", used)
	}

	iter, err := subject.Sort()
	if err != nil {
		t.Fatal(err)
	}
	if used := memUsed(); used >= 4096 {
		t.Errorf("expected memory usage < 4096, got %d", used)
	}
	defer iter.Close()

	var prev []byte
	for iter.Next() {
		data := iter.Data()
		if bytes.Compare(prev, data) != -1 {
			t.Fatalf("expected %q to be < than %q", prev, data)
		}
		prev = append(prev[:0], data...)
	}
	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}
	if used := memUsed(); used >= 2048 {
		t.Errorf("expected memory usage < 2048, got %d", used)
	}
}

func TestSorter_Size(t *testing.T) {
	subject, _ := newTestSorter(t)

	mustAppend(t, subject, "foo")
	if got, exp := subject.Size(), int64(3); got != exp {
		t.Errorf("got %d, want %d", got, exp)
	}

	rnd := rand.New(rand.NewSource(33))
	buf := make([]byte, 100)
	for range 10_000 {
		buf = buf[:rnd.Intn(cap(buf))]
		if n, err := rnd.Read(buf); err != nil {
			t.Fatal(err)
		} else if n != len(buf) {
			t.Fatalf("read %d bytes, want %d", n, len(buf))
		}
		if err := subject.Append(buf); err != nil {
			t.Fatal(err)
		}
	}
	if got, exp := subject.Size(), int64(491_063); got != exp {
		t.Errorf("got %d, want %d", got, exp)
	}
}

// --------------------------------------------------------------------

func seedData() (string, error) {
	f, err := os.CreateTemp("", "extsort-test")
	if err != nil {
		return "", err
	}
	defer f.Close()

	rnd := rand.New(rand.NewSource(33))
	buf := make([]byte, 100)
	b64 := base64.RawStdEncoding
	val := make([]byte, b64.EncodedLen(len(buf)))

	for range int(1e5) {
		buf = buf[:20+rnd.Intn(40)]
		val = val[:b64.EncodedLen(len(buf))]

		if _, err := rnd.Read(buf); err != nil {
			return "", err
		}
		b64.Encode(val, buf)
		if _, err := f.Write(append(val, '\n')); err != nil {
			return "", err
		}
	}
	return f.Name(), f.Close()
}

type fixture struct {
	*bufio.Scanner
	f *os.File
}

func seedFixture() (*fixture, error) {
	fn, err := seedData()
	if err != nil {
		return nil, err
	}

	fix := new(fixture)
	return fix, fix.Reset(fn)
}

func (f *fixture) Reset(fn string) error {
	if f.f != nil {
		_ = f.f.Close()
	}

	file, err := os.Open(fn)
	if err != nil {
		return err
	}

	f.f = file
	f.Scanner = bufio.NewScanner(file)
	return nil
}

func (f *fixture) Close() error {
	err := f.f.Close()
	_ = os.RemoveAll(f.f.Name())
	return err
}
