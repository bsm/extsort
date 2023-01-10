package extsort_test

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"

	"github.com/bsm/extsort"

	. "github.com/bsm/ginkgo"
	. "github.com/bsm/gomega"
)

var _ = Describe("Sorter", func() {
	var subject *extsort.Sorter
	var workDir string

	memUsed := func() uint64 {
		var ms runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&ms)
		return ms.Alloc / 1024
	}

	drain := func(s *extsort.Sorter) ([][2]string, error) {
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

	keys := func(s *extsort.Sorter) ([]string, error) {
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

	fileSize := func() (int64, error) {
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

	BeforeEach(func() {
		var err error
		workDir, err = ioutil.TempDir("", "extsort-test")
		Expect(err).NotTo(HaveOccurred())

		subject = extsort.New(&extsort.Options{
			BufferSize: 1024 * 1024,
			WorkDir:    workDir,
		})
	})

	AfterEach(func() {
		Expect(subject.Close()).To(Succeed())
		Expect(filepath.Glob(workDir + "/*")).To(BeEmpty())
		Expect(os.RemoveAll(workDir)).To(Succeed())
	})

	It("puts/sorts data", func() {
		Expect(subject.Put([]byte("foo"), []byte("v1"))).To(Succeed())
		Expect(subject.Put([]byte("bar"), []byte("v2"))).To(Succeed())
		Expect(subject.Put([]byte("baz"), []byte("v3"))).To(Succeed())
		Expect(subject.Put([]byte("foo"), []byte("v4"))).To(Succeed())
		Expect(subject.Put([]byte("dau"), []byte("v5"))).To(Succeed())
		Expect(subject.Put([]byte("bar"), []byte("v6"))).To(Succeed())
		Expect(drain(subject)).To(Equal([][2]string{
			{"bar", "v2"},
			{"bar", "v6"},
			{"baz", "v3"},
			{"dau", "v5"},
			{"foo", "v1"},
			{"foo", "v4"},
		}))
	})

	It("appends/sorts data", func() {
		Expect(subject.Append([]byte("foo"))).To(Succeed())
		Expect(subject.Append([]byte("bar"))).To(Succeed())
		Expect(subject.Append([]byte("baz"))).To(Succeed())
		Expect(subject.Append([]byte("foo"))).To(Succeed())
		Expect(subject.Append([]byte("dau"))).To(Succeed())
		Expect(subject.Append([]byte("bar"))).To(Succeed())
		Expect(drain(subject)).To(Equal([][2]string{
			{"bar", ""},
			{"bar", ""},
			{"baz", ""},
			{"dau", ""},
			{"foo", ""},
			{"foo", ""},
		}))
	})

	It("can de-duplicate", func() {
		deduped := extsort.New(&extsort.Options{
			BufferSize: 64 * 1024,
			Dedupe:     bytes.Equal,
			WorkDir:    workDir,
			Sort:       sort.Stable,
		})
		defer deduped.Close()

		for i := 0; i < 100_000; i++ {
			val := []byte(fmt.Sprintf("x%d", i%10))
			Expect(deduped.Put([]byte("foo"), val)).To(Succeed())
			Expect(deduped.Put([]byte("baz"), val)).To(Succeed())
		}
		Expect(deduped.Put([]byte("bar"), []byte("v1"))).To(Succeed())
		Expect(deduped.Put([]byte("dau"), []byte("v2"))).To(Succeed())
		Expect(drain(deduped)).To(Equal([][2]string{
			{"bar", "v1"},
			{"baz", "x4"},
			{"dau", "v2"},
			{"foo", "x9"},
		}))
	})

	It("supports custom sorting", func() {
		reverse := extsort.New(&extsort.Options{
			BufferSize: 1024 * 1024,
			WorkDir:    workDir,
			Sort:       func(v sort.Interface) { sort.Sort(sort.Reverse(v)) },
		})
		defer reverse.Close()

		Expect(reverse.Append([]byte("foo"))).To(Succeed())
		Expect(reverse.Append([]byte("bar"))).To(Succeed())
		Expect(reverse.Append([]byte("baz"))).To(Succeed())
		Expect(reverse.Append([]byte("dau"))).To(Succeed())
		Expect(keys(reverse)).To(Equal([]string{"foo", "dau", "baz", "bar"}))
	})

	Context("supports compression", func() {
		test := func(c extsort.Compression) {
			compressed := extsort.New(&extsort.Options{
				BufferSize:  1024 * 1024,
				Dedupe:      bytes.Equal,
				WorkDir:     workDir,
				Compression: c,
			})
			defer compressed.Close()

			for i := 0; i < 100; i++ {
				Expect(compressed.Append([]byte("foo"))).To(Succeed())
				Expect(compressed.Append([]byte("bar"))).To(Succeed())
				Expect(compressed.Append([]byte("baz"))).To(Succeed())
				Expect(compressed.Append([]byte("dau"))).To(Succeed())
			}
			Expect(keys(compressed)).To(Equal([]string{"bar", "baz", "dau", "foo"}))
		}
		It("gzip compresses", func() { test(extsort.CompressionGzip) })
		It("snappy compresses", func() { test(extsort.CompressionSnappy) })
	})

	Context("compresses temporary files", func() {
		test := func(c extsort.Compression, expSize int) {
			compressed := extsort.New(&extsort.Options{
				BufferSize:  1024 * 1024,
				WorkDir:     workDir,
				Compression: c,
			})
			defer compressed.Close()

			val := bytes.Repeat([]byte{'x'}, 4096)
			for i := 0; i < 50; i++ {
				Expect(compressed.Put([]byte("foo"), val)).To(Succeed())
			}
			Expect(drain(compressed)).To(HaveLen(50))
			Expect(fileSize()).To(BeNumerically("~", expSize, 5))
		}
		It("gzip compresses", func() { test(extsort.CompressionGzip, 420) })
		It("snappy compresses", func() { test(extsort.CompressionSnappy, 9717) })
	})

	It("copies values", func() {
		var val []byte
		Expect(subject.Append(append(val[:0], "foo"...))).To(Succeed())
		Expect(subject.Append(append(val[:0], "bar"...))).To(Succeed())
		Expect(subject.Append(append(val[:0], "baz"...))).To(Succeed())
		Expect(subject.Append(append(val[:0], "dau"...))).To(Succeed())
		Expect(keys(subject)).To(Equal([]string{"bar", "baz", "dau", "foo"}))
	})

	It("does not fail when blank", func() {
		Expect(drain(subject)).To(BeEmpty())
	})

	It("sorts large data sets with constant memory", func() {
		val := bytes.Repeat([]byte{'x'}, 1024)

		fix, err := seedFixture()
		Expect(err).NotTo(HaveOccurred())
		defer fix.Close()

		for fix.Scan() {
			Expect(subject.Put(fix.Bytes(), val)).To(Succeed())
		}
		Expect(fix.Err()).NotTo(HaveOccurred())
		Expect(memUsed()).To(BeNumerically("<", 4096))

		iter, err := subject.Sort()
		Expect(err).NotTo(HaveOccurred())
		Expect(memUsed()).To(BeNumerically("<", 4096))
		defer iter.Close()

		var prev []byte
		for iter.Next() {
			data := iter.Data()
			Expect(bytes.Compare(prev, data)).To(Equal(-1), "expected %q to be < than %q", prev, data)
			prev = append(prev[:0], data...)
		}
		Expect(iter.Err()).NotTo(HaveOccurred())
		Expect(iter.Close()).To(Succeed())
		Expect(memUsed()).To(BeNumerically("<", 2048))
	})

	It("allows access to appended size", func() {
		Expect(subject.Append([]byte("foo"))).To(Succeed())
		Expect(subject.Size()).To(BeNumerically("==", 3))

		rnd := rand.New(rand.NewSource(33))
		buf := make([]byte, 100)
		for i := 0; i < 10_000; i++ {
			buf = buf[:rnd.Intn(cap(buf))]
			Expect(rnd.Read(buf)).To(Equal(len(buf)))
			Expect(subject.Append(buf)).To(Succeed())
		}
		Expect(subject.Size()).To(BeNumerically("==", 491_063))
	})
})

// --------------------------------------------------------------------

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "extsort")
}

// --------------------------------------------------------------------

func seedData() (string, error) {
	f, err := ioutil.TempFile("", "extsort-test")
	if err != nil {
		return "", err
	}
	defer f.Close()

	rnd := rand.New(rand.NewSource(33))
	buf := make([]byte, 100)
	b64 := base64.RawStdEncoding
	val := make([]byte, b64.EncodedLen(len(buf)))

	for i := 0; i < 1e5; i++ {
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
