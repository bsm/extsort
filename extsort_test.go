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
	"testing"

	"github.com/bsm/extsort"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
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

	drain := func(s *extsort.Sorter) ([]string, error) {
		iter, err := s.Sort()
		if err != nil {
			return nil, err
		}
		defer iter.Close()

		read := make([]string, 0, 4)
		for iter.Next() {
			read = append(read, string(iter.Data()))
		}
		if err := iter.Err(); err != nil {
			return nil, err
		}
		return read, iter.Close()
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

	It("appends/sorts data", func() {
		Expect(subject.Append([]byte("foo"))).To(Succeed())
		Expect(subject.Append([]byte("bar"))).To(Succeed())
		Expect(subject.Append([]byte("baz"))).To(Succeed())
		Expect(subject.Append([]byte("foo"))).To(Succeed())
		Expect(subject.Append([]byte("dau"))).To(Succeed())
		Expect(subject.Append([]byte("bar"))).To(Succeed())
		Expect(drain(subject)).To(Equal([]string{"bar", "bar", "baz", "dau", "foo", "foo"}))
	})

	It("can de-duplicate", func() {
		subject = extsort.New(&extsort.Options{
			BufferSize: 64 * 1024,
			Dedupe:     bytes.Equal,
			WorkDir:    workDir,
		})

		for i := 0; i < 100_000; i++ {
			Expect(subject.Append([]byte("foo"))).To(Succeed())
			Expect(subject.Append([]byte("baz"))).To(Succeed())
		}
		Expect(subject.Append([]byte("bar"))).To(Succeed())
		Expect(subject.Append([]byte("dau"))).To(Succeed())
		Expect(drain(subject)).To(Equal([]string{"bar", "baz", "dau", "foo"}))
	})

	It("supports compression", func() {
		compressed := extsort.New(&extsort.Options{
			BufferSize:  1024 * 1024,
			WorkDir:     workDir,
			Compression: extsort.CompressionGzip,
		})
		defer compressed.Close()

		Expect(compressed.Append([]byte("foo"))).To(Succeed())
		Expect(compressed.Append([]byte("bar"))).To(Succeed())
		Expect(compressed.Append([]byte("baz"))).To(Succeed())
		Expect(compressed.Append([]byte("dau"))).To(Succeed())
		Expect(drain(compressed)).To(Equal([]string{"bar", "baz", "dau", "foo"}))
	})

	It("compresses temporary files", func() {
		compressed := extsort.New(&extsort.Options{
			BufferSize:  1024 * 1024,
			WorkDir:     workDir,
			Compression: extsort.CompressionGzip,
		})
		defer compressed.Close()

		for i := 0; i < 200; i++ {
			Expect(compressed.Append([]byte("foo"))).To(Succeed())
		}
		Expect(drain(compressed)).To(HaveLen(200))
		Expect(fileSize()).To(BeNumerically("~", 50, 5))
	})

	It("copies values", func() {
		var val []byte
		Expect(subject.Append(append(val[:0], "foo"...))).To(Succeed())
		Expect(subject.Append(append(val[:0], "bar"...))).To(Succeed())
		Expect(subject.Append(append(val[:0], "baz"...))).To(Succeed())
		Expect(subject.Append(append(val[:0], "dau"...))).To(Succeed())
		Expect(drain(subject)).To(Equal([]string{"bar", "baz", "dau", "foo"}))
	})

	It("does not fail when blank", func() {
		Expect(drain(subject)).To(BeEmpty())
	})

	It("sorts large data sets with constant memory", func() {
		fix, err := seedFixture()
		Expect(err).NotTo(HaveOccurred())
		defer fix.Close()

		for fix.Scan() {
			Expect(subject.Append(fix.Bytes())).To(Succeed())
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
		Expect(memUsed()).To(BeNumerically("<", 1024))
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

	for i := 0; i < 1e6; i++ {
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
