package extsort_test

import (
	"testing"

	"github.com/bsm/extsort"
)

func BenchmarkSorter(b *testing.B) {
	fix, err := seedFixture()
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = fix.Close() }()

	sorter := extsort.New(&extsort.Options{
		BufferSize: 2 * 1024 * 1024,
	})
	defer func() { _ = sorter.Close() }()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if !fix.Scan() {
			b.StopTimer()
			if err := fix.Reset(fix.f.Name()); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
		if err := sorter.Append(fix.Bytes()); err != nil {
			b.Fatal(err)
		}
	}

	iter, err := sorter.Sort()
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = iter.Close() }()

	for i := range b.N {
		if !iter.Next() {
			b.Fatalf("cannot advance to chunk %d", i+1)
		}
	}
	if err := iter.Err(); err != nil {
		b.Fatal(err)
	}
}
