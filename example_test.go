package extsort_test

import (
	"bytes"
	"fmt"

	"github.com/bsm/extsort"
)

func Example_map() {
	// Init with de-duplication.
	sorter := extsort.New(&extsort.Options{
		Dedupe: bytes.Equal,
	})
	defer sorter.Close()

	// Put key/value data.
	_ = sorter.Put([]byte("foo"), []byte("v1"))
	_ = sorter.Put([]byte("bar"), []byte("v2"))
	_ = sorter.Put([]byte("baz"), []byte("v3"))
	_ = sorter.Put([]byte("bar"), []byte("v4")) // duplicate
	_ = sorter.Put([]byte("dau"), []byte("v5"))

	// Sort and iterate.
	iter, err := sorter.Sort()
	if err != nil {
		panic(err)
	}
	defer iter.Close()

	for iter.Next() {
		fmt.Println(string(iter.Key()), string(iter.Value()))
	}
	if err := iter.Err(); err != nil {
		panic(err)
	}

	// Output:
	// bar v4
	// baz v3
	// dau v5
	// foo v1
}

func Example_plain() {
	// Init sorter.
	sorter := extsort.New(nil)
	defer sorter.Close()

	// Append plain data.
	_ = sorter.Append([]byte("foo"))
	_ = sorter.Append([]byte("bar"))
	_ = sorter.Append([]byte("baz"))
	_ = sorter.Append([]byte("dau"))

	// Sort and iterate.
	iter, err := sorter.Sort()
	if err != nil {
		panic(err)
	}
	defer iter.Close()

	for iter.Next() {
		fmt.Println(string(iter.Data()))
	}
	if err := iter.Err(); err != nil {
		panic(err)
	}

	// Output:
	// bar
	// baz
	// dau
	// foo
}
