package extsort_test

import (
	"fmt"

	"github.com/bsm/extsort"
)

func Example() {
	// Create sorter.
	sorter := extsort.New(nil)
	defer sorter.Close()

	// Append data.
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
