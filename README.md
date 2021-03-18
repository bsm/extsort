# ExtSort

[![Go Reference](https://pkg.go.dev/badge/github.com/bsm/extsort.svg)](https://pkg.go.dev/github.com/bsm/extsort)
[![Test](https://github.com/bsm/extsort/actions/workflows/test.yml/badge.svg)](https://github.com/bsm/extsort/actions/workflows/test.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

External merge sort algorithm, implemented in [Go](https://golang.org). Sort arbitrarily large data sets
with a predictable amount of memory using disk.

## Example:

Sorting lines:

```go
import(
  "fmt"

  "github.com/bsm/extsort"
)

func main() {
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

}
```

Map-style API with de-duplication:

```go
import(
  "fmt"

  "github.com/bsm/extsort"
)

func main() {
	// Init with de-duplication.
	sorter := extsort.New(&extsort.Options{
		Dedupe: bytes.Equal,
	})
	defer sorter.Close()

	// Put key/value data.
	_ = sorter.Put([]byte("foo"), []byte("v1"))
	_ = sorter.Put([]byte("bar"), []byte("v2"))
	_ = sorter.Put([]byte("baz"), []byte("v3"))
	_ = sorter.Put([]byte("bar"), []byte("v4"))	// duplicate
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

}
```
