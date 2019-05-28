# ExtSort

[![Build Status](https://travis-ci.org/bsm/extsort.png?branch=master)](https://travis-ci.org/bsm/extsort)
[![GoDoc](https://godoc.org/github.com/bsm/extsort?status.png)](http://godoc.org/github.com/bsm/extsort)
[![Go Report Card](https://goreportcard.com/badge/github.com/bsm/extsort)](https://goreportcard.com/report/github.com/bsm/extsort)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

External merge sort algorithm, implemented in [Go](https://golang.org). Sort arbitrarily large data sets
with a predictable amount of memory using disk.

## Example:

```go
import(
  "fmt"

  "github.com/bsm/extsort"
)

func main() {
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

}
```
