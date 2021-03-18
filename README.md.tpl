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

func main() {{ "Example_plain" | code }}
```

Map-style API with de-duplication:

```go
import(
  "fmt"

  "github.com/bsm/extsort"
)

func main() {{ "Example_map" | code }}
```
