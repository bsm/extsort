# Changelog

## v0.6.0 (2023-01-13)

### Improvements

- Support `O_TMPFILE` to avoid orphanned files on Linux [#14](https://github.com/bsm/extsort/pull/14)

## v0.5.0 (2021-03-25)

### Improvements

- Better memory allocation, improved buffer pooling [#5](https://github.com/bsm/extsort/pull/5)

### New Features

- Allow to remove duplicates [#6](https://github.com/bsm/extsort/pull/6)
- Added map-style API [#7](https://github.com/bsm/extsort/pull/7), see [example](https://pkg.go.dev/github.com/bsm/extsort#example-map)
- Allow custom sort algorithms [#8](https://github.com/bsm/extsort/pull/8)
- Expose size [#9](https://github.com/bsm/extsort/pull/9)
