# Changelog

## v0.6.1 (2023-04-11)

### Improvements

- Support removing temp files immediately [#13](https://github.com/bsm/extsort/pull/13)

# v0.6.0 (2023-04-03)

### Improvements

- Keep last item when deduplicating [#15](https://github.com/bsm/extsort/pull/15)
- Use more modern snappy compression implementation [#16](https://github.com/bsm/extsort/pull/16)

### Breaking Changes

- `Less` option has been replaced by more generic `Compare` [#15](https://github.com/bsm/extsort/pull/15)

# v0.5.0 (2021-03-25)

### Improvements

- Better memory allocation, improved buffer pooling [#5](https://github.com/bsm/extsort/pull/5)

### New Features

- Allow to remove duplicates [#6](https://github.com/bsm/extsort/pull/6)
- Added map-style API [#7](https://github.com/bsm/extsort/pull/7), see [example](https://pkg.go.dev/github.com/bsm/extsort#example-map)
- Allow custom sort algorithms [#8](https://github.com/bsm/extsort/pull/8)
- Expose size [#9](https://github.com/bsm/extsort/pull/9)
