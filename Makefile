default: test

test:
	go test ./...

bench:
	go test ./... -run=NONE -bench=. -benchmem

errcheck:
	errcheck ./...

README.md: README.md.tpl $(wildcard *.go)
	becca -package github.com/bsm/extsort
