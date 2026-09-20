<!-- What changes and why, in a few sentences. Link the ROADMAP.md item if there is one. -->

## Description


## Checklist

- [ ] `make ci` passes locally (gofmt, vet, golangci-lint from `./bin`, `go mod tidy -diff`, race tests, govulncheck)
- [ ] tests cover the happy path, the edges (empty stream, short-circuit, cancelled context) and the error path; anything that starts goroutines has `defer goleak.VerifyNone(t)`
- [ ] every new public operation has a runnable `Example*` with `// Output:`
- [ ] docs updated where they apply: README tables, `doc.go`, a `CONVENTIONS.md` entry with its reason for a new convention, the matching ROADMAP.md checkbox
- [ ] no exported type carries internal state; no new operator without a concrete use case (rule of three)
- [ ] if performance is the point, `make bench` numbers are in the description; the CI benchstat job is advisory only

## Breaking changes

<!-- None, or: what breaks and how a caller migrates. Pre-v1, breaking changes are allowed and go into the next minor with a note here. -->
