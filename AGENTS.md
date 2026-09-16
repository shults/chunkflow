# Working on ChunkFlow

Operating instructions for contributors and AI assistants. This file is deliberately short:
design decisions and their reasons live in [CONVENTIONS.md](CONVENTIONS.md), the plan in
[ROADMAP.md](ROADMAP.md), the user-facing overview in [README.md](README.md) and `doc.go`.
If anything here contradicts CONVENTIONS.md, CONVENTIONS.md wins and this file gets fixed.

## Before you start

```bash
make setup   # installs golangci-lint + govulncheck into ./bin and enables the pre-commit hook
```

Never use a globally installed `golangci-lint`: the module targets Go 1.27 and older binaries
refuse to run or panic inside staticcheck. Always go through `make lint` / `./bin/golangci-lint`.

## The loop

```bash
make check   # gofmt, go vet, golangci-lint, go mod tidy -diff  (the pre-commit hook runs the same)
make test    # go test -race -shuffle=on
make cover   # per-function coverage; 100% statement coverage is the working target
make ci      # check + test + govulncheck, mirrors .github/workflows/ci.yml
```

Prefer wrapping long-running shell steps in `timeout`.

## What a change must include

- Tests for the happy path, the edge cases (empty stream, short-circuit, cancelled context) and
  the error path. Anything that starts goroutines gets `defer goleak.VerifyNone(t)`; the package
  already runs `goleak.VerifyTestMain`.
- A runnable `Example*` with `// Output:` for every new public operation — they are compiled, run
  and shown in godoc, so they cannot go stale.
- Docs: the README tables, `doc.go` if the overview changes, a `CONVENTIONS.md` entry (with the
  reason) if a new convention appears, and the matching ROADMAP.md checkbox.
- When a semantic question is disputed, settle it with a failing test or a mutation check before
  arguing about it.

## Rules that are easy to break by accident

- Intermediate operators forward errored elements with `if !yield(...) { return }; continue` and
  never `return` on an error, never drop one, never log one. Terminals go through `each`.
- Every derived `Stream` is built through `derive` / `errStream` so `ctx` and options propagate.
- Do not export a type to carry internal state; expose a sentinel or a constructor instead.
- No new operator without a concrete use case (rule of three); when in doubt, add an example
  showing the composition instead.

## Git

- Only `master` exists. Commits are made by the repository owner; assistants stage changes and
  report what is staged. Amend or tag only when explicitly asked, and check the commit is not
  pushed first (`git status -sb`).
