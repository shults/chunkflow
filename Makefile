GOLANGCI_LINT_VERSION ?= v2.13.2
GOVULNCHECK_VERSION   ?= latest

BIN        := $(CURDIR)/bin
LINT       := $(BIN)/golangci-lint
VULNCHECK  := $(BIN)/govulncheck

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

.PHONY: setup
setup: tools hooks ## Install dev tools into ./bin and enable git hooks

.PHONY: tools
tools: $(LINT) $(VULNCHECK) ## Install golangci-lint and govulncheck into ./bin

$(LINT):
	GOBIN=$(BIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

$(VULNCHECK):
	GOBIN=$(BIN) go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)

.PHONY: hooks
hooks: ## Point git at the versioned hooks in .githooks
	git config core.hooksPath .githooks
	chmod +x .githooks/*

.PHONY: fmt
fmt: ## Format all Go files
	gofmt -w .

.PHONY: fmt-check
fmt-check: ## Fail if any Go file is not gofmt-formatted
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: lint
lint: $(LINT) ## Run golangci-lint
	$(LINT) run ./...

.PHONY: tidy-check
tidy-check: ## Fail if go mod tidy would change go.mod/go.sum
	@go mod tidy && git diff --quiet --exit-code go.mod go.sum || { echo "go mod tidy changed go.mod/go.sum"; exit 1; }

.PHONY: check
check: fmt-check vet lint tidy-check ## Everything the pre-commit hook runs

.PHONY: test
test: ## Run tests with the race detector
	go test -race -shuffle=on -count=1 ./...

.PHONY: cover
cover: ## Run tests and print per-function coverage
	go test -race -coverprofile=coverage.out ./... && go tool cover -func=coverage.out

.PHONY: vuln
vuln: $(VULNCHECK) ## Run govulncheck
	$(VULNCHECK) ./...

.PHONY: ci
ci: check test vuln ## Mirror the CI pipeline locally

.PHONY: clean
clean: ## Remove installed tools and coverage output
	rm -rf $(BIN) coverage.out
