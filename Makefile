.DEFAULT_GOAL := help

.PHONY: help lint lint-code fmt vet tests test-race testsuite testsuite-deps run build up down dev codegen tools clean-gen ci

TOOLS_BIN := $(CURDIR)/.bin
APP_BIN := $(TOOLS_BIN)/scipio
GO_CACHE ?= $(CURDIR)/.cache/go-build
GOLANGCI_LINT_CACHE_DIR ?= $(CURDIR)/.cache/golangci-lint
TESTSUITE_VENV ?= $(CURDIR)/.testsuite-venv
TESTSUITE_PYTHON := $(TESTSUITE_VENV)/bin/python
TESTSUITE_DEPS_STAMP := $(TESTSUITE_VENV)/.deps.stamp
COMPOSE ?= docker compose
PROTOC_GEN_GO_VERSION ?= v1.36.11
PROTOC_GEN_GO_GRPC_VERSION ?= v1.6.2
OAPI_CODEGEN_VERSION ?= v2.7.0
SQLC_VERSION ?= v1.30.0
PROTOC_GEN_GO := $(TOOLS_BIN)/protoc-gen-go
PROTOC_GEN_GO_GRPC := $(TOOLS_BIN)/protoc-gen-go-grpc
OAPI_CODEGEN := $(TOOLS_BIN)/oapi-codegen
SQLC := $(TOOLS_BIN)/sqlc

PROTO_SRC_DIR := api/proto
PROTO_FILE := saga.proto
PROTO_OUT_DIR := gen/proto
OPENAPI_FILE := api/openapi/saga.yaml
OPENAPI_CONFIG := api/openapi/oapi-codegen.yaml
OPENAPI_OUT_DIR := gen/openapi

help:
	@printf "%s\n" \
		"Available targets:" \
		"  help           Show this help" \
		"  up             Start PostgreSQL and Redis via docker compose" \
		"  down           Stop docker compose services" \
		"  dev            One-command local start (up + run)" \
		"  run            Run scipio from source" \
		"  build          Build binary into .bin/scipio" \
		"  fmt            Run go fmt" \
		"  vet            Run go vet" \
		"  lint           Run golangci-lint" \
		"  tests          Run go tests with -tags test_dep" \
		"  test-race      Run go tests with race detector" \
		"  testsuite      Run Python functional tests" \
		"  codegen        Regenerate proto/openapi/sqlc artifacts"

fmt:
	mkdir -p $(GO_CACHE)
	GOCACHE=$(GO_CACHE) go fmt ./...

vet:
	mkdir -p $(GO_CACHE)
	GOCACHE=$(GO_CACHE) go vet ./...

lint:
	mkdir -p $(GO_CACHE) $(GOLANGCI_LINT_CACHE_DIR)
	GOCACHE=$(GO_CACHE) GOLANGCI_LINT_CACHE=$(GOLANGCI_LINT_CACHE_DIR) golangci-lint run ./...

lint-code: lint

tests:
	mkdir -p $(GO_CACHE)
	GOCACHE=$(GO_CACHE) go test -count=1 -tags test_dep ./...

test-race:
	mkdir -p $(GO_CACHE)
	GOCACHE=$(GO_CACHE) go test -count=1 -race -tags test_dep ./...

testsuite-deps: $(TESTSUITE_DEPS_STAMP)

$(TESTSUITE_PYTHON):
	python3 -m venv $(TESTSUITE_VENV)

$(TESTSUITE_DEPS_STAMP): testsuite/requirements.txt | $(TESTSUITE_PYTHON)
	$(TESTSUITE_PYTHON) -m pip install -r testsuite/requirements.txt
	touch $(TESTSUITE_DEPS_STAMP)

testsuite: testsuite-deps
	$(TESTSUITE_PYTHON) -m pytest testsuite

run:
	go run ./cmd/scipio

build:
	mkdir -p $(TOOLS_BIN) $(GO_CACHE)
	GOCACHE=$(GO_CACHE) go build -o $(APP_BIN) ./cmd/scipio

up:
	$(COMPOSE) up -d postgres redis

down:
	$(COMPOSE) down --remove-orphans

dev: up run

tools: $(PROTOC_GEN_GO) $(PROTOC_GEN_GO_GRPC) $(OAPI_CODEGEN) $(SQLC)

$(PROTOC_GEN_GO):
	mkdir -p $(TOOLS_BIN)
	GOBIN=$(TOOLS_BIN) go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)

$(PROTOC_GEN_GO_GRPC):
	mkdir -p $(TOOLS_BIN)
	GOBIN=$(TOOLS_BIN) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

$(OAPI_CODEGEN):
	mkdir -p $(TOOLS_BIN)
	GOBIN=$(TOOLS_BIN) go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@$(OAPI_CODEGEN_VERSION)

$(SQLC):
	mkdir -p $(TOOLS_BIN)
	GOBIN=$(TOOLS_BIN) go install github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION)

codegen: tools clean-gen
	mkdir -p $(PROTO_OUT_DIR) $(OPENAPI_OUT_DIR)
	protoc \
		--proto_path=$(PROTO_SRC_DIR) \
		--plugin=protoc-gen-go=$(PROTOC_GEN_GO) \
		--plugin=protoc-gen-go-grpc=$(PROTOC_GEN_GO_GRPC) \
		--go_out=$(PROTO_OUT_DIR) \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_OUT_DIR) \
		--go-grpc_opt=paths=source_relative \
		--descriptor_set_out=$(PROTO_OUT_DIR)/saga.pb \
		--include_imports \
		$(PROTO_FILE)
	$(OAPI_CODEGEN) --config $(OPENAPI_CONFIG) $(OPENAPI_FILE)
	$(SQLC) generate

clean-gen:
	rm -f $(PROTO_OUT_DIR)/*.pb.go $(PROTO_OUT_DIR)/*.pb $(OPENAPI_OUT_DIR)/*.gen.go $(OPENAPI_OUT_DIR)/*.openapi.yaml

ci: lint tests testsuite
