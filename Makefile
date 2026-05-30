BINARY_SERVER  := oncloudkv
BINARY_CLI     := oncloudkv-cli
MODULE         := github.com/makhskham/oncloudkv
GOBIN          ?= $(shell go env GOPATH)/bin
PROTOC         ?= protoc

.PHONY: all build build-server build-cli proto test bench lint docker-build docker-up docker-down k8s-apply k8s-delete clean help

all: proto build

## ── Code generation ──────────────────────────────────────────────────────────
proto:
	@echo "Generating protobuf..."
	$(PROTOC) \
		--proto_path=proto \
		--go_out=proto/gen \
		--go_opt=paths=source_relative \
		--go-grpc_out=proto/gen \
		--go-grpc_opt=paths=source_relative \
		proto/oncloudkv.proto
	@echo "Done."

## ── Build ────────────────────────────────────────────────────────────────────
build: build-server build-cli

build-server:
	@echo "Building server..."
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/$(BINARY_SERVER) ./cmd/server

build-cli:
	@echo "Building CLI..."
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/$(BINARY_CLI) ./cmd/cli

## ── Test & Benchmark ─────────────────────────────────────────────────────────
test:
	go test -v -race -timeout 120s ./...

bench:
	@echo "Running benchmarks - results prove throughput, latency, and failover claims"
	go test -v -run='^$$' -bench=. -benchmem -benchtime=10s ./bench/...

## ── Lint ─────────────────────────────────────────────────────────────────────
lint:
	@which golangci-lint > /dev/null 2>&1 || go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	golangci-lint run ./...

## ── Docker ───────────────────────────────────────────────────────────────────
docker-build:
	docker build -t oncloudkv:latest -f deploy/docker/Dockerfile .

docker-up:
	docker compose -f deploy/docker/docker-compose.yml up -d --build
	@echo "Cluster starting - wait ~5s for leader election, then:"
	@echo "  go run ./cmd/cli put hello world --server localhost:7102"
	@echo "  go run ./cmd/cli get hello --server localhost:7102"

docker-down:
	docker compose -f deploy/docker/docker-compose.yml down -v

docker-logs:
	docker compose -f deploy/docker/docker-compose.yml logs -f

## ── Kubernetes ───────────────────────────────────────────────────────────────
k8s-apply:
	kubectl create namespace oncloudkv --dry-run=client -o yaml | kubectl apply -f -
	kubectl apply -f deploy/k8s/

k8s-delete:
	kubectl delete -f deploy/k8s/ --ignore-not-found
	kubectl delete namespace oncloudkv --ignore-not-found

helm-install:
	helm upgrade --install oncloudkv deploy/helm/ \
		--namespace oncloudkv \
		--create-namespace \
		--wait

helm-uninstall:
	helm uninstall oncloudkv --namespace oncloudkv

## ── Utilities ────────────────────────────────────────────────────────────────
tidy:
	go mod tidy

clean:
	rm -rf bin/

help:
	@echo "OnCloudKV - distributed key-value store"
	@echo ""
	@echo "Usage:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'
	@echo ""
	@echo "Quick start:"
	@echo "  make docker-up          # spin up 3-node cluster"
	@echo "  make bench              # run full benchmark suite"
	@echo "  make test               # run all tests"
