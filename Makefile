IMG ?= interlink-dispatch:latest
GO ?= go
CONTROLLER_GEN ?= $(shell command -v controller-gen 2>/dev/null || echo $(HOME)/go/bin/controller-gen)

.PHONY: all build test generate manifests vet docker-build

all: generate manifests build test

build:
	$(GO) build -o bin/ ./cmd/...

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

generate:
	$(CONTROLLER_GEN) object paths=./api/...

manifests:
	$(CONTROLLER_GEN) crd paths=./api/... output:crd:artifacts:config=config/crd/bases
	$(CONTROLLER_GEN) rbac:roleName=interlink-dispatch paths=./pkg/dispatcher/... output:rbac:artifacts:config=config/rbac
	$(CONTROLLER_GEN) rbac:roleName=interlink-membership paths=./pkg/membership/... output:rbac:artifacts:config=config/rbac/membership

docker-build:
	docker build -t $(IMG) .
