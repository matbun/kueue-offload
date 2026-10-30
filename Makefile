IMG ?= kueue-offload:latest
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
	$(CONTROLLER_GEN) crd:generateEmbeddedObjectMeta=true paths=./api/... output:crd:artifacts:config=config/crd/bases
	$(CONTROLLER_GEN) rbac:roleName=kueue-offload paths=./pkg/offload/... output:rbac:artifacts:config=config/rbac
	$(CONTROLLER_GEN) rbac:roleName=kueue-offload-membership paths=./pkg/membership/... output:rbac:artifacts:config=config/rbac/membership

docker-build:
	docker build -t $(IMG) .
