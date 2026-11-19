IMG ?= ghcr.io/matbun/kueue-offload:latest
VERSION ?= $(shell git describe --tags --always)
GO ?= go
CONTROLLER_GEN ?= $(shell command -v controller-gen 2>/dev/null || echo $(HOME)/go/bin/controller-gen)
# The executor's tests run an interLink API server built from INTERLINK_SRC; unset,
# it is this release of interlink-hq/interLink, cloned once into .cache.
INTERLINK_VERSION ?= 0.6.3
INTERLINK_CACHE := .cache/interLink-$(INTERLINK_VERSION)

.PHONY: all build test generate manifests vet docker-build interlink-src

all: generate manifests build test

build:
	$(GO) build -o bin/ ./cmd/...

test: $(if $(INTERLINK_SRC)$(INTERLINK_API_SERVER),,$(INTERLINK_CACHE)/cmd/interlink)
	INTERLINK_SRC=$(abspath $(or $(INTERLINK_SRC),$(INTERLINK_CACHE))) $(GO) test ./...

interlink-src: $(INTERLINK_CACHE)/cmd/interlink
	@echo $(abspath $(INTERLINK_CACHE))

$(INTERLINK_CACHE)/cmd/interlink:
	rm -rf $(INTERLINK_CACHE)
	git -c advice.detachedHead=false clone -q --depth 1 --branch $(INTERLINK_VERSION) https://github.com/interlink-hq/interLink $(INTERLINK_CACHE)

vet:
	$(GO) vet ./...

generate:
	$(CONTROLLER_GEN) object paths=./api/...

manifests:
	$(CONTROLLER_GEN) crd:generateEmbeddedObjectMeta=true paths=./api/... output:crd:artifacts:config=config/crd/bases
	$(CONTROLLER_GEN) rbac:roleName=kueue-offload paths="./pkg/offload/...;./pkg/node/..." output:rbac:artifacts:config=config/rbac
	$(CONTROLLER_GEN) rbac:roleName=kueue-offload-membership paths=./pkg/membership/... output:rbac:artifacts:config=config/rbac/membership

docker-build:
	docker build --build-arg VERSION=$(VERSION) -t $(IMG) .
