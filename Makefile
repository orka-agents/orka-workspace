SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

CONTROLLER_TOOLS_VERSION := v0.20.0
LOCALBIN := $(CURDIR)/bin
CONTROLLER_GEN := $(LOCALBIN)/controller-gen-$(CONTROLLER_TOOLS_VERSION)

.PHONY: all build test lint check generate manifests check-generated check-boundaries controller-gen

all: build

build:
	go build ./...
	cd providers && go build ./...

# GO_TEST_TIMEOUT bounds each go test binary. The race-enabled substrate
# provider package alone takes 7 to 8 minutes, close to go test's 10-minute
# default; 20m leaves headroom on slower runners while staying inside the CI
# job's 30-minute limit, so a hang still fails with go test's goroutine dump.
GO_TEST_TIMEOUT ?= 20m

test:
	go test -race -timeout $(GO_TEST_TIMEOUT) ./...
	cd providers && go test -race -timeout $(GO_TEST_TIMEOUT) ./...
	cd providers && go run ./fake/cmd/orka-workspace-fake --conformance

lint:
	@files="$$(gofmt -l api sdk conformance providers)"; test -z "$$files" || { echo "Run gofmt on:"; echo "$$files"; exit 1; }
	go vet ./...
	cd providers && go vet ./...

check: build lint test check-generated check-boundaries

generate: controller-gen
	"$(CONTROLLER_GEN)" object:headerFile=hack/boilerplate.go.txt paths=./api/...

manifests: controller-gen
	"$(CONTROLLER_GEN)" crd:allowDangerousTypes=true,generateEmbeddedObjectMeta=true paths=./api/... output:crd:artifacts:config=config/crd/bases

check-generated: controller-gen
	bash scripts/check-generated.sh "$(CONTROLLER_GEN)"
	bash providers/fake/check-generated.sh "$(CONTROLLER_GEN)"
	bash scripts/check-provider-generated.sh "$(CONTROLLER_GEN)" sandbox
	bash scripts/check-provider-generated.sh "$(CONTROLLER_GEN)" substrate

check-boundaries:
	python3 scripts/check-boundaries.py

controller-gen: $(CONTROLLER_GEN)

$(CONTROLLER_GEN):
	mkdir -p "$(LOCALBIN)"
	GOBIN="$(LOCALBIN)" go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)
	mv "$(LOCALBIN)/controller-gen" "$@"
