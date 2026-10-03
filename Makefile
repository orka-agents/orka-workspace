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

test:
	go test -race ./...
	cd providers && go test -race ./...
	cd providers && go run ./fake/cmd/orka-workspace-fake --conformance

lint:
	@files="$$(gofmt -l api sdk conformance providers)"; test -z "$$files" || { echo "Run gofmt on:"; echo "$$files"; exit 1; }
	go vet ./...
	cd providers && go vet ./...

check: build lint test check-generated check-boundaries

generate: controller-gen
	"$(CONTROLLER_GEN)" object:headerFile=hack/boilerplate.go.txt paths=./api/...

manifests: controller-gen
	"$(CONTROLLER_GEN)" crd:allowDangerousTypes=true paths=./api/... output:crd:artifacts:config=config/crd/bases

check-generated: controller-gen
	bash scripts/check-generated.sh "$(CONTROLLER_GEN)"
	bash providers/fake/check-generated.sh "$(CONTROLLER_GEN)"

check-boundaries:
	python3 scripts/check-boundaries.py

controller-gen: $(CONTROLLER_GEN)

$(CONTROLLER_GEN):
	mkdir -p "$(LOCALBIN)"
	GOBIN="$(LOCALBIN)" go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)
	mv "$(LOCALBIN)/controller-gen" "$@"
