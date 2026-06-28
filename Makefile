# Image URL to use all building/pushing image targets.
# Defaults to ghcr.io/cnmsql/cnmsql tagged with the current commit, with a
# "-dirty" suffix appended when the working tree has uncommitted changes.
# ("+" is not a valid character in an OCI image tag, so we use "-dirty".)
COMMIT_TAG ?= $(shell git rev-parse --short HEAD 2>/dev/null)$(shell git diff --quiet 2>/dev/null || echo "-dirty")
IMG ?= ghcr.io/cnmsql/cnmsql:$(COMMIT_TAG)
# YEAR defines the year value used for substituting the YEAR placeholder in the boilerplate header.
YEAR ?= $(shell date +%Y)

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	"$(CONTROLLER_GEN)" rbac:roleName=manager-role crd webhook paths="./api/..." paths="./cmd/..." paths="./internal/controller/..." paths="./internal/webhook/..." output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate: controller-gen generate-scrapers ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	"$(CONTROLLER_GEN)" object paths="./api/..."

.PHONY: generate-scrapers
generate-scrapers: ## Re-vendor the mysqld_exporter scrapers from the git submodule.
	git submodule update --init pkg/vendor/prometheus/mysqld_exporter
	go generate ./pkg/management/mysql/metrics/scrapers/...

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: manifests generate fmt vet setup-envtest ## Run tests.
	KUBEBUILDER_ASSETS="$(shell "$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path)" go test $$(go list ./... | grep -v /e2e | grep -v /test/integration) -coverprofile cover.out

.PHONY: test-integration
test-integration: ## Run the instance-manager integration tests against real Percona containers (requires Docker).
	go test -tags integration -timeout 600s ./test/integration/...

# TODO(user): To use a different vendor for e2e tests, modify the setup under 'tests/e2e'.
# The default setup assumes Kind is pre-installed and builds/loads the Manager Docker image locally.
# kubectl kuberc is disabled by default for test isolation; enable with:
# - KUBECTL_KUBERC=true
# CertManager is installed by default; skip with:
# - CERT_MANAGER_INSTALL_SKIP=true
KIND_CLUSTER ?= cnmsql-test-e2e
# K8S_VERSION pins the kindest/node image (e.g. K8S_VERSION=v1.34.0) so the e2e
# matrix can exercise a specific Kubernetes version. Empty uses Kind's default.
K8S_VERSION ?=
KIND_IMAGE_ARG = $(if $(K8S_VERSION),--image kindest/node:$(K8S_VERSION),)

.PHONY: setup-test-e2e
setup-test-e2e: ## Set up a Kind cluster for e2e tests if it does not exist. Optional: K8S_VERSION=v1.34.0.
	@command -v $(KIND) >/dev/null 2>&1 || { \
		echo "Kind is not installed. Please install Kind manually."; \
		exit 1; \
	}
	@case "$$($(KIND) get clusters)" in \
		*"$(KIND_CLUSTER)"*) \
			echo "Kind cluster '$(KIND_CLUSTER)' already exists. Skipping creation." ;; \
		*) \
			echo "Creating Kind cluster '$(KIND_CLUSTER)' $(if $(K8S_VERSION),(kindest/node:$(K8S_VERSION)),(default node image))..."; \
			$(KIND) create cluster --name $(KIND_CLUSTER) $(KIND_IMAGE_ARG) ;; \
	esac

GINKGO_VERSION ?= v2.27.2
# Empty GINKGO_PROCS lets hack/e2e.sh auto-size parallelism from CPU/RAM.
GINKGO_PROCS ?=
GINKGO_TIMEOUT ?= 120m
# GINKGO_LABEL_FILTER is honored by hack/e2e.sh when neither --label nor --tier
# is passed (the CI lanes set it directly).
GINKGO_LABEL_FILTER ?=

.PHONY: test-e2e
test-e2e: ## Run the full e2e suite via hack/e2e.sh. For focus / --k8s / --mysql / --tier, run ./hack/e2e.sh --help.
	KIND="$(KIND)" KIND_CLUSTER="$(KIND_CLUSTER)" GINKGO="$(GINKGO)" \
		GINKGO_PROCS="$(GINKGO_PROCS)" GINKGO_TIMEOUT="$(GINKGO_TIMEOUT)" \
		GINKGO_LABEL_FILTER="$(GINKGO_LABEL_FILTER)" ./hack/e2e.sh

.PHONY: e2e-build-images
e2e-build-images: docker-build ## Build the manager image and load it into the e2e Kind cluster (build happens outside the suite; used by hack/e2e.sh).
	$(KIND) load docker-image $(IMG) --name $(KIND_CLUSTER)

.PHONY: cleanup-test-e2e
cleanup-test-e2e: ## Tear down the Kind cluster used for e2e tests
	@$(KIND) delete cluster --name $(KIND_CLUSTER)

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	"$(GOLANGCI_LINT)" run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	"$(GOLANGCI_LINT)" run --fix

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	"$(GOLANGCI_LINT)" config verify

##@ Documentation

NPM ?= npm
PYTHON ?= python3
DOCUSAURUS_DOCKER_IMAGE ?= node:22-bookworm
DOCS_PORT ?= 1313

.PHONY: docs-install
docs-install: ## Install Docusaurus documentation dependencies.
	NO_UPDATE_NOTIFIER=1 $(NPM) --prefix docs install

CRD_REF_DOCS_CONFIG ?= config/crd-ref-docs/config.yaml

.PHONY: docs-build
docs-build: ## Build the Docusaurus documentation site.
	NO_UPDATE_NOTIFIER=1 $(NPM) --prefix docs run build

.PHONY: api-docs
api-docs: ## Generate API reference documentation from CRDs.
	go run github.com/elastic/crd-ref-docs \
		--config $(CRD_REF_DOCS_CONFIG) \
		--source-path api \
		--renderer markdown \
		--max-depth 10 \
		--output-path docs/src/api-reference-generated.md
	@echo "Generated docs/src/api-reference-generated.md"
	@echo "Review the output and merge narrative sections from docs/src/api-reference.md"

.PHONY: docs-serve
docs-serve: docs-build ## Serve the built Docusaurus documentation site locally.
	cd docs/build && $(PYTHON) -m http.server $(DOCS_PORT) --bind 0.0.0.0

.PHONY: docs-dev
docs-dev: ## Run the Docusaurus hot-reload development server.
	BROWSER=none NO_UPDATE_NOTIFIER=1 $(NPM) --prefix docs run start -- --host 0.0.0.0 --port $(DOCS_PORT)

.PHONY: docs-serve-docker
docs-serve-docker: ## Serve Docusaurus docs through Docker when Node dependencies are not installed locally.
	$(CONTAINER_TOOL) run --rm -p $(DOCS_PORT):$(DOCS_PORT) -v "$(CURDIR)/docs:/src" -w /src -e BROWSER=none -e NO_UPDATE_NOTIFIER=1 $(DOCUSAURUS_DOCKER_IMAGE) sh -lc 'npm install && npm run build && cd build && python3 -m http.server $(DOCS_PORT) --bind 0.0.0.0'

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager binary.
	go build -o bin/manager cmd/main.go

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	go run ./cmd/main.go

# kubectl-cnmsql plugin.
PLUGIN_PKG := github.com/cnmsql/cnmsql/cmd/kubectl-cnmsql/cmd
PLUGIN_VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
PLUGIN_COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
PLUGIN_DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
PLUGIN_LDFLAGS := -X $(PLUGIN_PKG).Version=$(PLUGIN_VERSION) -X $(PLUGIN_PKG).Commit=$(PLUGIN_COMMIT) -X $(PLUGIN_PKG).BuildDate=$(PLUGIN_DATE)

.PHONY: build-plugin
build-plugin: fmt vet ## Build the kubectl-cnmsql plugin binary.
	go build -ldflags "$(PLUGIN_LDFLAGS)" -o bin/kubectl-cnmsql ./cmd/kubectl-cnmsql

.PHONY: install-plugin
install-plugin: build-plugin ## Install the plugin + completion shim onto your PATH (~/.local/bin).
	install -D -m 0755 bin/kubectl-cnmsql $(HOME)/.local/bin/kubectl-cnmsql
	install -D -m 0755 hack/kubectl_complete-cnmsql $(HOME)/.local/bin/kubectl_complete-cnmsql

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	$(CONTAINER_TOOL) build -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	$(CONTAINER_TOOL) push ${IMG}

# The slim instance images are built and published from the separate containers
# repo (ghcr.io/cnmsql/cnmsql-instance). The operator and
# e2e suite consume those published images; they are no longer built here.

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64,linux/s390x,linux/ppc64le
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- $(CONTAINER_TOOL) buildx create --name cnmsql-builder
	$(CONTAINER_TOOL) buildx use cnmsql-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --tag ${IMG} -f Dockerfile.cross .
	- $(CONTAINER_TOOL) buildx rm cnmsql-builder
	rm Dockerfile.cross

# OVERLAY selects the deployment topology: config/default (cluster-wide, default)
# or config/namespaced (one namespace per operator). For namespaced deployments,
# pass NAMESPACE and NAME_PREFIX so cohabiting operators get a unique namespace
# and a unique cluster-scoped ValidatingWebhookConfiguration name, e.g.:
#   make deploy-namespaced NAMESPACE=tenant-a NAME_PREFIX=tenant-a-
OVERLAY ?= config/default

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment. Override OVERLAY=config/namespaced for namespaced mode.
	mkdir -p dist
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build $(OVERLAY) | \
		sed "s|--operator-image=controller:latest|--operator-image=${IMG}|g" > dist/install.yaml

.PHONY: build-installer-namespaced
build-installer-namespaced: ## Generate a consolidated YAML for namespaced mode. Pass NAMESPACE and NAME_PREFIX.
	$(MAKE) build-installer OVERLAY=config/namespaced

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" apply --server-side --force-conflicts -f -; else echo "No CRDs to install; skipping."; fi

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -; else echo "No CRDs to delete; skipping."; fi

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config. Override OVERLAY=config/namespaced for namespaced mode.
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build $(OVERLAY) | \
		sed "s|--operator-image=controller:latest|--operator-image=${IMG}|g" | \
		"$(KUBECTL)" apply --server-side --force-conflicts -f -

.PHONY: deploy-namespaced
deploy-namespaced: manifests kustomize ## Deploy a namespaced operator. Pass NAMESPACE and NAME_PREFIX, e.g. make deploy-namespaced NAMESPACE=tenant-a NAME_PREFIX=tenant-a-
ifdef NAMESPACE
	cd config/namespaced && "$(KUSTOMIZE)" edit set namespace $(NAMESPACE)
endif
ifdef NAME_PREFIX
	cd config/namespaced && "$(KUSTOMIZE)" edit set nameprefix $(NAME_PREFIX)
endif
	$(MAKE) deploy OVERLAY=config/namespaced

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion. Override OVERLAY for namespaced mode.
	"$(KUSTOMIZE)" build $(OVERLAY) | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p "$(LOCALBIN)"

## Tool Binaries
KUBECTL ?= kubectl
KIND ?= kind
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint
GINKGO ?= $(LOCALBIN)/ginkgo

## Tool Versions
KUSTOMIZE_VERSION ?= v5.8.1
CONTROLLER_TOOLS_VERSION ?= v0.20.1

#ENVTEST_VERSION is the version of controller-runtime release branch to fetch the envtest setup script (i.e. release-0.20)
ENVTEST_VERSION ?= $(shell v='$(call gomodver,sigs.k8s.io/controller-runtime)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_VERSION manually (controller-runtime replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v" | sed -E 's/^v?([0-9]+)\.([0-9]+).*/release-\1.\2/')

#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell v='$(call gomodver,k8s.io/api)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_K8S_VERSION manually (k8s.io/api replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v" | sed -E 's/^v?[0-9]+\.([0-9]+).*/1.\1/')

GOLANGCI_LINT_VERSION ?= v2.11.4
.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@"$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))
	@test -f .custom-gcl.yml && { \
		echo "Building custom golangci-lint with plugins..." && \
		$(GOLANGCI_LINT) custom --destination $(LOCALBIN) --name golangci-lint-custom && \
		mv -f $(LOCALBIN)/golangci-lint-custom $(GOLANGCI_LINT); \
	} || true

.PHONY: ginkgo
ginkgo: $(GINKGO) ## Download ginkgo locally if necessary.
$(GINKGO): $(LOCALBIN)
	$(call go-install-tool,$(GINKGO),github.com/onsi/ginkgo/v2/ginkgo,$(GINKGO_VERSION))

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] && [ "$$(readlink -- "$(1)" 2>/dev/null)" = "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f "$(1)" ;\
GOBIN="$(LOCALBIN)" go install $${package} ;\
mv "$(LOCALBIN)/$$(basename "$(1)")" "$(1)-$(3)" ;\
} ;\
ln -sf "$$(realpath "$(1)-$(3)")" "$(1)"
endef

define gomodver
$(shell go list -m -f '{{if .Replace}}{{.Replace.Version}}{{else}}{{.Version}}{{end}}' $(1) 2>/dev/null)
endef
