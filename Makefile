.DEFAULT_GOAL := help

CARGO ?= cargo
DOCKER ?= docker
MUSL_CC ?= musl-gcc

RUST_VERSION := $(shell awk -F '"' '/^[[:space:]]*channel[[:space:]]*=/ { print $$2; exit }' rust-toolchain.toml)
RUST_TARGET := x86_64-unknown-linux-musl
STATIC_CARGO_ENV := \
	CC_x86_64_unknown_linux_musl=$(MUSL_CC) \
	CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=$(MUSL_CC)
BUILD_FLAGS ?=
IMAGE ?= gtd:local
CONTAINER ?= gtd
VOLUME ?= gtd-data
PORT ?= 4040
BIND ?= 127.0.0.1:$(PORT)
DATABASE ?= $(CURDIR)/gtd.db
SERVER_URL ?= http://127.0.0.1:$(PORT)
ARGS ?= server --bind $(BIND) --database $(DATABASE)

ifeq ($(strip $(RUST_VERSION)),)
$(error failed to read Rust channel from rust-toolchain.toml)
endif

.PHONY: help build unit lint run image deploy

help:
	@echo "GTD commands"
	@echo "Rust toolchain: $(RUST_VERSION) (from rust-toolchain.toml)"
	@echo
	@echo "  make build                       Build the local static gtd binary"
	@echo "  make unit                        Run the unit test suite"
	@echo "  make lint                        Check formatting and run Clippy"
	@echo "  make run                         Build and run the local static server"
	@echo "  make run ARGS='add task'         Run any local gtd command"
	@echo "  make image                       Build IMAGE=$(IMAGE)"
	@echo "  make deploy                      Build and deploy to Docker"
	@echo
	@echo "Overrides: MUSL_CC, BUILD_FLAGS, IMAGE, PORT, BIND, DATABASE, SERVER_URL, CONTAINER, VOLUME, ARGS"

build:
	$(STATIC_CARGO_ENV) $(CARGO) build --locked --target $(RUST_TARGET) $(BUILD_FLAGS)

unit:
	$(CARGO) test --locked --all-targets --all-features

lint:
	$(CARGO) fmt --all -- --check
	$(CARGO) clippy --locked --all-targets --all-features -- -D warnings

run:
	$(STATIC_CARGO_ENV) $(CARGO) run --locked --target $(RUST_TARGET) -- --server-url $(SERVER_URL) $(ARGS)

image:
	$(DOCKER) build \
		--build-arg RUST_VERSION=$(RUST_VERSION) \
		--tag $(IMAGE) \
		.

# Replacing the container preserves the named SQLite data volume.
deploy: image
	@if $(DOCKER) container inspect $(CONTAINER) >/dev/null 2>&1; then \
		$(DOCKER) rm --force $(CONTAINER); \
	fi
	@$(DOCKER) volume create $(VOLUME) >/dev/null
	$(DOCKER) run --detach \
		--name $(CONTAINER) \
		--restart unless-stopped \
		--publish $(PORT):4040 \
		--volume $(VOLUME):/data \
		$(IMAGE)
