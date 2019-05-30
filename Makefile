# Copyright 2019 The OpenPitrix Authors. All rights reserved.
# Use of this source code is governed by a Apache license
# that can be found in the LICENSE file.

TRAG.Gopkg:=./
TARG.Name:=libqueue

GO_MOD_TIDY:=go mod tidy
GO_FMT:=gofmt -w  $(TRAG.Gopkg)
GO_MOD_TIDY:=go mod tidy
GO_RACE:=go build -race
GO_VET:=go vet
GO_FILES:=./
GO_PATH_FILES:=./

define get_diff_files
    $(eval DIFF_FILES=$(shell git diff --name-only --diff-filter=ad | grep -e "^(cmd|pkg)/.+\.go"))
endef

.PHONY: fmt-all
fmt-all: ## Format all code
	$ $(GO_FMT) $(GO_FILES)
	@echo "fmt done"

.PHONY: tidy
tidy: ## Tidy go.mod
	env GO111MODULE=on $(GO_MOD_TIDY)
	@echo "go mod tidy done"

.PHONY: fmt-check
fmt-check:fmt-all tidy ## Check whether all files be formatted
	$(call get_diff_files)
	$(if $(DIFF_FILES), \
		exit 2 \
	)

.PHONY: check
check: ## go vet and race
	env GO111MODULE=on $(GO_RACE) $(GO_PATH_FILES)
	env GO111MODULE=on $(GO_VET) $(GO_PATH_FILES)



