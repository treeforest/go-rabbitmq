# go-rabbitmq development tasks.

GO           ?= go
LOCAL_BIN    ?= $(HOME)/.local/bin
GOLANGCI_LINT ?= $(LOCAL_BIN)/golangci-lint
GOLANGCI_LINT_MODULE ?= github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.8.0
GOLANGCI_LINT_TIMEOUT ?= 15m
GOSEC        ?= $(LOCAL_BIN)/gosec
STATICCHECK  ?= $(LOCAL_BIN)/staticcheck
STATICCHECK_MODULE ?= honnef.co/go/tools/cmd/staticcheck@v0.8.1
GOVULNCHECK_MODULE ?= golang.org/x/vuln/cmd/govulncheck@latest
NILAWAY      ?= $(LOCAL_BIN)/nilaway
NILAWAY_MODULE ?= go.uber.org/nilaway/cmd/nilaway@latest
GOTOOLCHAIN  ?= go1.26.8
AUDIT_DIR    ?= tmp/audit-$(shell date +%Y%m%d)
AUDIT_GO     = env GOTOOLCHAIN=$(GOTOOLCHAIN) $(GO)

.PHONY: audit-init audit audit-quick audit-gosec audit-staticcheck audit-vet audit-race
.PHONY: audit-govulncheck audit-nilaway

##@ 代码质量

# 审计结果默认写入 tmp/audit-YYYYMMDD/（已在 .gitignore 中忽略 tmp/）。
# 关闭落盘：make audit AUDIT_DIR=
# 指定目录：make audit AUDIT_DIR=tmp/audit-custom

audit-init: ## 安装 Go 系审计工具到 LOCAL_BIN
	mkdir -p "$(LOCAL_BIN)"
	GOBIN="$(LOCAL_BIN)" env GOTOOLCHAIN=$(GOTOOLCHAIN) $(GO) install $(GOLANGCI_LINT_MODULE)
	GOBIN="$(LOCAL_BIN)" env GOTOOLCHAIN=$(GOTOOLCHAIN) $(GO) install $(STATICCHECK_MODULE)
	GOBIN="$(LOCAL_BIN)" env GOTOOLCHAIN=$(GOTOOLCHAIN) $(GO) install github.com/securego/gosec/v2/cmd/gosec@v2.22.2
	GOBIN="$(LOCAL_BIN)" env GOTOOLCHAIN=$(GOTOOLCHAIN) $(GO) install $(GOVULNCHECK_MODULE)
	GOBIN="$(LOCAL_BIN)" env GOTOOLCHAIN=$(GOTOOLCHAIN) $(GO) install $(NILAWAY_MODULE)
	@echo "audit-init: Go 工具已安装到 $(LOCAL_BIN)（GOTOOLCHAIN=$(GOTOOLCHAIN)）"

audit-gosec: ## 独立 gosec 安全扫描（未安装时回退 golangci-lint gosec）
	@if [ -n "$(AUDIT_DIR)" ]; then mkdir -p "$(AUDIT_DIR)"; fi
	@if [ -x "$(GOSEC)" ]; then \
		if [ -n "$(AUDIT_DIR)" ]; then \
			"$(GOSEC)" -quiet -tests ./... 2>&1 | tee "$(AUDIT_DIR)/gosec.txt"; \
		else \
			"$(GOSEC)" -quiet -tests ./...; \
		fi; \
	elif [ -x "$(GOLANGCI_LINT)" ]; then \
		echo "gosec: 未找到 $(GOSEC)，使用 $(GOLANGCI_LINT) 内置 gosec"; \
		if [ -n "$(AUDIT_DIR)" ]; then \
			"$(GOLANGCI_LINT)" run ./... --enable-only=gosec --timeout=$(GOLANGCI_LINT_TIMEOUT) 2>&1 | tee "$(AUDIT_DIR)/gosec.txt"; \
		else \
			"$(GOLANGCI_LINT)" run ./... --enable-only=gosec --timeout=$(GOLANGCI_LINT_TIMEOUT); \
		fi; \
	else \
		echo "gosec: 请先执行 make audit-init 安装审计工具"; exit 1; \
	fi

audit-staticcheck: ## staticcheck 静态分析
	@if [ -x "$(STATICCHECK)" ]; then \
		if [ -n "$(AUDIT_DIR)" ]; then mkdir -p "$(AUDIT_DIR)"; "$(STATICCHECK)" ./... | tee "$(AUDIT_DIR)/staticcheck.txt"; else "$(STATICCHECK)" ./...; fi; \
	else \
		echo "staticcheck: 未找到 $(STATICCHECK)，尝试 go run（需网络）或先执行 make audit-init"; \
		if [ -n "$(AUDIT_DIR)" ]; then mkdir -p "$(AUDIT_DIR)"; $(AUDIT_GO) run $(STATICCHECK_MODULE) ./... | tee "$(AUDIT_DIR)/staticcheck.txt"; else $(AUDIT_GO) run $(STATICCHECK_MODULE) ./...; fi; \
	fi

audit-vet: ## go vet 静态检查（GOTOOLCHAIN 与 go.mod 对齐）
	@if [ -n "$(AUDIT_DIR)" ]; then mkdir -p "$(AUDIT_DIR)"; $(AUDIT_GO) vet ./... 2>&1 | tee "$(AUDIT_DIR)/go-vet.txt"; else $(AUDIT_GO) vet ./...; fi

audit-quick: ## 快速静态审计（golangci-lint + go vet + staticcheck + gosec）
	@if [ ! -x "$(GOLANGCI_LINT)" ]; then echo "golangci-lint: 未找到 $(GOLANGCI_LINT)，请先执行 make audit-init（须 GOTOOLCHAIN=$(GOTOOLCHAIN) 编译）"; exit 1; fi
	@if [ -n "$(AUDIT_DIR)" ]; then mkdir -p "$(AUDIT_DIR)"; "$(GOLANGCI_LINT)" run ./... --timeout=$(GOLANGCI_LINT_TIMEOUT) | tee "$(AUDIT_DIR)/golangci-lint.txt"; else "$(GOLANGCI_LINT)" run ./... --timeout=$(GOLANGCI_LINT_TIMEOUT); fi
	@$(MAKE) audit-vet AUDIT_DIR="$(AUDIT_DIR)"
	@$(MAKE) audit-staticcheck AUDIT_DIR="$(AUDIT_DIR)"
	@$(MAKE) audit-gosec AUDIT_DIR="$(AUDIT_DIR)"

audit-race: ## 竞态检测（go test -race ./...）
	@if [ -n "$(AUDIT_DIR)" ]; then mkdir -p "$(AUDIT_DIR)"; $(AUDIT_GO) test -race ./... -count=1 | tee "$(AUDIT_DIR)/race.txt"; else $(AUDIT_GO) test -race ./... -count=1; fi

audit-govulncheck: ## Go 官方依赖漏洞扫描（使用 GOTOOLCHAIN）
	@if [ -n "$(AUDIT_DIR)" ]; then \
		mkdir -p "$(AUDIT_DIR)"; \
		env GOTOOLCHAIN=$(GOTOOLCHAIN) $(GO) run $(GOVULNCHECK_MODULE) ./... | tee "$(AUDIT_DIR)/govulncheck.txt"; \
	else \
		env GOTOOLCHAIN=$(GOTOOLCHAIN) $(GO) run $(GOVULNCHECK_MODULE) ./...; \
	fi

audit-nilaway: ## nilaway 空指针静态分析
	@if [ ! -x "$(NILAWAY)" ]; then \
		echo "nilaway: 未找到 $(NILAWAY)，请先执行 make audit-init"; exit 1; \
	fi
	@if [ -n "$(AUDIT_DIR)" ]; then mkdir -p "$(AUDIT_DIR)"; "$(NILAWAY)" ./... 2>&1 | tee "$(AUDIT_DIR)/nilaway.txt"; else "$(NILAWAY)" ./...; fi

audit: ## 全量 Go 代码审计（结果默认在 tmp/audit-YYYYMMDD）
	@$(MAKE) audit-quick AUDIT_DIR="$(AUDIT_DIR)"
	@$(MAKE) audit-race AUDIT_DIR="$(AUDIT_DIR)"
	@$(MAKE) audit-govulncheck AUDIT_DIR="$(AUDIT_DIR)"
	@$(MAKE) audit-nilaway AUDIT_DIR="$(AUDIT_DIR)"
