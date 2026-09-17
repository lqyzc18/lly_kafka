# LlyKafka 开发校验入口
#
# Windows 请在 Git Bash / MSYS2 环境下执行（Fyne 需要 CGO 与 gcc）。
# 没有 make 时可直接运行 `go run ./` 与下方 targets 中等价的 go 命令。

GO      ?= go
BINARY  ?= lly_kafka.exe
PKGS    ?= ./...

.DEFAULT_GOAL := help

.PHONY: help fmt fmt-check vet test race build package check clean

help: ## 显示可用目标
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

fmt: ## 自动格式化代码
	$(GO) fmt $(PKGS)

fmt-check: ## 检查格式（不修改文件，CI 使用）
	@files=$$(gofmt -l .); \
	if [ -n "$$files" ]; then \
		echo "以下文件未通过 gofmt，请执行 make fmt："; \
		echo "$$files"; \
		exit 1; \
	fi; \
	echo "gofmt 检查通过"

vet: ## 静态检查
	$(GO) vet $(PKGS)

test: ## 运行单元测试
	$(GO) test $(PKGS)

race: ## 运行单元测试（含竞态检测）
	$(GO) test -race $(PKGS)

build: ## 构建可执行文件
	$(GO) build -o $(BINARY) .

package: ## 使用 fyne 打包为桌面安装包
	fyne package -os windows

check: ## 完整校验（gofmt / vet / test / build）
	bash scripts/check.sh

clean: ## 清理构建产物
	rm -f $(BINARY) go_build_*.exe
