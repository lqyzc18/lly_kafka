#!/usr/bin/env bash
#
# LlyKafka 校验脚本：gofmt / go vet / go test / go build。
# 不依赖 make，Git Bash、MSYS2、Linux、macOS 均可直接运行：
#
#   bash scripts/check.sh
#
# 需要 CGO（Fyne）：Windows 需 MinGW/MSYS2，Linux 需 libgl1-mesa-dev 与 xorg-dev。

set -euo pipefail

cd "$(dirname "$0")/.."

# 预检 Go 环境：模块缓存目录无法确定时直接给出可操作的提示，
# 而不是让后续每个子命令各报一次错。
if ! go env GOMODCACHE >/dev/null 2>&1; then
	echo "无法确定 Go 模块缓存目录，请检查 GOPATH 或 GOMODCACHE 环境变量（Windows 下还需确认 HOME/USERPROFILE 可访问）。" >&2
	exit 1
fi

failed=0

echo "==> gofmt 检查"
unformatted="$(gofmt -l . || true)"
if [ -n "$unformatted" ]; then
	echo "以下文件未通过 gofmt，请执行 gofmt -w <文件>："
	echo "$unformatted"
	failed=1
else
	echo "    OK"
fi

echo "==> go vet ./..."
go vet ./...
echo "    OK"

echo "==> go test ./..."
go test ./...
echo "    OK"

echo "==> go build ./..."
go build ./...
echo "    OK"

if [ "$failed" -ne 0 ]; then
	echo "校验未通过"
	exit 1
fi

echo "全部校验通过"
