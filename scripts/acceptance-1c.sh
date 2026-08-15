#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

go test ./... -count=1 -timeout=30m
FILECLOUD_RUN_1C=1 go test ./cmd/filecloud \
  -run '^TestPerformanceBaseline(SmallFiles|LargeFile|KDF)$' \
  -count=1 -timeout=90m -v
FILECLOUD_RUN_1C=1 go test ./internal/object \
  -run '^TestPerformanceBaselineWideDirectory$' \
  -count=1 -timeout=30m -v
