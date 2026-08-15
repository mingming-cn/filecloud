# filecloud

面向多设备的内容寻址文件同步系统。当前已实现单节点服务端、Linux/ext4、macOS/APFS、Windows/NTFS 共用的 1B 跨平台验收门禁，以及 watch、离线 GC、完整性检查、健康检查、优雅停止和部署性能基线组成的 1C 运维闭环。

## 文档

- [领域词汇](./CONTEXT.md)
- [Seafile 文件同步调研](./docs/research/seafile-sync.md)
- [macOS/APFS 文件系统原语 spike](./docs/research/macos-apfs-primitives.md)
- [Windows/NTFS 文件系统原语 spike](./docs/research/windows-ntfs-primitives.md)
- [同步架构](./docs/design/architecture.md)
- [HTTP API 契约](./docs/design/http-api.md)
- [第一阶段实施计划](./docs/design/phase-1-plan.md)
- [第一阶段验收规范](./docs/design/acceptance-tests.md)
- [1C 部署性能基线](./docs/operations/performance-baseline.md)
- [分发、许可与平台支持范围](./docs/distribution.md)
- [架构决策](./docs/adr/)

## 验证

普通回归测试：

```bash
go test ./...
```

1B 跨平台门禁在 Linux/ext4、macOS/APFS、Windows/NTFS 上使用同一测试名、场景清单和证明契约。必须在对应的本地文件系统 checkout 中显式运行；交叉编译不能代替实机门禁：

```bash
FILECLOUD_RUN_1B=1 go test ./cmd/filecloud \
  -run '^TestCrossPlatformAcceptanceMatrix$' \
  -count=1 -timeout=30m -v
```

PowerShell 使用相同入口：

```powershell
$env:FILECLOUD_RUN_1B=1
go test ./cmd/filecloud -run '^TestCrossPlatformAcceptanceMatrix$' -count=1 -timeout=30m -v
```

完整 1C 运维正确性门禁与部署性能基线：

```bash
./scripts/acceptance-1c.sh
```

性能测试会创建 10000 个小文件，并完整读取一个 10 GiB 稀疏文件，默认回归测试不会运行这些昂贵场景。

绑定只接受本地 ext4、APFS 或固定 NTFS。NFS、SMB、FAT/exFAT、网络映射盘和无法确认必要原子性的卷会在创建客户端状态或访问远端前明确拒绝。

## 版本与分发

开发构建可用 `filecloud version` 查看构建元数据。正式 `vMAJOR.MINOR.PATCH` tag 会在 1B/1C 门禁通过后生成 Linux amd64、macOS arm64 和 Windows amd64 归档，并对每个平台的成品二进制执行初始化、登录和同步冒烟测试。

项目采用 [MIT License](./LICENSE)。正式归档同时提供基于实际编译依赖生成的第三方模块版本清单和原始许可文本。
