# filecloud

面向多设备的内容寻址文件同步系统。当前已实现单节点服务端，以及 Linux/ext4、macOS/APFS、Windows/NTFS 共用同一契约的 1B 跨平台验收门禁；1C 运维能力仍按第一阶段计划推进。

## 文档

- [领域词汇](./CONTEXT.md)
- [Seafile 文件同步调研](./docs/research/seafile-sync.md)
- [macOS/APFS 文件系统原语 spike](./docs/research/macos-apfs-primitives.md)
- [Windows/NTFS 文件系统原语 spike](./docs/research/windows-ntfs-primitives.md)
- [同步架构](./docs/design/architecture.md)
- [HTTP API 契约](./docs/design/http-api.md)
- [第一阶段实施计划](./docs/design/phase-1-plan.md)
- [第一阶段验收规范](./docs/design/acceptance-tests.md)
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

绑定只接受本地 ext4、APFS 或固定 NTFS。NFS、SMB、FAT/exFAT、网络映射盘和无法确认必要原子性的卷会在创建客户端状态或访问远端前明确拒绝。
