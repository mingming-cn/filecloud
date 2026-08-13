# filecloud

面向多设备的内容寻址文件同步系统。当前已实现单节点服务端、Linux/ext4 1A 正确性闭环，以及 macOS/APFS 客户端原语和完整 1B 验收门禁；Windows/NTFS、APFS 实机验收与 1C 运维能力仍按第一阶段计划推进。

## 文档

- [领域词汇](./CONTEXT.md)
- [Seafile 文件同步调研](./docs/research/seafile-sync.md)
- [macOS/APFS 文件系统原语 spike](./docs/research/macos-apfs-primitives.md)
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

Linux/ext4 1A 验收门禁必须在本地 ext4 checkout 中显式运行：

```bash
FILECLOUD_RUN_1A=1 go test ./cmd/filecloud \
  -run '^TestLinuxExt4AcceptanceMatrix$' \
  -count=1 -timeout=30m -v
```

macOS/APFS 1B 门禁必须在本地 APFS checkout 中显式运行；交叉编译不能代替该门禁：

```bash
FILECLOUD_RUN_1B_APFS=1 go test ./cmd/filecloud \
  -run '^TestMacOSAPFSAcceptanceMatrix$' \
  -count=1 -timeout=30m -v
```
