# 第一阶段实施计划

状态：第一阶段实现已完成，并按 1A、1B、1C 递进验收。单节点 Go 服务端与 Linux/macOS/Windows Go CLI 已落地；各平台支持声明以对应目标文件系统的最近一次实机门禁记录为准。CAS、pending publication、对象校验和 checkout journal 不因拆阶段而删减。

详细故障注入和平台断言见 [验收规范](./acceptance-tests.md)。

## 产品边界

第一阶段是同步产品，不是备份产品。服务端保留完整历史是完整性和未来恢复能力的基础，但第一阶段不承诺用户可浏览或恢复任意历史版本。大批删除必须通过提交级确认保护，避免永久历史暂不可用时误删扩散。

第一阶段只有管理员创建的可信用户。即使如此，服务端仍必须限制登录 KDF 并发、Head 验证并发和用户滚动上传字节，防止单个凭据耗尽全局 CPU、内存或磁盘；这属于安全预算，不是用户可配置容量配额。

## 1A：Linux 正确性闭环

目标平台：本地 ext4。只提供显式 `filecloud sync`，不提供 watch 或 GC 删除。

1. **对象规范**
   - 初始化 Go module 和单 `filecloud` 二进制。
   - 实现规范 UUID、RFC 8785、SHA-256、固定 4 MiB 分块、Unicode 15.1 路径规则。
   - 固定空 Directory、1 byte/边界 block、File、Directory 和 Commit 测试向量。

2. **本地对象存储**
   - 同文件系统临时写、摘要校验、no-replace 发布、文件和父目录持久化。
   - 实现读取、存在检查、当前快照和新引入 parent 子图验证。
   - 故障注入覆盖截断写、错误摘要、重复 PUT 和每个 fsync/rename 边界。

3. **SQLite 与服务控制面**
   - `init`、`serve`、`user add`、`user reset-password`、`login`、`logout`。
   - migrations、用户、令牌、资料库、Head CAS、安全预算；1A 容量测试冻结预算窗口和默认值。
   - 业务代码只通过集中查询包使用 `database/sql`，不创建未来数据库空接口。

4. **HTTP 数据面**
   - 创建/列出/读取资料库和 Head。
   - 批量检查、元数据对象和 block 流式 PUT/GET。
   - 强 ETag、统一错误、服务端摘要重算和工作量门控。

5. **客户端绑定、索引与扫描**
   - `library create/list/inspect/bind/unbind`。
   - 只允许文档定义的四种首次绑定情形；两侧非空且无基线必须拒绝。
   - SQLite 保存 Sync Base、pending publication、checkout journal 和路径状态。
   - 双重文件读取与目录枚举；发现不稳定、不可读或不完整则整轮禁止发布。

6. **同步与并发**
   - 单客户端上传、下载和断点恢复。
   - 递归三方目录合并、mtime 规则、删除/修改和类型冲突。
   - 连续 HeadConflict、pending Commit 已成为祖先、CAS 响应丢失。
   - write-ahead checkout 和可见冲突 inode。

7. **删除保护**
   - 候选提交删除已跟踪文件或目录路径超过 100 个，或达到当前已跟踪路径数的 10%（任一满足，10% 边界包含）时，先持久化候选和统计再默认中止；对象 PUT、Commit PUT、Head CAS、Sync Base 与路径索引均保持不变。
   - 错误只显示固定 12 位小写 Candidate Commit 前缀和删除统计，不显示路径或内容。用户必须再次执行 `sync --confirm-delete <prefix>`，且参数恰好匹配当前工作目录的同一 pending 候选；短前缀、完整 CommitId、其他候选均拒绝。
   - 确认前完整重扫。先检查远端 Head 是否已经发布候选，再处理工作树变化；已发布候选必须推进本地 Sync Base 和索引且保留工作树内容。旧 schema pending 必须重新计算删除统计，不能沿用迁移前授权。工作树变化会丢弃尚未发布的旧候选并要求普通 `sync` 生成新前缀；未变化则复用持久化 CommitId。已确认候选的网络或 CAS 失败可由后续普通 `sync` 继续。没有受保护 pending 候选时确认参数明确失败且不改变状态。

1A 完成条件：两名用户互相不可见；同一资料库两个 Linux 客户端可在故障注入下收敛，所有确认内容可达，Head、Sync Base 和工作树断言通过。

## 1B：跨平台文件系统验证

1. APFS 和 NTFS 与 ext4 执行相同的对象、锁、扫描、checkout 和崩溃测试；三平台均通过复用完整 1A 清单的实机门禁。
2. spike 并冻结以下平台实现：no-follow/reparse point、文件身份、no-replace、父目录持久化、跨进程锁、占用文件 rename；macOS 结论见 [APFS 原语 spike](../research/macos-apfs-primitives.md)。
3. 文件系统不满足必要原子性时明确拒绝绑定，不做静默降级。
4. 第一阶段明确不支持 NFS、SMB、FAT/exFAT 和网络映射盘；后续必须独立验证后才能移出排除列表。

1B 完成条件：Linux、macOS、Windows 使用同一对象向量生成相同 ObjectId，并通过同名、同场景清单和同一结构化证明契约的实机验收矩阵。固定双设备并发场景还必须在三平台生成相同 Head、同步基线和工作目录 Snapshot。

## 1C：运维闭环

1. `watch --interval` 只作为定时触发器，扫描仍是事实来源。
2. `gc --dry-run`、离线 `gc` 和 `integrity check`；均与 `serve` 数据目录锁互斥。
3. 优雅停止、结构化日志、`healthz`、`readyz` 和未发布对象统计。
4. 基于部署基准调优已在 1A 冻结的有界安全默认值，固化性能基线和跨平台发布构建。
5. 分发二进制前确定项目许可证并生成第三方依赖许可证清单。

## 完成标准

- 管理员可创建和重置用户；用户可登录、注销、创建、列出、读取、绑定和解除绑定自己的资料库。
- 跨用户访问资料库统一返回 NotFound。
- 创建、修改、删除和移动普通文件可双向传播；同路径并发变化不静默丢失。
- 网络、服务端或客户端在定义的任一故障点退出后，可恢复到旧基线或继续目标 Commit。
- Linux/macOS/Windows 的支持边界明确且由自动化测试证明，不承诺未经测试的文件系统。

## 第一阶段明确不做

- Seafile 协议兼容或源码复用
- GUI、系统托盘、文件图标扩展
- Android/iOS 客户端、FUSE/虚拟盘
- 原生文件事件监听
- WebDAV
- 共享、组、目录权限和协作文件锁
- 端到端加密
- S3、多节点和在线 GC
- PostgreSQL/MySQL 驱动
- 用户可配置容量配额、历史过期和已发布历史自动清理
- 历史浏览和恢复 UI/CLI

这些能力只在 1C 验收通过后逐项设计，不能提前建立空接口或配置占位。
