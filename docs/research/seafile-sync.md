# Seafile 文件同步调研

调研日期：2026-08-09。本文只使用 Seafile 官方文档、`haiwen` 官方仓库和 RFC 等一手资料。源码结论固定到列出的提交，不以易变的 `master` 链接作为证据。

## 结论

Seafile 最值得复用的是四层不可变对象图、上传前协商缺失对象、所有对象落盘后再发布 Head，以及冲突时保留双方内容。其当前实现同时背负 SHA-1、多个认证控制面、C/GLib 历史状态和服务端合并逻辑，本项目不应做协议兼容或源码移植。

桌面端、移动端和虚拟盘不是同一种客户端：

- 桌面同步由独立 `seaf-daemon` 维护完整工作目录；Qt GUI 负责账号、资料库选择和状态展示。
- Android 主要是浏览、显式传输、相册/目录单向备份，以及对已下载文件的有限回传，不是完整目录双向镜像。
- iOS 通过 File Provider 枚举远端、按需物化和逐项回传，缓存不等于完整工作目录。
- SeaDrive/FUSE 同步命名空间元数据，内容在读取时按需下载；它是虚拟盘，不应混入第一阶段镜像同步内核。

## 官方数据模型

官方 Data Model 文档把 Seafile 描述为类似 Git 的 `Repo`、`Commit`、`FS`、`Block` 模型：Commit 指向根 FS，目录项继续指向目录或文件对象，文件对象保存块列表；未变化对象可跨提交复用。文件使用内容定义分块，平均块大小约 8 MiB。[官方 Data Model](https://haiwen.github.io/seafile-admin-docs/12.0/develop/data_model)

服务端可把 commits、fs 和 blocks 放在不同后端；文件系统布局位于 `storage/commits`、`storage/fs`、`storage/blocks`。[官方多存储后端文档](https://haiwen.github.io/seafile-admin-docs/12.0/setup/setup_with_multiple_storage_backends)

本项目采用同类对象图，但作出三项不兼容变更：SHA-256、固定 4 MiB 分块、RFC 8785 规范化 JSON。

## 服务端调用链

服务端仓库：[`haiwen/seafile-server`](https://github.com/haiwen/seafile-server)，固定提交 [`746546a`](https://github.com/haiwen/seafile-server/tree/746546ad4c45d6724da3e041e8961da22ee99f94)。

### HTTP 同步面

Go fileserver 在 [`fileserver/fileserver.go`](https://github.com/haiwen/seafile-server/blob/746546ad4c45d6724da3e041e8961da22ee99f94/fileserver/fileserver.go#L351-L416) 注册 Head、commit、block、FS 列表、缺失检查和接收接口。同步处理主要位于 [`fileserver/sync_api.go`](https://github.com/haiwen/seafile-server/blob/746546ad4c45d6724da3e041e8961da22ee99f94/fileserver/sync_api.go#L453-L627)。

同步 token 通过 `Seafile-Repo-Token` 或 Authorization 进入，随后检查资料库和权限。Web 文件操作还有 Seahub/JWT 链路，因此 Seafile 的控制面与同步数据面并非一个简单 API。

### 对象与存储

- Commit 是 JSON，定义见 [`commitmgr.Commit`](https://github.com/haiwen/seafile-server/blob/746546ad4c45d6724da3e041e8961da22ee99f94/fileserver/commitmgr/commitmgr.go#L18-L50)。
- 文件/目录对象采用固定字段顺序 JSON、SHA-1 ID 和 zlib 存储，见 [`fsmgr`](https://github.com/haiwen/seafile-server/blob/746546ad4c45d6724da3e041e8961da22ee99f94/fileserver/fsmgr/fsmgr.go#L27-L243)。
- block 是原始字节，管理器见 [`blockmgr`](https://github.com/haiwen/seafile-server/blob/746546ad4c45d6724da3e041e8961da22ee99f94/fileserver/blockmgr/blockmgr.go#L11-L45)。
- 文件系统后端使用 `kind/store-id/id前两位/id其余部分`，临时写完后 rename，见 [`backend_fs.go`](https://github.com/haiwen/seafile-server/blob/746546ad4c45d6724da3e041e8961da22ee99f94/fileserver/objstore/backend_fs.go#L10-L112)。

值得修正的一点：所读同步上传入口没有统一重算 URL 中对象 ID 与正文摘要。新系统必须对每个上传对象重算 SHA-256，拒绝 key/content 不一致，并在发布 Head 前遍历校验整个可达图。Seafile 12.0 文档也新增了客户端上传后检查块完整性的配置，说明完整性校验是实际运维问题。[fileserver 配置](https://haiwen.github.io/seafile-admin-docs/12.0/config/seafile-conf)

### Head、并发与 GC

Seafile Head 更新会装载新 commit 并调用 `fastForwardOrMerge`，必要时执行服务端三方树合并；同路径双方修改时保留当前 Head 路径，并把上传侧改成冲突名。入口见 [`putUpdateBranchCB`](https://github.com/haiwen/seafile-server/blob/746546ad4c45d6724da3e041e8961da22ee99f94/fileserver/sync_api.go#L1016-L1093)，文件合并规则见 [`mergeEntries`](https://github.com/haiwen/seafile-server/blob/746546ad4c45d6724da3e041e8961da22ee99f94/fileserver/merge.go#L143-L210)，Branch 行锁和重试见 [`fileop.go`](https://github.com/haiwen/seafile-server/blob/746546ad4c45d6724da3e041e8961da22ee99f94/fileserver/fileop.go#L2022-L2252)。本项目保留原子 CAS，但把三方合并放在客户端，使服务端发布接口保持小而确定。

GC 从保留提交遍历目录树并标记 block；实现见 [`gc-core.c`](https://github.com/haiwen/seafile-server/blob/746546ad4c45d6724da3e041e8961da22ee99f94/server/gc/gc-core.c#L19-L70)。Seafile 使用 Bloom filter，允许假阳性但不误删活对象。本项目第一阶段永久保留所有提交，因此 GC 只清理未被任何已发布历史引用的上传残留。

## 桌面同步调用链

同步内核仓库：[`haiwen/seafile`](https://github.com/haiwen/seafile)，固定提交 [`2ebf6ac`](https://github.com/haiwen/seafile/tree/2ebf6ac8b0755c9a88d5ed9295f50e7c6c9a3255)。

主调度从 [`sync_repo_v2`](https://github.com/haiwen/seafile/blob/2ebf6ac8b0755c9a88d5ed9295f50e7c6c9a3255/daemon/sync-mgr.c#L1500-L1602) 开始。工作树事件进入索引，事件溢出或强制同步时退回目录扫描；对象传输由 [`http-tx-mgr.c`](https://github.com/haiwen/seafile/blob/2ebf6ac8b0755c9a88d5ed9295f50e7c6c9a3255/daemon/http-tx-mgr.c#L3033-L3900) 完成。

上传顺序是：

1. 创建本地 commit 和 FS 对象。
2. 上传 commit。
3. 协商并上传缺失 FS 对象。
4. 协商并并发上传缺失 blocks。
5. 更新远端 Head。
6. 再次查询 Head，处理上传期间发生的远端变化。

下载先获取 Head、commit 和 FS 对象，再对比树并 checkout 文件。下载目标 commit 会写入 `download-head`；未加密文件以 `.seafilepart` 和块边界续传，见 [`repo-mgr.c`](https://github.com/haiwen/seafile/blob/2ebf6ac8b0755c9a88d5ed9295f50e7c6c9a3255/daemon/repo-mgr.c#L5284-L5306)。索引通过 shadow 文件写完后 rename，checkout 也使用临时文件和备份后原子替换。

需要精确区分两层：服务端会合并两个已形成的提交；当前客户端 v2 主链本身不是通用三方内容合并器。下载 checkout 前，客户端比较工作树与本地 index；mtime 不同且内容也不同于远端对象时设置 `force_conflict`，见 [`fetch_file_thread_func`](https://github.com/haiwen/seafile/blob/2ebf6ac8b0755c9a88d5ed9295f50e7c6c9a3255/daemon/repo-mgr.c#L5572-L5612)，随后 checkout 层把本地旧内容改为冲突文件，见 [`fs-mgr.c`](https://github.com/haiwen/seafile/blob/2ebf6ac8b0755c9a88d5ed9295f50e7c6c9a3255/common/fs-mgr.c#L267-L488)。官方用户手册从用户视角描述为“云端保留先到版本，另一版本改名为冲突文件”。[File conflicts](https://haiwen.github.io/seafile-user-manual/syncing_client/file_conflicts)

## GUI 与同步内核边界

桌面 GUI 仓库：[`haiwen/seafile-client`](https://github.com/haiwen/seafile-client)，固定提交 [`d6547e7`](https://github.com/haiwen/seafile-client/tree/d6547e7d7bc14db6644da6e77228814a337db9ef)。

Qt GUI 通过 `QProcess` 启动 `seaf-daemon`，见 [`DaemonManager::startSeafileDaemon`](https://github.com/haiwen/seafile-client/blob/d6547e7d7bc14db6644da6e77228814a337db9ef/src/daemon-mgr.cpp#L102-L121)，再通过本机 named pipe RPC 获取资料库和同步状态。账号登录及远端资料库列表由 GUI 直接调用 Seahub API；扫描、索引、上传、下载和真实同步状态属于 daemon。

这说明未来桌面 GUI 应作为同步内核的薄控制面，但第一阶段单二进制 CLI 不需要提前建立进程 RPC。

## 移动端与虚拟盘

### Android

仓库 [`haiwen/seadroid`](https://github.com/haiwen/seadroid)，固定提交 [`537febc`](https://github.com/haiwen/seadroid/tree/537febc3ef3b964ce16e23abc268fc923177419b)。

`FileSyncService` 只对已下载缓存的修改做回传，或触发本地备份目录扫描；删除传播代码未启用，见 [`FileSyncService.java`](https://github.com/haiwen/seadroid/blob/537febc3ef3b964ce16e23abc268fc923177419b/app/src/main/java/com/seafile/seadroid2/framework/file_monitor/FileSyncService.java#L320-L410)。目录备份是本地到远端的单向队列，不存在完整远端 reconciliation。

建议未来 Android 产品边界：远端浏览、显式传输、按需离线、相册/指定目录单向备份。完整双向目录镜像只有在 Android 后台和 Scoped Storage 约束下建立独立 SLA 后再讨论。

### iOS

仓库 [`haiwen/seafile-iOS`](https://github.com/haiwen/seafile-iOS)，固定提交 [`6bdf6b8`](https://github.com/haiwen/seafile-iOS/tree/6bdf6b8c8383227733a6df9c5e64cfb078857e84)。

File Provider 在打开时按需物化，编辑后逐项上传，停止提供时可删除物化副本，见 [`FileProvider.m`](https://github.com/haiwen/seafile-iOS/blob/6bdf6b8c8383227733a6df9c5e64cfb078857e84/SeafFileProvider/FileProvider.m#L204-L333)。目录枚举只是远端元数据分页和缓存 fallback，见 [`SeafEnumerator.m`](https://github.com/haiwen/seafile-iOS/blob/6bdf6b8c8383227733a6df9c5e64cfb078857e84/SeafFileProvider/SeafEnumerator.m#L45-L180)。

建议未来 iOS 使用现代 File Provider 的 item identity、物化、回收和 pin 语义，不移植桌面常驻 daemon。

### 虚拟盘

仓库 [`haiwen/seadrive-fuse`](https://github.com/haiwen/seadrive-fuse)，固定提交 [`5ac6222`](https://github.com/haiwen/seadrive-fuse/tree/5ac6222168d2943f6d5d6b659c1f0e6c0d7b4f29)。

FUSE `readdir/getattr` 读取本地 RepoTree 元数据，`read` 才通过 FileCacheMgr 获取内容；远端新文件可只加入树而不下载内容，见 [`fuse-ops.c`](https://github.com/haiwen/seadrive-fuse/blob/5ac6222168d2943f6d5d6b659c1f0e6c0d7b4f29/src/fuse-ops.c#L1390-L1563) 与 [`sync-mgr.c`](https://github.com/haiwen/seadrive-fuse/blob/5ac6222168d2943f6d5d6b659c1f0e6c0d7b4f29/src/sync-mgr.c#L3268-L3438)。虚拟盘的“树已同步”和“内容已缓存”是两个状态，不能复用第一阶段完整镜像的完成语义。

## 文件系统与冲突经验

官方手册明确记录了大小写冲突、Windows 非法字符、尾随空格/句点、文件被应用锁定、大批量删除确认和本地未上传目录被远端删除时移入回收目录等问题。[同步错误 FAQ](https://haiwen.github.io/seafile-user-manual/faq)

本项目第一阶段因此采用跨平台可移植名称、普通文件/目录范围、临时文件原子替换和冲突副本。文件监听仅作为后续低延迟提示，扫描和持久同步基线才是正确性来源。

## 许可证边界

Seafile 项目 README 声明：服务端核心 AGPLv3、桌面同步 daemon GPLv2、Android GPLv3、iOS Apache 2.0；GitHub API 对部分仓库显示 `NOASSERTION`，应以各仓库实际 LICENSE 和法律审查为准。[官方仓库说明](https://github.com/haiwen/seafile#license)

本项目采用 clean-room 方式复用公开思想、行为和协议研究结论，不复制或链接 GPL/AGPL 源码、对象编码或具体实现。项目自身采用 MIT License；该许可选择不改变上述 clean-room 边界。

## 对本项目的取舍

| 能力 | 第一阶段 | 后续条件 |
|---|---|---|
| 不可变版本树 | 实现 | 核心基础 |
| SHA-256 内容寻址 | 实现并服务端重算 | 核心完整性 |
| 固定 4 MiB 分块 | 实现 | 有真实去重数据再评估 CDC |
| 缺失对象协商 | 实现 | 支持断点与去重 |
| Head CAS | 实现 | 并发正确性 |
| 客户端三方树合并 | 实现 | 同路径保留冲突副本 |
| 原生文件事件 | 不实现 | 大资料库或秒级延迟需求 |
| WebDAV | 不实现 | 同步领域层稳定后加适配层 |
| GUI/移动端/虚拟盘 | 不实现 | CLI 内核通过跨平台验收后分开设计 |
| E2EE、共享、S3、多节点 | 不实现 | 各自需要单独协议与威胁模型 |

## 资料可靠性说明

- 代码仓库均由本地 `git clone --depth 1 --filter=blob:none` 取证并记录 SHA。
- 上游多个 HEAD 的提交日期晚于常规公开版本时间线；本文以不可变提交 SHA 为依据，不把提交日期用于版本判断。
- 未构建或运行 Seafile；调用链结论来自固定源码静态分析。
- 未确认的信息不用于第一阶段设计。
