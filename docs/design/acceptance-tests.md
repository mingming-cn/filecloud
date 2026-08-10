# 第一阶段验收规范

状态：阶段 0 评审中。每项测试必须记录平台、文件系统、服务端 Head、客户端 Sync Base、工作树快照和对象存储可达性。只检查命令退出码不足以通过验收。

## 通用 Oracle

每个场景结束后执行：

1. 重算工作树规范快照，确认稳定客户端的快照等于其 Sync Base Root。
2. 读取服务端 Head，确认稳定客户端 Sync Base 等于 Head。
3. 从 Head 遍历当前 Root 和首次引入历史的 parent 子图，确认对象存在、类型正确且摘要匹配。
4. 确认所有场景输入内容至少存在于 Head 可达历史、可见冲突副本或 journal 恢复路径之一。
5. 确认不存在未登记的 `.filecloud-internal-` 路径。

## 对象测试向量

所有平台必须生成完全相同的规范字节和 ObjectId：

| 向量 | 输入 |
|---|---|
| 空文件 | Blocks 为空、Size 为 `"0"` |
| 最小文件 | 1 byte |
| 尾块边界 | `4 MiB - 1` |
| 完整块 | `4 MiB` |
| 两个块 | `4 MiB + 1` |
| Unicode | NFC/NFD 输入、Unicode 15.1 大小写折叠碰撞 |
| UUID | 规范小写通过，大写/无连字符拒绝 |
| JSON | key 顺序变化得到同一规范字节；对象/请求中的重复 key、浮点和未知字段拒绝 |

## HTTP wire contract

使用原始 HTTP 请求逐项断言状态、header 和 body：

- CreateSession、CreateLibrary、CheckObjects、PutMetadataObject、PutBlock 和 UpdateLibraryHead 超过各自正文/对象上限时返回 `413/3005`，不进入 JSON 大对象分配、KDF 或最终对象写入。
- PutBlock 缺少/非法 Content-Length、零长度返回 InvalidArgument；大于 4 MiB 返回 PayloadTooLarge。File 引用非尾部短 block 时拒绝 Head，旧 Head 不变。
- 所有 `401` 携带 `WWW-Authenticate: Bearer`；会话成功响应携带 `Cache-Control: no-store`。
- CreateLibrary 首次返回 201；相同 ID/Name 重放返回 200；同 ID 不同 Name 返回 409/ObjectConflict。
- Head 缺少 `If-Match` 返回 428；弱 ETag、列表、`*` 和非法语法返回 400；旧强 ETag 返回 412 和当前 Head。
- `If-None-Match` 命中返回 304 空正文；注销返回 204 空正文。
- Object GET 返回规范对象字节而非 envelope，并携带强 ObjectId ETag；错误仍返回 JSON envelope。
- PageToken 不透明且分页稳定；非法和过期 token 返回 InvalidArgument。
- 服务端响应增加未知可选字段时，`/v1` 客户端必须忽略；客户端请求及对象中的未知字段仍拒绝。
- RetCode、Message 与文档列出的 HTTP 状态组合逐项锁定，错误响应不包含内部路径、SQL 或凭据。

## 首次绑定

| 场景 | 操作 | Oracle |
|---|---|---|
| 双空 | 空目录绑定空 Head | 发布引用规范空 Directory 的初始 Commit；两端 Base 等于 Head |
| 本地导入 | 非空目录绑定空 Head并确认 | 所有本地内容可达，parent 为初始空 Commit |
| 远端 checkout | 空目录绑定非空 Head | 本地完整等于远端，不产生新提交 |
| 双非空 | 非空目录绑定非空 Head且无 Base | 拒绝，双方均不变化 |
| 重复绑定 | 同一路径或同资料库再次绑定 | 幂等或明确冲突，不创建第二个活动绑定 |
| 换目录 | 未解除旧绑定就绑定新目录 | 拒绝 |

## 同步与合并

| 场景 | 操作 | Oracle |
|---|---|---|
| 不同文件 | 两端离线修改同目录不同文件 | 递归合并后同时保留两项，不产生整目录冲突；新 DirectoryId 和 max(Local,Remote) mtime 在所有客户端一致 |
| 同一文件 | 两端离线写入不同内容 | 远端保留原路径，本地成为确定命名冲突副本 |
| 删除/修改 | 一端删除，另一端修改 | 修改内容存在于原路径或冲突副本 |
| 目录删除/修改 | 一端删除目录，另一端修改子项 | 修改侧完整子树保存到冲突目录 |
| 类型互换 | 文件和目录并发占用同一路径 | 远端类型留原路径，本地完整对象/子树进入冲突路径 |
| mtime-only | 单侧及双侧只修改 mtime | 按确定规则收敛，不反复生成提交 |
| 连续竞争 | CAS 前两次被其他客户端推进 Head | 每次使用上次 Expected Head 重新设 Base，不制造虚假冲突 |
| pending 祖先 | CAS 响应丢失后其他客户端再发布 | 识别 pending Commit 为当前 Head 祖先，不重复发布本地变化 |

## 扫描竞态

- 文件读取中改写、truncate、替换 inode、保留 mtime 的同大小改写：可观察变化必须导致重试或整轮失败。
- 同一目录在枚举期间创建、删除和 rename：两次规范枚举不一致时整轮失败。
- 第一个文件完成读取后、全树扫描结束前再修改它：最终验证遍历必须发现变化并丢弃整轮。
- 已跟踪文件变为 symlink/reparse point，或目录变得不可读：不得发布为删除。
- 持续写入超过重试预算：命令明确失败，Head 和 Sync Base 不变。
- 持有旧 fd 并在 checkout 后继续写：记录 ext4/APFS/NTFS 的实际行为；超出第一阶段强保证时必须保留文档化警告，不能声称绝对无损。
- checkout 每一步前把父目录替换为 symlink/reparse point 或不同 inode：必须失败，且不得在工作目录根句柄之外创建、改名或删除路径。

## 故障注入

对每个 checkout 文件系统动作，在以下位置强制终止客户端并重启：

1. journal Intent 已 INSERT、事务 COMMIT 前（非持久边界）。
2. Intent COMMIT 返回后、文件系统动作前（持久边界）。
3. create 的 `mkdir/open(O_EXCL)` 成功后、identity UPDATE 前（`between_create_identity`）；重启必须把未知 inode 自动移到可见 recovery leaf，禁止收养或删除。预建 file/directory 可见名碰撞并在每次 successor Intent 提交后 `SIGKILL`，恢复必须递增有界后缀且保持所有碰撞内容不变。
4. 动作后、父目录同步前。
5. 父目录同步后、Completed 提交前。
6. Completed 提交后。
7. 全部文件完成后、Sync Base 事务提交前后。
8. 内部恢复路径完成后、cleanup metadata 事务提交前后。

SQLite WAL 与 `synchronous=FULL` 的同步发生在 COMMIT 内部；SQLite 不提供受支持的 intra-COMMIT 故障 hook，因此测试只在 COMMIT 调用前和成功返回后夹住非持久/持久边界，不宣称覆盖“WAL 同步中途”。重启必须仅依据 journal 和路径存在组合幂等完成或回滚；不得误删用户路径、提前推进 Sync Base 或遗留未登记内部文件。子进程故障注入统一使用 `SIGKILL`，只证明进程崩溃恢复；断电持久性仍需外部电源切断与 ext4 硬件测试，不能由进程测试宣称已经证明。

服务端对象写入在临时文件写入、文件同步、no-replace、父目录同步和 Head 条件 UPDATE 前后执行同类故障注入。旧 Head 必须始终可读；新 Head 一旦可见，其当前快照必须完整。

## 传输恢复

| 场景 | Oracle |
|---|---|
| 100 MiB 上传中止 | 重启后只上传服务端缺失 blocks |
| block PUT 响应丢失 | 重试返回已存在，不生成不同对象 |
| 下载中止 | 已完成 blocks 不重复下载；目标 Commit 不漂移 |
| CAS 响应丢失 | 读取 Head 判定，不盲目重放 |
| 错误摘要/截断正文 | 最终对象路径不存在，Head 不变 |

## 权限与资源保护

- 用户 A 对用户 B 的 LibraryId、ObjectId 和 Head 请求全部返回相同 NotFound，不泄露存在性。
- 两名用户以相同 LibraryId 分别创建资料库时都成功，名称、Head、对象目录和后续读写完全隔离；任何对象路径和查询都不得只以 LibraryId 定位。
- 不存在用户名和错误密码执行等价认证工作；KDF 并发超过上限返回 RateLimited。
- 单资料库只能有一个 Head 验证；请求超时后不继续占用工作池。构造 1025 个首次引入 commits 或总计超过 2000000 个对象的第二-parent 子图必须在预算边界拒绝，旧 Head 不变。
- 以测试配置设置很小的滚动上传预算：并发上传首次创建对象时原子计费，达到阈值后返回 RateLimited；重传已存在对象不计费；窗口跨界按服务器时钟切换，重启后计数不丢失。
- 启动超过全局/用户并发槽数量的慢速对象 PUT：超额请求在读取正文和创建临时文件前返回 RateLimited；中止请求释放槽和预留字节。
- 磁盘低水位命中时拒绝新对象，但已有 Head 仍可读取。
- 大批删除未确认时 Head 不变；确认 token 不能用于不同 Candidate Commit。

## 平台矩阵

| 里程碑 | OS/文件系统 | 必须验证 |
|---|---|---|
| 1A | Linux/ext4 | 全部场景 |
| 1B | macOS/APFS | 全部 1A 场景；特别记录 no-follow、文件身份、no-replace、锁、rename、目录持久化和全部崩溃点 |
| 1B | Windows/NTFS | 全部 1A 场景；特别记录 reparse point、文件身份、no-replace、锁、占用文件、目录持久化和全部崩溃点 |

NFS、SMB、FAT/exFAT 和网络映射盘必须拒绝绑定，除非后续 ADR 和相同级别测试明确支持。

## 1C 运维命令

### GC

1. `serve` 持有数据目录锁时，`gc --dry-run` 和 `gc` 必须在修改前失败；GC 持有独占锁时 `serve` 也必须拒绝启动。
2. 构造以下对象：当前 Head 快照、普通 parent 历史、合并 Commit 的第二 parent、其他资料库历史、宽限期内未发布对象、宽限期外未发布对象。
3. `gc --dry-run` 只报告最后一类对象，不修改文件、SQLite 或 Head；输出包含对象类型、数量和总字节，不暴露内容。
4. 随后 `gc` 的候选集合必须与同一数据状态下的 dry-run 一致。所有已发布历史及宽限期内对象保持逐字节不变，只有宽限期外不可达对象可删除。
5. 在逐个删除前后强制终止 GC；重启后所有 Head 仍完整可读，再次运行 GC 得到剩余候选并最终幂等收敛。
6. 多资料库和跨用户场景必须从全部 Head 标记，不能只从最近访问或单个所有者标记。

### 完整性检查

- `serve` 运行时 `integrity check` 必须在读取对象前失败；检查持有独占锁时 `serve`、`gc` 和第二个检查进程都必须拒绝启动。
- 分别损坏 Commit、Directory、File 和 Block，或删除其引用对象；`integrity check` 必须返回非零并报告所属资料库、对象类型和脱敏 ObjectId。
- 检查覆盖当前 Root、普通 parent 和第二 parent 历史；损坏不在当前快照但仍属永久历史时也必须报告。
- 命令严格只读，不重写对象、不移动 Head、不自动删除；对未损坏数据重复运行输出稳定。

### Watch 与服务生命周期

- watch 必须调用与显式 sync 相同的完整扫描器；重复“扫描竞态”全部场景，结果和错误语义必须一致，不能仅依据 mtime 或事件列表发布。
- watch 持有绑定锁时显式 sync 必须拒绝，同一绑定第二个 watch 也必须拒绝；不同绑定可以并行。
- 当一次扫描耗时超过 interval 时不得并发启动下一轮；停止信号等待当前持久化步骤到达可恢复边界后退出。
- `healthz` 不依赖存储；元数据库不可读、对象目录不可写或数据目录锁异常时 `readyz` 返回 503，且不泄露内部路径。
- 服务退出和重启不能丢失 token 撤销、上传预算、Head 或客户端可恢复状态。

## 性能基线

性能不是第一阶段首要目标，但必须防止规格与实现明显失配。记录：

- 10000 个 4 KiB 文件的全量扫描时间和峰值内存。
- 10 GiB 单文件的扫描、增量修改传输量和 Head 验证时间。
- 100000 个 Directory Entries 的规范化和验证峰值内存。

`watch` 只在 1C 引入；如果一次扫描耗时大于 interval，不并发启动下一轮，只记录延迟并顺延。
