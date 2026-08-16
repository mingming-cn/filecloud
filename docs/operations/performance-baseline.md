# 1C 部署性能基线

记录日期：2026-08-15

本基线用于冻结第一阶段的有界安全默认值，并检查规格与实现是否失配。它不是跨硬件性能承诺，也不以缓存或额外并发替代正确性扫描。

## 测量环境

- CPU：AMD Ryzen 7 9700X，8 核
- 内存：24 GiB
- 存储：NVMe，ext4，可用空间约 1.1 TiB
- 系统：Linux x86-64
- Go：1.26.5
- 每个场景独立运行 3 次，表中记录中位数
- 峰值内存是操作开始前强制 GC 后，按 1 ms 采样 `runtime.MemStats.HeapAlloc` 得到的堆增量

## 结果

| 场景 | 中位耗时 | 峰值堆增量 | 其他结果 |
|---|---:|---:|---|
| 10000 个 4 KiB 文件全量扫描 | 3.128 s | 34.1 MiB | 扫描 10000 个路径；每个文件执行双读和最终树验证 |
| 10 GiB 单文件全量扫描 | 8.791 s | 未要求 | 稀疏分配但 2560 个逻辑块均写入不同标记，扫描器读取并哈希全部逻辑字节 |
| 10 GiB 文件单块增量修改 | 未单独计时 | 未要求 | 新增对象总传输 4366342 bytes：1 个 4 MiB Block 加 File、Directory、Commit |
| 10 GiB 文件对象图 Head 验证 | 6.602 s | 未要求 | 通过真实 HTTP Handler、对象存储和 Head CAS 验证 2560 个唯一 Block |
| 100000 个 Directory Entries 规范化与验证 | 232.7 ms | 47.1 MiB | 规范编码 14500044 bytes |
| 默认 Argon2id KDF | 41.84 ms | 64.0 MiB | `m=65536 KiB, t=3, p=2` |

原始结果由测试以 `FILECLOUD_PERFORMANCE <json>` 输出，字段使用纳秒和字节，避免展示精度丢失。

## 复验记录

2026-08-16 在提交 `98b3030`、Linux 7.1.8-zen1-3-zen、Go 1.26.5、本地 ext4 上执行 `./scripts/acceptance-1c.sh`，普通回归与全部性能场景通过：

| 场景 | 耗时 | 峰值堆增量 | 其他结果 |
|---|---:|---:|---|
| 10000 个 4 KiB 文件全量扫描 | 2.645 s | 40.0 MiB | 10000 个路径 |
| 10 GiB 单文件全量扫描 | 9.041 s | 未要求 | 单块增量对象传输 4366342 bytes；Head 验证 8.261 s |
| 100000 个 Directory Entries 规范化与验证 | 228.2 ms | 47.2 MiB | 规范编码 14500044 bytes |
| 默认 Argon2id KDF | 39.23 ms | 64.0 MiB | 冻结参数未变 |

本次复验未发现需要调整冻结安全默认值的规格失配。

2026-08-16 在提交 `4449976bc427ecbe7149a2ada7bef8f8e5b6852f`、同一 Linux/ext4 环境上再次执行完整 `./scripts/acceptance-1c.sh`，普通回归与全部性能场景通过：10000 个 4 KiB 文件扫描 2.706 s、10 GiB 文件扫描 9.107 s、Head 验证 7.434 s、100000 个目录项规范化与验证 224.5 ms、默认 Argon2id 40.43 ms；单块增量对象仍为 4366342 bytes。本次结果仍未发现需要调整冻结安全默认值的规格失配。

首次 `v0.1.0` 托管 workflow 在较慢 runner 上发现 CLI 的 30 秒 HTTP 超时早于服务端 2 分钟 Head 校验预算。提交 `b59d60e6a0dd03a983054894a902035e40fac23e` 将有界客户端预算调整为 3 分钟但保持服务端安全默认值不变；同一 Linux/ext4 环境的完整复验通过：10000 个 4 KiB 文件扫描 2.860 s、10 GiB 文件扫描 9.349 s、Head 验证 7.521 s、100000 个目录项规范化与验证 223.3 ms、默认 Argon2id 39.89 ms；单块增量对象仍为 4366342 bytes。

## 冻结默认值

| 保护项 | 第一阶段默认值 | 测量依据 |
|---|---|---|
| Argon2id | 64 MiB、3 次迭代、并行度 2 | 单次约 42 ms/64 MiB；全局并发 2 将 KDF 堆工作集约束在约 128 MiB |
| KDF 并发 | 全局 2、每来源 IP 1、每用户名 1 | 在保留登录吞吐的同时限制单来源和单身份 CPU/内存占用 |
| 对象上传并发 | 全局 8、每用户 2 | 每请求至多处理一个 4 MiB Block；单用户不能占满全局槽 |
| 对象上传读取 deadline | 1 min | 远高于本机单块处理时间，同时保持慢请求有界 |
| 滚动上传预算 | 每用户 12 GiB/1 h | 原 10 GiB 无法容纳 10 GiB 文件及其 File、Directory、Commit 元数据；12 GiB 为对象图和后续增量保留 20% 余量且仍有界，已存在对象的重传仍不重复计费 |
| Head 验证并发 | 全局 2、每资料库 1 | 2560 个唯一 Block 的 10 GiB 文件对象图验证中位数 6.602 s；并发继续受内存和图预算保护 |
| Head 验证 deadline | 2 min | 为最坏 2000000 对象图保留余量，取消请求会释放工作槽 |
| Head 图预算 | 深度 256、遍历上下文 65536、parent 深度 1024、首次引入 Commit 1024、去重对象 2000000 | 与协议上限一致，不因本机基线较快而放宽 |
| HTTP 请求读取 deadline | 30 s | 阻止无界慢请求；对象 PUT 另受 1 min 上传 deadline 约束 |
| 优雅停止 deadline | 5 s | 停止接收新工作后等待当前持久化边界；超时执行受控关闭 |
| 磁盘低水位 | 总容量 5% 或 1 GiB，取较大值 | 保留现有硬保护，部署基准不取消空间边界 |

## 重复执行

完整 1C 正确性门禁和性能采集：

```bash
./scripts/acceptance-1c.sh
```

只重复性能场景：

```bash
FILECLOUD_RUN_1C=1 go test ./cmd/filecloud \
  -run '^TestPerformanceBaseline(SmallFiles|LargeFile|KDF)$' \
  -count=3 -timeout=90m -v
FILECLOUD_RUN_1C=1 go test ./internal/object \
  -run '^TestPerformanceBaselineWideDirectory$' \
  -count=3 -timeout=30m -v
```
