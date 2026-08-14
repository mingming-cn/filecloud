# macOS/APFS 文件系统原语 spike

状态：实现与自动化门禁已落地，并已在 macOS/APFS 实机通过完整 1B 门禁。后续平台或文件系统变更仍必须重新运行显式门禁，不能用交叉编译代替。

## 平台实现

| 正确性要求 | Darwin 实现 | 绑定/门禁边界 |
|---|---|---|
| no-follow | 每个路径段使用目录 fd + `openat(O_NOFOLLOW)`；扫描先 `fstatat(AT_SYMLINK_NOFOLLOW)`，普通文件以 `O_NONBLOCK` 打开后复核身份 | symlink、父身份变化或跨设备路径立即失败 |
| 文件身份 | 打开的 fd 与路径均用 `(st_dev, st_ino, type, nlink)` 比较 | 任何不一致中止整轮，不收养未知 inode |
| no-replace | checkout/cache 使用 `renameatx_np(..., RENAME_EXCL)`；服务端不可变对象发布使用同目录 `link` | 绑定通过 `fgetattrlist(ATTR_VOL_CAPABILITIES)` 要求 `VOL_CAP_INT_RENAME_EXCL` 的 valid/support 位同时存在；门禁分别实测 rename 与 hard-link 目标已存在时不覆盖 |
| 名称大小写 | 应用始终按 Unicode 默认大小写折叠拒绝结构碰撞；APFS 卷可配置为大小写敏感或不敏感 | 门禁实测并记录 `case-sensitive-distinct` 或 `case-insensitive-alias`；需要两个物理别名共存的测试只在前者运行，后者由卷原生排他 lookup 覆盖 |
| 同目录/跨目录 rename | 源、目标均使用已验证父目录 fd 和 leaf name | `EXDEV` 或父身份变化失败，不退化为 copy-and-replace |
| 文件与父目录持久化 | 文件 `fsync` 后 rename，再同步受影响父目录 | 自动化只声明进程崩溃恢复；Apple 未给出 APFS 目录 fsync 的断电保证 |
| 绑定/数据目录锁 | `flock(LOCK_EX|LOCK_NB)` | 要求 `VOL_CAP_INT_FLOCK` 的 valid/support 位；门禁用独立进程验证排他性 |
| 原始 mtime 恢复 | fd 上 `futimes` | Darwin 可设置到微秒，rollback 证据在平台边界规范到微秒；协议 mtime 仍为 UTC 整秒 |
| 文件系统识别 | 对已打开工作目录执行 `fstatfs` | 只接受 `Fstypename == "apfs"` 且 `MNT_LOCAL`；NFS、SMB、FAT/exFAT 和未知卷明确拒绝 |

## 自动化证明

`TestMacOSAPFSPrimitives` 在门禁根上实测：

1. `O_NOFOLLOW` 不打开 symlink。
2. rename 前后已打开文件保持同一 `(device, inode)`。
3. 目标已存在时 `RENAME_EXCL` 返回 `EEXIST` 且不覆盖目标。
4. 目标已存在时对象发布使用的 hard link 返回 exists 且不覆盖目标。
5. 大小写变体 lookup 被实测并记录为卷的敏感/不敏感模式，且不覆盖已有文件。
6. 同目录 no-replace rename 成功。
7. APFS 目录 fd 的 `Sync` 成功。
8. 独立进程无法取得已经持有的排他 `flock`。
9. 活动路径安装新 inode 后，旧 fd 写入仍修改旧 inode，不修改活动路径。

该测试输出 `filesystem-primitives` 类型的 `FILECLOUD_ATTESTATION`。macOS 1B 顶层门禁还会在同一个已验证 APFS 根上执行完整 1A 对象、扫描、checkout、故障注入、传输恢复、权限、资源限制和同步收敛矩阵；任何测试失败、证明缺失/重复或平台标签错误都会使门禁失败。非 helper skip 只有 5 个需要物理共存大小写别名的精确测试名可在 Darwin 放行，并由同次门禁的 `casefoldLookup=case-insensitive-alias` 实测证明补位；大小写敏感 APFS 不触发这些 skip。

2026-08-14 的验收记录：macOS 26.5.1（Darwin 25.5.0，arm64）、Go 1.26.5、本地大小写不敏感 APFS；完整门禁耗时 496.76 秒，required-pass 与 109 条结构化证明全部通过，`casefoldLookup=case-insensitive-alias`。

运行命令：

```bash
FILECLOUD_RUN_1B_APFS=1 go test ./cmd/filecloud \
  -run '^TestMacOSAPFSAcceptanceMatrix$' \
  -count=1 -timeout=30m -v
```

## 旧 fd 警告边界

Unix 文件描述符引用打开时的文件对象，不会在 pathname 被 checkout 替换后自动重新绑定。APFS spike 要求观测到：

- checkout 后活动路径仍是新 inode 和远端内容；
- 旧 fd 写入旧 inode；若旧 inode 已被提升为可见冲突副本，写入出现在该副本；
- 若 checkout 已完成并清理了未变化的 recovery inode，随后才发生的旧 fd 写入可能只存在于无目录项的旧 inode，关闭最后一个 fd 后消失。

因此第一阶段保证 checkout 时已经确认或捕获的内容，不保证任意进程在 checkout 成功后继续通过旧 fd 写入的内容必然同步或长期可见。用户必须避免同步期间由其他进程持续写入绑定目录；扫描或 checkout 能观察到身份/内容变化时会中止或保留冲突，但无法撤销另一个进程持有的 fd。

## 一手资料

- Apple/XNU [`open(2)`](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/man/man2/open.2)：`openat`、`O_NOFOLLOW` 语义。
- Apple/XNU [`rename(2)`](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/man/man2/rename.2)：`renameatx_np`、`RENAME_EXCL`、`EEXIST` 与 `EXDEV`。
- Apple/XNU [`attr.h`](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/sys/attr.h)：`VOL_CAP_INT_FLOCK`、`VOL_CAP_INT_RENAME_EXCL` 和卷能力 valid/support 位。
- Apple/XNU [`fsync(2)`](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/man/man2/fsync.2)：`fsync` 与断电/驱动缓存限制。
- Apple/XNU [`flock(2)`](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/man/man2/flock.2)：跨进程 advisory lock 语义。
- Apple/XNU [`unlink(2)`](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/man/man2/unlink.2) 与 [`write(2)`](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/man/man2/write.2)：打开引用与 pathname 分离后的旧 fd 行为。
- Go [`x/sys/unix`](https://github.com/golang/sys/tree/master/unix)：Darwin syscall wrapper、结构体和常量。

Apple 公共资料没有给出 APFS 父目录 fsync 的完整掉电持久化配方。本阶段门禁证明进程崩溃后的状态机恢复，不宣称物理断电、控制器缓存或硬件故障下的持久性。
