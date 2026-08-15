# Filecloud 分发说明

Filecloud 采用 [MIT License](../LICENSE) 发布。每个正式归档均包含项目 `LICENSE`、`THIRD_PARTY_NOTICES.md`，以及构建所用第三方 Go 模块的原始许可文件。

## 支持范围

正式二进制支持以下本地文件系统：

- Linux：本地 ext4。
- macOS：具备所需原子 rename 和文件锁能力的本地 APFS。
- Windows：具备 reparse point、hard-link、persistent ACL 和 Unicode 能力的本地固定 NTFS 卷。

NFS、SMB、FAT、exFAT、Windows 网络映射盘、可移动 NTFS 卷，以及无法确认所需原子性和路径安全能力的文件系统不受支持。Filecloud 会在创建客户端状态或访问远端之前拒绝这些工作目录；不会降级其持久性或路径安全保证。

## 发布门禁

正式 tag 的归档仅在以下门禁全部通过后生成：

1. Linux/ext4、macOS/APFS 和 Windows/NTFS 上的统一 1B 验收，包括固定跨平台对象向量。
2. 完整 1C 运维正确性测试和部署性能基线。
3. 每个平台已注入同一 tag、commit 和构建日期的二进制构建。
4. 每个平台二进制的初始化、用户创建、登录、资料库绑定和同步冒烟测试。
5. 从三个目标平台实际编译依赖生成第三方许可材料。

门禁证明进程崩溃恢复，不声明物理断电、存储控制器缓存或硬件故障下的绝对持久性。
