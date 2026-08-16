# Filecloud 服务端部署与客户端使用

本文对应当前第一阶段实现。Filecloud 只有一个命令行程序 `filecloud`：服务端和客户端使用同一个二进制。项目暂不提供 Docker 镜像、Web 管理界面、桌面 GUI、移动客户端或 WebDAV。

## 1. 支持范围

正式发布包支持：

- Linux amd64，本地 ext4。
- macOS arm64，本地 APFS。
- Windows amd64，本地固定 NTFS 卷。

客户端状态目录和同步工作目录必须位于受支持的本地文件系统。NFS、SMB、FAT/exFAT、Windows 网络映射盘、可移动 NTFS 卷不受支持。程序会在绑定阶段检查文件系统，不会降级运行。

生产环境还应满足以下条件：

- 服务端数据目录有足够空间；Filecloud 会在剩余空间低于总容量 5% 或 1 GiB 时停止接收新对象，两者取较大值。
- 公网流量通过可信反向代理使用 HTTPS。服务端自身只提供 HTTP。
- 服务端数据目录、客户端状态目录和工作目录不要相互嵌套。
- 同步期间避免让其他程序持续写入工作目录。

## 2. 获取 `filecloud`

正式版本可从项目的 [GitHub Releases](https://github.com/mingming-cn/filecloud/releases) 下载。归档中包含二进制、项目许可证、第三方模块清单和许可证文本。

也可以从源码构建。当前仓库要求 Go 1.26.5：

```bash
git clone https://github.com/mingming-cn/filecloud.git
cd filecloud
CGO_ENABLED=0 go build -trimpath -o filecloud ./cmd/filecloud
./filecloud version
```

安装到系统路径：

```bash
sudo install -m 0755 ./filecloud /usr/local/bin/filecloud
```

开发构建的版本信息会显示为 `dev`。正式发布包会显示 tag、commit 和构建日期。

## 3. 本机快速启动

下面的命令适合在当前用户下试用。数据目录必须先初始化，不能创建一个空目录后直接运行 `serve`。

```bash
DATA_DIR="$HOME/Documents/filecloud/data"

./filecloud init --data-dir "$DATA_DIR"
```

初始化会创建：

```text
<data-dir>/
  .filecloud.lock
  metadata.db
  objects/
  tmp/
```

然后创建用户。zsh 和 Bash 的 `read` 参数不同，请按当前 shell 选择命令。

zsh：

```zsh
read -rs 'PASSWORD?Password: '
printf '\n'
printf '%s\n' "$PASSWORD" |
  ./filecloud user add \
    --data-dir "$DATA_DIR" \
    --username ming \
    --password-stdin
unset PASSWORD
```

Bash：

```bash
read -rsp "Password: " PASSWORD
printf '\n'
printf '%s\n' "$PASSWORD" |
  ./filecloud user add \
    --data-dir "$DATA_DIR" \
    --username ming \
    --password-stdin
unset PASSWORD
```

启动服务：

```bash
./filecloud serve \
  --data-dir "$DATA_DIR" \
  --listen 127.0.0.1:8080
```

成功后会输出：

```text
listening on 127.0.0.1:8080
```

在另一个终端检查服务：

```bash
curl -fsS http://127.0.0.1:8080/healthz
curl -fsS http://127.0.0.1:8080/readyz
```

正常响应为：

```json
{"RetCode":0,"Message":"success"}
```

`healthz` 只检查 HTTP 进程是否存活。`readyz` 还会检查数据库、对象目录写入能力和数据目录锁。

## 4. Linux 生产部署

以下示例使用：

```text
程序：/usr/local/bin/filecloud
数据：/var/lib/filecloud
监听：127.0.0.1:8080
用户：filecloud
```

创建系统用户和数据目录：

```bash
sudo useradd \
  --system \
  --home-dir /var/lib/filecloud \
  --shell /usr/sbin/nologin \
  filecloud

sudo install -d -o filecloud -g filecloud -m 0700 /var/lib/filecloud
sudo -u filecloud /usr/local/bin/filecloud init --data-dir /var/lib/filecloud
```

创建第一个用户。`user add` 和 `user reset-password` 会取得数据目录管理锁，应在服务停止时执行。

zsh：

```zsh
read -rs 'PASSWORD?Password: '
printf '\n'
printf '%s\n' "$PASSWORD" |
  sudo -u filecloud /usr/local/bin/filecloud user add \
    --data-dir /var/lib/filecloud \
    --username alice \
    --password-stdin
unset PASSWORD
```

Bash：

```bash
read -rsp "Password: " PASSWORD
printf '\n'
printf '%s\n' "$PASSWORD" |
  sudo -u filecloud /usr/local/bin/filecloud user add \
    --data-dir /var/lib/filecloud \
    --username alice \
    --password-stdin
unset PASSWORD
```

创建 `/etc/systemd/system/filecloud.service`：

```ini
[Unit]
Description=Filecloud server
After=network.target

[Service]
Type=simple
User=filecloud
Group=filecloud
UMask=0077
ExecStart=/usr/local/bin/filecloud serve --data-dir /var/lib/filecloud --listen 127.0.0.1:8080
Restart=on-failure
RestartSec=3s
TimeoutStopSec=10s
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/filecloud

[Install]
WantedBy=multi-user.target
```

启动并查看日志：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now filecloud
sudo systemctl status filecloud
sudo journalctl -u filecloud -f
```

服务收到 `SIGTERM` 后会停止接收新请求，并在最多 5 秒内等待正在处理的请求结束。

### 新增用户或重置密码

先停止服务：

```bash
sudo systemctl stop filecloud
```

新增用户使用前面的 `user add`。重置密码前，zsh 运行 `read -rs 'PASSWORD?Password: '`，Bash 运行 `read -rsp "Password: " PASSWORD`，然后执行：

```bash
printf '\n'
printf '%s\n' "$PASSWORD" |
  sudo -u filecloud /usr/local/bin/filecloud user reset-password \
    --data-dir /var/lib/filecloud \
    --username alice \
    --password-stdin
unset PASSWORD
```

操作完成后重新启动：

```bash
sudo systemctl start filecloud
```

## 5. 配置 HTTPS 反向代理

客户端仅允许两类服务地址：

- 回环地址可以使用 HTTP，例如 `http://127.0.0.1:8080`。
- 非回环地址必须使用 HTTPS，例如 `https://cloud.example.com`。

因此不要把 `filecloud serve --listen 0.0.0.0:8080` 直接暴露到公网。下面是 Nginx 配置的核心部分：

```nginx
server {
    listen 443 ssl;
    server_name cloud.example.com;

    ssl_certificate     /etc/letsencrypt/live/cloud.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/cloud.example.com/privkey.pem;

    # Directory 元数据对象最大为 32 MiB，给协议头和代理留出余量。
    client_max_body_size 40m;

    # 客户端请求预算为 3 分钟，Head 校验的服务端预算为 2 分钟。
    proxy_connect_timeout 10s;
    proxy_read_timeout 300s;
    proxy_send_timeout 300s;
    proxy_request_buffering off;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
    }
}
```

配置完成后检查公网地址：

```bash
curl -fsS https://cloud.example.com/healthz
curl -fsS https://cloud.example.com/readyz
```

客户端使用系统证书池验证 TLS。自签名证书必须先加入客户端操作系统的信任存储。

## 6. 客户端登录

以下示例使用 `jq` 从登录响应中读取 token：

```bash
SERVER="https://cloud.example.com"
```

zsh：

```zsh
read -rs 'PASSWORD?Password: '
printf '\n'
LOGIN_JSON=$(
  printf '%s\n' "$PASSWORD" |
    filecloud login \
      --server "$SERVER" \
      --username alice \
      --device-name laptop \
      --password-stdin
)
unset PASSWORD

TOKEN=$(printf '%s' "$LOGIN_JSON" | jq -er '.Session.AccessToken')
unset LOGIN_JSON
```

Bash 只需将第一行换成：

```bash
read -rsp "Password: " PASSWORD
```

登录 token 的有效期是 30 天。服务端数据库只保存 token 的 SHA-256 摘要，但客户端绑定后会把原始 token 保存在权限受限的客户端数据库中。不要把 token 写入命令行参数、Git 仓库或普通日志。

本机试用时，服务地址可以改为：

```bash
SERVER="http://127.0.0.1:8080"
```

## 7. 创建和查看资料库

每个资料库和每台设备都需要一个规范的小写 UUID。

Linux/macOS：

```bash
LIBRARY_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')
DEVICE_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')
```

Windows PowerShell：

```powershell
$LibraryId = [guid]::NewGuid().ToString().ToLowerInvariant()
$DeviceId = [guid]::NewGuid().ToString().ToLowerInvariant()
```

创建资料库：

```bash
printf '%s\n' "$TOKEN" |
  filecloud library create \
    --server "$SERVER" \
    --library-id "$LIBRARY_ID" \
    --name "Documents" \
    --token-stdin
```

列出当前用户的资料库：

```bash
printf '%s\n' "$TOKEN" |
  filecloud library list \
    --server "$SERVER" \
    --token-stdin
```

查看指定资料库：

```bash
printf '%s\n' "$TOKEN" |
  filecloud library inspect \
    --server "$SERVER" \
    --library-id "$LIBRARY_ID" \
    --token-stdin
```

当前 CLI 没有删除资料库的子命令。

## 8. 绑定工作目录

`client-dir` 保存 `client.db`、绑定关系、同步基线和崩溃恢复日志。`worktree` 是用户实际编辑文件的目录。两者必须分开，`client-dir` 不能位于 `worktree` 内。

```bash
CLIENT_DIR="$HOME/.local/share/filecloud"
WORKTREE="$HOME/Filecloud/Documents"

mkdir -p "$CLIENT_DIR" "$WORKTREE"
```

### 空工作目录

远端为空时，首次绑定会创建初始空提交。远端已有内容时，首次绑定会下载远端内容。

```bash
printf '%s\n' "$TOKEN" |
  filecloud library bind \
    --client-dir "$CLIENT_DIR" \
    --server "$SERVER" \
    --library-id "$LIBRARY_ID" \
    --worktree "$WORKTREE" \
    --device-id "$DEVICE_ID" \
    --token-stdin
```

### 导入已有本地文件

只有远端资料库为空时才能导入非空工作目录，必须显式使用 `--import-local`：

```bash
printf '%s\n' "$TOKEN" |
  filecloud library bind \
    --client-dir "$CLIENT_DIR" \
    --server "$SERVER" \
    --library-id "$LIBRARY_ID" \
    --worktree "$WORKTREE" \
    --device-id "$DEVICE_ID" \
    --token-stdin \
    --import-local
```

如果本地和远端同时非空且没有共同同步基线，绑定会拒绝继续。此时应改用新的空工作目录，不能让客户端猜测合并起点。

绑定成功后，后续 `sync` 和 `watch` 从 `client.db` 读取服务地址、设备 ID 和 token，不再要求输入 token。

重新登录得到新 token 后，可以使用相同的 `bind` 参数刷新现有绑定的凭据。刷新时工作目录必须尚未发生未同步变化；否则先使用仍有效的旧凭据完成同步。

## 9. 同步

执行一轮同步：

```bash
filecloud library sync \
  --client-dir "$CLIENT_DIR" \
  --worktree "$WORKTREE"
```

没有变化时输出：

```text
library already synchronized
```

持续轮询，例如每 30 秒同步一次：

```bash
filecloud library watch \
  --client-dir "$CLIENT_DIR" \
  --worktree "$WORKTREE" \
  --interval 30s
```

`watch` 使用定时轮询，不依赖文件系统事件。不要同时对同一工作目录运行多个 `sync` 或 `watch`，绑定锁会拒绝并发操作。

### 多设备

第二台设备使用同一个 `SERVER` 和 `LIBRARY_ID`，但必须生成新的 `DEVICE_ID`，并使用独立的 `CLIENT_DIR` 和 `WORKTREE`。先登录，再绑定一个空工作目录即可下载现有资料库。

双方同时修改同一路径时，远端版本保留原文件名，本地版本会生成带 `Filecloud conflict` 标记的冲突副本。冲突副本也会进入资料库并同步到其他设备。

### 大量删除保护

候选提交删除超过 100 个已跟踪路径，或者删除比例达到 10% 时，同步会停止并输出一个 12 位候选前缀。确认工作目录内容无误后，按错误消息给出的前缀重新执行：

```bash
filecloud library sync \
  --client-dir "$CLIENT_DIR" \
  --worktree "$WORKTREE" \
  --confirm-delete 0123456789ab
```

不能使用更短前缀、完整 commit ID 或其他候选的前缀。

## 10. 解除绑定和注销

解除绑定会删除客户端绑定状态，不会删除工作目录中的普通文件，也不会删除服务端资料库：

```bash
filecloud library unbind \
  --client-dir "$CLIENT_DIR" \
  --worktree "$WORKTREE"
```

注销会撤销当前 token：

```bash
printf '%s\n' "$TOKEN" |
  filecloud logout \
    --server "$SERVER" \
    --token-stdin
unset TOKEN
```

已经保存在绑定数据库中的同一 token 也会失效。之后需要重新登录并刷新绑定凭据。

## 11. 服务端维护

垃圾回收和完整性检查必须离线运行。先停止服务：

```bash
sudo systemctl stop filecloud
```

完整性检查：

```bash
sudo -u filecloud /usr/local/bin/filecloud integrity check \
  --data-dir /var/lib/filecloud
```

预览垃圾回收：

```bash
sudo -u filecloud /usr/local/bin/filecloud gc \
  --data-dir /var/lib/filecloud \
  --dry-run
```

执行垃圾回收：

```bash
sudo -u filecloud /usr/local/bin/filecloud gc \
  --data-dir /var/lib/filecloud
```

GC 只处理未发布对象，默认宽限期为 24 小时。已经发布的完整历史会永久保留。

维护完成后启动服务：

```bash
sudo systemctl start filecloud
curl -fsS http://127.0.0.1:8080/readyz
```

### 备份与恢复

备份必须同时包含 SQLite 元数据和对象目录。推荐流程：

```bash
sudo install -d -m 0700 /srv/backup
sudo systemctl stop filecloud
sudo tar -C /var/lib -czf "/srv/backup/filecloud-$(date +%Y%m%d-%H%M%S).tar.gz" filecloud
sudo systemctl start filecloud
```

恢复时先停止服务，把整个数据目录恢复到同一路径，确认所有者和权限，然后运行完整性检查：

```bash
sudo chown -R filecloud:filecloud /var/lib/filecloud
sudo chmod 0700 /var/lib/filecloud
sudo -u filecloud /usr/local/bin/filecloud integrity check --data-dir /var/lib/filecloud
sudo systemctl start filecloud
```

不要只备份 `metadata.db` 或只备份 `objects/`，两者单独恢复都不能组成一致的资料库。

## 12. 常见错误

### `open data-directory lock ... no such file or directory`

原因：数据目录存在，但没有执行过 `filecloud init`，所以缺少 `.filecloud.lock`。

处理：

```bash
filecloud init --data-dir /path/to/data
```

初始化成功后再创建用户并启动服务。已有正式数据的目录不要删除后重新初始化。

### zsh: `read: -p: no coprocess`

原因：`read -p` 在 zsh 中表示从 coprocess 读取；它不是 Bash 的提示参数。读取失败后密码变量为空，随后还会看到 `password must be 1-1024 bytes`。

zsh 使用：

```zsh
read -rs 'PASSWORD?Password: '
```

Bash 使用：

```bash
read -rsp "Password: " PASSWORD
```

### `password must be 1-1024 bytes`

传入的密码为空或超过 1024 bytes。先确认 `read` 命令适用于当前 shell，并确认管道左侧确实输出了一行密码。

### `server must use https unless it is a loopback url`

客户端正在访问非回环 HTTP 地址。为服务配置 HTTPS，并把 `--server` 改成 `https://...`。本机测试可以使用 `http://127.0.0.1:8080` 或 `http://localhost:8080`。

### `data directory is locked by another process`

`serve`、用户管理、GC 或完整性检查正在争用数据目录锁。停止正在运行的服务后再执行离线管理命令。

### `worktree is not bound`

传给 `sync` 的工作目录没有在指定 `client-dir` 中完成绑定，或者使用了另一套客户端状态目录。检查 `--client-dir` 和 `--worktree` 是否与绑定时完全一致。

### 文件系统被拒绝

确认客户端状态目录和工作目录位于本地 ext4、APFS 或固定 NTFS。网络共享、软链接绕转、挂载边界和不受支持的卷会被拒绝。

## 13. 命令速查

```text
filecloud version
filecloud init --data-dir PATH
filecloud serve --data-dir PATH [--listen ADDRESS]
filecloud user add --data-dir PATH --username NAME --password-stdin
filecloud user reset-password --data-dir PATH --username NAME --password-stdin
filecloud login --server URL --username NAME --device-name NAME --password-stdin
filecloud logout --server URL --token-stdin
filecloud library create --server URL --library-id UUID --name NAME --token-stdin
filecloud library list --server URL --token-stdin
filecloud library inspect --server URL --library-id UUID --token-stdin
filecloud library bind --client-dir PATH --server URL --library-id UUID --worktree PATH --device-id UUID --token-stdin [--import-local]
filecloud library sync --client-dir PATH --worktree PATH [--confirm-delete 12_HEX]
filecloud library watch --client-dir PATH --worktree PATH --interval DURATION
filecloud library unbind --client-dir PATH --worktree PATH
filecloud integrity check --data-dir PATH
filecloud gc --data-dir PATH [--dry-run] [--grace-period DURATION]
```

协议、冲突处理和崩溃恢复边界见 [同步架构](../design/architecture.md)，接口细节见 [HTTP API 契约](../design/http-api.md)。
