# Sync HTTP API 契约

状态：阶段 0 评审中。基础路径 `/v1`。生产环境只能经 HTTPS 调用；服务端自身由反向代理提供 TLS。冻结前必须通过对象测试向量、状态机测试和目标文件系统 spike；任何语义修订必须同时更新架构文档和验收用例。

本 API 是新协议，不兼容 Seafile。JSON 字段使用 PascalCase；所有控制面 JSON 成功或失败响应都包含 `RetCode` 和 `Message`。二进制成功响应及 `GetMetadataObject` 返回的规范对象字节除外；这些接口失败时仍返回 JSON 错误对象。

## 通用规则

### 鉴权

除创建会话和健康检查外，使用：

```http
Authorization: Bearer <opaque-token>
```

服务端先鉴权，再查找用户请求的资料库。第一阶段只有资料库所有者可访问。

### JSON 响应

```json
{
  "RetCode": 0,
  "Message": "success",
  "Library": {}
}
```

错误不得包含 SQL、堆栈、内部文件路径或 token。HTTP 状态表达协议类别，`RetCode` 表达稳定的程序错误。

| HTTP | RetCode | 名称 | 说明 |
|---:|---:|---|---|
| 200/201/204 | 0 | Success | 成功 |
| 400 | 1000 | InvalidArgument | 字段、路径、对象格式或分页参数非法 |
| 401 | 1001 | Unauthenticated | token 缺失、无效、过期或撤销 |
| 403 | 1002 | PermissionDenied | 已认证但无权限 |
| 404 | 2000 | NotFound | 资源不存在 |
| 409 | 3000 | AlreadyExists | 唯一资源已存在 |
| 409 | 3001 | ObjectConflict | 同一 ObjectId 已存在不同内容 |
| 412 | 3002 | HeadConflict | `If-Match` 与当前 Head 不一致 |
| 422 | 3003 | MissingObject | 新 Head 的可达图缺对象 |
| 422 | 3004 | HashMismatch | 正文摘要与 URL ObjectId 不一致 |
| 413 | 3005 | PayloadTooLarge | 正文、对象数量或单批次超限 |
| 428 | 3006 | PreconditionRequired | Head 更新缺少 `If-Match` |
| 429 | 4000 | RateLimited | 超限；响应携带 `Retry-After` |
| 500 | 5000 | Internal | 未公开的服务端错误 |
| 503 | 5001 | Unavailable | 存储暂不可用，可重试 |

错误示例：

```json
{
  "RetCode": 3002,
  "Message": "library head changed",
  "Head": {
    "CommitId": "<current>",
    "ETag": "\"head-version-8\""
  }
}
```

### 时间、ID 与分页

- 时间是 RFC3339 UTC。
- UserId、LibraryId、DeviceId 只接受 RFC 9562 规范小写连字符形式（例如 `01234567-89ab-4def-8123-456789abcdef`）；其他等价文本表示一律拒绝。
- ObjectId 是 64 位小写 SHA-256 十六进制。
- List 使用 `PageSize` 和 `PageToken`；默认 100，最大 500；响应返回 `NextPageToken`。
- page token 是不透明值，客户端不得解析。

### 幂等与重试

- 所有对象 `PUT` 由 ObjectId 天然幂等。
- 资料库 ID 由客户端生成，创建使用资源 `PUT`，重复请求天然幂等。
- Head 更新以 `If-Match` 实现并发控制；网络结果未知时先读取 Head，不盲目重放。
- 客户端只自动重试明确幂等请求的网络错误、429 和 503；`CreateSession` 禁止自动重试。其他 4xx 必须重新计算或提示用户。

## CreateSession

`POST /v1/sessions`

创建可撤销访问令牌。正文只允许经 HTTPS 发送；不得记录。请求正文最大 8 KiB，超过返回 `PayloadTooLarge`。

请求：

```json
{
  "Username": "alice",
  "Password": "...",
  "DeviceName": "alice-laptop"
}
```

约束：Username 1-128 字符；Password 1-1024 字节；DeviceName 1-128 字符。`CreateSession` 不可自动重试，响应必须带 `Cache-Control: no-store`。

响应 `200`：

```json
{
  "RetCode": 0,
  "Message": "success",
  "Session": {
    "AccessToken": "<shown-once>",
    "ExpiresAt": "2026-09-08T00:00:00Z",
    "UserId": "<UUID>"
  }
}
```

错误：`InvalidArgument`、`Unauthenticated`、`RateLimited`。用户名不存在和密码错误必须执行等价 KDF 工作并返回相同消息与状态。所有 `401` 响应携带 `WWW-Authenticate: Bearer`。

登录按来源 IP、规范用户名和全局 KDF 并发数限流；达到并发上限立即返回 `429`，不得无界排队。具体默认值在目标硬件基准后写入部署配置，但“必须有界”属于协议安全要求。

## DeleteCurrentSession

`DELETE /v1/sessions/current`

撤销当前 token。首次成功返回 `204`；之后该 token 已不能通过鉴权，重复请求返回 `401 Unauthenticated`。

## CreateLibrary

`PUT /v1/libraries/{LibraryId}`

LibraryId 由客户端生成 UUID。请求正文最大 8 KiB；Name 为 NFC UTF-8、1-128 字符且最多 512 bytes。请求：

```json
{
  "Name": "Documents"
}
```

首次创建响应 `201`；相同用户以同一 LibraryId 和 Name 重试返回 `200`；同一 ID 携带不同 Name 返回 `ObjectConflict`。响应：

```json
{
  "RetCode": 0,
  "Message": "success",
  "Library": {
    "LibraryId": "<UUID>",
    "Name": "Documents",
    "HeadCommitId": null,
    "ETag": "\"head-version-0\"",
    "CreatedAt": "2026-08-09T00:00:00Z",
    "UpdatedAt": "2026-08-09T00:00:00Z"
  }
}
```

同一所有者下 Name 唯一。错误：`InvalidArgument`、`AlreadyExists`、`ObjectConflict`。

## ListLibraries

`GET /v1/libraries?PageSize=100&PageToken=...`

响应：

```json
{
  "RetCode": 0,
  "Message": "success",
  "Libraries": [
    {
      "LibraryId": "<UUID>",
      "Name": "Documents",
      "HeadCommitId": "<CommitId>",
      "ETag": "\"head-version-8\"",
      "CreatedAt": "2026-08-09T00:00:00Z",
      "UpdatedAt": "2026-08-09T01:00:00Z"
    }
  ],
  "NextPageToken": ""
}
```

默认按 `CreatedAt, LibraryId` 升序稳定分页。

## GetLibrary

`GET /v1/libraries/{LibraryId}`

返回与 `CreateLibrary` 相同的 Library 对象，并在 HTTP `ETag` header 返回当前 Head 版本。不存在返回 `NotFound`。

## GetLibraryHead

`GET /v1/libraries/{LibraryId}/head`

响应：

```json
{
  "RetCode": 0,
  "Message": "success",
  "Head": {
    "CommitId": "<CommitId-or-null>",
    "ETag": "\"head-version-8\""
  }
}
```

HTTP 同时返回 `ETag`。客户端可发送 `If-None-Match`；未变化返回 `304`，无 JSON body。

## CheckObjects

`POST /v1/libraries/{LibraryId}/object-checks`

检查服务端缺失对象，只是传输优化，不是发布验证。每次最多 1000 个 ID，整个 JSON 最大 1 MiB。

请求：

```json
{
  "Objects": [
    {"ObjectId": "<sha256>", "ObjectType": "Block"},
    {"ObjectId": "<sha256>", "ObjectType": "Directory"}
  ]
}
```

`ObjectType` 取 `Block`、`File`、`Directory`、`Commit`。

响应：

```json
{
  "RetCode": 0,
  "Message": "success",
  "MissingObjects": [
    {"ObjectId": "<sha256>", "ObjectType": "Block"}
  ]
}
```

对象存在不代表其可被新 Head 引用；`UpdateLibraryHead` 仍执行完整图验证。

## 资源预算

- Commit JSON 最大 64 KiB；File JSON 最大 20 MiB；Directory JSON 最大 32 MiB。
- 一个 File 最多 262144 个 blocks，最大逻辑大小 1 TiB。
- 一个 Directory 最多 100000 个 entries；快照树深最多 256。
- 一次 Head 验证的共享总预算为 2000000 个去重对象，包含当前 Root 和第二 parent 首次引入的全部对象；首次引入的 Commit 最多 1024 个，parent 遍历深度最多 1024。
- 普通提交验证当前 Root。合并提交还必须验证第二 parent 首次带入已发布历史的 Commit 和快照，直到遇到已发布可达对象；作者、类型、摘要和引用均需验证。
- JSON 解析、数组分配和图遍历必须先检查预算；任一预算超限返回 `PayloadTooLarge`，不得部分接受 Head。Head 验证受全局工作池、每资料库单并发和请求 deadline 约束，相同 ETag 的重复昂贵验证可短期缓存结果。
- block 始终流式处理，不按 `Content-Length` 一次性分配内存。
- 每个对象 PUT 在读取正文前取得全局和用户并发槽并设置读取 deadline；无槽返回 `RateLimited`。PutBlock 还按 Content-Length 原子预留临时字节，失败时释放。
- 磁盘剩余空间低于总容量 5% 或 1 GiB（取较大值）时拒绝新的对象 PUT，返回 `Unavailable`。用户滚动时间窗内首次接收的对象字节达到安全预算时返回 `RateLimited`；重传已存在对象不重复计费。这些限制用于保护共享服务，不是用户可配置容量配额。

## PutMetadataObject

`PUT /v1/libraries/{LibraryId}/objects/{ObjectType}/{ObjectId}`

`ObjectType` 只能是 `files`、`directories`、`commits`。Header：`Content-Type: application/json`。正文可为普通 JSON，服务端解析、校验、按 RFC 8785 重新规范化并计算 SHA-256；摘要必须等于 URL ObjectId。

成功首次写入返回 `201`，同内容已存在返回 `200`：

```json
{
  "RetCode": 0,
  "Message": "success",
  "Object": {
    "ObjectId": "<sha256>",
    "ObjectType": "File",
    "Created": true
  }
}
```

未知字段、重复 JSON key、错误 Type/Version 或非法路径名返回 `InvalidArgument`。正文、数组、深度或对象数量超过硬限制返回 `PayloadTooLarge`。同 ID 不同内容返回 `ObjectConflict`；摘要不同返回 `HashMismatch`。

## GetMetadataObject

`GET /v1/libraries/{LibraryId}/objects/{ObjectType}/{ObjectId}`

成功返回 `application/json` 的 RFC 8785 规范字节，并带强 ETag：`ETag: "<ObjectId>"`、`Cache-Control: private, immutable`。不存在返回 `NotFound`。

## PutBlock

`PUT /v1/libraries/{LibraryId}/blocks/{ObjectId}`

Header：`Content-Type: application/octet-stream`、`Content-Length` 必填。缺失或非法 Content-Length 返回 `InvalidArgument`；正文超过 4 MiB 返回 `PayloadTooLarge`；零长度返回 `InvalidArgument`。空文件由空 Blocks 数组表示。File 校验时，除最后一个 block 外都必须恰好为 4 MiB，最后一个 block 为 1 byte 至 4 MiB。服务端流式写临时文件并计算 SHA-256，匹配后原子发布。

成功首次写入 `201`，已存在相同 block `200`：

```json
{
  "RetCode": 0,
  "Message": "success",
  "Block": {
    "ObjectId": "<sha256>",
    "Size": "4194304",
    "Created": true
  }
}
```

连接中断不得留下最终对象；临时文件由宽限期清理。

## GetBlock

`GET /v1/libraries/{LibraryId}/blocks/{ObjectId}`

成功返回 `application/octet-stream`，带 `Content-Length`、`ETag: "<ObjectId>"`、`Cache-Control: private, immutable`。第一阶段按 block 恢复，不要求单 block Range；客户端重新请求未完成 block。

## UpdateLibraryHead

`PUT /v1/libraries/{LibraryId}/head`

必须带从 `GetLibraryHead` 获得的单个强 `If-Match`；初始空 Head 使用 `"head-version-0"`。请求正文最大 4 KiB。缺失 `If-Match` 返回 `PreconditionRequired`；弱 ETag、ETag 列表、`*` 或语法错误返回 `InvalidArgument`。请求：

```json
{
  "CommitId": "<CommitId>"
}
```

服务端在一个事务语义内：

1. 鉴权并读取当前 Head/ETag。
2. 比较 `If-Match`。
3. 在对象存储共享锁下读取 Commit，验证 `AuthorUserId` 等于认证用户，并验证其第一个 Parent 与当前 Head 一致；初始提交 Parents 为空。
4. 遍历并校验当前 Root 的完整可达图、对象类型、摘要、block 总大小和路径规则。若存在第二 parent，同时验证它首次引入发布历史的子图。
5. 用短数据库事务再次比较 Head/ETag，并原子更新 Head 和 head version。第一阶段 GC 必须离线取得独占锁，因此第 4、5 步之间对象不会消失。

响应 `200`：

```json
{
  "RetCode": 0,
  "Message": "success",
  "Head": {
    "CommitId": "<CommitId>",
    "ETag": "\"head-version-9\""
  }
}
```

`If-Match` 不匹配始终返回 `HeadConflict` 和当前 Head；不得修改 Head。缺对象返回 `MissingObject`，响应最多列出前 100 个缺失 ObjectId 并给出 `Truncated`，不得接受部分发布。

CAS 请求结果因断网而未知时，客户端必须调用 `GetLibraryHead`：当前 CommitId 等于待发布 CommitId 表示已成功，否则按 pending publication 状态重试或合并。服务端不得仅因 CommitId 相同绕过旧 ETag 返回成功。

## Health

`GET /healthz` 仅表示进程存活，不访问数据库。

`GET /readyz` 检查元数据库可读和对象目录可写；成功返回 `200`，失败返回 `503`。响应不得泄露内部路径或数据库 DSN。

`204` 与 `304` 没有响应正文，不受 JSON envelope 规则约束。已认证用户访问不属于自己的 Library 与资源不存在统一返回 `404 NotFound`，避免泄露资源身份。

## 协议版本演进

- URL 主版本 `/v1` 内只允许向响应增加可忽略的可选字段。
- 客户端发送未知请求字段默认拒绝，防止拼写被静默忽略。
- `/v1` 只接受对象 `Version: 1`。对象编码一旦发布不可原地改变。
- 新对象 Version、字段删除、类型变化、哈希输入或路径语义变化必须发布新的 HTTP 主版本；第一阶段不实现 capability negotiation。
- WebDAV 未来使用独立 URL 根和认证适配器，不复用这些对象路由作为 DAV 路径。
