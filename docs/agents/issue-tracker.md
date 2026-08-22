# Issue tracker: GitHub

本仓库的议题和规格保存在 GitHub Issues 中。所有操作使用 `gh` CLI。

## 约定

- **创建议题**：`gh issue create --title "..." --body "..."`。多行正文使用 heredoc。
- **读取议题**：`gh issue view <number> --comments`，并使用 `jq` 筛选评论、获取标签。
- **列出议题**：`gh issue list --state open --json number,title,body,labels,comments --jq '[.[] | {number, title, body, labels: [.labels[].name], comments: [.comments[].body]}]'`，按需添加 `--label` 和 `--state`。
- **评论议题**：`gh issue comment <number> --body "..."`
- **添加或移除标签**：`gh issue edit <number> --add-label "..."` / `--remove-label "..."`
- **关闭议题**：`gh issue close <number> --comment "..."`

仓库由当前目录中的 `git remote -v` 推断；`gh` 在 Git clone 内运行时会自动完成此操作。

## 将 Pull Request 作为 triage 输入

**PRs as a request surface: no.** 如需将外部 PR 当作功能请求处理，可将此值改为 `yes`；`/triage` 会读取该标志。

设为 `yes` 后，PR 使用与议题相同的标签和状态：

- **读取 PR**：`gh pr view <number> --comments`，使用 `gh pr diff <number>` 获取差异。
- **列出待 triage 的外部 PR**：`gh pr list --state open --json number,title,body,labels,author,authorAssociation,comments`，仅保留 `authorAssociation` 为 `CONTRIBUTOR`、`FIRST_TIME_CONTRIBUTOR` 或 `NONE` 的记录。
- **评论、标记或关闭**：使用 `gh pr comment`、`gh pr edit --add-label`、`gh pr edit --remove-label` 和 `gh pr close`。

GitHub 的议题和 PR 共享编号空间。遇到 `#42` 时，先运行 `gh pr view 42`，失败后再运行 `gh issue view 42`。

## 技能要求“发布到 issue tracker”时

创建一个 GitHub Issue。

## 技能要求“获取相关 ticket”时

运行 `gh issue view <number> --comments`。

## Wayfinding 操作

`/wayfinder` 使用一个 map 议题和多个 child 议题：

- **Map**：带 `wayfinder:map` 标签的单一议题，正文保存 Notes、Decisions-so-far 和 Fog。使用 `gh issue create --label wayfinder:map` 创建。
- **Child ticket**：通过 GitHub sub-issue API 关联到 map。若 sub-issue 不可用，则加入 map 的任务列表，并在 child 正文顶部写入 `Part of #<map>`。标签为 `wayfinder:<type>`，其中类型为 `research`、`prototype`、`grilling` 或 `task`。领取后分配给执行开发者。
- **Blocking**：优先使用 GitHub 原生 issue dependencies。通过 `gh api --method POST repos/<owner>/<repo>/issues/<child>/dependencies/blocked_by -F issue_id=<blocker-db-id>` 添加依赖；`blocker-db-id` 必须是 `gh api repos/<owner>/<repo>/issues/<n> --jq .id` 返回的数据库 ID。若原生依赖不可用，则在 child 正文顶部写入 `Blocked by: #<n>, #<n>`。
- **Frontier query**：列出 map 的未关闭 child，排除仍有未关闭 blocker 或已有 assignee 的记录，按 map 中的顺序选择第一个。
- **Claim**：运行 `gh issue edit <n> --add-assignee @me`；这是会话中的第一次写操作。
- **Resolve**：运行 `gh issue comment <n> --body "<answer>"`，关闭议题，再将上下文指针和链接追加到 map 的 Decisions-so-far。
