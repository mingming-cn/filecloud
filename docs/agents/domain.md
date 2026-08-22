# Domain Docs

工程技能探索代码库前，应按以下规则读取本仓库的领域文档。

## 探索前读取

- 读取仓库根目录的 `CONTEXT.md`。
- 如果根目录存在 `CONTEXT-MAP.md`，读取其中指向的、与当前任务相关的 `CONTEXT.md`。
- 读取 `docs/adr/` 中与当前工作区域相关的 ADR。

如果这些文件不存在，静默继续，不要预先建议创建。`/domain-modeling` 技能会在领域术语或架构决策实际明确后按需创建它们。

## 文件结构

本仓库采用 single-context 布局：

```text
/
├── CONTEXT.md
└── docs/adr/
    ├── 0001-use-a-new-sync-protocol.md
    ├── 0002-use-content-addressed-snapshots.md
    ├── 0003-merge-on-the-client.md
    └── 0004-restore-history-with-a-forward-commit.md
```

## 使用词汇表中的术语

输出中出现领域概念时，包括议题标题、重构建议、假设和测试名称，应使用 `CONTEXT.md` 定义的术语，不要改用词汇表明确要求避免的同义词。

若所需概念尚未出现在词汇表中，应重新判断它是否属于项目语言；若确实存在领域词汇缺口，则记录并交由 `/domain-modeling` 处理。

## 标明与 ADR 的冲突

如果输出与已有 ADR 冲突，应明确指出，不要静默覆盖：

> 与 ADR-0007（事件溯源订单）冲突，但值得重新讨论，因为……
