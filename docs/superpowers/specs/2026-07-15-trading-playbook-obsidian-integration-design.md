# 交易预案与 Obsidian 集成设计

日期：2026-07-15

状态：已在对话中确认，待书面规格复核

## 1. 背景

项目已经具备两套相关能力：

- 交易预案系统从 8 份文字稿归纳 19 种交易模式，并在 14:40、15:10、15:30、08:50、09:26 生成预案、复盘和提醒。
- `ObsidianKnowledgeService` 已能把每日资讯、产业趋势和旧超短信号导出到外部 Obsidian Vault，并支持状态查询、幂等 Markdown 写入和可选 Git 提交。

当前缺口是新版交易预案数据没有进入 Obsidian。现有每日知识导出只读取 `DailyInfoDigest`、`DailyAnalysisRecord`、`JiegeModeSignal` 等旧知识域数据，无法完整呈现新版规则版本、预案版本、候选、独立提醒和执行复盘。

本设计在现有 Obsidian 基础设施上增加独立的交易预案导出边界，不重建第二套 Vault 配置，也不让 Obsidian 成为交易系统的写入入口。

## 2. 已确认的产品决策

1. 采用单向同步。项目数据库是唯一事实源，Obsidian 不反向更新规则、预案、候选、提醒或复盘。
2. 第一版导出完整闭环：19 种模式、所有预案版本、候选、提醒时间线、15:10 初步复盘和 15:30 最终复盘。
3. 14:40、15:10、15:30、08:50、09:26 每个阶段完成后自动同步，同时保留页面手动重导入口。
4. 系统自动页与个人手记彻底分离。系统只写 `Auto` 目录和指定 Dashboard；`Notes` 页面由用户在 Obsidian 内创建，系统永不创建或修改。
5. 使用独立的 `TradingPlaybookObsidianExporter`，不继续扩大现有 `ObsidianKnowledgeService` 的业务职责。

## 3. 目标

- 在 Obsidian 中形成从“文字稿证据 → 交易模式 → 每日预案版本 → 候选与提醒 → 执行复盘”的可追溯知识链。
- 保留每个预案和复盘阶段的历史，不用新内容覆盖不可变版本。
- Vault 暂时不可用、磁盘写入失败或 Git 失败时，不影响预案、复盘、提醒和人工确认。
- 支持服务重启后的自动补偿和指定交易日的手动重导。
- 保证自动同步不会覆盖个人笔记、提交用户无关 Git 修改或写出 Vault 边界。

## 4. 非目标

- 不从 Obsidian 读取或解析用户修改。
- 不通过 Obsidian 确认预案、修改候选或记录真实账户成交。
- 不自动下单，不推断账户资金或真实盈亏。
- 不启用微信机器人或其他外部通知通道。
- 不把完整原始文字稿复制进 Vault；模式页只保存项目已采用的短引用和对应 SHA-256。
- 不替换现有产业趋势、每日资讯和旧超短线导出。

## 5. 总体架构

### 5.1 `ObsidianVaultWriter`

从现有服务中提取可复用的 Vault 写入基础能力：

- 读取现有 `OBSIDIAN_ENABLED`、`OBSIDIAN_VAULT_PATH`、`OBSIDIAN_AUTO_GIT_ENABLED`。
- 解析并限制目标路径，确保所有写入位于 Vault 和调用方声明的系统目录中。
- 规范化 UTF-8 和换行符。
- 使用同目录临时文件加原子替换，避免进程中断产生半份 Markdown。
- 内容未变化时不重写文件。
- 可选执行仅限明确生成路径的 Git 提交。

现有 `ObsidianKnowledgeService` 和新的交易预案导出器共同使用该 Writer。此次提取只改变写入基础设施，不改变现有知识页面格式和接口行为。

### 5.2 `TradingPlaybookObsidianExporter`

该组件只负责把结构化快照渲染成确定性 Markdown：

- 模式规则页。
- 不可变预案版本页。
- 交易日索引页。
- 初步和最终复盘页。
- 每日提醒时间线。
- 交易预案 Dashboard。

Exporter 不负责调度、不修改交易数据、不读取用户手记，也不直接决定重试。

### 5.3 `TradingPlaybookObsidianSyncCoordinator`

该组件负责同步编排：

- 在业务事务已经提交后创建导出快照。
- 调用 Exporter 和 Writer。
- 管理幂等、失败状态、补偿重试和手动强制重导。
- 聚合同一批写入后再执行一次可选 Git 提交。
- 返回页面需要的最近同步状态、待处理数、失败数和生成文件列表。

任何同步异常都在协调器边界内被记录，不向上回滚已经成功的交易预案事务。

## 6. 持久化导出快照

新增 `TradingPlaybookObsidianExport`，用于保存可重试的导出事实。核心字段：

- `id`
- `snapshot_key`：逻辑产物键，例如 `rule:v2:trend_core_pullback`、`plan:123`、`review:45:initial`、`alerts:2026-07-15`。
- `snapshot_version`：同一逻辑产物的单调版本号。
- `trade_date`
- `entity_type`：`rule`、`plan`、`review`、`alerts`、`daily_index`、`dashboard`。
- `entity_id`：可为空；有数据库实体时保存其主键。
- `phase`：`catalog`、`preclose`、`initial_review`、`after_close`、`final_review`、`overnight`、`auction`、`reconcile`。
- `target_path`：Vault 内规范化相对路径。
- `source_hash`：结构化快照的 canonical SHA-256。
- `snapshot_json`：渲染所需的完整、严格 JSON 快照。
- `immutable`：规则版本、预案版本和初步/最终复盘为 `true`；日期索引、提醒时间线和 Dashboard 为 `false`。
- `status`：`pending`、`written`、`paused`、`failed`、`superseded`。
- `attempt_no`、`next_attempt_at`、`last_error`、`git_status_json`、`exported_at`、`created_at`、`updated_at`。

约束与行为：

- `(snapshot_key, snapshot_version)` 唯一。
- 相同 `snapshot_key` 和 `source_hash` 重复入队时复用已有快照。
- 不可变快照键出现不同哈希时失败关闭，不能静默覆盖。
- 可变页面产生新版本时，旧的未写版本可标记为 `superseded`；Worker 只允许最新版本写入同一目标路径，避免旧任务迟到覆盖新内容。
- 15:10 初步复盘在 15:30 最终校正前保存为独立不可变快照，因此最终数据库行更新后仍可重建初步复盘页面。

## 7. Vault 目录与文件命名

```text
30_TradingPlaybook/
├── Modes/
│   └── Auto/
│       └── v2/
│           └── <mode_key>.md
├── Daily/
│   └── Auto/
│       └── <year>/
│           └── <target_trade_date>/
│               ├── index.md
│               ├── preclose-v<version_no>.md
│               ├── after_close-v<version_no>.md
│               ├── overnight-v<version_no>.md
│               └── auction-v<version_no>.md
├── Reviews/
│   └── Auto/
│       └── <year>/
│           └── <trade_date>/
│               ├── initial-review-<plan_version_id>.md
│               └── final-review-<plan_version_id>.md
├── Alerts/
│   └── Auto/
│       └── <year>/
│           └── <trade_date>.md
└── Notes/
    └── <year>/
        └── <trade_date>.md

Dashboards/
└── 交易预案.md
```

`Notes` 只作为 Wiki 链接目标出现在自动页面中。同步程序不得创建该目录下的文件，也不得把该目录加入自动 Git 路径列表。

模式路径包含规则目录版本，未来 v3 不会覆盖 v2。预案文件名包含业务 `stage` 和 `version_no`，一个数据库不可变版本对应一个不可变文件。

## 8. Markdown 内容合同

### 8.1 通用 Frontmatter

所有自动页面至少包含：

- `type`
- `date`
- `source: stock-limit-up-system`
- `source_hash`
- `generated_at`
- `status`
- `manual_required: true`
- `auto_execute: false`

实体页面按需增加 `mode_key`、`rule_version`、`plan_version_id`、`plan_version_no`、`stage`、`source_trade_date`、`target_trade_date`、`action_trade_date`、`stocks`、`themes`、`risk_level` 和 `data_quality`。

### 8.2 模式页

每个模式页包含：

- 名称、家族、风格、适用窗口和优先级。
- 识别条件。
- 入场触发、失效条件、退出条件和风险纪律。
- 自动化等级；所有现有模式继续标明需要人工确认。
- 文字稿短引用、`source_key` 和 `source_content_hash`。
- 使用 Dataview 查询按 `mode_key` 动态列出采用该模式的每日预案版本；规则页本身保持不可变。

### 8.3 预案版本页

每个不可变预案版本包含：

- 来源交易日、目标交易日、阶段、版本号、父版本和状态。
- 市场状态、题材排序、数据质量和风险设置。
- 最多 3 个候选；每个候选包含主模式、辅助模式、角色、排名、识别证据、入场触发、失效条件、退出条件、风险等级和仓位参考。
- 人工覆盖字段和人工确认时间，但不包含自动执行入口。
- 相对父版本的新增、删除和变化摘要。
- 规则快照及绑定的文字稿 SHA-256。
- 指向日期索引、模式页、提醒时间线、复盘页和个人手记的链接。

### 8.4 日期索引页

按中国时间展示：

- 14:40 提前预案和尾盘建议。
- 15:10 初步复盘。
- 15:30 正式次日预案和最终复盘。
- 08:50 隔夜刷新。
- 09:26 竞价最终版本。

索引明确区分 `source_trade_date`、`target_trade_date` 和候选 `action_trade_date`，避免把 14:40 次日预案和当日尾盘建议混为同一执行日期。索引标记当前有效版本，但保留所有历史版本链接。

### 8.5 复盘页

初步和最终复盘各自包含：

- 信号是否触发、是否失效、是否执行。
- 提醒送达和确认审计。
- 人工执行记录和计划纪律评价。
- 结果快照、数据质量和最终校正时间。
- 计划内与计划外执行情况。

系统不根据行情结果推断真实账户盈亏。

### 8.6 提醒时间线

每日提醒时间线包含：

- 事件类型、严重级别和触发时间。
- 关联预案版本和候选。
- 提醒消息。
- 选取后的市场快照事实。
- 项目内通道状态和人工确认时间。

页面不包含微信发送记录，因为第一版微信通道仍禁用。

## 9. 五阶段同步流程

统一流程：

1. 调度器完成原有预案或复盘事务并提交。
2. 调度器调用协调器，为本阶段涉及的不可变实体和可变索引创建 canonical 快照。
3. Writer 在后台线程执行文件 I/O，逐个原子写入发生变化的系统文件。
4. 写入成功后更新快照状态。
5. 一批文件完成后，可选执行一次路径受限的 Git 提交。
6. 页面可查询结果；失败任务由补偿任务重试。

阶段映射：

| 中国时间 | 业务阶段 | Obsidian 产物 |
| --- | --- | --- |
| 14:40 | `preclose` | 提前预案、尾盘候选、日期索引、提醒时间线 |
| 15:10 | `initial_review` | 独立初步复盘、日期索引、提醒时间线 |
| 15:30 | `after_close` + `final_review` | 正式次日预案、独立最终复盘、日期索引、提醒时间线 |
| 08:50 | `overnight` | 隔夜刷新版本、日期索引 |
| 09:26 | `auction` | 竞价最终版本、日期索引、提醒时间线 |

规则页在首次启用同步、规则目录版本变化或手动全量重导时导出。Dashboard 在任何批次产生新内容时更新。

## 10. 补偿、幂等与并发

- 新增每 60 秒执行一次的补偿任务，按 `next_attempt_at` 扫描 `pending` 和 `failed` 快照。
- 重试采用 1 分钟、5 分钟、15 分钟的退避上限，失败快照不会自动删除。
- Vault 禁用或未配置时状态为 `paused`，不增加失败次数；启用后自动恢复。
- 服务启动补跑会继续所有未完成的不可变快照，并重新计算数据库中最新来源交易日和最新目标交易日的可变索引。
- 手动强制重导重置失败状态并重新生成当前数据库事实的快照。
- 同一目标路径的可变快照只允许最新版本落盘；迟到任务不能覆盖新版本。
- 多进程环境使用数据库唯一约束和条件更新取得写入权，不依赖进程内锁保证正确性。
- Writer 内容哈希相同时跳过文件替换；可选 Git 不产生空提交。

## 11. 安全边界

### 11.1 文件系统

- Vault 根目录必须由现有配置明确给出。
- 所有目标路径必须是规范化相对路径。
- 拒绝绝对目标路径、`..`、非法 Windows 文件名、保留设备名和空路径段。
- 根目录与目标路径都 `resolve`，解析结果必须仍位于 Vault 和调用方允许的系统目录内。
- 拒绝通过符号链接或目录联接逃逸。
- 自动页只能写 `Auto` 和指定 Dashboard；`Notes` 只读为链接字符串，不进行文件操作。

### 11.2 Git

- 自动 Git 默认继续关闭。
- 只允许使用本批 `written_files` 的精确路径列表。
- 禁止 `git add -A`、`git add .` 或提交整个 Vault。
- 提交采用路径限定方式，不能带入用户已暂存的其他文件。
- `Notes` 和非系统路径永远不进入自动提交参数。
- Git 错误只影响同步状态，不回滚已写 Markdown，更不影响交易业务。

### 11.3 交易边界

- Obsidian 页面明确标记 `manual_required: true` 和 `auto_execute: false`。
- Obsidian 页面和 URI 不能调用确认、修订或交易 API。
- Obsidian 不成为提醒发送通道。
- 现有微信禁用状态保持不变。

## 12. API 与前端

保留现有 `GET /api/v1/intelligence/obsidian/status` 作为 Vault 配置状态来源。

新增交易预案域接口：

### 12.1 `GET /api/v1/trading-playbook/obsidian/status`

返回：

- Vault 启用、配置、存在和自动 Git 状态。
- 最近成功同步时间。
- 最近同步交易日和阶段。
- `pending`、`paused`、`failed` 数量。
- 最近错误。
- 最近生成文件列表。
- Dashboard 相对路径和可打开状态。

### 12.2 `POST /api/v1/trading-playbook/obsidian/export`

请求体：

- `trade_date`：必填，中国交易日。
- `include_rules`：默认 `false`；首次配置或规则升级时可显式全量导出。
- `force`：默认 `false`；为 `true` 时重新生成当前数据库事实并重置失败状态。

响应返回批次状态、写入/跳过/待重试文件、Git 结果和错误摘要。接口不修改任何交易预案实体。

### 12.3 交易预案页面

独立交易预案页面增加 Obsidian 区域：

- 配置状态。
- 最近同步状态和失败计数。
- “导出到 Obsidian”按钮。
- “打开交易预案 Dashboard”按钮。
- 未配置、同步中、成功、部分失败和待重试状态。

按钮复用现有 Obsidian 状态和打开方式，不在前端增加第二套 Vault 路径输入。

## 13. 错误处理

- 业务阶段成功、Obsidian 入队失败：记录错误；补偿任务通过数据库事实重新入队。
- 快照成功、文件写入失败：快照保持 `failed`，记录安全截断后的错误和下次重试时间。
- 部分文件成功：逐文件更新状态；再次执行只处理未完成或变化的文件。
- 可变页面渲染失败：保留上一个完整文件，不进行部分替换。
- Git 失败：Markdown 保持成功状态，Git 结果单独记录为失败并可重试。
- Vault 被删除或暂时离线：快照暂停或失败，不影响业务；恢复后重建目录并补偿。
- 不可变快照哈希冲突：失败关闭并报警，不能覆盖既有规则、预案或复盘阶段页面。

## 14. 测试策略

### 14.1 单元测试

- 19 个模式分别生成版本化 Markdown，引用和 SHA-256 完整。
- Frontmatter 和正文使用稳定排序，重复渲染字节一致。
- 各预案、候选、复盘和提醒字段映射正确。
- 文件名清洗覆盖 Windows 非法字符和保留设备名。
- 路径边界覆盖绝对路径、`..`、符号链接和目录联接逃逸。
- 原子写入失败不破坏旧文件。
- 相同内容不重写。
- Git 命令只包含本批生成路径，不包含个人手记和预存暂存文件。

### 14.2 持久化与并发测试

- 相同导出键和哈希幂等。
- 不可变键不同哈希失败关闭。
- 可变页面旧任务迟到不会覆盖新任务。
- 两个 Worker 竞争时只有一个取得写入权。
- 15:10 和 15:30 复盘保存为不同不可变快照并可分别重建。
- 服务重启后可继续 `pending`、`paused` 和 `failed` 快照。

### 14.3 阶段集成测试

- 14:40、15:10、15:30、08:50、09:26 分别入队正确日期、阶段和版本。
- 14:40 的次日预案与当日尾盘候选保持独立行动日期。
- Obsidian 异常不改变阶段任务的成功状态、预案版本、提醒 outbox 或人工确认。
- 启动补跑和定时补偿可恢复漏写。
- 自动 Git 关闭时不启动外部进程。

### 14.4 API 与前端测试

- 状态接口覆盖未配置、暂停、成功、部分失败和待重试。
- 手动导出覆盖普通、强制、包含规则和非法日期。
- 页面按钮覆盖 loading、成功、错误和禁用状态。
- 页面打开 Dashboard 时使用现有 Vault 状态，不暴露文件系统写能力。
- 现有产业趋势、每日资讯、旧超短线和交易预案测试继续通过。

### 14.5 验收回放

- 使用临时 Vault 回放 19 个黄金场景和五个阶段。
- 验证规则、预案、复盘、提醒、索引和 Dashboard 链接闭环。
- 验证不存在未来事实、自动交易指令、Obsidian 反向写入或微信发送。

## 15. 验收标准

1. 五个阶段完成后，Obsidian 在一次同步或补偿周期内出现对应自动页面。
2. 19 个模式可追溯到规则版本、短引用和文字稿 SHA-256。
3. 所有预案版本、15:10 初步复盘和 15:30 最终复盘互不覆盖。
4. 每个自动页面可导航到相关模式、候选、提醒、复盘和个人手记链接。
5. Vault 离线或服务重启后能自动补齐；手动重导能从数据库快照恢复自动页面。
6. 自动同步永不创建或修改 `Notes`。
7. 自动 Git 不提交用户无关内容。
8. Obsidian 故障不影响交易预案、提醒、复盘或人工确认。
9. 系统继续不自动下单、不反向读取 Obsidian、不发送微信消息。

## 16. 上线与启用顺序

1. 保持 `OBSIDIAN_ENABLED=false` 部署数据库和代码变更。
2. 在测试 Vault 验证状态接口、全量规则导出和一个历史交易日的完整重导。
3. 配置 `OBSIDIAN_VAULT_PATH`，暂不启用自动 Git。
4. 启用 Obsidian 自动同步，观察五阶段和补偿状态。
5. 确认 Vault 是独立 Git 仓库且路径限定测试通过后，再选择是否启用 `OBSIDIAN_AUTO_GIT_ENABLED`。

真实 PostgreSQL、真实外部行情供应商、用户实际 Vault 和自动 Git 必须在部署阶段单独联调；设计验证和单元测试使用临时目录，不能写入用户真实 Vault。
