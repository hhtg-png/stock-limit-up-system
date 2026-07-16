# Trading Playbook Obsidian Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将交易预案的 19 个模式、五阶段预案、提醒和初步/最终复盘可靠地单向导出到 Obsidian，同时确保 Obsidian 故障不影响交易业务、系统不触碰个人 `Notes`、不产生自动交易或微信发送。

**Architecture:** 从现有知识导出服务提取路径受限、原子写入、精确 Git 提交的 `ObsidianVaultWriter`；用快照构建器冻结数据库事实，用确定性 Exporter 渲染 Markdown，用持久化 SyncCoordinator 管理幂等、并发、重试和手动重导；调度器只在业务事务提交成功后触发同步，前端只展示状态并调用交易预案域导出接口。

**Tech Stack:** Python 3.11, FastAPI, SQLAlchemy 2 async, Pydantic v2, APScheduler, unittest, Vue 3, Pinia, TypeScript, Node test runner, Obsidian Markdown/Dataview, Git CLI.

---

## 0. 实施边界与文件地图

实施时始终从隔离工作树 `D:\code\stock-limit-up-system\.worktrees\trading-playbook-alerts` 开始。不要修改用户主工作树，不要部署，不要启用真实 Vault、自动 Git、微信或自动交易。

### 新增文件

- `backend/app/services/obsidian_vault_writer.py`：Vault 路径验证、UTF-8 原子写入、内容幂等、精确路径 Git 提交。
- `backend/app/services/trading_playbook/obsidian_types.py`：严格 JSON 快照、canonical hash、产物 DTO 和同步结果 DTO。
- `backend/app/services/trading_playbook/obsidian_snapshot_builder.py`：从规则、预案、候选、提醒、复盘读取并冻结导出事实。
- `backend/app/services/trading_playbook/obsidian_exporter.py`：将快照确定性渲染为 Markdown，不访问数据库或文件系统。
- `backend/app/services/trading_playbook/obsidian_sync.py`：持久化入队、抢占、写盘、重试、补偿、状态聚合和手动重导。
- `backend/tests/test_obsidian_vault_writer.py`：Writer 安全和 Git 隔离测试。
- `backend/tests/test_trading_playbook_obsidian_types.py`：严格 JSON 与 hash 稳定性测试。
- `backend/tests/test_trading_playbook_obsidian_snapshot_builder.py`：规则、预案、提醒、复盘和索引快照测试。
- `backend/tests/test_trading_playbook_obsidian_exporter.py`：Markdown 合同、链接和字节稳定性测试。
- `backend/tests/test_trading_playbook_obsidian_sync.py`：幂等、并发、暂停、退避、恢复、Git 状态测试。

### 修改文件

- `backend/app/services/obsidian_knowledge_service.py`：复用 Writer，保持现有知识页内容和 API 行为不变。
- `backend/app/models/trading_playbook.py`：新增 `TradingPlaybookObsidianExport`。
- `backend/app/models/__init__.py`：导出新模型，保证 `Base.metadata.create_all` 可发现新表。
- `backend/app/schemas/trading_playbook.py`：新增严格的导出请求和状态/结果响应模型。
- `backend/app/api/v1/trading_playbook.py`：新增状态与手动导出接口。
- `backend/app/data_collectors/scheduler.py`：安装协调器、五阶段提交后入队、60 秒补偿和启动恢复。
- `backend/app/main.py`：生产组合、`app.state` 注入和 shutdown 清理。
- `backend/tests/test_obsidian_knowledge_service.py`：保护旧导出格式与行为。
- `backend/tests/test_trading_playbook_models.py`：新表结构、约束和旧库补表测试。
- `backend/tests/test_trading_playbook_api.py`：交易预案 Obsidian API 测试。
- `backend/tests/test_trading_playbook_scheduler.py`：五阶段、异常隔离和补偿注册测试。
- `backend/tests/test_main_lifespan.py`：协调器生命周期测试。
- `frontend/src/types/trading-playbook.ts`：交易预案 Obsidian 类型。
- `frontend/src/api/trading-playbook.ts`：状态查询与手动导出调用。
- `frontend/src/stores/trading-playbook.ts`：状态、并发保护和导出动作。
- `frontend/src/views/TradingPlaybook.vue`：独立 Obsidian 状态卡片、导出和打开 Dashboard。
- `frontend/tests/tradingPlaybookStore.test.mjs`：store 状态、竞态和错误测试。
- `frontend/tests/tradingPlaybookUi.test.mjs`：UI 合同与安全文案测试。
- `README.md`：配置、目录、接口、启用顺序和验证命令。

### 全局数据合同

使用以下固定枚举；数据库、Pydantic、TypeScript 和测试必须逐字一致：

```python
OBSIDIAN_EXPORT_STATUSES = (
    "pending",
    "written",
    "paused",
    "failed",
    "superseded",
)

OBSIDIAN_ENTITY_TYPES = (
    "rule",
    "plan",
    "review",
    "alerts",
    "daily_index",
    "dashboard",
)

OBSIDIAN_PHASES = (
    "catalog",
    "preclose",
    "initial_review",
    "after_close",
    "final_review",
    "overnight",
    "auction",
    "reconcile",
)
```

所有自动页只能位于：

```python
TRADING_PLAYBOOK_ALLOWED_ROOTS = (
    "30_TradingPlaybook/Modes/Auto",
    "30_TradingPlaybook/Daily/Auto",
    "30_TradingPlaybook/Reviews/Auto",
    "30_TradingPlaybook/Alerts/Auto",
    "Dashboards/交易预案.md",
)
```

`30_TradingPlaybook/Notes` 只能出现在 Markdown 链接文本中，绝不能传给 Writer 或 Git。

## Task 1: 提取安全的 Obsidian Vault Writer

**Files:**
- Create: `backend/app/services/obsidian_vault_writer.py`
- Create: `backend/tests/test_obsidian_vault_writer.py`
- Modify: `backend/app/services/obsidian_knowledge_service.py`
- Modify: `backend/tests/test_obsidian_knowledge_service.py`

- [ ] **Step 1: 写路径拒绝和幂等写入的失败测试**

在 `backend/tests/test_obsidian_vault_writer.py` 覆盖：正常 `Auto` 路径；绝对路径；空段；`.`；`..`；Windows 非法字符；`CON`、`PRN`、`AUX`、`NUL`、`COM1` 到 `COM9`、`LPT1` 到 `LPT9`；符号链接/目录联接逃逸；相同内容第二次不替换；原子替换失败时旧文件仍完整。

测试构造使用临时 Vault，绝不读取用户配置的真实路径。关键断言形式：

```python
with self.assertRaisesRegex(ValueError, "Vault relative path is unsafe"):
    writer.resolve_target("../Notes/private.md", allowed_roots=("30_TradingPlaybook/Daily/Auto",))

first = writer.write_text(
    "30_TradingPlaybook/Daily/Auto/2026/2026-07-15/index.md",
    "first\n",
    allowed_roots=("30_TradingPlaybook/Daily/Auto",),
)
second = writer.write_text(
    "30_TradingPlaybook/Daily/Auto/2026/2026-07-15/index.md",
    "first\r\n",
    allowed_roots=("30_TradingPlaybook/Daily/Auto",),
)
self.assertTrue(first.changed)
self.assertFalse(second.changed)
```

- [ ] **Step 2: 运行测试确认红灯**

Run:

```powershell
Set-Location backend
python -m unittest discover -s tests -p 'test_obsidian_vault_writer.py' -v
```

Expected: `ModuleNotFoundError: No module named 'app.services.obsidian_vault_writer'`。

- [ ] **Step 3: 实现路径解析、原子写入和结果 DTO**

实现公共接口：

```python
@dataclass(frozen=True)
class VaultWriteResult:
    relative_path: str
    absolute_path: Path
    changed: bool


class ObsidianVaultWriter:
    def __init__(
        self,
        *,
        enabled: bool,
        vault_path: str,
        auto_git_enabled: bool,
        command_runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    ) -> None:
        self.enabled = enabled
        self.vault_path = vault_path
        self.auto_git_enabled = auto_git_enabled
        self.command_runner = command_runner

    def configured_vault(self) -> Path | None:
        if not self.enabled or not self.vault_path.strip():
            return None
        return Path(self.vault_path).expanduser().resolve(strict=False)

    def ensure_vault(self) -> Path | None:
        vault = self.configured_vault()
        if vault is None:
            return None
        vault.mkdir(parents=True, exist_ok=True)
        return vault

    def resolve_target(self, relative_path: str, *, allowed_roots: tuple[str, ...]) -> Path:
        return _resolve_vault_target(self.ensure_vault(), relative_path, allowed_roots)

    def write_text(
        self,
        relative_path: str,
        content: str,
        *,
        allowed_roots: tuple[str, ...],
    ) -> VaultWriteResult:
        normalized = content.replace("\r\n", "\n").replace("\r", "\n")
        target = self.resolve_target(relative_path, allowed_roots=allowed_roots)
        if target.exists() and target.read_text(encoding="utf-8") == normalized:
            return VaultWriteResult(relative_path, target, False)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
        try:
            temporary.write_text(normalized, encoding="utf-8", newline="\n")
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
        return VaultWriteResult(relative_path, target, True)
```

`_resolve_vault_target` 必须先验证纯相对 `PurePosixPath`，再逐段拒绝非法/保留名称，最后对 Vault、允许根和目标执行 `resolve(strict=False)` 与 `Path.is_relative_to`。父目录已存在时逐级 resolve，以便拒绝符号链接和 Windows junction 逃逸。

- [ ] **Step 4: 写精确路径 Git 的失败测试**

覆盖自动 Git 关闭、空变更、不在允许根的路径、`Notes`、用户已有暂存文件、Git add/commit 失败。断言命令不得出现 `add -A` 或 `add .`，并且 commit 使用路径限定：

```python
self.assertEqual(
    commands,
    [
        ["git", "add", "--", "30_TradingPlaybook/Daily/Auto/2026/2026-07-15/index.md"],
        [
            "git",
            "commit",
            "--only",
            "-m",
            "obsidian: export trading playbook 2026-07-15",
            "--",
            "30_TradingPlaybook/Daily/Auto/2026/2026-07-15/index.md",
        ],
    ],
)
```

- [ ] **Step 5: 实现 `commit_paths`**

接口返回可持久化 JSON：

```python
def commit_paths(
    self,
    relative_paths: Sequence[str],
    *,
    allowed_roots: tuple[str, ...],
    message: str,
) -> dict[str, object]:
    if not self.auto_git_enabled:
        return {"status": "disabled", "paths": []}
    paths = sorted(set(relative_paths))
    for relative_path in paths:
        self.resolve_target(relative_path, allowed_roots=allowed_roots)
    if not paths:
        return {"status": "skipped", "paths": []}
    vault = self.ensure_vault()
    if vault is None:
        return {"status": "paused", "paths": paths}
    self.command_runner(["git", "add", "--", *paths], cwd=vault, check=True, text=True, capture_output=True)
    self.command_runner(
        ["git", "commit", "--only", "-m", message, "--", *paths],
        cwd=vault,
        check=True,
        text=True,
        capture_output=True,
    )
    return {"status": "committed", "paths": paths}
```

- [ ] **Step 6: 让旧知识服务委托给 Writer 并保护行为**

给 `ObsidianKnowledgeService` 注入可选 Writer。保留现有 `_ensure_vault`、`_write_if_changed`、`_maybe_git_commit` 私有方法名，内部转换为 Vault 相对路径并委托 Writer，从而最小化旧页面渲染改动。旧服务允许根固定为其已有的 `00_Inbox/Auto`、`10_Industry`、`40_UltraShort`、`50_Daily`、`60_Signals` 和已有 Dashboard。

新增回归断言：相同输入导出的文件集合与关键正文不变；旧服务不能写交易预案 `Notes`。

- [ ] **Step 7: 运行 Writer 和旧服务测试**

Run:

```powershell
Set-Location backend
python -m unittest discover -s tests -p 'test_obsidian*.py' -v
```

Expected: 全部通过，测试输出包含 `OK`。

- [ ] **Step 8: 提交**

```powershell
git add backend/app/services/obsidian_vault_writer.py backend/app/services/obsidian_knowledge_service.py backend/tests/test_obsidian_vault_writer.py backend/tests/test_obsidian_knowledge_service.py
git commit -m "refactor: extract safe Obsidian vault writer"
```

## Task 2: 增加持久化导出快照模型

**Files:**
- Modify: `backend/app/models/trading_playbook.py`
- Modify: `backend/app/models/__init__.py`
- Modify: `backend/tests/test_trading_playbook_models.py`

- [ ] **Step 1: 写模型结构和旧库补表的失败测试**

断言表名、全部字段、`(snapshot_key, snapshot_version)` 唯一约束、状态/重试索引、JSON 字段和中国交易日日期类型。再创建一个只含现有表的 SQLite 文件，调用现有 `init_db()`/metadata 初始化路径，断言新表被 `create_all` 自动创建且旧数据保留。

- [ ] **Step 2: 运行测试确认红灯**

```powershell
Set-Location backend
python -m unittest discover -s tests -p 'test_trading_playbook_models.py' -v
```

Expected: 因 `TradingPlaybookObsidianExport` 尚不存在而失败。

- [ ] **Step 3: 实现 SQLAlchemy 模型**

在 `backend/app/models/trading_playbook.py` 新增：

```python
class TradingPlaybookObsidianExport(Base):
    __tablename__ = "trading_playbook_obsidian_exports"
    __table_args__ = (
        UniqueConstraint(
            "snapshot_key",
            "snapshot_version",
            name="uq_trading_playbook_obsidian_snapshot_version",
        ),
        Index(
            "ix_trading_playbook_obsidian_due",
            "status",
            "next_attempt_at",
        ),
        Index(
            "ix_trading_playbook_obsidian_trade_date",
            "trade_date",
            "phase",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_key = Column(String(255), nullable=False)
    snapshot_version = Column(Integer, nullable=False)
    trade_date = Column(Date, nullable=False)
    entity_type = Column(String(32), nullable=False)
    entity_id = Column(Integer, nullable=True)
    phase = Column(String(32), nullable=False)
    target_path = Column(String(1024), nullable=False)
    source_hash = Column(String(64), nullable=False)
    snapshot_json = Column(JSON, nullable=False)
    immutable = Column(Boolean, nullable=False, default=False)
    status = Column(String(32), nullable=False, default="pending")
    attempt_no = Column(Integer, nullable=False, default=0)
    next_attempt_at = Column(DateTime, nullable=True)
    last_error = Column(Text, nullable=True)
    git_status_json = Column(JSON, nullable=True)
    exported_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(
        DateTime,
        nullable=False,
        default=datetime.now,
        onupdate=datetime.now,
    )
```

时间列遵循当前交易预案模型的数据库约定：数据库保存无时区 datetime，进入 Obsidian 快照时显式按 `Asia/Shanghai` 解释并转换成带 offset 的 ISO 8601。将类加入 `backend/app/models/__init__.py` 的导入和 `__all__`。

- [ ] **Step 4: 运行模型和初始化测试**

```powershell
Set-Location backend
python -m unittest discover -s tests -p 'test_trading_playbook_models.py' -v
```

Expected: 全部通过，旧库初始化测试证明缺失表由 `Base.metadata.create_all` 补齐，不需要重复手写 DDL。

- [ ] **Step 5: 提交**

```powershell
git add backend/app/models/trading_playbook.py backend/app/models/__init__.py backend/tests/test_trading_playbook_models.py
git commit -m "feat: persist Obsidian export snapshots"
```

## Task 3: 定义严格快照与 canonical hash

**Files:**
- Create: `backend/app/services/trading_playbook/obsidian_types.py`
- Create: `backend/tests/test_trading_playbook_obsidian_types.py`

- [ ] **Step 1: 写严格 JSON 和稳定 hash 的失败测试**

覆盖 dict 键顺序不影响 hash；日期、timezone-aware datetime、tuple 和 Decimal 的规范化；NaN/Infinity、naive datetime、set、bytes 和自定义对象必须失败；同一 DTO 重复序列化字节一致。另测 `database_datetime_to_cn()` 把 ORM 的 naive datetime 明确解释为 `Asia/Shanghai`，避免依赖服务器本地时区。

- [ ] **Step 2: 运行测试确认红灯**

```powershell
Set-Location backend
python -m unittest discover -s tests -p 'test_trading_playbook_obsidian_types.py' -v
```

Expected: 新模块不存在。

- [ ] **Step 3: 实现 DTO、规范化和 hash**

实现不可变 DTO：

```python
@dataclass(frozen=True)
class ObsidianArtifact:
    snapshot_key: str
    trade_date: date
    entity_type: str
    entity_id: int | None
    phase: str
    target_path: str
    immutable: bool
    payload: dict[str, JsonValue]

    @property
    def source_hash(self) -> str:
        encoded = canonical_json_bytes(self.payload)
        return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class ObsidianSyncBatchResult:
    trade_date: date
    phase: str
    written_files: tuple[str, ...]
    skipped_files: tuple[str, ...]
    pending_files: tuple[str, ...]
    failed_files: tuple[str, ...]
    git_status: dict[str, JsonValue]
```

`canonical_json_bytes` 先递归转成严格 JSON 值，再执行：

```python
json.dumps(
    normalized,
    ensure_ascii=False,
    allow_nan=False,
    sort_keys=True,
    separators=(",", ":"),
).encode("utf-8")
```

日期使用 ISO 8601；进入 canonical JSON 的 datetime 必须带时区并转 UTC 的 `Z` 形式；Decimal 使用规范十进制字符串；list/tuple 保序；dict 键必须为字符串。Snapshot Builder 对当前 ORM 的 naive datetime 一律先调用 `database_datetime_to_cn()`，其他调用方传入 naive datetime 继续失败关闭。

- [ ] **Step 4: 运行测试**

```powershell
Set-Location backend
python -m unittest discover -s tests -p 'test_trading_playbook_obsidian_types.py' -v
```

Expected: 全部通过。

- [ ] **Step 5: 提交**

```powershell
git add backend/app/services/trading_playbook/obsidian_types.py backend/tests/test_trading_playbook_obsidian_types.py
git commit -m "feat: define canonical Obsidian artifacts"
```

## Task 4: 构建规则与预案版本快照

**Files:**
- Create: `backend/app/services/trading_playbook/obsidian_snapshot_builder.py`
- Create: `backend/tests/test_trading_playbook_obsidian_snapshot_builder.py`

- [ ] **Step 1: 写规则目录快照失败测试**

在异步临时数据库 seed 当前 v2 的 19 个模式，调用 `build_rule_artifacts()`，断言：数量为 19；路径为 `30_TradingPlaybook/Modes/Auto/v2/{mode_key}.md`；snapshot key 为 `rule:v2:{mode_key}`；不可变；每页含规则版本、短引用、`source_key`、真实文字稿 `source_content_hash`、人工确认和禁止自动执行标志。

- [ ] **Step 2: 写四类预案版本快照失败测试**

分别建立 `preclose`、`after_close`、`overnight`、`auction` 版本及最多 3 个候选。断言路径、来源/目标/行动日期、父版本、人工覆盖、规则快照、候选顺序和差异摘要都被冻结。候选超过 3 个时构建器必须失败，而不是静默截断数据库异常。

- [ ] **Step 3: 运行测试确认红灯**

```powershell
Set-Location backend
python -m unittest discover -s tests -p 'test_trading_playbook_obsidian_snapshot_builder.py' -v
```

Expected: 新模块不存在。

- [ ] **Step 4: 实现查询和产物接口**

构建器只读数据库并返回 DTO，不渲染 Markdown：

```python
class TradingPlaybookObsidianSnapshotBuilder:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory

    async def build_rule_artifacts(self, catalog_version: str = "v2") -> tuple[ObsidianArtifact, ...]:
        rules = await self._load_rules(catalog_version)
        return tuple(self._rule_artifact(rule, catalog_version) for rule in rules)

    async def build_plan_artifact(self, plan_version_id: int) -> ObsidianArtifact:
        plan, candidates = await self._load_plan_graph(plan_version_id)
        if len(candidates) > 3:
            raise ValueError(f"Plan {plan_version_id} has more than 3 candidates")
        return self._plan_artifact(plan, candidates)
```

预案 payload 固定包含：

```python
payload = {
    "type": "trading_plan_version",
    "plan_version_id": plan.id,
    "version_no": plan.version_no,
    "stage": plan.stage,
    "status": plan.status,
    "source_trade_date": plan.source_trade_date,
    "target_trade_date": plan.target_trade_date,
    "parent_plan_version_id": plan.parent_plan_version_id,
    "market_state": plan.market_state_json,
    "theme_ranking": plan.theme_ranking_json,
    "mode_radar": plan.mode_radar_json,
    "rule_snapshot": plan.rule_snapshot_json,
    "data_quality": plan.data_quality_json,
    "risk_settings": plan.risk_settings_json,
    "change_summary": plan.change_summary_json,
    "input_hash": plan.input_hash,
    "generated_at": database_datetime_to_cn(plan.generated_at),
    "confirmed_at": database_datetime_to_cn(plan.confirmed_at),
    "confirmed_by": plan.confirmed_by,
    "candidates": candidate_payloads,
    "manual_required": True,
    "auto_execute": False,
}
```

每个 `candidate_payload` 精确映射 `stock_code`、`stock_name`、`action_trade_date`、`theme_name`、`primary_mode_key`、`supporting_mode_keys_json`、`role`、`rank`、`recognition_json`、`entry_trigger_json`、`invalidation_json`、`exit_trigger_json`、`risk_level`、`position_reference`、`evidence_json`、`manual_overrides_json` 和 `status`。不能把 ORM `__dict__` 整体写入快照。

- [ ] **Step 5: 运行测试**

```powershell
Set-Location backend
python -m unittest discover -s tests -p 'test_trading_playbook_obsidian_snapshot_builder.py' -v
```

Expected: 规则和预案测试全部通过。

- [ ] **Step 6: 提交**

```powershell
git add backend/app/services/trading_playbook/obsidian_snapshot_builder.py backend/tests/test_trading_playbook_obsidian_snapshot_builder.py
git commit -m "feat: build Obsidian rule and plan snapshots"
```

## Task 4A: 让真实预案绑定风控文字来源版本

**Files:**
- Modify: `backend/app/services/trading_playbook/plan_service.py`
- Modify: `backend/tests/test_trading_playbook_plan_service.py`

> Task 4 质量复审发现：严格快照构建器要求 `risk_settings.source_refs` 带精确 `source_content_hash`，但真实预案生产者只持久化了 `source_key` 和短引用。若不先修复，真实生成的预案无法进入 Obsidian 导出闭环。

- [ ] **Step 1: 写真实生产者合同红灯测试**

断言新预案为 `03-loss-qa` 与 `04-trading-plan` 写入当前数据库中最新且 `ready` 的精确 hash；相同来源保持幂等，来源版本变化必须改变预案输入 hash。最新来源缺失或未就绪时失败，并且不得回退到旧的 ready 版本。

- [ ] **Step 2: 写修订与历史边界测试**

绑定来源的父预案修订后必须保留完全相同的来源引用。旧的未绑定父预案必须在创建子版本前失败，提示重新生成；不得原地回填或修改不可变历史行。

- [ ] **Step 3: 实现固定次数来源查询与 canonical 引用**

将数值风控校验和数据库来源绑定分离；一次查询同时解析两个来源，使用 `canonical_rule_source_refs()` 生成精确引用，并在计算 `input_hash` 前放入 `risk_settings`。来源缺失、最新版本未就绪或 hash 非法按上游溯源不可用处理。

- [ ] **Step 4: 回归验证并提交**

```powershell
Set-Location backend
python -m unittest discover -s tests -p 'test_trading_playbook_plan_service.py' -v
python -m unittest discover -s tests -p 'test_trading_playbook_obsidian_snapshot_builder.py' -v
git diff --check
git add backend/app/services/trading_playbook/plan_service.py backend/tests/test_trading_playbook_plan_service.py
git commit -m "fix: bind plan risk sources to transcripts"
```

## Task 5: 构建复盘、提醒、日期索引和 Dashboard 快照

**Files:**
- Modify: `backend/app/services/trading_playbook/obsidian_snapshot_builder.py`
- Modify: `backend/tests/test_trading_playbook_obsidian_snapshot_builder.py`

- [ ] **Step 1: 写初步/最终复盘独立快照测试**

先建立 15:10 初步复盘，调用 `build_review_artifact(review_id, phase="initial_review")` 并保存返回 DTO；再模拟同一 ORM 行在 15:30 被最终校正，调用 `phase="final_review"`。断言两份 snapshot key、target path、payload 和 hash 独立，初步 DTO 未被最终更新污染。

- [ ] **Step 2: 写提醒和可变页测试**

建立已送达、待确认、已确认和失败提醒，断言时间线含事件类型、严重级别、时间、关联预案/候选、消息、市场事实、项目内通道状态和人工确认时间，且不含微信字段。日期索引必须区分 source、target、action trade date，列出全部历史版本并标记当前有效版本。

Dashboard 快照只包含 Dataview 查询和固定导航事实；Notes 链接为 `[[30_TradingPlaybook/Notes/2026/2026-07-15]]`，构建器不得创建 Notes 产物。

- [ ] **Step 3: 运行新增测试确认红灯**

```powershell
Set-Location backend
python -m unittest discover -s tests -p 'test_trading_playbook_obsidian_snapshot_builder.py' -v
```

Expected: 缺失 review/alerts/index/dashboard 构建方法而失败。

- [ ] **Step 4: 实现阶段批次构建**

新增接口：

```python
async def build_stage_artifacts(
    self,
    *,
    trade_date: date,
    phase: str,
    plan_version_ids: Sequence[int] = (),
    review_ids: Sequence[int] = (),
    include_rules: bool = False,
) -> tuple[ObsidianArtifact, ...]:
    artifacts: list[ObsidianArtifact] = []
    if include_rules:
        artifacts.extend(await self.build_rule_artifacts("v2"))
    for plan_version_id in sorted(set(plan_version_ids)):
        artifacts.append(await self.build_plan_artifact(plan_version_id))
    for review_id in sorted(set(review_ids)):
        artifacts.append(await self.build_review_artifact(review_id, phase=phase))
    artifacts.append(await self.build_alerts_artifact(trade_date))
    artifacts.append(await self.build_daily_index_artifact(trade_date))
    artifacts.append(await self.build_dashboard_artifact(trade_date))
    return tuple(artifacts)
```

规则/预案/初步复盘/最终复盘设 `immutable=True`；提醒、日期索引和 Dashboard 设 `immutable=False`。日期索引按中国时间排序但存储 timezone-aware 时间。

- [ ] **Step 5: 运行测试**

```powershell
Set-Location backend
python -m unittest discover -s tests -p 'test_trading_playbook_obsidian_snapshot_builder.py' -v
```

Expected: 全部通过，测试明确证明没有 `entity_type == "notes"` 或 Notes 目标路径。

- [ ] **Step 6: 提交**

```powershell
git add backend/app/services/trading_playbook/obsidian_snapshot_builder.py backend/tests/test_trading_playbook_obsidian_snapshot_builder.py
git commit -m "feat: build complete Obsidian trading snapshots"
```

## Task 6: 确定性渲染 Obsidian Markdown

**Files:**
- Create: `backend/app/services/trading_playbook/obsidian_exporter.py`
- Create: `backend/tests/test_trading_playbook_obsidian_exporter.py`

- [ ] **Step 1: 写 Markdown 合同失败测试**

为六类 entity type 分别建立固定 DTO，断言两次渲染字节一致、frontmatter 键顺序稳定、字符串安全转义、列表排序明确。所有页面必须含：

```yaml
source: stock-limit-up-system
manual_required: true
auto_execute: false
```

规则页含 Dataview 的 `mode_key` 反向查询；预案页含模式、提醒、复盘、日期索引和 Notes 链接；日期索引含五阶段；提醒页不含“微信发送”；Dashboard 只查询 `Auto`。

- [ ] **Step 2: 运行测试确认红灯**

```powershell
Set-Location backend
python -m unittest discover -s tests -p 'test_trading_playbook_obsidian_exporter.py' -v
```

Expected: 新 exporter 模块不存在。

- [ ] **Step 3: 实现纯渲染器**

Exporter 不接受 Session、Writer 或 settings：

```python
class TradingPlaybookObsidianExporter:
    def render(self, artifact: ObsidianArtifact, *, generated_at: datetime) -> str:
        renderers = {
            "rule": self._render_rule,
            "plan": self._render_plan,
            "review": self._render_review,
            "alerts": self._render_alerts,
            "daily_index": self._render_daily_index,
            "dashboard": self._render_dashboard,
        }
        try:
            body = renderers[artifact.entity_type](artifact.payload)
        except KeyError as exc:
            raise ValueError(f"Unsupported Obsidian entity type: {artifact.entity_type}") from exc
        frontmatter = self._frontmatter(artifact, generated_at=generated_at)
        return f"---\n{frontmatter}---\n\n{body.rstrip()}\n"
```

Frontmatter `source_hash` 来自 artifact，而 `generated_at` 来自协调器一次批次固定时间。不可变实体重试必须读取持久化 snapshot JSON 并沿用原 snapshot 的生成事实；不得重新查询数据库或混入当前时间导致无意义改写。Dataview 查询只读 `30_TradingPlaybook/*/Auto`。

- [ ] **Step 4: 增加路径与链接闭环测试**

使用 19 个规则和五阶段快照渲染到内存，解析所有系统 Wiki 链接，断言每个 `Auto` 链接都能映射到本批或已知可变产物；Notes 链接只能位于 `30_TradingPlaybook/Notes`，且不在产物路径集合。

- [ ] **Step 5: 运行测试**

```powershell
Set-Location backend
python -m unittest discover -s tests -p 'test_trading_playbook_obsidian_exporter.py' -v
```

Expected: 全部通过，稳定性测试比较完整字符串相等。

- [ ] **Step 6: 提交**

```powershell
git add backend/app/services/trading_playbook/obsidian_exporter.py backend/tests/test_trading_playbook_obsidian_exporter.py
git commit -m "feat: render trading playbook Obsidian pages"
```

## Task 7: 实现快照入队的幂等与并发合同

**Files:**
- Create: `backend/app/services/trading_playbook/obsidian_sync.py`
- Create: `backend/tests/test_trading_playbook_obsidian_sync.py`

- [ ] **Step 1: 写入队状态机失败测试**

覆盖：同 key+hash 复用原行；不可变 key+不同 hash 失败关闭；可变页面新 hash 的版本号加一并把旧 `pending`/`failed` 标记 `superseded`；已经 `written` 的历史行保留；两个并发 session 入队最终只有一个唯一版本。

- [ ] **Step 2: 运行测试确认红灯**

```powershell
Set-Location backend
python -m unittest discover -s tests -p 'test_trading_playbook_obsidian_sync.py' -v
```

Expected: 协调器模块不存在。

- [ ] **Step 3: 实现协调器入队接口**

```python
class TradingPlaybookObsidianSyncCoordinator:
    RETRY_DELAYS = (
        timedelta(minutes=1),
        timedelta(minutes=5),
        timedelta(minutes=15),
    )

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        builder: TradingPlaybookObsidianSnapshotBuilder,
        exporter: TradingPlaybookObsidianExporter,
        writer: ObsidianVaultWriter,
        clock: Callable[[], datetime],
    ) -> None:
        self.session_factory = session_factory
        self.builder = builder
        self.exporter = exporter
        self.writer = writer
        self.clock = clock

    async def enqueue_artifacts(
        self,
        artifacts: Sequence[ObsidianArtifact],
    ) -> tuple[TradingPlaybookObsidianExport, ...]:
        rows: list[TradingPlaybookObsidianExport] = []
        for artifact in artifacts:
            rows.append(await self._enqueue_one(artifact))
        return tuple(rows)
```

`snapshot_json` 保存完整 payload，并额外固定 `generated_at`，以便不可变页重试时字节稳定。唯一约束冲突时 rollback 后重新读取，而不是把 IntegrityError 暴露给调度器。不可变 key 出现新 hash 时，以 `snapshot_version + 1` 保存一条 `failed` 冲突记录，`last_error` 固定为 `immutable_snapshot_hash_conflict`；原快照和原文件保持不变，冲突记录永不进入 Writer。

- [ ] **Step 4: 写条件抢占失败测试**

两个 worker 同时处理一行时，只有一个通过条件 update 把行从 due 状态切换为本次处理态。不要新增未在状态合同中的持久化状态；使用原子更新 `next_attempt_at` 到短租约截止时间并检查 affected row count。租约超时后可由补偿任务重试。

- [ ] **Step 5: 实现原子 claim**

条件必须同时检查：状态是 `pending`/`failed`；`next_attempt_at` 为空或已到期；可变页面没有更高版本；当前行未 superseded。SQLite 和 PostgreSQL 都走相同条件更新语义。

- [ ] **Step 6: 运行状态机测试**

```powershell
Set-Location backend
python -m unittest discover -s tests -p 'test_trading_playbook_obsidian_sync.py' -v
```

Expected: 入队与竞争测试全部通过。

- [ ] **Step 7: 提交**

```powershell
git add backend/app/services/trading_playbook/obsidian_sync.py backend/tests/test_trading_playbook_obsidian_sync.py
git commit -m "feat: coordinate durable Obsidian snapshots"
```

## Task 8: 实现写盘、暂停、退避、Git 与恢复

**Files:**
- Modify: `backend/app/services/trading_playbook/obsidian_sync.py`
- Modify: `backend/tests/test_trading_playbook_obsidian_sync.py`

- [ ] **Step 1: 写 disabled/unconfigured 暂停测试**

Writer disabled 或 Vault path 为空时，due 行变为 `paused`，`attempt_no` 不增加、`last_error` 不伪装成业务失败。配置恢复后 `resume_paused()` 把行恢复 `pending`。

- [ ] **Step 2: 写写盘和退避测试**

成功时逐文件 `written`，内容相同进入 `written` 但记入 skipped files；异常时旧文件保持，行 `failed`，attempt 依次对应 1 分钟、5 分钟、15 分钟、15 分钟；错误文本安全截断到模型允许长度。

- [ ] **Step 3: 写 Git 独立状态测试**

一批 Markdown 全部写完后仅调用一次 `commit_paths`。Git 失败不得把 Markdown 行从 `written` 改回 `failed`；将失败记录在各相关行 `git_status_json` 和 batch result，后续可只重试 Git。路径集合必须只来自本批 changed files。

- [ ] **Step 4: 实现 due worker**

```python
async def process_due(self, *, limit: int = 100) -> ObsidianSyncBatchResult:
    if self.writer.configured_vault() is None:
        await self._pause_due(limit=limit)
        return self._paused_result()
    claimed = await self._claim_due(limit=limit)
    written: list[str] = []
    skipped: list[str] = []
    failed: list[str] = []
    for row in claimed:
        try:
            artifact = self._artifact_from_row(row)
            content = self.exporter.render(artifact, generated_at=self._generated_at(row))
            result = await asyncio.to_thread(
                self.writer.write_text,
                row.target_path,
                content,
                allowed_roots=TRADING_PLAYBOOK_ALLOWED_ROOTS,
            )
            await self._mark_written(row.id)
            (written if result.changed else skipped).append(row.target_path)
        except Exception as exc:
            await self._mark_failed(row.id, exc)
            failed.append(row.target_path)
    git_status = await self._commit_changed_paths(written)
    await self._store_git_status(claimed, git_status)
    return self._batch_result(written, skipped, failed, git_status)
```

`asyncio.to_thread` 只包文件和 Git I/O；Session 不得跨线程。异常捕获边界必须重新抛出 `CancelledError`。

- [ ] **Step 5: 实现手动重导与启动恢复**

`export_trade_date(trade_date, include_rules, force)` 重新从数据库构建当前事实；`force=True` 只重置该日期/相关 Dashboard 的失败、暂停和 Git 状态，不修改交易实体。`startup_reconcile()` 继续全部未完成不可变行，并只重算数据库的最新 source trade date 和最新 target trade date 的 alerts/index/dashboard。

- [ ] **Step 6: 运行同步测试**

```powershell
Set-Location backend
python -m unittest discover -s tests -p 'test_trading_playbook_obsidian_sync.py' -v
```

Expected: 全部通过，重试时间使用注入 clock 做精确断言，不 sleep。

- [ ] **Step 7: 提交**

```powershell
git add backend/app/services/trading_playbook/obsidian_sync.py backend/tests/test_trading_playbook_obsidian_sync.py
git commit -m "feat: write and recover Obsidian exports"
```

## Task 9: 接入五阶段调度和应用生命周期

**Files:**
- Modify: `backend/app/data_collectors/scheduler.py`
- Modify: `backend/app/main.py`
- Modify: `backend/tests/test_trading_playbook_scheduler.py`
- Modify: `backend/tests/test_main_lifespan.py`

- [ ] **Step 1: 写调度注册失败测试**

断言新增补偿 job 每 60 秒执行，固定 job id 为 `trading_playbook_obsidian_reconcile`，`max_instances=1`，并在 scheduler restart 时不重复注册。断言 install/reset 方法管理协调器。

- [ ] **Step 2: 写五阶段提交后触发测试**

对 14:40、15:10、15:30、08:50、09:26 各跑一个最小场景，断言只有原业务 commit 成功后才调用 `enqueue_stage`，日期/phase/plan IDs/review IDs 准确：

```python
expected_calls = [
    (date(2026, 7, 15), "preclose"),
    (date(2026, 7, 15), "initial_review"),
    (date(2026, 7, 15), "after_close"),
    (date(2026, 7, 15), "final_review"),
    (date(2026, 7, 16), "overnight"),
    (date(2026, 7, 16), "auction"),
]
```

15:30 同一次业务运行允许先后入队 after_close 和 final_review，但每个 phase 独立。14:40 预案的 target/action date 不能被改成 source date。

- [ ] **Step 3: 写故障隔离测试**

协调器入队或 process_due 抛异常时，原计划版本、review、提醒 outbox 和 job claim 的成功语义不变；scheduler 记录异常后继续。业务阶段失败时不得导出未提交实体。

- [ ] **Step 4: 实现 scheduler 注入与安全触发**

新增：

```python
def install_trading_playbook_obsidian_sync(self, coordinator: Any) -> None:
    self._trading_playbook_obsidian_sync = coordinator

def reset_trading_playbook_services(self) -> None:
    self._trading_playbook_orchestrator = None
    self._trading_playbook_alert_service = None
    self._trading_playbook_review_service = None
    self._trading_playbook_obsidian_sync = None
```

把共享 reset 的实际已有成员完整保留。阶段 hook 使用一个 `_sync_trading_playbook_obsidian_after_commit` 包装器，捕获普通异常、记录 phase/trade date，不改变原阶段 return。补偿 job 调 `process_due()`，startup catch-up 调 `startup_reconcile()`。

- [ ] **Step 5: 先运行 scheduler 测试**

```powershell
Set-Location backend
python -m unittest discover -s tests -p 'test_trading_playbook_scheduler.py' -v
```

Expected: 全部通过。

- [ ] **Step 6: 写并实现 lifespan 组合测试**

在 `backend/app/main.py` 用现有 settings 创建共享 Writer，再创建 builder、exporter、coordinator；builder 和 coordinator 使用当前 `async_session_maker`；安装到 scheduler 并写入 `app.state.trading_playbook_obsidian_sync`。shutdown 时先停止 scheduler，再 reset service，最后从 `app.state` 移除或设为 `None`。

测试断言构造使用当前 `async_session_maker`，Writer 配置复用 `OBSIDIAN_*`，没有新 Vault 配置；异常启动不遗留半安装服务。

- [ ] **Step 7: 运行 lifespan 测试**

```powershell
Set-Location backend
python -m unittest discover -s tests -p 'test_main_lifespan.py' -v
```

Expected: 全部通过。

- [ ] **Step 8: 提交**

```powershell
git add backend/app/data_collectors/scheduler.py backend/app/main.py backend/tests/test_trading_playbook_scheduler.py backend/tests/test_main_lifespan.py
git commit -m "feat: schedule Obsidian trading playbook sync"
```

## Task 10: 增加状态和手动导出 API

**Files:**
- Modify: `backend/app/schemas/trading_playbook.py`
- Modify: `backend/app/api/v1/trading_playbook.py`
- Modify: `backend/tests/test_trading_playbook_api.py`

- [ ] **Step 1: 写 strict schema 和 API 失败测试**

覆盖：

- `GET /api/v1/trading-playbook/obsidian/status`
- `POST /api/v1/trading-playbook/obsidian/export`
- 必填 `trade_date`
- `include_rules=false`
- `force=false`
- 额外字段返回 422
- 非法日期返回 422
- 未配置返回 paused 状态而不是 500
- 协调器异常返回现有统一错误格式
- API 不修改 plan、candidate、alert、review 或 settings

- [ ] **Step 2: 运行 API 测试确认红灯**

```powershell
Set-Location backend
python -m unittest discover -s tests -p 'test_trading_playbook_api.py' -v
```

Expected: 新路由返回 404 或 schema 不存在。

- [ ] **Step 3: 实现 schema**

```python
class TradingPlaybookObsidianExportRequest(StrictRequest):
    trade_date: JsonDate
    include_rules: bool = False
    force: bool = False


class TradingPlaybookObsidianStatusResponse(BaseModel):
    enabled: bool
    configured: bool
    vault_exists: bool
    auto_git_enabled: bool
    last_success_at: datetime | None
    last_trade_date: date | None
    last_phase: str | None
    pending_count: int
    paused_count: int
    failed_count: int
    last_error: str | None
    recent_files: list[str]
    dashboard_path: str
    dashboard_openable: bool
```

导出响应显式建模 `written_files`、`skipped_files`、`pending_files`、`failed_files`、`git_status` 和 error summary；不要返回裸 ORM 或内部绝对路径。

- [ ] **Step 4: 实现依赖和路由**

从 `Request.app.state.trading_playbook_obsidian_sync` 取协调器。测试用 FastAPI dependency override 或 app.state stub，生产不创建模块级第二个协调器。接口只调用 `get_status()` 和 `export_trade_date()`。

- [ ] **Step 5: 运行 API 测试**

```powershell
Set-Location backend
python -m unittest discover -s tests -p 'test_trading_playbook_api.py' -v
```

Expected: 全部通过。

- [ ] **Step 6: 提交**

```powershell
git add backend/app/schemas/trading_playbook.py backend/app/api/v1/trading_playbook.py backend/tests/test_trading_playbook_api.py
git commit -m "feat: expose trading playbook Obsidian API"
```

## Task 11: 增加前端 API 与 Pinia 状态

**Files:**
- Modify: `frontend/src/types/trading-playbook.ts`
- Modify: `frontend/src/api/trading-playbook.ts`
- Modify: `frontend/src/stores/trading-playbook.ts`
- Modify: `frontend/tests/tradingPlaybookStore.test.mjs`

- [ ] **Step 1: 写 store 失败测试**

覆盖 load status、manual export、loading、success、partial failure、unconfigured、network error，以及旧请求晚返回不能覆盖新请求。状态加载同时读取现有 intelligence Obsidian 配置状态和新的 trading-playbook 同步状态；导出成功后必须刷新两者；导出失败不得清空上一份成功状态。

- [ ] **Step 2: 运行测试确认红灯**

```powershell
Set-Location frontend
node tests/tradingPlaybookStore.test.mjs
```

Expected: 缺少 Obsidian store 成员或 API 调用而失败。

- [ ] **Step 3: 增加 TypeScript 类型和 API**

```typescript
export interface TradingPlaybookObsidianStatus {
  enabled: boolean
  configured: boolean
  vault_exists: boolean
  auto_git_enabled: boolean
  last_success_at: string | null
  last_trade_date: string | null
  last_phase: string | null
  pending_count: number
  paused_count: number
  failed_count: number
  last_error: string | null
  recent_files: string[]
  dashboard_path: string
  dashboard_openable: boolean
}

export interface TradingPlaybookObsidianExportRequest {
  trade_date: string
  include_rules?: boolean
  force?: boolean
}
```

API 方法名固定为 `getObsidianStatus()` 和 `exportToObsidian(request)`，路径与 Task 10 一致。

- [ ] **Step 4: 实现 store 状态与竞态保护**

增加：

```typescript
const obsidianStatus = ref<TradingPlaybookObsidianStatus | null>(null)
const obsidianVaultStatus = ref<ObsidianStatus | null>(null)
const obsidianStatusLoading = ref(false)
const obsidianExporting = ref(false)
const obsidianError = ref<string | null>(null)
let obsidianStatusRequestId = 0

async function exportToObsidian(tradeDate: string, includeRules = false, force = false) {
  obsidianExporting.value = true
  obsidianError.value = null
  try {
    const result = await tradingPlaybookApi.exportToObsidian({
      trade_date: tradeDate,
      include_rules: includeRules,
      force,
    })
    await loadObsidianStatus()
    return result
  } catch (error) {
    obsidianError.value = normalizeApiError(error)
    throw error
  } finally {
    obsidianExporting.value = false
  }
}
```

`loadObsidianStatus()` 在同一个 request id 下并行调用 `tradingPlaybookApi.getObsidianStatus()` 和现有 `getObsidianStatus()`；只有最新一对响应可以同时更新 `obsidianStatus` 与 `obsidianVaultStatus`。前者提供同步计数和 Dashboard 相对路径，后者继续作为 `enabled`、`vault_configured`、`vault_path` 和 Vault 名称的唯一来源。复用 store 当前的错误规范化和 request-id 写法，不复制第三套 helper。

- [ ] **Step 5: 运行 store 测试**

```powershell
Set-Location frontend
node tests/tradingPlaybookStore.test.mjs
```

Expected: 全部通过。

- [ ] **Step 6: 提交**

```powershell
git add frontend/src/types/trading-playbook.ts frontend/src/api/trading-playbook.ts frontend/src/stores/trading-playbook.ts frontend/tests/tradingPlaybookStore.test.mjs
git commit -m "feat: manage Obsidian sync in trading store"
```

## Task 12: 在交易预案页面加入 Obsidian 区域

**Files:**
- Modify: `frontend/src/views/TradingPlaybook.vue`
- Modify: `frontend/tests/tradingPlaybookUi.test.mjs`

- [ ] **Step 1: 写 UI 合同失败测试**

断言页面有“Obsidian 同步”“导出到 Obsidian”“打开交易预案 Dashboard”；未配置时导出/打开禁用；同步中避免重复提交；failed/pending/paused 分开显示；页面明确“只导出、不会从 Obsidian 回写”“需要人工确认”“不会自动交易”。

打开按钮只能使用项目已有的 `obsidian://open?vault=...&file=...` URI 约定和交易状态返回的相对 Dashboard 路径；Vault 名称来自现有 intelligence Obsidian 状态，不能把绝对文件系统路径放进 `file` 参数。

- [ ] **Step 2: 运行 UI 测试确认红灯**

```powershell
Set-Location frontend
node tests/tradingPlaybookUi.test.mjs
```

Expected: 缺少 Obsidian 面板文案和 handler 而失败。

- [ ] **Step 3: 实现紧凑状态卡片**

卡片放在独立交易预案页面现有设置/状态区域，不重排候选与复盘主流程。进入页面时与其他首屏请求并行加载 status；trade date 使用页面当前选择的交易日。成功提示写入、跳过和待重试数量；部分失败展示后端摘要，不声称完整成功。

打开逻辑：仅在 `dashboard_openable` 为真，且现有 intelligence Obsidian 状态为 enabled/configured 时，取 `vault_path` 的最后一个路径段作为编码后的 Vault 名称，取 `dashboard_path` 作为编码后的文件参数。没有配置时给出配置提示，不在 UI 新增 Vault 路径输入。

- [ ] **Step 4: 运行 UI 和 store 测试**

```powershell
Set-Location frontend
node tests/tradingPlaybookUi.test.mjs
node tests/tradingPlaybookStore.test.mjs
```

Expected: 全部通过。

- [ ] **Step 5: 运行类型检查和构建**

```powershell
Set-Location frontend
npm run build
```

Expected: `vue-tsc` 和 Vite 成功，进程 exit code 0。

- [ ] **Step 6: 提交**

```powershell
git add frontend/src/views/TradingPlaybook.vue frontend/tests/tradingPlaybookUi.test.mjs
git commit -m "feat: add Obsidian controls to trading playbook"
```

## Task 13: 临时 Vault 验收回放与文档

**Files:**
- Modify: `backend/tests/test_trading_playbook_obsidian_snapshot_builder.py`
- Modify: `backend/tests/test_trading_playbook_obsidian_exporter.py`
- Modify: `backend/tests/test_trading_playbook_obsidian_sync.py`
- Modify: `README.md`

- [ ] **Step 1: 写临时 Vault 闭环验收测试**

使用临时 SQLite 和临时 Vault，seed 已有黄金 catalog v2 的 19 个模式以及五阶段最小完整交易日。依次调用 preclose、initial_review、after_close/final_review、overnight、auction 入队和 process。断言：

- 19 个规则页存在并带 8 个实际来源 hash 的引用集合。
- 四个预案阶段文件互不覆盖。
- 15:10 初步复盘和 15:30 最终复盘是两个文件且内容不同。
- 提醒、日期索引和 Dashboard 是最新版本。
- 所有系统链接闭环。
- `30_TradingPlaybook/Notes` 不存在。
- 所有 frontmatter 都是 `manual_required: true`、`auto_execute: false`。
- 内容没有未来阶段事实、微信发送记录或账户盈亏推断。

- [ ] **Step 2: 写故障恢复验收测试**

先让 Writer 失败，确认业务数据已经提交且 export 行失败；重建协调器模拟服务重启，恢复 Writer 后运行 `startup_reconcile()` 和 `process_due()`，断言全部补齐。再修改可变索引两次，故意让旧版本晚到，断言文件保留新版本。

- [ ] **Step 3: 运行 Obsidian 专项测试**

```powershell
Set-Location backend
python -m unittest discover -s tests -p 'test_obsidian*.py' -v
python -m unittest discover -s tests -p 'test_trading_playbook_obsidian_*.py' -v
```

Expected: 全部通过。

- [ ] **Step 4: 更新 README**

记录：复用 `OBSIDIAN_ENABLED`、`OBSIDIAN_VAULT_PATH`、`OBSIDIAN_AUTO_GIT_ENABLED`；目录结构；五阶段时点；API 请求示例；单向同步；Notes 禁写；手动确认；自动交易/微信禁用；60 秒补偿；测试 Vault 启用顺序；真实 PostgreSQL、真实 Vault 与可选 Git 必须部署时单独联调。

手动导出示例使用固定日期：

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri 'http://127.0.0.1:8000/api/v1/trading-playbook/obsidian/export' `
  -ContentType 'application/json' `
  -Body '{"trade_date":"2026-07-15","include_rules":true,"force":false}'
```

- [ ] **Step 5: 运行交易预案后端回归**

```powershell
Set-Location backend
python -m unittest discover -s tests -p 'test_trading_playbook_*.py' -v
python -m unittest discover -s tests -p 'test_websocket_manager.py' -v
python -m unittest discover -s tests -p 'test_main_lifespan.py' -v
```

Expected: 三条专项命令全部 exit code 0。不要用全量后端已知基线替代专项结果；若运行全量测试，单独报告既有 THS 日期相关失败，不把它们归因于本功能。

- [ ] **Step 6: 运行前端完整回归和构建**

```powershell
Set-Location frontend
npm test
npm run build
```

Expected: Node 测试全部通过，Vite build exit code 0。

- [ ] **Step 7: 验证工作树和变更边界**

```powershell
Set-Location ..
git status --short
git diff --check
git diff --name-only HEAD~12..HEAD
```

Expected: `git diff --check` 无输出；文件列表只包含本计划列出的 backend/frontend/README 路径，不包含真实 Vault 文件、环境文件、数据库文件或用户主工作树内容。

- [ ] **Step 8: 提交文档与验收测试**

```powershell
git add README.md backend/tests/test_trading_playbook_obsidian_snapshot_builder.py backend/tests/test_trading_playbook_obsidian_exporter.py backend/tests/test_trading_playbook_obsidian_sync.py
git commit -m "docs: document Obsidian trading playbook sync"
```

## Task 14: 完成前复核

- [ ] **Step 1: 规格覆盖审计**

逐项对照 `docs/superpowers/specs/2026-07-15-trading-playbook-obsidian-integration-design.md` 的目标、非目标、持久化字段、目录、六类页面、五阶段、60 秒补偿、退避、startup、API、UI、安全、测试和九条验收标准。每项都必须能指向代码和测试。

- [ ] **Step 2: 占位符与危险命令扫描**

```powershell
rg -n "TODO|TBD|FIXME|NotImplemented|pass$|git add -A|git add \.|30_TradingPlaybook/Notes.*write" backend/app backend/tests frontend/src frontend/tests README.md
```

Expected: 本功能新增代码没有占位实现；没有宽泛 Git add；没有 Notes 写入路径。已有无关命中必须逐条确认并在交付说明中排除。

- [ ] **Step 3: 类型与枚举一致性审计**

检查 SQLAlchemy、Pydantic、Python DTO、API JSON、TypeScript 对 status/entity_type/phase、日期、nullable 字段和文件列表的定义一致。特别确认 `initial_review` 与 `final_review` 不被误写成计划 stage。

- [ ] **Step 4: 最终验证**

重复 Task 13 的专项后端、前端测试和 build，并保存实际测试数量和 exit code。仅在这些命令刚刚成功后才能说明功能完成。

- [ ] **Step 5: 最终提交状态检查**

```powershell
git status --short
git log --oneline -15
```

Expected: 工作树 clean，能看到每个 Task 的小提交；不 push、不 merge、不 deploy，等待用户选择集成方式。
