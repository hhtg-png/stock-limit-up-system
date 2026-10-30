# Daily History Snapshots Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** 下线 Obsidian 写出链路，并每日生成可跨重启复用的历史涨停监控与报表数据。

**Architecture:** 保留现有行情与主报表事实表，新增一日一行的监控及断板表现归档；API 优先复用有效历史归档，盘中仍走实时。统一小批次后台任务生成和补齐，并将旧每日统计改为可重入的数据库汇总。

**Tech Stack:** Python / FastAPI / SQLAlchemy / APScheduler / Vue 3。

**Spec:** `docs/superpowers/specs/2026-10-30-daily-history-design.md`

## Global Constraints

- 原始工作树有无关修改；仅使用当前隔离工作树与 `codex/daily-history-snapshots-20261030`。
- 所有日期按 Asia/Shanghai；昨日字段基于行情日期的前一交易日。
- 保留历史笔记、数据库业务数据、实时行情与播报能力。
- 仅部署已验证并推送 GitHub main 的代码。

## Review Focus

- 混合历史和今天的查询，今天盘中不得命中旧最终快照。
- 数据修复、无数据、部分行情与超时不得产生永久错误缓存。
- 同日期并发和补跑不得重复版本膨胀或覆盖完整结果。
- 周末、节假日和日期回退必须沿用真实交易日语义。
- 下线 Obsidian 不得误删仍被产业趋势/超短信号使用的数据库聚合。

### Task 1: Retire Obsidian production integration

**Files:** main.py、intelligence_service.py、相关 API、前端六个页面及 store；主代理集成 scheduler。
- [x] 增加启动无 coordinator、自动同步和旧导出 API 不写临时 Vault 的失败测试。
- [x] 停掉生产安装、调度和提交后 enqueue，旧 POST 返回 410，移除 UI 入口。
- [x] 更新退役契约，核验业务分析与交易预案仍工作。

### Task 2: Persist historical monitor responses

**Files:** models/limit_up.py、limit_up_monitor_archive_service.py、limit_up_context_service.py、api/v1/limit_up.py。
**Interface:** `await limit_up_monitor_archive_service.ensure_for_date(trade_date, force=False)`。
- [x] 增加跨实例复用、实时 bypass、回退日期、排序筛选、部分/空/失败与源指纹变化测试并见红。
- [x] 保存未筛选列表，批量复用竞价与前日事实，完整性和最终事实屏障确认后归档。
- [x] 聚焦测试通过并交独立复核。

### Task 3: Persist historical broken-board performance

**Files:** models/market_review.py、broken_board_performance_service.py、api/v1/review.py。
**Interface:** `await broken_board_performance_service.build_after_close(trade_date, force=False)`。
- [x] 增加跨实例、混合日期、当前盘中、停牌/无样本、源修复及失败测试并见红。
- [x] 逐日保存原有复权算法的完整结果，批读有效历史，仅缺失重算。
- [x] 保留其余报表现有数据库链路，聚焦测试通过。

### Task 4: Daily preparation, recovery and statistics

**Files:** services/history_preparation_service.py、data_collectors/scheduler.py、models/__init__.py、相关 tests。
- [x] 先验证统计在重复执行、不同日期、无源数据及盘中边界的失败测试。
- [x] 以 SQL case 正确汇总并按日期更新；小批次、互斥、逐日容错生成归档。
- [x] 注册 15:35 / 20:35、启动与每 5 分钟补齐；不阻塞实时请求。
- [x] 更新 scheduler 的 Obsidian 退役契约；核验完整性屏障、重复调度与取消。

### Task 5: Release

- [x] 独立审查、完整后端套件与前端构建通过。
- [x] 提交、推送 main、部署并核对线上 HEAD 与资源。
- [x] 预生成近期历史，实测历史接口冷查询/重启复用、盘中刷新、退役入口与调度状态。

### Task 6: Repair historical opening-price failover

- [x] 实测旧腾讯 proxy 返回 501/WAF，同参数 web 地址返回有效 qfqday；确认旧数据库开盘价为空，不能据此推算。
- [x] 先红后绿覆盖 EM 断开、web 正常、proxy 501；保留反向兜底、除权同序列与 raw day 拒绝用例。
- [x] 增加 web → proxy 自动兜底，三源单股等待预算保持在整体实时 7.5 秒上限内。

## Final pre-release verification

- Backend: `python -m unittest discover tests -v`, 1208 tests, 328.116 seconds, exit 0, `OK (skipped=2)`.
- Frontend: `npm test`, 152 tests, exit 0; `npm run build`, 6.33 seconds, exit 0. Existing large-chunk warning only.
- Entry asset: `index-CV8nbHvv.js`; index SHA256 `ed2c89fd62872aeac040b7e0888efc5ea01e30c730d6d7d41fada9c457f7171a`.
- Independent reviews found and verified fixes for archive retry starvation, stale ready rows, final quote timestamps and D/D-1 source revisions.
- Retired scheduler/claim setup tests now retain database persistence/replay/lease assertions while rejecting any legacy export call. Targeted suites and final full discovery pass.
- Production baseline: monitor historical cold 3.97–4.28 s; cached broken-board 30-session request 0.609 s. Initial DailyStatistics rows: 0; existing source dates: limit-up 169, review 136.

## Production follow-up verification

- First release `9303133` deployed successfully; automatic exports disabled, old export outbox MAX(id) unchanged at 187698.
- Valid partial monitor archives now return immediately while a shared background task retries missing facts. Schema/source changes invalidate partial display, including provider failures after a source repair (100 → 999 regression).
- Preparation uses the China A-share trading calendar off-thread, bounded to 5 seconds and cached per China calendar day. The production audit also found one final review on a public holiday, so final facts cannot establish trading-day validity. Calendar failures skip new generation and retry after 5 minutes; persisted historical reads remain available. The failure/recovery regression includes a legacy partial final review on a holiday.
- Read-only production calendar audit found 9 invalid derived statistics rows and 6 invalid monitor cache rows; raw facts and old notes are retained. Correcting those derived caches requires an exact-date assertion and a recoverable row backup.
- Full follow-up discovery: 1212 tests, 335.940 seconds, exit 0, `OK (skipped=2)`. Latest archive regression (including the subsequently added provider-failure case): 16 tests, 13.172 seconds, exit 0. History preparation: 10 tests, exit 0. Independent review resolved the stale partial failure and found no remaining blocking issue.
- Fresh unchanged frontend build: exit 0, 6.17 seconds, same entry asset and index SHA256 above.
- Production cross-process reuse (9303133, separate venv PID, SQLite query-only): real-calendar 30-session report query returned 30 ready points in 0.8417 s with history/fallback/quote/suspension provider calls all zero; monitor 2026-10-23 returned 94 ready rows in 0.0111 s with enrichment calls zero and no database write attempts. Shanghai 600838 previous session remains 2026-10-22, yesterday final seal 10:52:37.
- Final frozen-source backend discovery: 1213 tests, 334.201 seconds, exit 0, `OK (skipped=2)`, including the stricter calendar outage recovery and latest partial fallback cases.
- Release 2ff0697: GitHub main/remote HEAD and public index hash matched; managed backend active with no restarts, 52 current realtime items, TTS audio and WebSocket snapshot/delta verified. Corrected derived holiday caches have recoverable row backups; raw source counts unchanged, derived non-trading dates zero. Statistics 159 dates; monitor/report 30 dates each. Public historical monitor/report probes: 0.063/0.062 s.
- Completeness audit found monitor 30-day rows 2296, missing yesterday/today opening percentages 1755/1733 (5 ready dates, 25 partial). Main cause: EM disconnect plus sole Tencent proxy returning 501/WAF. Same-parameter Tencent web queries returned valid adjusted series. Provider repair context suite: 28 tests, 1.859 s, exit 0; source/valuation algorithms unchanged.
- A follow-up 1216-test run passed business assertions but failed Windows teardown for the 20 ms monitor timeout case: an initial NullPool/aiosqlite connection could still hold the temporary file. The test fixture now retains shielded session-close task ownership, waits for cleanup, and uses a real disk SQLite queue pool with 3 prewarmed connections for that case. The timeout/build/persistence assertions remain intact; the affected case and deterministic close-cancellation regression passed 5 consecutive rounds (10 checks). Production connection handling is unchanged.
- Final acceptance with provider failover and fixture cleanup: `python -m unittest discover tests -v`, 1217 tests, 335.696 s, exit 0, `OK (skipped=2)`. Fresh frontend build: 6.18 s, exit 0, unchanged entry asset/hash; existing chunk-size warning only. Independent source/budget review and fixture review passed.
