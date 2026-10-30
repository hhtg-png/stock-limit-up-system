# 通达信复合播报无数据：恢复与根因修复

## 故障证据

- 2026-10-30 用户报告通达信复合页面无数据，地址为 `/tdx/composite/dark#xxxxxx`。
- 线上版本 `179d90c`，systemd 服务虽为 active，但 8000 监听队列达到 2049/2048，配置、行情、WebSocket、播放日志等多路请求均超时或返回 504。
- 应用最后日志在 10 月 29 日 16:00:40，此前 TTS 已返回音频并收到客户端 playing/ended；本次并非独立的静音或语音合成故障。
- 非阻塞进程取样定位到 SQLAlchemy 全量 ORM 行转换；数据属于 Obsidian 导出历史，原始行从历史首条开始读取。该表有 187,697 行，快照 JSON 合计 479,038,445 字节，最大单条 45,304,472 字节。进程约占 1.8 GiB RSS，2 GiB swap 已满。
- `_load_git_intent_rows` 在读取全部 written 行及快照正文后才用 Python 筛选和截断；每分钟补偿同步即使没有待写项目也会调用。可变快照 enqueue 同样读取全部版本；两个日期轮流生成相同 dashboard key 又造成无变化时持续增加版本。

## 临时恢复

保留故障堆栈后，通过 `/run/systemd/system/stock-backend.service.d/90-tdx-recovery.conf` 的临时进程环境将 `TRADING_PLAYBOOK_ENABLED=false`，由 systemd 重启恢复通达信和核心行情请求。没有删除历史记录，也没有更改持久化的用户配置。修复部署后须移除此覆盖并恢复交易预案任务，不能以暂停任务作为最终完成状态。

## 修复范围

1. Git 重试读取采用有界元数据 keyset 分页，保留原有 legacy/null/error 判断，不加载 snapshot_json，避免已完成项遮挡后续重试项；页间让出事件循环。
2. 为 `(status, trade_date, id)` 补建幂等索引，确保已有线上表也升级，避免分页时重复全表排序。
3. 可变快照只读取最新版本；不可变快照按 hash 定向复用并保留原有版本、冲突和 writer lease 栅栏。
4. 每轮 dashboard 只对最新业务日期生成一次，其他按日工件仍保留原有日期覆盖。
5. 补充行为、损坏历史快照、分页边界、版本稳定及已有数据库索引升级回归。保留全部历史和未完成任务。

## 交付验证

- 运行完整后端测试及前端构建，独立复核后提交并推送 origin/main，使用既有部署流程发布。
- 撤销临时覆盖、恢复交易预案任务，观察启动恢复和至少两个补偿周期，验证服务内存和接口响应没有再次恶化。
- 直接核验复合页面的数据、实时状态 API、WebSocket 及 TTS 音频接口，记录线上 HEAD 和资源一致性。

## 本地验证记录

- 独立复核最终分页边界：同日满页、跨日补页、无待重试项、limit、row_ids 和 allowed_paths 过滤均正确。
- 最终分页专项 9 项、已有 Obsidian 167 项（2 个平台跳过）、调度器 92 项、索引升级 3 项通过；完整后端套件另行执行最终来源。
- `npm run build` 通过，2168 个模块，6.68 秒；仅现有 bundle 大小提示。前端源码未修改。
- 构建 `frontend/dist/index.html` SHA256：`6a60fec79bfca89b989c4e43112f2b311bfe54b7f4ed897b65ef68bdda2cfead`；入口 `index-CT0vFCO2.js`。
- 临时恢复后只读检查：TDX API 200，页面从 32 增到 38 条；12 秒 WebSocket 收到 snapshot、delta、新的真实涨停播报及新闻事件；TTS 200，audio/mpeg，11808 字节。此结果尚不代替恢复全部后台任务后的最终观察。
- 最终完整后端验证：`python -m unittest discover tests -v`，1187 项，297.919 秒，exit 0，`OK (skipped=2)`。模型索引精确契约已同步新增索引。
- 最终实现与专项测试经独立只读审查通过，无阻塞问题；部署前 `git diff --check` 通过。
