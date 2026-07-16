# 股票涨停统计分析系统

专业的A股涨停统计分析系统，提供实时涨停监控、大单分析、数据可视化等功能。

## 功能特性

- **多源数据集成**: 同花顺爬虫 + 开盘啦爬虫 + 通达信Level-2
- **涨停分析**: 首次涨停时间(秒级)、连板统计、涨停原因分类、开板/回封检测
- **大单分析**: 可配置阈值、主动/被动买卖识别、涨停价大单监控
- **实时推送**: WebSocket实时广播涨停、大单、状态变化
- **可视化**: 涨停热力图、趋势图、板块统计、K线图
- **播报功能**: 语音播报、桌面通知、可开关控制

## 技术栈

- **后端**: Python 3.10+ / FastAPI / SQLAlchemy / SQLite
- **前端**: Vue 3 / TypeScript / Vite / Element Plus / ECharts
- **数据**: pytdx / BeautifulSoup / APScheduler

## 快速开始

### 环境要求

- Python 3.10+
- Node.js 18+

### 启动方式

**Windows一键启动**:
```bash
双击 start.bat
```

**手动启动**:

1. 启动后端:
```bash
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

2. 启动前端:
```bash
cd frontend
npm install
npm run dev
```

3. 访问:
- 前端界面: http://localhost:3000
- API文档: http://localhost:8000/docs

## 交易预案

交易预案是项目内的独立页面，按 Asia/Shanghai 时间在交易日生成并保留多个审计版本：

| 时间 | 用途 |
|------|------|
| 14:40 | 生成次日提前预案，同时给出当日尾盘建议 |
| 15:10 | 生成当日执行复盘 |
| 15:30 | 生成正式次日预案 |
| 次日 08:50 | 根据隔夜信息刷新预案 |
| 次日 09:26 | 根据集合竞价生成最终版本 |

系统最多给出 3 只正式候选。目标预案日与复盘日是两个独立的日期选择；确认前可以人工修订，修订会生成可追溯的审计子版本，不会覆盖原版本。数据标记为 `stale` 或 `degraded` 时仅供观察，不开放行动级提醒。

行动级提醒必须在 [交易预案页面](http://localhost:3000/trading-playbook) 人工确认后才会启用。确认的含义仅是开启提醒，系统不会自动下单或执行任何交易。第一版提醒仅在本项目内独立展示；微信机器人尚未接入，页面中的微信设置保持禁用。后续可以增加微信机器人通道，但当前版本不会向微信发送消息。

### 启动与访问

分别打开两个 PowerShell 窗口，在项目根目录执行：

```powershell
cd backend
pip install -r requirements.txt
python -m uvicorn app.main:app --reload --port 8000
```

```powershell
cd frontend
npm install
npm run dev
```

启动后访问：

- 页面：<http://localhost:3000/trading-playbook>
- 规则 API：<http://localhost:8000/api/v1/trading-playbook/rules>
- 完整 API 文档：<http://localhost:8000/docs>

### Obsidian 交易预案单向同步

交易预案复用项目已有的 Obsidian Vault 配置，不设置第二套目录。建议初次验证保持自动 Git 关闭：

```env
OBSIDIAN_ENABLED=false
OBSIDIAN_VAULT_PATH=C:\path\to\test-vault
OBSIDIAN_AUTO_GIT_ENABLED=false
```

启用后，系统只把已提交的数据库事实单向导出到 Vault，不从 Obsidian 读取或回写交易数据。所有自动页都标记 `manual_required: true` 和 `auto_execute: false`；预案仍须在项目内人工确认，系统不会自动下单或交易。Obsidian 不是提醒发送通道，当前微信机器人继续禁用，不会产生微信发送记录。

系统写入范围如下：

```text
30_TradingPlaybook/
├── Modes/Auto/v2/                         # 19 个版本化模式页
├── Daily/Auto/YYYY/YYYY-MM-DD/            # 四阶段预案与日期 index.md
├── Reviews/Auto/YYYY/YYYY-MM-DD/          # initial-review / final-review
├── Alerts/Auto/YYYY/YYYY-MM-DD.md          # 项目内提醒时间线
└── Notes/                                  # 仅作为链接目标，系统永不创建或修改
Dashboards/
└── 交易预案.md                             # 最新交易预案 Dashboard
```

五个中国时间同步点及产物：

| 时间 | 阶段 | Obsidian 产物 |
|------|------|---------------|
| 14:40 | `preclose` | 次日提前预案、当日尾盘候选、索引与提醒 |
| 15:10 | `initial_review` | 独立且不可变的初步复盘、索引与提醒 |
| 15:30 | `after_close` + `final_review` | 正式次日预案、独立最终复盘、索引与提醒 |
| 次日 08:50 | `overnight` | 隔夜刷新版本与索引 |
| 次日 09:26 | `auction` | 竞价最终版本、索引与提醒 |

每 60 秒运行一次补偿扫描；写入失败按 1、5、15 分钟退避重试。Vault 暂时禁用或未配置时任务进入暂停状态，不影响预案、复盘或提醒的业务事务；服务重启后会从已提交的数据库事实补齐缺失页面。可选 Git 只提交本批发生变化的系统文件，绝不包含 `Notes` 或 Vault 中其他用户文件。

查询状态可调用 `GET /api/v1/trading-playbook/obsidian/status`。按固定日期手动重导示例：

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri 'http://127.0.0.1:8000/api/v1/trading-playbook/obsidian/export' `
  -ContentType 'application/json' `
  -Body '{"trade_date":"2026-07-15","include_rules":true,"force":false}'
```

首次上线按以下顺序启用：

1. 保持 `OBSIDIAN_ENABLED=false` 部署代码和数据库变更。
2. 使用独立测试 Vault 验证状态接口、19 个规则页和一个历史交易日的完整重导。
3. 配置 `OBSIDIAN_VAULT_PATH`，保持 `OBSIDIAN_AUTO_GIT_ENABLED=false`。
4. 设置 `OBSIDIAN_ENABLED=true`，观察五个时点以及 60 秒补偿状态。
5. 仅在确认测试 Vault 是独立 Git 仓库且路径限定正确后，选择是否启用 `OBSIDIAN_AUTO_GIT_ENABLED=true`。

测试使用临时 SQLite 和临时 Vault，不会触碰用户目录。真实 PostgreSQL、真实 Vault 和可选 Git 必须在部署环境单独联调后再启用，不能以临时测试结果替代。

### 从文字稿导入 19 种模式

导入命令会校验指定目录中的文字稿来源，并把项目内版本化的 19 种交易模式目录写入数据库。在项目根目录的 PowerShell 中执行：

```powershell
cd backend
python -m app.scripts.import_trading_playbook_rules --source-root 'C:\Users\Administrator\Documents\Codex\2026-07-07\ysheba257-lgtm-xiaoe-scraper-https-github\xiaoe-scraper\videos'
```

### 19 模式黄金回放

```powershell
cd backend
python -m app.scripts.replay_trading_playbook --date 2026-07-10 --stage preclose --no-notify
```

`--date` 和 `--stage` 是本次命令的请求上下文，会在输出中显示为 `requested_date`、`requested_stage`；当前黄金回放始终校验仓库内固定 fixture 的 19 组事实及其 `fixture_as_of`，不会按请求参数重写历史事实。`--no-notify` 明确禁止回放发送提醒。

### 验证

以下命令均从项目根目录开始执行：

```powershell
cd frontend
npm test
npm run build
```

```powershell
cd backend
python -m unittest discover -s tests -p 'test_trading_playbook_*.py' -v
python -m unittest discover -s tests -p 'test_websocket_manager.py' -v
python -m unittest discover -s tests -p 'test_main_lifespan.py' -v
```

如需检查整个后端测试基线，可运行 `python -m unittest discover -s tests -v`；应按输出逐项核对，不要用全量结果替代上面的交易预案专项测试。

### Docker部署

```bash
docker-compose up -d
```

## 项目结构

```
stock-limit-up-system/
├── backend/                # Python后端
│   ├── app/
│   │   ├── api/v1/         # API路由
│   │   ├── models/         # 数据库模型
│   │   ├── schemas/        # 数据验证
│   │   ├── crawlers/       # 爬虫模块
│   │   ├── data_collectors/# 数据采集
│   │   ├── analyzers/      # 分析引擎
│   │   └── core/           # 核心组件
│   └── requirements.txt
│
├── frontend/               # Vue3前端
│   ├── src/
│   │   ├── views/          # 页面组件
│   │   ├── components/     # 通用组件
│   │   ├── stores/         # 状态管理
│   │   └── api/            # API请求
│   └── package.json
│
├── docker-compose.yml
├── start.bat               # Windows启动脚本
└── README.md
```

## API接口

| 接口 | 说明 |
|------|------|
| GET /api/v1/limit-up/realtime | 实时涨停列表 |
| GET /api/v1/limit-up/{code} | 涨停详情 |
| GET /api/v1/statistics/daily | 日统计数据 |
| GET /api/v1/statistics/sectors | 板块热度 |
| GET /api/v1/market/{code}/orderbook | 五档盘口 |
| GET /api/v1/market/{code}/big-orders | 大单记录 |
| WS /ws/realtime | WebSocket实时推送 |

## 配置说明

复制 `backend/.env.example` 为 `backend/.env` 并修改:

```env
# 通达信服务器配置
TDX_HOST=119.147.212.81
TDX_PORT=7709

# 大单阈值
DEFAULT_BIG_ORDER_THRESHOLD=500000

# 爬虫间隔(秒)
CRAWLER_INTERVAL_THS=300
```

## 注意事项

1. 爬虫模块可能需要根据网站实际结构调整
2. 通达信pytdx需要配置可用的行情服务器
3. 生产环境建议配置代理IP池
4. 交易时间外数据采集任务会自动暂停
