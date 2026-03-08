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
