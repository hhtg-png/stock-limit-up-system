# Stock Detail Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the stock detail page into a cleaner trading-workbench view with period switching, candlestick zoom, overlay comparison lines, limit-up highlighting, order book, big orders, and limit-up timeline.

**Architecture:** Keep the first implementation on the existing Vue 3 + ECharts stack. Add backend K-line and compare endpoints in `market.py`, expose typed frontend API helpers, then replace the detail page layout and chart option builder while preserving existing order book, big order, and limit-up detail APIs.

**Tech Stack:** FastAPI, SQLAlchemy async sessions, httpx, Vue 3 Composition API, TypeScript, Element Plus, ECharts 5, Sass.

---

## File Structure

- Modify `backend/app/api/v1/market.py`
  - Add K-line response models.
  - Add Eastmoney K-line fetch helper.
  - Add compare-series normalization helper and endpoint.
  - Keep current order book, big order, fund flow, and timeline behavior intact.
- Create `backend/tests/test_market_kline_api.py`
  - Unit-test helper parsing, limit-up detection, endpoint fallback, and compare-series normalization.
- Modify `frontend/src/types/market.ts`
  - Add `KlinePeriod`, `KlinePoint`, `KlineResponse`, `ComparePoint`, and `CompareSeries`.
- Modify `frontend/src/api/market.ts`
  - Add `getKline()` and `getCompareSeries()`.
- Modify `frontend/src/views/StockDetail.vue`
  - Replace the current table-heavy layout with the approved workbench layout.
  - Add period state, overlay state, K-line fetching, chart option builder, resize handling, and cleaner side panels.

## Task 1: Backend K-Line Contract

**Files:**
- Modify: `backend/app/api/v1/market.py`
- Create: `backend/tests/test_market_kline_api.py`

- [ ] **Step 1: Write failing helper and endpoint tests**

Create `backend/tests/test_market_kline_api.py` with this content:

```python
import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.api.v1 import market


class FakeScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar_one_or_none(self):
        return self.value


class FakeSession:
    def __init__(self, stock):
        self.stock = stock

    async def execute(self, _query):
        return FakeScalarResult(self.stock)


class MarketKlineApiTests(unittest.IsolatedAsyncioTestCase):
    def test_format_kline_item_marks_main_board_limit_up(self):
        raw = "2026-05-12,96.10,103.42,103.42,95.60,560000,1820000000,8.20,10.00,9.40,17.42"

        point = market._format_kline_item(raw, "603893")

        self.assertEqual(point["date"], date(2026, 5, 12))
        self.assertEqual(point["open"], 96.10)
        self.assertEqual(point["close"], 103.42)
        self.assertEqual(point["high"], 103.42)
        self.assertEqual(point["low"], 95.60)
        self.assertEqual(point["volume"], 560000)
        self.assertEqual(point["amount"], 1820000000)
        self.assertEqual(point["change_pct"], 10.00)
        self.assertTrue(point["is_limit_up"])

    def test_format_kline_item_uses_twenty_percent_board_for_chinext(self):
        raw = "2026-05-12,10.00,11.00,11.00,9.90,1000,1100000,11.00,10.00,1.00,3.20"

        point = market._format_kline_item(raw, "300001")

        self.assertFalse(point["is_limit_up"])

    def test_normalize_symbol_infers_market_from_suffix_or_code(self):
        self.assertEqual(market._normalize_symbol("000001.SH"), ("000001", "SH", "1.000001"))
        self.assertEqual(market._normalize_symbol("603893"), ("603893", "SH", "1.603893"))
        self.assertEqual(market._normalize_symbol("300001"), ("300001", "SZ", "0.300001"))

    async def test_get_kline_data_fetches_by_stock_market(self):
        stock = SimpleNamespace(stock_code="603893", market="SH")
        fake_db = FakeSession(stock)
        fetched = [
            {
                "date": date(2026, 5, 12),
                "open": 96.1,
                "close": 103.42,
                "high": 103.42,
                "low": 95.6,
                "volume": 560000,
                "amount": 1820000000,
                "change_pct": 10.0,
                "is_limit_up": True,
            }
        ]

        with patch.object(market, "_fetch_kline_from_em", AsyncMock(return_value=fetched)) as fetcher:
            response = await market.get_kline_data("603893", "day", 250, fake_db)

        fetcher.assert_awaited_once_with("603893", "SH", "day", 250)
        self.assertEqual(response.stock_code, "603893")
        self.assertEqual(response.period, "day")
        self.assertEqual(response.data[0].close, 103.42)
        self.assertTrue(response.data[0].is_limit_up)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```powershell
cd D:\code\stock-limit-up-system\backend
python -m unittest tests.test_market_kline_api -v
```

Expected: FAIL because `_format_kline_item`, `_normalize_symbol`, `_fetch_kline_from_em`, and `get_kline_data` do not exist.

- [ ] **Step 3: Add K-line models, helpers, and endpoint**

In `backend/app/api/v1/market.py`, change the typing import:

```python
from typing import Optional, List, Literal
```

Add these models after `FundFlowResponse`:

```python
class KlinePointResponse(BaseModel):
    """K线点位"""
    date: date
    open: float
    close: float
    high: float
    low: float
    volume: int
    amount: float
    change_pct: Optional[float] = None
    is_limit_up: bool = False


class KlineResponse(BaseModel):
    """K线响应"""
    stock_code: str
    period: Literal["day", "week", "month"]
    data: List[KlinePointResponse] = Field(default_factory=list)
```

Add these helpers above the order book endpoint:

```python
PERIOD_TO_KLT = {
    "day": "101",
    "week": "102",
    "month": "103",
}


def _limit_up_threshold(stock_code: str) -> float:
    if stock_code.startswith("3") or stock_code.startswith("68"):
        return 19.9
    return 9.9


def _normalize_symbol(symbol: str) -> tuple[str, str, str]:
    raw = symbol.strip().upper()
    if "." in raw:
        code, market = raw.split(".", 1)
    else:
        code = raw
        market = "SH" if code.startswith("6") else "SZ"

    prefix = "1" if market == "SH" else "0"
    return code, market, f"{prefix}.{code}"


def _format_kline_item(raw: str, stock_code: str) -> dict:
    parts = raw.split(",")
    if len(parts) < 11:
        raise ValueError(f"K线数据字段不足: {raw}")

    change_pct = float(parts[8]) if parts[8] not in ("", "-") else None
    return {
        "date": date.fromisoformat(parts[0]),
        "open": float(parts[1]),
        "close": float(parts[2]),
        "high": float(parts[3]),
        "low": float(parts[4]),
        "volume": int(float(parts[5] or 0)),
        "amount": float(parts[6] or 0),
        "change_pct": change_pct,
        "is_limit_up": change_pct is not None and change_pct >= _limit_up_threshold(stock_code),
    }


async def _fetch_kline_from_em(
    stock_code: str,
    market: str,
    period: str,
    limit: int,
) -> List[dict]:
    """从东方财富获取日/周/月K线"""
    if period not in PERIOD_TO_KLT:
        raise HTTPException(status_code=400, detail="period 仅支持 day/week/month")

    prefix = "0" if market == "SZ" else "1"
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": f"{prefix}.{stock_code}",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": PERIOD_TO_KLT[period],
        "fqt": "1",
        "end": "20500101",
        "lmt": str(limit),
    }

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"}, params=params)
            result = resp.json()

        klines = result.get("data", {}).get("klines") or []
        formatted = []
        for item in klines:
            try:
                formatted.append(_format_kline_item(item, stock_code))
            except (ValueError, TypeError) as exc:
                logger.warning(f"跳过{stock_code}异常K线: {exc}")
        return formatted
    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"从东方财富获取{stock_code} {period} K线失败: {e}")
        return []
```

Add this endpoint after `get_timeline_data`:

```python
@router.get("/{stock_code}/kline", response_model=KlineResponse, summary="获取K线数据")
async def get_kline_data(
    stock_code: str,
    period: Literal["day", "week", "month"] = Query("day", description="周期 day/week/month"),
    limit: int = Query(250, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """获取日/周/月K线数据"""
    stock_query = select(Stock).where(Stock.stock_code == stock_code)
    stock_result = await db.execute(stock_query)
    stock = stock_result.scalar_one_or_none()

    if not stock:
        raise HTTPException(status_code=404, detail="股票不存在")

    points = await _fetch_kline_from_em(stock_code, stock.market, period, limit)
    return KlineResponse(stock_code=stock_code, period=period, data=points)
```

- [ ] **Step 4: Run K-line tests and verify they pass**

Run:

```powershell
cd D:\code\stock-limit-up-system\backend
python -m unittest tests.test_market_kline_api -v
```

Expected: PASS.

- [ ] **Step 5: Commit backend K-line contract**

Run:

```powershell
git add backend/app/api/v1/market.py backend/tests/test_market_kline_api.py
git commit -m "feat: add stock kline api"
```

## Task 2: Backend Compare Series

**Files:**
- Modify: `backend/app/api/v1/market.py`
- Modify: `backend/tests/test_market_kline_api.py`

- [ ] **Step 1: Add failing compare tests**

Append these tests inside `MarketKlineApiTests` in `backend/tests/test_market_kline_api.py`:

```python
    def test_build_compare_series_normalizes_from_first_close(self):
        points = [
            {"date": date(2026, 5, 10), "close": 10.0},
            {"date": date(2026, 5, 11), "close": 11.0},
            {"date": date(2026, 5, 12), "close": 9.5},
        ]

        series = market._build_compare_series("603893", "瑞芯微", points)

        self.assertEqual(series["symbol"], "603893")
        self.assertEqual(series["name"], "瑞芯微")
        self.assertEqual(series["data"][0]["change_pct_from_start"], 0.0)
        self.assertEqual(series["data"][1]["change_pct_from_start"], 10.0)
        self.assertEqual(series["data"][2]["change_pct_from_start"], -5.0)

    async def test_get_compare_data_fetches_each_symbol(self):
        with patch.object(
            market,
            "_fetch_kline_from_em",
            AsyncMock(
                side_effect=[
                    [
                        {"date": date(2026, 5, 10), "close": 10.0},
                        {"date": date(2026, 5, 11), "close": 11.0},
                    ],
                    [
                        {"date": date(2026, 5, 10), "close": 3000.0},
                        {"date": date(2026, 5, 11), "close": 3030.0},
                    ],
                ]
            ),
        ) as fetcher:
            response = await market.get_compare_data("603893,000001.SH", "day", 250)

        self.assertEqual(fetcher.await_count, 2)
        self.assertEqual([item.symbol for item in response], ["603893", "000001.SH"])
        self.assertEqual(response[0].data[1].change_pct_from_start, 10.0)
        self.assertEqual(response[1].data[1].change_pct_from_start, 1.0)
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```powershell
cd D:\code\stock-limit-up-system\backend
python -m unittest tests.test_market_kline_api -v
```

Expected: FAIL because `_build_compare_series` and `get_compare_data` do not exist.

- [ ] **Step 3: Add compare response models and endpoint**

In `backend/app/api/v1/market.py`, add these models after `KlineResponse`:

```python
class ComparePointResponse(BaseModel):
    """叠加走势点位"""
    date: date
    change_pct_from_start: float


class CompareSeriesResponse(BaseModel):
    """叠加走势序列"""
    symbol: str
    name: str
    data: List[ComparePointResponse] = Field(default_factory=list)
```

Add this helper after `_fetch_kline_from_em`:

```python
def _build_compare_series(symbol: str, name: str, points: List[dict]) -> dict:
    valid_points = [point for point in points if point.get("close") not in (None, 0)]
    if not valid_points:
        return {"symbol": symbol, "name": name, "data": []}

    base_close = float(valid_points[0]["close"])
    return {
        "symbol": symbol,
        "name": name,
        "data": [
            {
                "date": point["date"],
                "change_pct_from_start": round((float(point["close"]) - base_close) / base_close * 100, 2),
            }
            for point in valid_points
        ],
    }
```

Add this endpoint after `get_kline_data`:

```python
@router.get("/compare", response_model=List[CompareSeriesResponse], summary="获取叠加走势")
async def get_compare_data(
    symbols: str = Query(..., description="逗号分隔代码，如 603893,000001.SH"),
    period: Literal["day", "week", "month"] = Query("day", description="周期 day/week/month"),
    limit: int = Query(250, ge=1, le=1000),
):
    """获取多标的归一化叠加走势"""
    result = []
    for symbol in [item.strip() for item in symbols.split(",") if item.strip()]:
        code, market, _secid = _normalize_symbol(symbol)
        points = await _fetch_kline_from_em(code, market, period, limit)
        result.append(_build_compare_series(symbol, symbol, points))
    return result
```

- [ ] **Step 4: Run compare tests and full backend test suite**

Run:

```powershell
cd D:\code\stock-limit-up-system\backend
python -m unittest tests.test_market_kline_api -v
python -m unittest discover tests -v
```

Expected: both commands PASS.

- [ ] **Step 5: Commit compare endpoint**

Run:

```powershell
git add backend/app/api/v1/market.py backend/tests/test_market_kline_api.py
git commit -m "feat: add stock compare series api"
```

## Task 3: Frontend Market Types and API

**Files:**
- Modify: `frontend/src/types/market.ts`
- Modify: `frontend/src/api/market.ts`

- [ ] **Step 1: Add frontend type definitions**

Append this to `frontend/src/types/market.ts`:

```ts
export type KlinePeriod = 'timeline' | 'day' | 'week' | 'month'

export interface KlinePoint {
  date: string
  open: number
  close: number
  high: number
  low: number
  volume: number
  amount: number
  change_pct?: number | null
  is_limit_up: boolean
}

export interface KlineResponse {
  stock_code: string
  period: Exclude<KlinePeriod, 'timeline'>
  data: KlinePoint[]
}

export interface ComparePoint {
  date: string
  change_pct_from_start: number
}

export interface CompareSeries {
  symbol: string
  name: string
  data: ComparePoint[]
}
```

- [ ] **Step 2: Add API helpers**

In `frontend/src/api/market.ts`, change the type import to:

```ts
import type { OrderBook, BigOrder, FundFlow, KlineResponse, CompareSeries } from '@/types/market'
```

Append these functions:

```ts
export async function getKline(stockCode: string, params?: {
  period?: 'day' | 'week' | 'month'
  limit?: number
}): Promise<KlineResponse> {
  const { data } = await api.get(`/market/${stockCode}/kline`, { params })
  return data
}

export async function getCompareSeries(params: {
  symbols: string[]
  period?: 'day' | 'week' | 'month'
  limit?: number
}): Promise<CompareSeries[]> {
  const { data } = await api.get('/market/compare', {
    params: {
      symbols: params.symbols.join(','),
      period: params.period || 'day',
      limit: params.limit || 250
    }
  })
  return data
}
```

- [ ] **Step 3: Run frontend build**

Run:

```powershell
cd D:\code\stock-limit-up-system\frontend
npm run build
```

Expected: PASS. If it fails from existing unrelated type errors, record the exact error before editing.

- [ ] **Step 4: Commit frontend API contract**

Run:

```powershell
git add frontend/src/types/market.ts frontend/src/api/market.ts
git commit -m "feat: add frontend kline market api"
```

## Task 4: Stock Detail Data State

**Files:**
- Modify: `frontend/src/views/StockDetail.vue`

- [ ] **Step 1: Replace imports and state**

In `frontend/src/views/StockDetail.vue`, update imports in `<script setup lang="ts">` to include new APIs and types:

```ts
import { ref, computed, onMounted, onUnmounted, watch, nextTick } from 'vue'
import { useRoute } from 'vue-router'
import { Star, Plus, Minus, Refresh } from '@element-plus/icons-vue'
import { ElMessage } from 'element-plus'
import * as echarts from 'echarts'
import { getLimitUpDetail } from '@/api/limit-up'
import { getOrderBook, getBigOrders, getTimeline, getKline, getCompareSeries } from '@/api/market'
import { useConfigStore } from '@/stores/config'
import type { LimitUpDetail, LimitUpStatusChange } from '@/types/limit-up'
import type { OrderBook, BigOrder, KlinePeriod, KlinePoint, CompareSeries } from '@/types/market'
```

Replace chart-related state with:

```ts
const activePeriod = ref<KlinePeriod>('day')
const chartLoading = ref(false)
const klineData = ref<KlinePoint[]>([])
const intradayData = ref<any[]>([])
const compareSeries = ref<CompareSeries[]>([])
const overlaySymbols = ref<string[]>(['000001.SH'])
const showLimitUpHighlight = ref(true)
const showMa = ref(true)
const showOverlay = ref(true)
```

- [ ] **Step 2: Add period and overlay fetchers**

Add these functions above `fetchData()`:

```ts
function isKlinePeriod(period: KlinePeriod): period is 'day' | 'week' | 'month' {
  return period === 'day' || period === 'week' || period === 'month'
}

async function fetchChartData() {
  if (!chart) return
  chartLoading.value = true
  try {
    if (activePeriod.value === 'timeline') {
      const data = await getTimeline(stockCode.value)
      intradayData.value = data?.data || []
      klineData.value = []
      compareSeries.value = []
    } else if (isKlinePeriod(activePeriod.value)) {
      const [kline, compares] = await Promise.all([
        getKline(stockCode.value, { period: activePeriod.value, limit: 250 }),
        showOverlay.value
          ? getCompareSeries({
              symbols: overlaySymbols.value,
              period: activePeriod.value,
              limit: 250
            }).catch(() => [])
          : Promise.resolve([])
      ])
      klineData.value = kline.data || []
      compareSeries.value = compares
      intradayData.value = []
    }
    updateChart()
  } catch (e) {
    console.error('Fetch chart data error:', e)
    ElMessage.warning('图表数据暂不可用')
  } finally {
    chartLoading.value = false
  }
}

function setPeriod(period: KlinePeriod) {
  if (activePeriod.value === period) return
  activePeriod.value = period
  fetchChartData()
}

function toggleOverlay() {
  showOverlay.value = !showOverlay.value
  fetchChartData()
}

function toggleLimitUpHighlight() {
  showLimitUpHighlight.value = !showLimitUpHighlight.value
  updateChart()
}
```

- [ ] **Step 3: Update existing data fetch flow**

In `fetchData()`, replace the old `fetchTimeline()` call with:

```ts
    await fetchChartData()
```

Remove the old `fetchTimeline()` function after confirming `fetchChartData()` covers timeline and K-line periods.

- [ ] **Step 4: Run frontend build and verify expected failures**

Run:

```powershell
cd D:\code\stock-limit-up-system\frontend
npm run build
```

Expected: FAIL because `updateChart()` and the new template controls are not implemented yet. Keep the failure output for the next task.

## Task 5: ECharts Option Builder

**Files:**
- Modify: `frontend/src/views/StockDetail.vue`

- [ ] **Step 1: Add chart helper functions**

Add these functions before `initChart()`:

```ts
function formatAmount(value?: number | null): string {
  if (value == null || Number.isNaN(value)) return '-'
  if (Math.abs(value) >= 100000000) return (value / 100000000).toFixed(2) + '亿'
  if (Math.abs(value) >= 10000) return (value / 10000).toFixed(0) + '万'
  return value.toFixed(0)
}

function getLimitUpColor(point: KlinePoint): string {
  if (showLimitUpHighlight.value && point.is_limit_up) return '#8b000f'
  return point.close >= point.open ? '#d82135' : '#1677ff'
}

function buildMaData(points: KlinePoint[], windowSize: number): (number | null)[] {
  return points.map((_point, index) => {
    if (index < windowSize - 1) return null
    const slice = points.slice(index - windowSize + 1, index + 1)
    const total = slice.reduce((sum, item) => sum + item.close, 0)
    return Number((total / windowSize).toFixed(2))
  })
}

function buildKlineOption() {
  const dates = klineData.value.map(item => item.date)
  const candleData = klineData.value.map(item => ({
    value: [item.open, item.close, item.low, item.high],
    itemStyle: {
      color: getLimitUpColor(item),
      color0: '#1677ff',
      borderColor: getLimitUpColor(item),
      borderColor0: '#1677ff'
    }
  }))

  const series: any[] = [
    {
      name: stockInfo.value.stock_name || stockCode.value,
      type: 'candlestick',
      data: candleData,
      xAxisIndex: 0,
      yAxisIndex: 0
    },
    {
      name: '成交量',
      type: 'bar',
      data: klineData.value.map(item => ({
        value: item.volume,
        itemStyle: { color: getLimitUpColor(item) }
      })),
      xAxisIndex: 1,
      yAxisIndex: 2
    }
  ]

  if (showMa.value) {
    series.push({
      name: 'MA5',
      type: 'line',
      data: buildMaData(klineData.value, 5),
      smooth: true,
      symbol: 'none',
      xAxisIndex: 0,
      yAxisIndex: 0,
      lineStyle: { width: 1.5, color: '#7c3aed' }
    })
  }

  if (showOverlay.value) {
    compareSeries.value.forEach((overlay, index) => {
      series.push({
        name: overlay.name || overlay.symbol,
        type: 'line',
        data: overlay.data.map(item => item.change_pct_from_start),
        smooth: true,
        symbol: 'none',
        xAxisIndex: 0,
        yAxisIndex: 1,
        lineStyle: {
          width: 1.5,
          color: ['#2563eb', '#f59e0b', '#059669'][index % 3]
        }
      })
    })
  }

  return {
    animation: false,
    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
    legend: { top: 8, left: 12 },
    grid: [
      { left: 56, right: 58, top: 42, height: '58%' },
      { left: 56, right: 58, top: '76%', height: '14%' }
    ],
    xAxis: [
      { type: 'category', data: dates, scale: true, boundaryGap: true, axisLabel: { show: false } },
      { type: 'category', data: dates, gridIndex: 1, scale: true, boundaryGap: true }
    ],
    yAxis: [
      { scale: true, splitArea: { show: true } },
      { scale: true, position: 'right', axisLabel: { formatter: '{value}%' }, splitLine: { show: false } },
      { scale: true, gridIndex: 1, splitNumber: 2 }
    ],
    dataZoom: [
      { type: 'inside', xAxisIndex: [0, 1], start: 55, end: 100 },
      { type: 'slider', xAxisIndex: [0, 1], bottom: 8, height: 18, start: 55, end: 100 }
    ],
    series
  }
}
```

- [ ] **Step 2: Add timeline option builder**

Add this function after `buildKlineOption()`:

```ts
function buildTimelineOption() {
  const times = intradayData.value.map((item: any) => item.time)
  return {
    animation: false,
    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
    grid: [
      { left: 56, right: 24, top: 32, height: '58%' },
      { left: 56, right: 24, top: '76%', height: '14%' }
    ],
    xAxis: [
      { type: 'category', data: times, axisLabel: { show: false } },
      { type: 'category', data: times, gridIndex: 1 }
    ],
    yAxis: [
      { scale: true },
      { scale: true, gridIndex: 1, splitNumber: 2 }
    ],
    dataZoom: [
      { type: 'inside', xAxisIndex: [0, 1], start: 0, end: 100 },
      { type: 'slider', xAxisIndex: [0, 1], bottom: 8, height: 18, start: 0, end: 100 }
    ],
    series: [
      {
        name: '现价',
        type: 'line',
        data: intradayData.value.map((item: any) => item.price),
        smooth: true,
        symbol: 'none',
        xAxisIndex: 0,
        yAxisIndex: 0,
        lineStyle: { color: '#d82135' },
        areaStyle: { color: 'rgba(216, 33, 53, 0.08)' }
      },
      {
        name: '成交量',
        type: 'bar',
        data: intradayData.value.map((item: any) => item.volume),
        xAxisIndex: 1,
        yAxisIndex: 1,
        itemStyle: { color: '#64748b' }
      }
    ]
  }
}
```

- [ ] **Step 3: Replace chart init and update logic**

Replace `initChart()` with:

```ts
function initChart() {
  if (!chartRef.value) return
  chart = echarts.init(chartRef.value)
  updateChart()
}

function updateChart() {
  if (!chart) return
  const hasData = activePeriod.value === 'timeline'
    ? intradayData.value.length > 0
    : klineData.value.length > 0

  if (!hasData) {
    chart.clear()
    chart.setOption({
      title: {
        text: '暂无图表数据',
        left: 'center',
        top: 'middle',
        textStyle: { color: '#94a3b8', fontSize: 14, fontWeight: 500 }
      }
    })
    return
  }

  chart.setOption(activePeriod.value === 'timeline' ? buildTimelineOption() : buildKlineOption(), true)
}

function resizeChart() {
  chart?.resize()
}

function zoomChart(delta: number) {
  if (!chart) return
  const option: any = chart.getOption()
  const zoom = option.dataZoom?.[0]
  if (!zoom) return
  const start = Math.max(0, Math.min(95, Number(zoom.start ?? 55) + delta))
  const end = Math.max(start + 5, Math.min(100, Number(zoom.end ?? 100) - delta))
  chart.dispatchAction({ type: 'dataZoom', start, end })
}
```

- [ ] **Step 4: Add watchers and lifecycle resize**

Inside `onMounted()`, after `fetchData()`, add:

```ts
  window.addEventListener('resize', resizeChart)
```

Inside `onUnmounted()`, before disposing chart, add:

```ts
    window.removeEventListener('resize', resizeChart)
```

Add this watcher after `onMounted()`:

```ts
watch(stockCode, async () => {
  await nextTick()
  fetchData()
})
```

- [ ] **Step 5: Run frontend build**

Run:

```powershell
cd D:\code\stock-limit-up-system\frontend
npm run build
```

Expected: FAIL only if the template still references removed names. Continue to Task 6 to finish the template.

## Task 6: Stock Detail Template

**Files:**
- Modify: `frontend/src/views/StockDetail.vue`

- [ ] **Step 1: Replace the old `<template>`**

Replace the entire `<template>` in `frontend/src/views/StockDetail.vue` with:

```vue
<template>
  <div class="stock-detail" v-loading="loading">
    <section class="stock-hero">
      <div class="stock-title-block">
        <div class="stock-name-row">
          <h2>{{ stockInfo.stock_name || stockCode }}</h2>
          <span class="stock-code">{{ stockCode }}</span>
          <el-tag v-if="stockInfo.market" size="small">{{ stockInfo.market }}</el-tag>
        </div>
        <div class="status-tags">
          <el-tag v-if="stockInfo.continuous_limit_up_days && stockInfo.continuous_limit_up_days > 1" type="danger" size="small">
            {{ stockInfo.continuous_limit_up_days }}连板
          </el-tag>
          <el-tag :type="stockInfo.is_final_sealed ? 'danger' : 'warning'" size="small">
            {{ stockInfo.is_final_sealed ? '涨停封板' : '开板' }}
          </el-tag>
          <el-tag v-if="stockInfo.reason_category" type="success" size="small">{{ stockInfo.reason_category }}</el-tag>
          <el-tag v-if="stockInfo.first_limit_up_time" size="small">首封 {{ stockInfo.first_limit_up_time }}</el-tag>
          <el-tag size="small">开板 {{ stockInfo.open_count ?? 0 }} 次</el-tag>
        </div>
      </div>

      <div class="price-summary">
        <div class="price-main">{{ formatPrice(stockInfo.current_price || stockInfo.limit_up_price) }}</div>
        <div class="summary-item"><span>涨停价</span><strong>{{ formatPrice(stockInfo.limit_up_price) }}</strong></div>
        <div class="summary-item"><span>封单</span><strong>{{ formatAmount(stockInfo.seal_amount ? stockInfo.seal_amount * 10000 : null) }}</strong></div>
        <div class="summary-item"><span>换手</span><strong>{{ formatTurnoverRate(stockInfo.turnover_rate) }}</strong></div>
      </div>

      <el-button :icon="Star" @click="toggleWatch">
        {{ isWatched ? '取消关注' : '加入自选' }}
      </el-button>
    </section>

    <section class="detail-workbench">
      <div class="chart-panel">
        <div class="panel-header">
          <h3>K线与叠加走势</h3>
          <div class="chart-actions">
            <el-button-group>
              <el-button :type="activePeriod === 'timeline' ? 'primary' : 'default'" size="small" @click="setPeriod('timeline')">分时</el-button>
              <el-button :type="activePeriod === 'day' ? 'primary' : 'default'" size="small" @click="setPeriod('day')">日K</el-button>
              <el-button :type="activePeriod === 'week' ? 'primary' : 'default'" size="small" @click="setPeriod('week')">周K</el-button>
              <el-button :type="activePeriod === 'month' ? 'primary' : 'default'" size="small" @click="setPeriod('month')">月K</el-button>
            </el-button-group>
            <el-button size="small" :type="showLimitUpHighlight ? 'danger' : 'default'" @click="toggleLimitUpHighlight">涨停变色</el-button>
            <el-button size="small" :type="showOverlay ? 'primary' : 'default'" @click="toggleOverlay">叠加指数</el-button>
            <el-button size="small" :icon="Plus" @click="zoomChart(8)" />
            <el-button size="small" :icon="Minus" @click="zoomChart(-8)" />
            <el-button size="small" :icon="Refresh" @click="fetchChartData" />
          </div>
        </div>
        <div class="chart-meta">
          <span class="legend stock"></span>{{ stockInfo.stock_name || stockCode }}
          <span v-if="showOverlay" class="legend index"></span><span v-if="showOverlay">叠加走势</span>
          <span v-if="showMa" class="legend ma"></span><span v-if="showMa">MA5</span>
        </div>
        <div ref="chartRef" v-loading="chartLoading" class="chart-container"></div>
      </div>

      <aside class="side-panels">
        <div class="side-card">
          <div class="panel-header compact"><h3>盘口</h3></div>
          <div class="orderbook">
            <div v-for="i in 3" :key="'ask' + i" class="book-row">
              <span>卖{{ 4 - i }}</span>
              <strong class="down">{{ formatPrice(orderBook.ask_prices?.[3 - i]) }}</strong>
              <span>{{ orderBook.ask_volumes?.[3 - i] || '-' }}</span>
            </div>
            <div class="current-row">
              <span>当前涨停价</span>
              <strong>{{ formatPrice(orderBook.current_price || stockInfo.limit_up_price) }}</strong>
            </div>
            <div v-for="i in 3" :key="'bid' + i" class="book-row">
              <span>买{{ i }}</span>
              <strong class="up">{{ formatPrice(orderBook.bid_prices?.[i - 1]) }}</strong>
              <span>{{ orderBook.bid_volumes?.[i - 1] || '-' }}</span>
            </div>
          </div>
        </div>

        <div class="side-card">
          <div class="panel-header compact">
            <h3>大单成交</h3>
            <span class="threshold-hint">≥{{ bigOrderThreshold }}手</span>
          </div>
          <div class="bigorder-list">
            <div v-if="filteredBigOrders.length === 0" class="empty-hint">暂无大单</div>
            <div v-for="order in filteredBigOrders" :key="order.id" class="bigorder-item" :class="order.direction">
              <span>{{ formatTime(order.trade_time) }}</span>
              <strong>{{ order.direction === 'buy' ? '买' : '卖' }}</strong>
              <span>{{ formatPrice(order.trade_price) }}</span>
              <span>{{ formatAmount(order.trade_amount) }}</span>
            </div>
          </div>
        </div>
      </aside>

      <div class="timeline-panel">
        <div class="panel-header compact"><h3>涨停时间线</h3></div>
        <div class="timeline-grid">
          <div v-for="item in timelineData" :key="item.change_time" class="timeline-event" :class="item.status">
            <span>{{ formatTime(item.change_time) }}</span>
            <strong>{{ getStatusText(item.status) }}</strong>
            <small>{{ item.price ? formatPrice(item.price) : '' }} {{ item.seal_amount ? '封单 ' + formatAmount(item.seal_amount * 10000) : '' }}</small>
          </div>
          <div v-if="timelineData.length === 0" class="empty-hint">暂无封板变化记录</div>
        </div>
      </div>

      <div class="info-panel">
        <div class="panel-header compact"><h3>核心数据</h3></div>
        <div class="info-grid">
          <div class="info-item"><span>题材</span><strong>{{ stockInfo.reason_category || '-' }}</strong></div>
          <div class="info-item"><span>行业</span><strong>{{ stockInfo.industry || '-' }}</strong></div>
          <div class="info-item"><span>成交额</span><strong>{{ formatAmount(stockInfo.amount ? stockInfo.amount * 10000 : null) }}</strong></div>
          <div class="info-item"><span>涨停原因</span><strong>{{ stockInfo.limit_up_reason || '-' }}</strong></div>
        </div>
      </div>
    </section>
  </div>
</template>
```

- [ ] **Step 2: Add missing price formatter**

Add this function near `formatTurnoverRate()`:

```ts
function formatPrice(value: number | undefined | null): string {
  if (value == null || Number.isNaN(value)) return '-'
  return value.toFixed(2)
}
```

- [ ] **Step 3: Run frontend build**

Run:

```powershell
cd D:\code\stock-limit-up-system\frontend
npm run build
```

Expected: PASS or a small set of TypeScript/template errors directly tied to names in the snippets. Fix those names before continuing.

## Task 7: Stock Detail Styles and Responsive Layout

**Files:**
- Modify: `frontend/src/views/StockDetail.vue`

- [ ] **Step 1: Replace the old scoped styles**

Replace the entire `<style lang="scss" scoped>` block with:

```scss
<style lang="scss" scoped>
.stock-detail {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.stock-hero,
.chart-panel,
.side-card,
.timeline-panel,
.info-panel {
  background: #fff;
  border: 1px solid #e5eaf3;
  border-radius: 8px;
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
}

.stock-hero {
  display: grid;
  grid-template-columns: minmax(260px, 1fr) auto auto;
  gap: 16px;
  align-items: center;
  padding: 16px;
}

.stock-name-row {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;

  h2 {
    margin: 0;
    font-size: 24px;
    color: #111827;
  }

  .stock-code {
    color: #64748b;
    font-weight: 600;
  }
}

.status-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-top: 8px;
}

.price-summary {
  display: flex;
  align-items: flex-end;
  gap: 16px;

  .price-main {
    font-size: 30px;
    line-height: 1;
    font-weight: 800;
    color: #d82135;
  }

  .summary-item {
    font-size: 12px;
    color: #64748b;

    strong {
      display: block;
      margin-top: 4px;
      color: #111827;
      font-size: 14px;
    }
  }
}

.detail-workbench {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 320px;
  gap: 12px;
}

.chart-panel {
  min-width: 0;
}

.panel-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 12px 14px;
  border-bottom: 1px solid #edf1f7;

  &.compact {
    padding: 10px 12px;
  }

  h3 {
    margin: 0;
    font-size: 15px;
    color: #111827;
  }
}

.chart-actions {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 6px;
  flex-wrap: wrap;
}

.chart-meta {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 14px 0;
  color: #64748b;
  font-size: 12px;

  .legend {
    width: 18px;
    height: 3px;
    border-radius: 999px;
    display: inline-block;
  }

  .stock { background: #d82135; }
  .index { background: #2563eb; }
  .ma { background: #7c3aed; }
}

.chart-container {
  height: 460px;
}

.side-panels {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.orderbook {
  padding: 10px 12px 12px;
}

.book-row {
  display: grid;
  grid-template-columns: 44px 1fr 1fr;
  gap: 10px;
  padding: 6px 0;
  font-size: 13px;
  color: #64748b;

  strong {
    text-align: right;
  }

  .up { color: #d82135; }
  .down { color: #1677ff; }
}

.current-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin: 8px 0;
  padding: 10px;
  border-radius: 6px;
  background: #fff1f0;
  color: #d82135;
  font-weight: 700;
}

.threshold-hint {
  color: #94a3b8;
  font-size: 12px;
}

.bigorder-list {
  max-height: 260px;
  overflow-y: auto;
  padding: 0 12px 10px;
}

.bigorder-item {
  display: grid;
  grid-template-columns: 58px 34px 1fr 64px;
  gap: 8px;
  padding: 8px 0;
  border-bottom: 1px solid #f1f5f9;
  color: #64748b;
  font-size: 13px;

  &.buy strong {
    color: #d82135;
  }

  &.sell strong {
    color: #1677ff;
  }
}

.timeline-panel {
  grid-column: 1 / 2;
}

.timeline-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
  padding: 12px;
}

.timeline-event {
  border: 1px solid #e5eaf3;
  border-radius: 8px;
  padding: 10px;
  min-height: 76px;

  span,
  small {
    color: #64748b;
    font-size: 12px;
  }

  strong {
    display: block;
    margin: 6px 0;
    color: #111827;
  }

  &.sealed,
  &.resealed {
    border-color: #ffc9cf;
    background: #fff7f7;

    strong {
      color: #d82135;
    }
  }

  &.opened {
    border-color: #ffe4ba;
    background: #fffaf0;
  }
}

.info-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
  padding: 12px;
}

.info-item {
  border: 1px solid #e5eaf3;
  border-radius: 8px;
  padding: 10px;
  background: #fbfdff;

  span {
    color: #64748b;
    font-size: 12px;
  }

  strong {
    display: block;
    margin-top: 6px;
    color: #111827;
    font-size: 14px;
  }
}

.empty-hint {
  padding: 18px;
  color: #94a3b8;
  text-align: center;
  font-size: 13px;
}

@media (max-width: 1180px) {
  .stock-hero {
    grid-template-columns: 1fr;
  }

  .price-summary {
    flex-wrap: wrap;
  }

  .detail-workbench {
    grid-template-columns: 1fr;
  }

  .timeline-panel {
    grid-column: auto;
  }

  .timeline-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 720px) {
  .chart-container {
    height: 360px;
  }

  .timeline-grid,
  .info-grid {
    grid-template-columns: 1fr;
  }

  .panel-header {
    align-items: flex-start;
    flex-direction: column;
  }
}
</style>
```

- [ ] **Step 2: Run frontend build**

Run:

```powershell
cd D:\code\stock-limit-up-system\frontend
npm run build
```

Expected: PASS.

- [ ] **Step 3: Commit stock detail redesign**

Run:

```powershell
git add frontend/src/views/StockDetail.vue
git commit -m "feat: redesign stock detail workbench"
```

## Task 8: End-to-End Verification

**Files:**
- Read only unless verification exposes a concrete bug.

- [ ] **Step 1: Run backend tests**

Run:

```powershell
cd D:\code\stock-limit-up-system\backend
python -m unittest discover tests -v
```

Expected: PASS.

- [ ] **Step 2: Run frontend build**

Run:

```powershell
cd D:\code\stock-limit-up-system\frontend
npm run build
```

Expected: PASS.

- [ ] **Step 3: Start backend**

Run:

```powershell
cd D:\code\stock-limit-up-system\backend
python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

Expected: backend starts and logs Uvicorn running on `http://127.0.0.1:8000`.

- [ ] **Step 4: Start frontend**

Run in a second terminal:

```powershell
cd D:\code\stock-limit-up-system\frontend
npm run dev -- --host 127.0.0.1 --port 3000
```

Expected: Vite serves the app on `http://127.0.0.1:3000`.

- [ ] **Step 5: Manual browser checks**

Open `http://127.0.0.1:3000/stock/603893` or another stock present in the local database.

Verify:

- Header shows stock name, code, current price, limit-up price, seal amount, turnover, and status tags.
- `分时`, `日K`, `周K`, and `月K` buttons switch the chart without crashing.
- K-line view shows candlesticks, volume bars, zoom slider, MA5 line, and an overlay line when compare data exists.
- `涨停变色` toggles deeper red limit-up K bars.
- `叠加指数` toggles compare-series fetch and display.
- `+` and `-` buttons adjust the dataZoom window.
- Right side order book and big order panels still refresh every five seconds.
- Timeline and core data remain visible below the chart.

- [ ] **Step 6: Commit verification fixes only when needed**

If verification required a code fix, run:

```powershell
git add backend/app/api/v1/market.py backend/tests/test_market_kline_api.py frontend/src/types/market.ts frontend/src/api/market.ts frontend/src/views/StockDetail.vue
git commit -m "fix: stabilize stock detail verification"
```

If no fix was required, do not create an empty commit.
