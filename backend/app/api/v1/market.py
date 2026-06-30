"""
行情数据API
"""
import json
import re
from decimal import Decimal, ROUND_HALF_UP

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from typing import Optional, List, Literal
from datetime import date, datetime

import httpx
from loguru import logger

from app.database import get_db
from app.models.stock import Stock
from app.models.big_order import BigOrder
from app.models.order_flow import OrderBookSnapshot
from pydantic import BaseModel, Field

router = APIRouter()


class OrderBookResponse(BaseModel):
    """盘口数据响应"""
    stock_code: str
    snapshot_time: datetime
    current_price: Optional[float]
    pre_close: Optional[float]
    bid_prices: List[float] = Field(default_factory=list)
    bid_volumes: List[int] = Field(default_factory=list)
    ask_prices: List[float] = Field(default_factory=list)
    ask_volumes: List[int] = Field(default_factory=list)
    volume: Optional[int]
    amount: Optional[float]


class BigOrderResponse(BaseModel):
    """大单记录响应"""
    id: int
    stock_code: str
    trade_time: datetime
    trade_price: float
    trade_volume: int
    trade_amount: float
    direction: str
    order_type: str
    is_limit_up_price: bool


class FundFlowResponse(BaseModel):
    """资金流向响应"""
    stock_code: str
    trade_date: date
    main_in: float = Field(0, description="主力流入")
    main_out: float = Field(0, description="主力流出")
    main_net: float = Field(0, description="主力净流入")
    retail_in: float = Field(0, description="散户流入")
    retail_out: float = Field(0, description="散户流出")
    retail_net: float = Field(0, description="散户净流入")


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


class ComparePointResponse(BaseModel):
    """叠加走势点位"""
    date: date
    open: float
    close: float
    low: float
    high: float
    volume: int = 0
    change_pct_from_start: float
    open_pct_from_start: float
    close_pct_from_start: float
    low_pct_from_start: float
    high_pct_from_start: float
    is_limit_up: bool = False


class CompareSeriesResponse(BaseModel):
    """叠加走势序列"""
    symbol: str
    name: str
    data: List[ComparePointResponse] = Field(default_factory=list)


class StockSearchItemResponse(BaseModel):
    """标的搜索结果"""
    stock_code: str
    stock_name: str
    market: str
    symbol: str
    pinyin: str = ""
    classify: str = ""
    security_type: str = ""


PERIOD_TO_KLT = {
    "day": "101",
    "week": "102",
    "month": "103",
}
MAX_COMPARE_SYMBOLS = 5
PRICE_QUANT = Decimal("0.01")
EASTMONEY_KLINE_URL = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
EASTMONEY_DETAILS_URL = "http://push2.eastmoney.com/api/qt/stock/details/get"
EASTMONEY_SEARCH_URL = "https://searchapi.eastmoney.com/api/suggest/get"
EASTMONEY_SEARCH_TOKEN = "D43BF722C8E33E6D9237BAE1524BBE7E"
SINA_KLINE_URL = "https://quotes.sina.cn/cn/api/jsonp.php/var%20_=/CN_MarketDataService.getKLineData"


def _is_st_stock(stock_name: Optional[str], is_st: Optional[int]) -> bool:
    if isinstance(is_st, str):
        if is_st.strip().lower() in ("1", "true", "yes"):
            return True
    elif is_st:
        return True
    if not stock_name:
        return False

    normalized_name = stock_name.strip().upper()
    return normalized_name.startswith("ST") or normalized_name.startswith("*ST")


def _limit_up_threshold(
    stock_code: str,
    market: Optional[str] = None,
    stock_name: Optional[str] = None,
    is_st: Optional[int] = None,
) -> float:
    if (market or "").upper() in ("BJ", "BSE") or stock_code.startswith(("4", "8", "920")):
        return 29.9
    if stock_code.startswith("3") or stock_code.startswith("68"):
        return 19.9
    if _is_st_stock(stock_name, is_st):
        return 4.9
    return 9.9


def _limit_up_ratio(
    stock_code: str,
    market: Optional[str] = None,
    stock_name: Optional[str] = None,
    is_st: Optional[int] = None,
) -> Decimal:
    if (market or "").upper() in ("BJ", "BSE") or stock_code.startswith(("4", "8", "920")):
        return Decimal("0.30")
    if stock_code.startswith("3") or stock_code.startswith("68"):
        return Decimal("0.20")
    if _is_st_stock(stock_name, is_st):
        return Decimal("0.05")
    return Decimal("0.10")


def _price_decimal(value: float) -> Decimal:
    return Decimal(str(value)).quantize(PRICE_QUANT, rounding=ROUND_HALF_UP)


def _calculate_limit_up_price(
    pre_close: float,
    stock_code: str,
    market: Optional[str] = None,
    stock_name: Optional[str] = None,
    is_st: Optional[int] = None,
) -> Decimal:
    ratio = _limit_up_ratio(stock_code, market=market, stock_name=stock_name, is_st=is_st)
    return (Decimal(str(pre_close)) * (Decimal("1") + ratio)).quantize(PRICE_QUANT, rounding=ROUND_HALF_UP)


def _is_close_at_limit_up_price(
    close: float,
    pre_close: Optional[float],
    stock_code: str,
    market: Optional[str] = None,
    stock_name: Optional[str] = None,
    is_st: Optional[int] = None,
) -> bool:
    if pre_close is None or pre_close <= 0:
        return False
    return _price_decimal(close) == _calculate_limit_up_price(
        pre_close,
        stock_code,
        market=market,
        stock_name=stock_name,
        is_st=is_st,
    )


def _eastmoney_market_prefix(market: str) -> str:
    return "1" if market.upper() == "SH" else "0"


def _normalize_symbol(symbol: str) -> tuple[str, str, str]:
    raw = symbol.strip().upper()
    if "." in raw:
        code, market = raw.split(".", 1)
    else:
        code = raw
        if code.startswith(("4", "8", "920")):
            market = "BJ"
        else:
            market = "SH" if code.startswith("6") else "SZ"

    prefix = _eastmoney_market_prefix(market)
    return code, market, f"{prefix}.{code}"


def _normalize_search_query(query: str) -> tuple[str, Optional[str]]:
    normalized = query.strip().upper().replace(" ", "")
    if "." not in normalized:
        return normalized, None

    code, market = normalized.split(".", 1)
    if market == "BSE":
        market = "BJ"
    return code, market


def _market_from_search_item(item: dict) -> str:
    quote_id = str(item.get("QuoteID") or "")
    code = str(item.get("Code") or item.get("UnifiedCode") or "")
    security_type = str(item.get("SecurityTypeName") or "")

    if "." in quote_id:
        prefix, quote_code = quote_id.split(".", 1)
        code = quote_code or code
        if prefix == "1":
            return "SH"

    if code.startswith(("4", "8", "920")) or "北" in security_type or "京" in security_type:
        return "BJ"
    return "SZ"


def _format_search_item(item: dict) -> Optional[dict]:
    classify = str(item.get("Classify") or "")
    if classify not in {"AStock", "Index"}:
        return None

    code = str(item.get("Code") or item.get("UnifiedCode") or "").strip().upper()
    name = str(item.get("Name") or "").strip()
    if not code or not name:
        return None

    market = _market_from_search_item(item)
    return {
        "stock_code": code,
        "stock_name": name,
        "market": market,
        "symbol": f"{code}.{market}",
        "pinyin": str(item.get("PinYin") or "").strip().upper(),
        "classify": classify,
        "security_type": str(item.get("SecurityTypeName") or "").strip(),
    }


def _parse_search_results(
    payload: dict,
    requested_market: Optional[str] = None,
    requested_code: Optional[str] = None,
    limit: int = 10,
) -> List[dict]:
    rows = (payload.get("QuotationCodeTable") or {}).get("Data") or []
    results = []
    seen = set()
    for item in rows:
        formatted = _format_search_item(item)
        if not formatted:
            continue
        if requested_code and formatted["stock_code"] != requested_code:
            continue
        if requested_market and formatted["market"] != requested_market:
            continue
        if formatted["symbol"] in seen:
            continue
        seen.add(formatted["symbol"])
        results.append(formatted)
        if len(results) >= limit:
            break
    return results


def _format_kline_item(
    raw: str,
    stock_code: str,
    market: Optional[str] = None,
    stock_name: Optional[str] = None,
    is_st: Optional[int] = None,
) -> dict:
    parts = raw.split(",")
    if len(parts) < 11:
        raise ValueError(f"K线数据字段不足: {raw}")

    close = float(parts[2])
    change_pct = float(parts[8]) if parts[8] not in ("", "-") else None
    change_amount = float(parts[9]) if parts[9] not in ("", "-") else None
    pre_close = close - change_amount if change_amount is not None else None
    return {
        "date": date.fromisoformat(parts[0]),
        "open": float(parts[1]),
        "close": close,
        "high": float(parts[3]),
        "low": float(parts[4]),
        "volume": int(float(parts[5] or 0)),
        "amount": float(parts[6] or 0),
        "change_pct": change_pct,
        "is_limit_up": _is_close_at_limit_up_price(
            close,
            pre_close,
            stock_code,
            market=market,
            stock_name=stock_name,
            is_st=is_st,
        ),
    }


def _apply_change_pct(
    points: List[dict],
    stock_code: str,
    market: Optional[str] = None,
    stock_name: Optional[str] = None,
    is_st: Optional[int] = None,
) -> List[dict]:
    previous_close: Optional[float] = None
    normalized_points = []
    for point in sorted(points, key=lambda item: item["date"]):
        close = float(point["close"])
        change_pct = None
        if previous_close:
            change_pct = round((close - previous_close) / previous_close * 100, 2)

        normalized = {
            **point,
            "change_pct": change_pct,
            "is_limit_up": _is_close_at_limit_up_price(
                close,
                previous_close,
                stock_code,
                market=market,
                stock_name=stock_name,
                is_st=is_st,
            ),
        }
        normalized_points.append(normalized)
        previous_close = close

    return normalized_points


def _aggregate_kline_points(points: List[dict], period: str) -> List[dict]:
    if period == "day":
        return points

    aggregated = []
    current_key = None
    current_point = None

    for point in sorted(points, key=lambda item: item["date"]):
        point_date = point["date"]
        if period == "week":
            iso = point_date.isocalendar()
            key = (iso.year, iso.week)
        else:
            key = (point_date.year, point_date.month)

        if key != current_key:
            if current_point:
                aggregated.append(current_point)
            current_key = key
            current_point = {
                **point,
                "date": point_date,
            }
            continue

        current_point["date"] = point_date
        current_point["close"] = point["close"]
        current_point["high"] = max(current_point["high"], point["high"])
        current_point["low"] = min(current_point["low"], point["low"])
        current_point["volume"] += point["volume"]
        current_point["amount"] += point["amount"]

    if current_point:
        aggregated.append(current_point)

    return aggregated


def _parse_sina_kline_payload(payload: str) -> list:
    match = re.search(r"var\s+_\s*=\s*\(?(\[.*\])\)?\s*;?", payload, re.S)
    if not match:
        return []
    return json.loads(match.group(1))


def _sina_symbol(stock_code: str, market: str) -> str:
    normalized_market = market.upper()
    if normalized_market == "SH":
        return f"sh{stock_code}"
    if normalized_market in ("BJ", "BSE"):
        return f"bj{stock_code}"
    return f"sz{stock_code}"


async def _fetch_kline_from_sina(
    stock_code: str,
    market: str,
    period: str,
    limit: int,
    *,
    stock_name: Optional[str] = None,
    is_st: Optional[int] = None,
) -> List[dict]:
    """从新浪日线兜底，并在本地聚合周/月K线"""
    if period not in PERIOD_TO_KLT:
        raise HTTPException(status_code=400, detail="period 仅支持 day/week/month")

    multiplier = {"day": 1, "week": 7, "month": 31}[period]
    datalen = min(max(limit * multiplier + 30, limit + 1), 1200)
    params = {
        "symbol": _sina_symbol(stock_code, market),
        "scale": "240",
        "ma": "no",
        "datalen": str(datalen),
    }

    async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
        resp = await client.get(SINA_KLINE_URL, headers={"User-Agent": "Mozilla/5.0"}, params=params)
        resp.raise_for_status()
        raw_items = _parse_sina_kline_payload(resp.text)

    daily_points = []
    for item in raw_items:
        try:
            daily_points.append(
                {
                    "date": date.fromisoformat(item["day"]),
                    "open": float(item["open"]),
                    "close": float(item["close"]),
                    "high": float(item["high"]),
                    "low": float(item["low"]),
                    "volume": int(float(item.get("volume") or 0)),
                    "amount": 0.0,
                }
            )
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning(f"跳过{stock_code}异常新浪K线: {exc}")

    aggregated = _aggregate_kline_points(daily_points, period)
    with_change_pct = _apply_change_pct(
        aggregated,
        stock_code,
        market=market,
        stock_name=stock_name,
        is_st=is_st,
    )
    return with_change_pct[-limit:]


async def _fetch_kline_from_em(
    stock_code: str,
    market: str,
    period: str,
    limit: int,
    *,
    stock_name: Optional[str] = None,
    is_st: Optional[int] = None,
) -> List[dict]:
    """从东方财富获取日/周/月K线"""
    if period not in PERIOD_TO_KLT:
        raise HTTPException(status_code=400, detail="period 仅支持 day/week/month")

    prefix = _eastmoney_market_prefix(market)
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
            resp = await client.get(EASTMONEY_KLINE_URL, headers={"User-Agent": "Mozilla/5.0"}, params=params)
            resp.raise_for_status()
            result = resp.json()

        data = result.get("data")
        if not isinstance(data, dict):
            return []

        klines = data.get("klines") or []
        formatted = []
        for item in klines:
            try:
                formatted.append(
                    _format_kline_item(
                        item,
                        stock_code,
                        market=market,
                        stock_name=stock_name,
                        is_st=is_st,
                    )
                )
            except (ValueError, TypeError) as exc:
                logger.warning(f"跳过{stock_code}异常K线: {exc}")
        return formatted
    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"从东方财富获取{stock_code} {period} K线失败，尝试新浪备用源: {e}")
        try:
            return await _fetch_kline_from_sina(
                stock_code,
                market,
                period,
                limit,
                stock_name=stock_name,
                is_st=is_st,
            )
        except Exception as fallback_exc:
            logger.warning(f"从新浪获取{stock_code} {period} K线失败: {fallback_exc}")
            raise HTTPException(status_code=502, detail="上游K线服务不可用") from fallback_exc


def _build_compare_series(symbol: str, name: str, points: List[dict]) -> dict:
    valid_points = [point for point in points if point.get("close") not in (None, 0)]
    if not valid_points:
        return {"symbol": symbol, "name": name, "data": []}

    base_close = float(valid_points[0]["close"])

    def pct(point: dict, field: str) -> float:
        value = point.get(field, point["close"])
        return round((float(value) - base_close) / base_close * 100, 2)

    return {
        "symbol": symbol,
        "name": name,
        "data": [
            {
                "date": point["date"],
                "open": float(point.get("open", point["close"])),
                "close": float(point["close"]),
                "low": float(point.get("low", point["close"])),
                "high": float(point.get("high", point["close"])),
                "volume": int(float(point.get("volume", 0) or 0)),
                "change_pct_from_start": pct(point, "close"),
                "open_pct_from_start": pct(point, "open"),
                "close_pct_from_start": pct(point, "close"),
                "low_pct_from_start": pct(point, "low"),
                "high_pct_from_start": pct(point, "high"),
                "is_limit_up": bool(point.get("is_limit_up")),
            }
            for point in valid_points
        ],
    }


@router.get("/search", response_model=List[StockSearchItemResponse], summary="搜索可叠加标的")
async def search_symbols(
    q: str = Query(..., min_length=1, description="代码、拼音缩写或中文关键字"),
    limit: int = Query(10, ge=1, le=20),
):
    """从东方财富搜索标的，支持代码、拼音缩写和中文关键字。"""
    query, requested_market = _normalize_search_query(q)
    if not query:
        return []

    try:
        async with httpx.AsyncClient(timeout=8) as client:
            resp = await client.get(
                EASTMONEY_SEARCH_URL,
                headers={"User-Agent": "Mozilla/5.0"},
                params={
                    "input": query,
                    "type": "14",
                    "count": max(limit * 2, 10),
                    "token": EASTMONEY_SEARCH_TOKEN,
                },
            )
            payload = resp.json()
    except Exception as exc:
        logger.warning(f"从东方财富搜索标的失败 {q}: {exc}")
        payload = {}

    requested_code = query if re.fullmatch(r"\d{6}", query) else None
    results = _parse_search_results(payload, requested_market, requested_code, limit)
    if not results and requested_market and re.fullmatch(r"\d{6}", query):
        results = [
            {
                "stock_code": query,
                "stock_name": f"{query}.{requested_market}",
                "market": requested_market,
                "symbol": f"{query}.{requested_market}",
                "pinyin": "",
                "classify": "Direct",
                "security_type": "",
            }
        ]

    return [StockSearchItemResponse(**item) for item in results[:limit]]


@router.get("/{stock_code}/orderbook", response_model=OrderBookResponse, summary="获取五档盘口")
async def get_order_book(
    stock_code: str,
    db: AsyncSession = Depends(get_db)
):
    """获取实时五档盘口数据，优先本地数据库，无数据时从东方财富获取"""
    # 查询股票
    stock_query = select(Stock).where(Stock.stock_code == stock_code)
    stock_result = await db.execute(stock_query)
    stock = stock_result.scalar_one_or_none()
    
    if not stock:
        raise HTTPException(status_code=404, detail="股票不存在")
    
    # 查询最新盘口快照
    snapshot_query = (
        select(OrderBookSnapshot)
        .where(OrderBookSnapshot.stock_id == stock.id)
        .order_by(OrderBookSnapshot.snapshot_time.desc())
        .limit(1)
    )
    snapshot_result = await db.execute(snapshot_query)
    snapshot = snapshot_result.scalar_one_or_none()
    
    if snapshot:
        return OrderBookResponse(
            stock_code=stock_code,
            snapshot_time=snapshot.snapshot_time,
            current_price=snapshot.current_price,
            pre_close=snapshot.pre_close,
            bid_prices=snapshot.bid_prices or [],
            bid_volumes=snapshot.bid_volumes or [],
            ask_prices=snapshot.ask_prices or [],
            ask_volumes=snapshot.ask_volumes or [],
            volume=snapshot.volume,
            amount=snapshot.amount
        )
    
    # 本地无数据，从东方财富获取实时盘口
    return await _fetch_orderbook_from_em(stock_code, stock.market)


async def _fetch_orderbook_from_em(stock_code: str, market: str):
    """从东方财富获取实时五档盘口"""
    prefix = "0" if market == "SZ" else "1"
    url = "https://push2.eastmoney.com/api/qt/stock/get"
    params = {
        "secid": f"{prefix}.{stock_code}",
        "fields": "f43,f44,f45,f46,f47,f48,f60,f11,f12,f13,f14,f15,f16,f17,f18,f19,f20,f31,f32,f33,f34,f35,f36,f37,f38,f39,f40",
        "ut": "fa5fd1943c7b386f172d6893dbbd1",
        "fltt": "2",
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"}, params=params)
            data = resp.json()
        
        if not data.get("data"):
            raise HTTPException(status_code=404, detail="暂无盘口数据")
        
        d = data["data"]
        bid_prices = [d.get(f"f{i}", 0) or 0 for i in [11, 13, 15, 17, 19]]
        bid_volumes = [d.get(f"f{i}", 0) or 0 for i in [12, 14, 16, 18, 20]]
        ask_prices = [d.get(f"f{i}", 0) or 0 for i in [31, 33, 35, 37, 39]]
        ask_volumes = [d.get(f"f{i}", 0) or 0 for i in [32, 34, 36, 38, 40]]
        
        return OrderBookResponse(
            stock_code=stock_code,
            snapshot_time=datetime.now(),
            current_price=d.get("f43"),
            pre_close=d.get("f60"),
            bid_prices=bid_prices,
            bid_volumes=bid_volumes,
            ask_prices=ask_prices,
            ask_volumes=ask_volumes,
            volume=d.get("f47"),
            amount=d.get("f48"),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"从东方财富获取{stock_code}盘口失败: {e}")
        raise HTTPException(status_code=404, detail="暂无盘口数据")


@router.get("/{stock_code}/big-orders", response_model=List[BigOrderResponse], summary="获取大单记录")
async def get_big_orders(
    stock_code: str,
    start_time: Optional[datetime] = Query(None),
    end_time: Optional[datetime] = Query(None),
    min_amount: Optional[float] = Query(None, description="最小金额(元)"),
    direction: Optional[str] = Query(None, description="方向(buy/sell)"),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=500),
    db: AsyncSession = Depends(get_db)
):
    """获取大单记录，优先本地数据库，无数据时从东方财富逐笔数据筛选"""
    # 查询股票
    stock_query = select(Stock).where(Stock.stock_code == stock_code)
    stock_result = await db.execute(stock_query)
    stock = stock_result.scalar_one_or_none()
    
    if not stock:
        raise HTTPException(status_code=404, detail="股票不存在")
    
    # 构建查询
    query = select(BigOrder).where(BigOrder.stock_id == stock.id)
    
    if start_time:
        query = query.where(BigOrder.trade_time >= start_time)
    if end_time:
        query = query.where(BigOrder.trade_time <= end_time)
    if min_amount:
        query = query.where(BigOrder.trade_amount >= min_amount)
    if direction:
        query = query.where(BigOrder.direction == direction)
    
    query = query.order_by(BigOrder.trade_time.desc())
    query = query.offset((page - 1) * page_size).limit(page_size)
    
    result = await db.execute(query)
    orders = result.scalars().all()
    
    if orders:
        return [
            BigOrderResponse(
                id=order.id,
                stock_code=stock_code,
                trade_time=order.trade_time,
                trade_price=order.trade_price,
                trade_volume=order.trade_volume,
                trade_amount=order.trade_amount,
                direction=order.direction,
                order_type=order.order_type,
                is_limit_up_price=bool(order.is_limit_up_price)
            )
            for order in orders
        ]
    
    # 本地无数据，从东方财富逐笔数据中筛选大单
    return await _fetch_big_orders_from_em(stock_code, stock.market, min_amount, page_size)


async def _fetch_big_orders_from_em(stock_code: str, market: str, min_amount: Optional[float], limit: int):
    """从东方财富逐笔成交中筛选大单（volume >= 500手 或 金额 >= 50万）"""
    prefix = "0" if market == "SZ" else "1"

    # 获取最近的逐笔数据
    all_big_orders = []
    for pos_start in range(0, -2000, -500):
        params = {
            "secid": f"{prefix}.{stock_code}",
            "fields1": "f1,f2,f3,f4",
            "fields2": "f51,f52,f53,f54,f55",
            "pos": str(pos_start),
            "ut": "fa5fd1943c7b386f172d6893dbbd1",
            "fltt": "2",
        }
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(EASTMONEY_DETAILS_URL, headers={"User-Agent": "Mozilla/5.0"}, params=params)
                data = resp.json()
            
            if not data.get("data") or not data["data"].get("details"):
                break
            
            details = data["data"]["details"]
            today_str = date.today().isoformat()
            
            vol_threshold = 500  # 500手以上为大单
            amt_threshold = min_amount or 500000  # 默认50万
            
            for i, d in enumerate(details):
                parts = d.split(",")
                if len(parts) < 5:
                    continue
                # 格式: 时间,价格,成交量(手),成交笔数,方向(1=买/2=卖/4=中性)
                time_str = parts[0]
                price = float(parts[1])
                volume = int(parts[2])  # 手
                direction_code = int(parts[4])
                amount = price * volume * 100  # 手 -> 股 -> 金额
                
                if volume >= vol_threshold or amount >= amt_threshold:
                    dir_str = "buy" if direction_code == 1 else "sell"
                    order_type = "active_buy" if direction_code == 1 else "active_sell"
                    
                    all_big_orders.append(BigOrderResponse(
                        id=abs(pos_start) + i + 1,
                        stock_code=stock_code,
                        trade_time=datetime.fromisoformat(f"{today_str}T{time_str}"),
                        trade_price=price,
                        trade_volume=volume,
                        trade_amount=amount,
                        direction=dir_str,
                        order_type=order_type,
                        is_limit_up_price=False,
                    ))
            
            if len(all_big_orders) >= limit:
                break
                
        except Exception as e:
            logger.warning(f"从东方财富获取{stock_code}逐笔数据失败: {e}")
            break
    
    # 按时间倒序排列
    all_big_orders.sort(key=lambda x: x.trade_time, reverse=True)
    return all_big_orders[:limit]


@router.get("/{stock_code}/fund-flow", response_model=FundFlowResponse, summary="获取资金流向")
async def get_fund_flow(
    stock_code: str,
    trade_date: Optional[date] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    """获取资金流向数据"""
    if trade_date is None:
        trade_date = date.today()
    
    # 查询股票
    stock_query = select(Stock).where(Stock.stock_code == stock_code)
    stock_result = await db.execute(stock_query)
    stock = stock_result.scalar_one_or_none()
    
    if not stock:
        raise HTTPException(status_code=404, detail="股票不存在")
    
    # 统计大单买入卖出（简化版，实际需要更复杂的计算）
    # 这里只统计当日大单，实际应该区分主力和散户
    
    # 查询当日大单
    orders_query = (
        select(BigOrder)
        .where(and_(
            BigOrder.stock_id == stock.id,
            BigOrder.trade_time >= datetime.combine(trade_date, datetime.min.time()),
            BigOrder.trade_time <= datetime.combine(trade_date, datetime.max.time())
        ))
    )
    result = await db.execute(orders_query)
    orders = result.scalars().all()
    
    # 统计资金流向
    main_in = sum(o.trade_amount for o in orders if o.direction == "buy")
    main_out = sum(o.trade_amount for o in orders if o.direction == "sell")
    
    return FundFlowResponse(
        stock_code=stock_code,
        trade_date=trade_date,
        main_in=main_in,
        main_out=main_out,
        main_net=main_in - main_out,
        retail_in=0,
        retail_out=0,
        retail_net=0
    )


@router.get("/{stock_code}/timeline", summary="获取分时数据")
async def get_timeline_data(
    stock_code: str,
    trade_date: Optional[date] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    """获取分时数据，优先从本地数据库，无数据时从东方财富API实时获取"""
    if trade_date is None:
        trade_date = date.today()
    
    # 查询股票
    stock_query = select(Stock).where(Stock.stock_code == stock_code)
    stock_result = await db.execute(stock_query)
    stock = stock_result.scalar_one_or_none()
    
    if not stock:
        code, market, _secid = _normalize_symbol(stock_code)
        return await _fetch_timeline_from_em(code, market, trade_date)
    
    # 先查本地数据库（L2盘口快照）
    snapshots_query = (
        select(OrderBookSnapshot)
        .where(and_(
            OrderBookSnapshot.stock_id == stock.id,
            OrderBookSnapshot.snapshot_time >= datetime.combine(trade_date, datetime.min.time()),
            OrderBookSnapshot.snapshot_time <= datetime.combine(trade_date, datetime.max.time())
        ))
        .order_by(OrderBookSnapshot.snapshot_time)
    )
    result = await db.execute(snapshots_query)
    snapshots = result.scalars().all()
    
    if snapshots:
        return {
            "stock_code": stock_code,
            "trade_date": trade_date.isoformat(),
            "data": [
                {
                    "time": s.snapshot_time.strftime("%H:%M:%S"),
                    "price": s.current_price,
                    "volume": s.volume,
                    "amount": s.amount
                }
                for s in snapshots
            ]
        }
    
    # 本地无数据，从东方财富API获取分时数据
    return await _fetch_timeline_from_em(stock_code, stock.market, trade_date)


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

    if stock:
        code = stock_code
        market = stock.market
        stock_name = getattr(stock, "stock_name", None)
        is_st = getattr(stock, "is_st", None)
    else:
        code, market, _secid = _normalize_symbol(stock_code)
        stock_name = None
        is_st = None

    points = await _fetch_kline_from_em(
        code,
        market,
        period,
        limit,
        stock_name=stock_name,
        is_st=is_st,
    )
    return KlineResponse(stock_code=code, period=period, data=points)


@router.get("/compare", response_model=List[CompareSeriesResponse], summary="获取叠加走势")
async def get_compare_data(
    symbols: str = Query(..., description="逗号分隔代码，如 603893,000001.SH"),
    period: Literal["day", "week", "month"] = Query("day", description="周期 day/week/month"),
    limit: int = Query(250, ge=1, le=1000),
):
    """获取多标的归一化叠加走势"""
    parsed_symbols = [item.strip() for item in symbols.split(",") if item.strip()]
    if not parsed_symbols:
        raise HTTPException(status_code=400, detail="symbols 不能为空")
    if len(parsed_symbols) > MAX_COMPARE_SYMBOLS:
        raise HTTPException(status_code=400, detail="最多支持5个叠加标的")

    result = []
    for symbol in parsed_symbols:
        code, market, _secid = _normalize_symbol(symbol)
        points = await _fetch_kline_from_em(code, market, period, limit)
        result.append(CompareSeriesResponse(**_build_compare_series(symbol, symbol, points)))
    return result


async def _fetch_timeline_from_em(stock_code: str, market: str, trade_date: date):
    """从东方财富获取分时数据"""
    prefix = _eastmoney_market_prefix(market)
    url = "https://push2.eastmoney.com/api/qt/stock/trends2/get"
    params = {
        "secid": f"{prefix}.{stock_code}",
        "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "ut": "fa5fd1943c7b386f172d6893dbbd1",
        "iscr": "0",
        "ndays": "1",
    }
    
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"}, params=params)
            result = resp.json()
        
        if not result.get("data") or not result["data"].get("trends"):
            return {"stock_code": stock_code, "trade_date": trade_date.isoformat(), "data": []}
        
        trends = result["data"]["trends"]
        data = []
        for t in trends:
            parts = t.split(",")
            if len(parts) >= 8:
                # 格式: 时间,开,收,高,低,成交量(手),成交额,均价
                time_str = parts[0]
                if " " in time_str:
                    time_str = time_str.split(" ")[1]  # 只取时间部分
                data.append({
                    "time": time_str,
                    "price": float(parts[2]),        # 收盘价/现价
                    "volume": int(parts[5]),          # 成交量(手)
                    "amount": float(parts[6]),        # 成交额
                    "avg_price": float(parts[7]),     # 均价
                })
        
        return {
            "stock_code": stock_code,
            "trade_date": trade_date.isoformat(),
            "pre_close": result["data"].get("preClose"),
            "data": data
        }
    except Exception as e:
        logger.warning(f"从东方财富获取{stock_code}分时数据失败: {e}")
        return {"stock_code": stock_code, "trade_date": trade_date.isoformat(), "data": []}


@router.get("/tdx/status", summary="获取通达信连接状态")
async def get_tdx_status():
    """获取通达信L2数据连接状态"""
    from app.data_collectors.tdx_collector import tdx_collector
    
    status = tdx_collector.get_connection_status()
    return {
        "code": 0,
        "message": "success",
        "data": status
    }


@router.post("/tdx/reconnect", summary="重连通达信服务器")
async def reconnect_tdx():
    """手动触发重连通达信服务器"""
    from app.data_collectors.tdx_collector import tdx_collector
    
    # 先断开
    await tdx_collector.disconnect()
    
    # 重新连接
    success = await tdx_collector.connect()
    
    return {
        "code": 0 if success else -1,
        "message": "连接成功" if success else "连接失败",
        "data": tdx_collector.get_connection_status()
    }


@router.get("/tdx/l2/status", summary="获取L2本地采集状态")
async def get_l2_status():
    """获取本地通达信L2数据采集状态"""
    from app.data_collectors.tdx_l2_local import tdx_l2_local
    
    return {
        "code": 0,
        "message": "success",
        "data": tdx_l2_local.get_status()
    }


@router.post("/tdx/l2/connect", summary="连接本地通达信L2")
async def connect_l2():
    """连接本地通达信客户端获取L2数据"""
    from app.data_collectors.tdx_l2_local import tdx_l2_local
    
    success = await tdx_l2_local.connect()
    
    return {
        "code": 0 if success else -1,
        "message": "连接成功" if success else "连接失败，请确保通达信客户端已启动",
        "data": tdx_l2_local.get_status()
    }
