"""
真实换手率计算模块

真实换手率 = 成交量(股) / 自由流通股本 * 100
自由流通股本 = 流通股本 - 持股比例>5%的大股东持股

数据来源：东方财富API
"""
import requests
import time
from typing import Dict, Optional, Tuple
from loguru import logger


_HEADERS = {"User-Agent": "Mozilla/5.0"}
_EM_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://emweb.securities.eastmoney.com/",
}


def get_stock_quote_em(code: str, market: str) -> Optional[Dict]:
    """从东方财富获取个股行情: 成交量、流通股本"""
    prefix = "0" if market == "SZ" else "1"
    url = "https://push2.eastmoney.com/api/qt/stock/get"
    params = {
        "secid": f"{prefix}.{code}",
        "fields": "f47,f85,f168",
        "ut": "fa5fd1943c7b386f172d6893dbbd1",
        "fltt": "2",
    }
    try:
        resp = requests.get(url, headers=_HEADERS, params=params, timeout=10)
        data = resp.json()
        if data.get("data"):
            d = data["data"]
            return {
                "volume": d.get("f47", 0),
                "circ_shares": d.get("f85", 0),
                "em_turnover": d.get("f168", 0),
            }
    except Exception as e:
        logger.warning(f"获取{code}行情失败: {e}")
    return None


def get_big_holder_shares_em(code: str, market: str) -> int:
    """获取最新报告期中持股>5%的大股东持股总量"""
    secucode = f"{code}.{'SZ' if market == 'SZ' else 'SH'}"
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "reportName": "RPT_F10_EH_FREEHOLDERS",
        "columns": "HOLDER_NAME,HOLD_NUM,FREE_HOLDNUM_RATIO",
        "filter": f'(SECUCODE="{secucode}")(IS_MAX_REPORTDATE=1)',
        "pageNumber": "1",
        "pageSize": "10",
        "sortTypes": "1",
        "sortColumns": "HOLDER_RANK",
        "source": "HSF10",
        "client": "PC",
    }
    try:
        resp = requests.get(url, headers=_EM_HEADERS, params=params, timeout=10)
        data = resp.json()
        big_shares = 0
        if data.get("result") and data["result"].get("data"):
            for item in data["result"]["data"]:
                ratio = item.get("FREE_HOLDNUM_RATIO", 0) or 0
                hold_num = item.get("HOLD_NUM", 0) or 0
                if ratio > 5.0:
                    big_shares += hold_num
        return big_shares
    except Exception as e:
        logger.warning(f"获取{code}股东数据失败: {e}")
        return 0


def calc_real_turnover_rate(code: str, market: str) -> Tuple[Optional[float], Optional[int]]:
    """
    计算单只股票的真实换手率

    Returns:
        (real_turnover_rate, circulating_shares) 或 (None, None)
    """
    quote = get_stock_quote_em(code, market)
    if not quote or not quote.get("circ_shares"):
        return None, None

    big_shares = get_big_holder_shares_em(code, market)
    volume_shares = quote["volume"] * 100  # 手 -> 股
    circ_shares = int(quote["circ_shares"])
    free_float = circ_shares - big_shares

    if free_float <= 0:
        free_float = circ_shares

    real_turnover = round(volume_shares / free_float * 100, 2)
    return real_turnover, circ_shares
