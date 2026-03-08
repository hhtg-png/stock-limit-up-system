"""
股票工具函数
"""
from typing import Tuple
import re


def parse_stock_code(code: str) -> Tuple[str, str]:
    """
    解析股票代码，返回 (纯代码, 市场)
    
    Examples:
        "sh600000" -> ("600000", "SH")
        "sz000001" -> ("000001", "SZ")
        "600000.SH" -> ("600000", "SH")
        "000001" -> ("000001", "SZ")  # 根据代码判断市场
    """
    code = code.strip().upper()
    
    # 处理带前缀的代码
    if code.startswith("SH"):
        return (code[2:], "SH")
    if code.startswith("SZ"):
        return (code[2:], "SZ")
    
    # 处理带后缀的代码
    if ".SH" in code:
        return (code.replace(".SH", ""), "SH")
    if ".SZ" in code:
        return (code.replace(".SZ", ""), "SZ")
    
    # 纯数字代码，根据开头判断市场
    pure_code = re.sub(r'[^0-9]', '', code)
    if len(pure_code) == 6:
        if pure_code.startswith(('6', '5', '9')):
            return (pure_code, "SH")
        else:
            return (pure_code, "SZ")
    
    return (pure_code, "SZ")


def get_full_code(stock_code: str, market: str) -> str:
    """获取完整股票代码（带市场前缀）"""
    return f"{market.lower()}{stock_code}"


def is_st_stock(stock_name: str) -> bool:
    """判断是否为ST股票"""
    return "ST" in stock_name.upper()


def is_kc_stock(stock_code: str) -> bool:
    """判断是否为科创板股票"""
    pure_code, _ = parse_stock_code(stock_code)
    return pure_code.startswith("688")


def is_cy_stock(stock_code: str) -> bool:
    """判断是否为创业板股票"""
    pure_code, _ = parse_stock_code(stock_code)
    return pure_code.startswith("30")


def get_limit_up_ratio(stock_code: str, stock_name: str = "") -> float:
    """
    获取涨停比例
    - ST股票: 5%
    - 科创板/创业板: 20%
    - 其他: 10%
    """
    if is_st_stock(stock_name):
        return 0.05
    if is_kc_stock(stock_code) or is_cy_stock(stock_code):
        return 0.20
    return 0.10


def calculate_limit_up_price(pre_close: float, stock_code: str, stock_name: str = "") -> float:
    """计算涨停价"""
    ratio = get_limit_up_ratio(stock_code, stock_name)
    limit_price = pre_close * (1 + ratio)
    
    # 价格精度处理（A股保留2位小数）
    return round(limit_price, 2)


def calculate_limit_down_price(pre_close: float, stock_code: str, stock_name: str = "") -> float:
    """计算跌停价"""
    ratio = get_limit_up_ratio(stock_code, stock_name)
    limit_price = pre_close * (1 - ratio)
    return round(limit_price, 2)


def is_at_limit_up(current_price: float, pre_close: float, 
                   stock_code: str, stock_name: str = "", 
                   tolerance: float = 0.001) -> bool:
    """
    判断是否涨停
    
    Args:
        current_price: 当前价格
        pre_close: 昨收价
        stock_code: 股票代码
        stock_name: 股票名称
        tolerance: 价格容差（默认0.1%）
    """
    limit_up_price = calculate_limit_up_price(pre_close, stock_code, stock_name)
    return abs(current_price - limit_up_price) / limit_up_price <= tolerance


def is_at_limit_down(current_price: float, pre_close: float,
                     stock_code: str, stock_name: str = "",
                     tolerance: float = 0.001) -> bool:
    """判断是否跌停"""
    limit_down_price = calculate_limit_down_price(pre_close, stock_code, stock_name)
    return abs(current_price - limit_down_price) / limit_down_price <= tolerance


def format_amount(amount: float) -> str:
    """
    格式化金额显示
    
    Args:
        amount: 金额（元）
    
    Returns:
        格式化后的字符串，如 "1.23亿" 或 "5432万"
    """
    if amount >= 100000000:  # 亿
        return f"{amount / 100000000:.2f}亿"
    elif amount >= 10000:  # 万
        return f"{amount / 10000:.2f}万"
    else:
        return f"{amount:.2f}"


def format_volume(volume: int) -> str:
    """
    格式化成交量显示
    
    Args:
        volume: 成交量（手）
    """
    if volume >= 10000:
        return f"{volume / 10000:.2f}万手"
    else:
        return f"{volume}手"
