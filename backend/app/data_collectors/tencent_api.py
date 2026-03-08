"""
腾讯股票API - 获取实时行情数据（包括准确的换手率）
"""
import httpx
from typing import Dict, List, Optional
from loguru import logger


class TencentStockAPI:
    """腾讯股票实时行情API"""
    
    BASE_URL = "http://qt.gtimg.cn/q="
    
    # 腾讯API返回字段索引（用~分隔）
    FIELD_INDEX = {
        "market": 0,           # 市场代码
        "name": 1,             # 股票名称
        "code": 2,             # 股票代码
        "price": 3,            # 当前价格
        "pre_close": 4,        # 昨收
        "open": 5,             # 开盘价
        "volume": 6,           # 成交量(手)
        "buy_volume": 7,       # 外盘(手)
        "sell_volume": 8,      # 内盘(手)
        "bid1_price": 9,       # 买一价
        "bid1_volume": 10,     # 买一量
        "bid2_price": 11,
        "bid2_volume": 12,
        "bid3_price": 13,
        "bid3_volume": 14,
        "bid4_price": 15,
        "bid4_volume": 16,
        "bid5_price": 17,
        "bid5_volume": 18,
        "ask1_price": 19,
        "ask1_volume": 20,
        "ask2_price": 21,
        "ask2_volume": 22,
        "ask3_price": 23,
        "ask3_volume": 24,
        "ask4_price": 25,
        "ask4_volume": 26,
        "ask5_price": 27,
        "ask5_volume": 28,
        "datetime": 30,        # 时间
        "change": 31,          # 涨跌
        "change_pct": 32,      # 涨跌幅(%)
        "high": 33,            # 最高价
        "low": 34,             # 最低价
        "amount": 37,          # 成交额(万)
        "turnover_rate": 38,   # 换手率(%)
        "pe": 39,              # 市盈率
        "amplitude": 43,       # 振幅(%)
        "circulating_value": 44, # 流通市值(亿)
        "total_value": 45,     # 总市值(亿)
        "pb": 46,              # 市净率
        "limit_up": 47,        # 涨停价
        "limit_down": 48,      # 跌停价
    }
    
    def __init__(self):
        self.client: Optional[httpx.AsyncClient] = None
    
    async def _get_client(self) -> httpx.AsyncClient:
        if not self.client:
            self.client = httpx.AsyncClient(timeout=10)
        return self.client
    
    async def close(self):
        if self.client:
            await self.client.aclose()
            self.client = None
    
    def _format_code(self, code: str) -> str:
        """格式化股票代码为腾讯格式 (sz000001 / sh600000)"""
        code = code.strip()
        if code.startswith(('sz', 'sh', 'SZ', 'SH')):
            return code.lower()
        
        # 根据股票代码判断市场
        if code.startswith(('6', '9', '5')):
            return f"sh{code}"
        else:
            return f"sz{code}"
    
    def _parse_response(self, text: str) -> Dict:
        """解析API响应"""
        try:
            # 格式: v_sz000001="data~data~...";
            if '="' not in text:
                return {}
            
            data_str = text.split('="')[1].rstrip('";')
            fields = data_str.split('~')
            
            if len(fields) < 50:
                return {}
            
            result = {}
            for name, idx in self.FIELD_INDEX.items():
                if idx < len(fields):
                    value = fields[idx]
                    # 尝试转换为数字
                    if name not in ("market", "name", "code", "datetime"):
                        try:
                            result[name] = float(value) if value else 0
                        except:
                            result[name] = 0
                    else:
                        result[name] = value
            
            return result
        except Exception as e:
            logger.warning(f"Parse Tencent API response error: {e}")
            return {}
    
    async def get_quote(self, code: str) -> Optional[Dict]:
        """获取单只股票行情"""
        client = await self._get_client()
        formatted_code = self._format_code(code)
        
        try:
            response = await client.get(f"{self.BASE_URL}{formatted_code}")
            if response.status_code == 200:
                return self._parse_response(response.text)
        except Exception as e:
            logger.warning(f"Get quote error for {code}: {e}")
        
        return None
    
    async def get_quotes_batch(self, codes: List[str]) -> Dict[str, Dict]:
        """批量获取股票行情"""
        client = await self._get_client()
        formatted_codes = [self._format_code(c) for c in codes]
        
        results = {}
        
        try:
            # 腾讯API支持批量查询，用逗号分隔
            codes_str = ",".join(formatted_codes)
            response = await client.get(f"{self.BASE_URL}{codes_str}")
            
            if response.status_code == 200:
                # 响应格式: v_sz000001="...";v_sz000002="...";
                for line in response.text.strip().split(';'):
                    if not line.strip():
                        continue
                    
                    data = self._parse_response(line + ';')
                    if data and data.get("code"):
                        results[data["code"]] = data
        except Exception as e:
            logger.warning(f"Batch get quotes error: {e}")
        
        return results
    
    async def get_turnover_rates(self, codes: List[str]) -> Dict[str, float]:
        """批量获取换手率"""
        quotes = await self.get_quotes_batch(codes)
        return {
            code: data.get("turnover_rate", 0)
            for code, data in quotes.items()
        }
    
    def is_limit_up(self, quote: Dict) -> bool:
        """判断是否涨停"""
        if not quote:
            return False
        
        price = quote.get("price", 0)
        limit_up = quote.get("limit_up", 0)
        
        if not price or not limit_up:
            return False
        
        # 当前价格 >= 涨停价（考虑精度问题，允许微小差异）
        return price >= limit_up - 0.001
    
    def is_sealed(self, quote: Dict) -> bool:
        """判断是否封板（涨停且买一有量）"""
        if not self.is_limit_up(quote):
            return False
        
        # 买一有量说明封板中
        bid1_volume = quote.get("bid1_volume", 0)
        return bid1_volume > 0
    
    async def get_all_limit_up_stocks(self, stock_codes: List[str]) -> List[Dict]:
        """
        从给定股票列表中筛选出涨停股
        返回涨停股的详细信息
        """
        if not stock_codes:
            return []
        
        quotes = await self.get_quotes_batch(stock_codes)
        limit_up_stocks = []
        
        for code, quote in quotes.items():
            if self.is_limit_up(quote):
                limit_up_stocks.append({
                    "stock_code": code,
                    "stock_name": quote.get("name", ""),
                    "price": quote.get("price", 0),
                    "change_pct": quote.get("change_pct", 0),
                    "limit_up_price": quote.get("limit_up", 0),
                    "is_sealed": self.is_sealed(quote),
                    "bid1_volume": quote.get("bid1_volume", 0),
                    "turnover_rate": quote.get("turnover_rate", 0),
                    "amount": quote.get("amount", 0),  # 成交额(万)
                })
        
        return limit_up_stocks


# 全局实例
tencent_api = TencentStockAPI()


# 测试代码
if __name__ == "__main__":
    import asyncio
    
    async def test():
        api = TencentStockAPI()
        
        # 测试单只股票
        quote = await api.get_quote("000021")
        if quote:
            print(f"股票: {quote.get('name')}")
            print(f"价格: {quote.get('price')}")
            print(f"换手率: {quote.get('turnover_rate')}%")
            print(f"成交额: {quote.get('amount')}万")
        
        # 测试批量
        codes = ["000021", "001896", "300059"]
        rates = await api.get_turnover_rates(codes)
        print(f"\n批量换手率: {rates}")
        
        await api.close()
    
    asyncio.run(test())
