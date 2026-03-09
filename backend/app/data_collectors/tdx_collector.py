"""
通达信Level-2数据采集器
支持心跳保活和自动重连机制
"""
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date
import asyncio
import time
from loguru import logger

try:
    from pytdx.hq import TdxHq_API
    from pytdx.exhq import TdxExHq_API
    TDX_AVAILABLE = True
except ImportError:
    TDX_AVAILABLE = False
    logger.warning("pytdx not installed, TDX collector will not work")

from app.config import settings
from app.utils.stock_utils import (
    parse_stock_code, is_at_limit_up, calculate_limit_up_price,
    get_limit_up_ratio
)


class TDXCollector:
    """通达信数据采集器，支持心跳和自动重连"""
    
    # 通达信市场代码映射
    MARKET_MAP = {
        "SH": 1,  # 上海
        "SZ": 0,  # 深圳
    }
    
    def __init__(self):
        self.api = None
        self.connected = False
        self._lock = asyncio.Lock()
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._last_heartbeat: float = 0
        self._reconnect_count: int = 0
        self._is_reconnecting: bool = False
    
    async def connect(self) -> bool:
        """连接通达信服务器"""
        if not TDX_AVAILABLE:
            logger.error("pytdx not available")
            return False
        
        async with self._lock:
            if self.connected:
                return True
            
            try:
                # 根据配置选择API类型
                if settings.TDX_L2_ENABLED:
                    self.api = TdxExHq_API()
                    logger.info("Using TdxExHq_API (L2 enabled)")
                else:
                    self.api = TdxHq_API()
                    logger.info("Using TdxHq_API (standard)")
                
                # 连接服务器
                result = self.api.connect(settings.TDX_HOST, settings.TDX_PORT)
                if result:
                    self.connected = True
                    self._reconnect_count = 0
                    self._last_heartbeat = time.time()
                    logger.info(f"Connected to TDX server: {settings.TDX_HOST}:{settings.TDX_PORT}")
                    
                    # 启动心跳任务
                    await self._start_heartbeat()
                    return True
                else:
                    logger.error("Failed to connect to TDX server")
                    return False
            except Exception as e:
                logger.error(f"TDX connection error: {e}")
                return False
    
    async def disconnect(self):
        """断开连接"""
        # 停止心跳任务
        await self._stop_heartbeat()
        
        async with self._lock:
            if self.api and self.connected:
                try:
                    self.api.disconnect()
                except:
                    pass
                self.connected = False
                logger.info("Disconnected from TDX server")
    
    async def _start_heartbeat(self):
        """启动心跳任务"""
        if self._heartbeat_task is not None:
            return
        
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        logger.debug("Heartbeat task started")
    
    async def _stop_heartbeat(self):
        """停止心跳任务"""
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None
            logger.debug("Heartbeat task stopped")
    
    async def _heartbeat_loop(self):
        """心跳循环，定期查询指数行情保持连接"""
        while True:
            try:
                await asyncio.sleep(settings.TDX_HEARTBEAT_INTERVAL)
                
                if not self.connected:
                    continue
                
                # 查询上证指数作为心跳
                try:
                    data = self.api.get_security_quotes([(1, "000001")])
                    if data:
                        self._last_heartbeat = time.time()
                        logger.debug(f"Heartbeat OK, index: {data[0].get('price', 0)}")
                    else:
                        logger.warning("Heartbeat failed: no data returned")
                        await self._handle_connection_lost()
                except Exception as e:
                    logger.warning(f"Heartbeat error: {e}")
                    await self._handle_connection_lost()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat loop error: {e}")
    
    async def _handle_connection_lost(self):
        """处理连接断开"""
        if self._is_reconnecting:
            return
        
        self._is_reconnecting = True
        self.connected = False
        
        logger.warning("Connection lost, attempting to reconnect...")
        
        # 尝试重连
        success = await self._reconnect_with_backoff()
        
        self._is_reconnecting = False
        
        if not success:
            logger.error("All reconnection attempts failed")
    
    async def _reconnect_with_backoff(self) -> bool:
        """指数退避重连"""
        for attempt in range(settings.TDX_RECONNECT_ATTEMPTS):
            self._reconnect_count = attempt + 1
            
            # 计算退避延迟
            delay = settings.TDX_RECONNECT_BASE_DELAY * (2 ** attempt)
            delay = min(delay, 60.0)  # 最大60秒
            
            logger.info(f"Reconnect attempt {attempt + 1}/{settings.TDX_RECONNECT_ATTEMPTS}, "
                       f"waiting {delay:.1f}s...")
            
            await asyncio.sleep(delay)
            
            try:
                # 先断开旧连接
                if self.api:
                    try:
                        self.api.disconnect()
                    except:
                        pass
                
                # 重新创建API并连接
                if settings.TDX_L2_ENABLED:
                    self.api = TdxExHq_API()
                else:
                    self.api = TdxHq_API()
                
                result = self.api.connect(settings.TDX_HOST, settings.TDX_PORT)
                
                if result:
                    self.connected = True
                    self._last_heartbeat = time.time()
                    logger.info(f"Reconnected to TDX server after {attempt + 1} attempts")
                    return True
                    
            except Exception as e:
                logger.warning(f"Reconnect attempt {attempt + 1} failed: {e}")
        
        return False
    
    async def ensure_connected(self) -> bool:
        """确保连接可用，必要时重连"""
        if self.connected:
            # 检查心跳是否超时
            if time.time() - self._last_heartbeat > settings.TDX_HEARTBEAT_INTERVAL * 3:
                logger.warning("Heartbeat timeout, reconnecting...")
                await self._handle_connection_lost()
            return self.connected
        
        # 未连接则尝试连接
        return await self.connect()
    
    def get_connection_status(self) -> Dict:
        """获取连接状态信息"""
        return {
            "connected": self.connected,
            "l2_enabled": settings.TDX_L2_ENABLED,
            "host": settings.TDX_HOST,
            "port": settings.TDX_PORT,
            "last_heartbeat": datetime.fromtimestamp(self._last_heartbeat).isoformat() if self._last_heartbeat else None,
            "reconnect_count": self._reconnect_count,
            "is_reconnecting": self._is_reconnecting
        }
    
    def _get_market_code(self, market: str) -> int:
        """获取通达信市场代码"""
        return self.MARKET_MAP.get(market.upper(), 0)
    
    async def get_quote(self, stock_code: str, market: str = None) -> Optional[Dict]:
        """
        获取实时行情
        
        Args:
            stock_code: 股票代码
            market: 市场(SH/SZ)，如果不指定则自动判断
        
        Returns:
            行情数据字典
        """
        if not await self.ensure_connected():
            return None
        
        try:
            # 解析股票代码
            pure_code, detected_market = parse_stock_code(stock_code)
            if market is None:
                market = detected_market
            
            market_code = self._get_market_code(market)
            
            # 获取行情
            data = self.api.get_security_quotes([(market_code, pure_code)])
            
            if data and len(data) > 0:
                self._last_heartbeat = time.time()  # 更新心跳时间
                quote = data[0]
                return {
                    "stock_code": pure_code,
                    "market": market,
                    "current_price": quote.get("price", 0),
                    "pre_close": quote.get("last_close", 0),
                    "open_price": quote.get("open", 0),
                    "high_price": quote.get("high", 0),
                    "low_price": quote.get("low", 0),
                    "volume": quote.get("vol", 0),  # 手
                    "amount": quote.get("amount", 0),  # 元
                    "bid1_price": quote.get("bid1", 0),
                    "bid1_volume": quote.get("bid_vol1", 0),
                    "bid2_price": quote.get("bid2", 0),
                    "bid2_volume": quote.get("bid_vol2", 0),
                    "bid3_price": quote.get("bid3", 0),
                    "bid3_volume": quote.get("bid_vol3", 0),
                    "bid4_price": quote.get("bid4", 0),
                    "bid4_volume": quote.get("bid_vol4", 0),
                    "bid5_price": quote.get("bid5", 0),
                    "bid5_volume": quote.get("bid_vol5", 0),
                    "ask1_price": quote.get("ask1", 0),
                    "ask1_volume": quote.get("ask_vol1", 0),
                    "ask2_price": quote.get("ask2", 0),
                    "ask2_volume": quote.get("ask_vol2", 0),
                    "ask3_price": quote.get("ask3", 0),
                    "ask3_volume": quote.get("ask_vol3", 0),
                    "ask4_price": quote.get("ask4", 0),
                    "ask4_volume": quote.get("ask_vol4", 0),
                    "ask5_price": quote.get("ask5", 0),
                    "ask5_volume": quote.get("ask_vol5", 0),
                    "buy_volume": quote.get("b_vol", 0),  # 外盘
                    "sell_volume": quote.get("s_vol", 0),  # 内盘
                    "timestamp": datetime.now()
                }
            return None
        except Exception as e:
            logger.error(f"Get quote error for {stock_code}: {e}")
            # 连接异常时尝试重连
            await self._handle_connection_lost()
            return None
    
    async def get_quotes_batch(self, stocks: List[Tuple[str, str]]) -> List[Dict]:
        """
        批量获取行情
        
        Args:
            stocks: [(stock_code, market), ...]
        
        Returns:
            行情数据列表
        """
        if not await self.ensure_connected():
            return []
        
        try:
            # 转换为通达信格式
            tdx_stocks = [
                (self._get_market_code(market), code)
                for code, market in stocks
            ]
            
            # 批量获取（通达信每次最多80只）
            results = []
            batch_size = 80
            
            for i in range(0, len(tdx_stocks), batch_size):
                batch = tdx_stocks[i:i + batch_size]
                data = self.api.get_security_quotes(batch)
                
                if data:
                    self._last_heartbeat = time.time()  # 更新心跳时间
                    for j, quote in enumerate(data):
                        idx = i + j
                        if idx < len(stocks):
                            code, market = stocks[idx]
                            results.append({
                                "stock_code": code,
                                "market": market,
                                "current_price": quote.get("price", 0),
                                "pre_close": quote.get("last_close", 0),
                                "open_price": quote.get("open", 0),
                                "high_price": quote.get("high", 0),
                                "low_price": quote.get("low", 0),
                                "volume": quote.get("vol", 0),
                                "amount": quote.get("amount", 0),
                                "bid_prices": [
                                    quote.get("bid1", 0),
                                    quote.get("bid2", 0),
                                    quote.get("bid3", 0),
                                    quote.get("bid4", 0),
                                    quote.get("bid5", 0),
                                ],
                                "bid_volumes": [
                                    quote.get("bid_vol1", 0),
                                    quote.get("bid_vol2", 0),
                                    quote.get("bid_vol3", 0),
                                    quote.get("bid_vol4", 0),
                                    quote.get("bid_vol5", 0),
                                ],
                                "ask_prices": [
                                    quote.get("ask1", 0),
                                    quote.get("ask2", 0),
                                    quote.get("ask3", 0),
                                    quote.get("ask4", 0),
                                    quote.get("ask5", 0),
                                ],
                                "ask_volumes": [
                                    quote.get("ask_vol1", 0),
                                    quote.get("ask_vol2", 0),
                                    quote.get("ask_vol3", 0),
                                    quote.get("ask_vol4", 0),
                                    quote.get("ask_vol5", 0),
                                ],
                                "buy_volume": quote.get("b_vol", 0),
                                "sell_volume": quote.get("s_vol", 0),
                                "timestamp": datetime.now()
                            })
            
            return results
        except Exception as e:
            logger.error(f"Get quotes batch error: {e}")
            await self._handle_connection_lost()
            return []
    
    async def get_stock_list(self, market: str = "SH") -> List[Dict]:
        """
        获取股票列表
        
        Args:
            market: 市场(SH/SZ)
        
        Returns:
            股票列表
        """
        if not await self.ensure_connected():
            return []
        
        try:
            market_code = self._get_market_code(market)
            stocks = []
            
            # 通达信股票列表需要分页获取
            start = 0
            while True:
                data = self.api.get_security_list(market_code, start)
                if not data or len(data) == 0:
                    break
                
                self._last_heartbeat = time.time()  # 更新心跳时间
                
                for item in data:
                    code = item.get("code", "")
                    name = item.get("name", "")
                    
                    # 过滤非股票代码
                    if market == "SH" and not code.startswith(("6", "5")):
                        continue
                    if market == "SZ" and not code.startswith(("0", "3")):
                        continue
                    
                    stocks.append({
                        "stock_code": code,
                        "stock_name": name,
                        "market": market
                    })
                
                start += len(data)
                if len(data) < 1000:  # 最后一页
                    break
            
            return stocks
        except Exception as e:
            logger.error(f"Get stock list error: {e}")
            await self._handle_connection_lost()
            return []
    
    async def get_transaction_data(self, stock_code: str, market: str = None, 
                                    start: int = 0, count: int = 100) -> List[Dict]:
        """
        获取逐笔成交数据
        
        Args:
            stock_code: 股票代码
            market: 市场
            start: 起始位置
            count: 获取数量
        
        Returns:
            逐笔成交数据列表
        """
        if not await self.ensure_connected():
            return []
        
        try:
            pure_code, detected_market = parse_stock_code(stock_code)
            if market is None:
                market = detected_market
            
            market_code = self._get_market_code(market)
            
            data = self.api.get_transaction_data(market_code, pure_code, start, count)
            
            if data:
                self._last_heartbeat = time.time()  # 更新心跳时间
                transactions = []
                for item in data:
                    transactions.append({
                        "time": item.get("time", ""),
                        "price": item.get("price", 0),
                        "volume": item.get("vol", 0),
                        "amount": item.get("price", 0) * item.get("vol", 0) * 100,  # 手转股
                        "direction": "buy" if item.get("buyorsell", 0) == 0 else "sell"
                    })
                return transactions
            return []
        except Exception as e:
            logger.error(f"Get transaction data error for {stock_code}: {e}")
            await self._handle_connection_lost()
            return []
    
    def check_limit_up(self, quote: Dict, stock_name: str = "") -> Tuple[bool, float]:
        """
        检查是否涨停
        
        Args:
            quote: 行情数据
            stock_name: 股票名称（用于判断ST）
        
        Returns:
            (是否涨停, 涨停价)
        """
        current_price = quote.get("current_price", 0)
        pre_close = quote.get("pre_close", 0)
        stock_code = quote.get("stock_code", "")
        
        if not current_price or not pre_close:
            return False, 0
        
        limit_up_price = calculate_limit_up_price(pre_close, stock_code, stock_name)
        is_limit_up = is_at_limit_up(current_price, pre_close, stock_code, stock_name)
        
        return is_limit_up, limit_up_price


# 全局采集器实例
tdx_collector = TDXCollector()
