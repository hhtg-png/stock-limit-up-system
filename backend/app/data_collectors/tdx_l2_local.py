"""
本地通达信客户端L2数据读取模块

通过以下方式获取L2逐笔数据:
1. 通达信TradeX组件(COM接口)
2. 内存读取方式
3. DDE接口(部分券商支持)

使用前需要:
1. 安装券商版通达信客户端并登录
2. 开启L2行情权限
3. Windows系统(COM组件仅支持Windows)
"""
from typing import Dict, List, Optional, Callable
from datetime import datetime
import asyncio
import time
from loguru import logger
import sys

# Windows COM支持检测
TRADEX_AVAILABLE = False
if sys.platform == 'win32':
    try:
        import win32com.client
        TRADEX_AVAILABLE = True
    except ImportError:
        logger.warning("pywin32 not installed, TDX TradeX COM interface unavailable")
        logger.info("Install with: pip install pywin32")


class TdxL2LocalCollector:
    """
    本地通达信L2数据采集器
    
    支持两种模式:
    1. TradeX COM模式: 通过通达信交易组件获取L2数据
    2. 内存读取模式: 直接读取通达信进程内存(需要管理员权限)
    """
    
    def __init__(self, mode: str = "tradex"):
        """
        初始化采集器
        
        Args:
            mode: 采集模式 "tradex" | "memory"
        """
        self.mode = mode
        self.connected = False
        self._tradex = None
        self._callbacks: List[Callable] = []
        self._running = False
        self._collect_task: Optional[asyncio.Task] = None
    
    async def connect(self) -> bool:
        """连接本地通达信客户端"""
        if self.mode == "tradex":
            return await self._connect_tradex()
        elif self.mode == "memory":
            return await self._connect_memory()
        return False
    
    async def _connect_tradex(self) -> bool:
        """通过TradeX COM接口连接"""
        if not TRADEX_AVAILABLE:
            logger.error("TradeX COM interface not available. Need pywin32.")
            return False
        
        try:
            # 尝试连接通达信TradeX组件
            # 不同券商的组件名可能不同，常见的有:
            # - TdxW.TdxClient
            # - Trade.Dial  
            # - TdxApi.TdxApi
            
            component_names = [
                "TdxW.TdxClient",
                "Trade.Dial",
                "TdxApi.TdxApi",
                "TdxTradeServer.TdxTradeServer",
            ]
            
            for name in component_names:
                try:
                    self._tradex = win32com.client.Dispatch(name)
                    logger.info(f"Connected to TradeX component: {name}")
                    self.connected = True
                    return True
                except Exception:
                    continue
            
            logger.warning("No TradeX COM component found. Make sure TDX client is installed and running.")
            return False
            
        except Exception as e:
            logger.error(f"TradeX connection error: {e}")
            return False
    
    async def _connect_memory(self) -> bool:
        """
        通过内存读取方式连接
        
        注意: 这种方式需要:
        1. 管理员权限运行
        2. 通达信客户端必须正在运行
        3. 找到正确的内存偏移地址
        """
        logger.warning("Memory reading mode is experimental and requires admin privileges")
        
        try:
            # 检查通达信进程是否运行
            import psutil
            
            tdx_processes = [
                p for p in psutil.process_iter(['name']) 
                if 'tdx' in p.info['name'].lower() or 'tongda' in p.info['name'].lower()
            ]
            
            if not tdx_processes:
                logger.error("TDX client process not found. Please start TDX first.")
                return False
            
            logger.info(f"Found TDX process: {tdx_processes[0].info['name']}")
            self.connected = True
            return True
            
        except ImportError:
            logger.error("psutil not installed. Run: pip install psutil")
            return False
        except Exception as e:
            logger.error(f"Memory mode connection error: {e}")
            return False
    
    async def disconnect(self):
        """断开连接"""
        self._running = False
        if self._collect_task:
            self._collect_task.cancel()
            try:
                await self._collect_task
            except asyncio.CancelledError:
                pass
        
        self._tradex = None
        self.connected = False
        logger.info("TDX L2 local collector disconnected")
    
    def register_callback(self, callback: Callable):
        """
        注册数据回调函数
        
        当收到新的L2数据时会调用此回调
        
        Args:
            callback: async def callback(stock_code: str, data: List[Dict])
        """
        self._callbacks.append(callback)
    
    async def start_collect(self, stock_codes: List[str], interval: float = 0.5):
        """
        开始采集L2数据
        
        Args:
            stock_codes: 要采集的股票代码列表
            interval: 采集间隔(秒)
        """
        if not self.connected:
            if not await self.connect():
                logger.error("Cannot start collection: not connected")
                return
        
        self._running = True
        self._collect_task = asyncio.create_task(
            self._collect_loop(stock_codes, interval)
        )
        logger.info(f"Started L2 data collection for {len(stock_codes)} stocks")
    
    async def _collect_loop(self, stock_codes: List[str], interval: float):
        """采集循环"""
        while self._running:
            try:
                for code in stock_codes:
                    if not self._running:
                        break
                    
                    # 获取L2逐笔数据
                    data = await self.get_l2_transactions(code)
                    
                    if data:
                        # 触发回调
                        for callback in self._callbacks:
                            try:
                                await callback(code, data)
                            except Exception as e:
                                logger.error(f"Callback error: {e}")
                
                await asyncio.sleep(interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Collect loop error: {e}")
                await asyncio.sleep(1)
    
    async def get_l2_transactions(self, stock_code: str, count: int = 50) -> List[Dict]:
        """
        获取L2逐笔成交数据
        
        Args:
            stock_code: 股票代码
            count: 获取条数
        
        Returns:
            逐笔成交数据列表
        """
        if self.mode == "tradex":
            return await self._get_transactions_tradex(stock_code, count)
        elif self.mode == "memory":
            return await self._get_transactions_memory(stock_code, count)
        return []
    
    async def _get_transactions_tradex(self, stock_code: str, count: int) -> List[Dict]:
        """通过TradeX获取逐笔数据"""
        if not self._tradex:
            return []
        
        try:
            # TradeX API调用示例
            # 具体方法名和参数需要根据实际COM组件确定
            # 这里提供常见的接口模式
            
            # 方法1: 直接获取逐笔
            # data = self._tradex.GetTransactionData(stock_code, count)
            
            # 方法2: 通过TQL查询
            # data = self._tradex.TdxQuery("l2transaction", stock_code)
            
            # 模拟数据结构
            logger.debug(f"TradeX: Getting L2 transactions for {stock_code}")
            
            # 实际实现需要根据具体的TradeX组件API
            return []
            
        except Exception as e:
            logger.error(f"TradeX get transactions error: {e}")
            return []
    
    async def _get_transactions_memory(self, stock_code: str, count: int) -> List[Dict]:
        """通过内存读取获取逐笔数据"""
        # 内存读取需要知道具体的内存结构和偏移
        # 这通常需要逆向工程通达信客户端
        logger.debug(f"Memory: Getting L2 transactions for {stock_code}")
        return []
    
    async def get_l2_orderbook(self, stock_code: str) -> Optional[Dict]:
        """
        获取L2十档盘口
        
        Args:
            stock_code: 股票代码
        
        Returns:
            十档盘口数据
        """
        if not self.connected:
            return None
        
        try:
            # L2十档盘口数据结构
            return {
                "stock_code": stock_code,
                "timestamp": datetime.now(),
                "bid_prices": [],   # 买1-10价格
                "bid_volumes": [],  # 买1-10数量
                "ask_prices": [],   # 卖1-10价格
                "ask_volumes": [],  # 卖1-10数量
                "total_bid_volume": 0,  # 委买总量
                "total_ask_volume": 0,  # 委卖总量
            }
        except Exception as e:
            logger.error(f"Get L2 orderbook error: {e}")
            return None
    
    async def get_l2_order_queue(self, stock_code: str) -> Optional[Dict]:
        """
        获取L2委托队列(买一卖一前50笔委托明细)
        
        Args:
            stock_code: 股票代码
        
        Returns:
            委托队列数据
        """
        if not self.connected:
            return None
        
        try:
            return {
                "stock_code": stock_code,
                "timestamp": datetime.now(),
                "bid1_queue": [],  # 买一前50笔委托
                "ask1_queue": [],  # 卖一前50笔委托
            }
        except Exception as e:
            logger.error(f"Get L2 order queue error: {e}")
            return None
    
    def get_status(self) -> Dict:
        """获取采集器状态"""
        return {
            "mode": self.mode,
            "connected": self.connected,
            "running": self._running,
            "tradex_available": TRADEX_AVAILABLE,
            "callbacks_count": len(self._callbacks),
        }


# 全局实例
tdx_l2_local = TdxL2LocalCollector(mode="tradex")


# 使用示例
async def example_usage():
    """使用示例"""
    
    # 1. 连接本地通达信
    success = await tdx_l2_local.connect()
    if not success:
        print("连接失败，请确保通达信客户端已启动并登录")
        return
    
    # 2. 注册数据回调
    async def on_l2_data(stock_code: str, data: List[Dict]):
        print(f"收到 {stock_code} 的L2数据: {len(data)} 条")
        for item in data[:3]:  # 打印前3条
            print(f"  {item}")
    
    tdx_l2_local.register_callback(on_l2_data)
    
    # 3. 开始采集
    await tdx_l2_local.start_collect(
        stock_codes=["000001", "600519", "300750"],
        interval=0.5
    )
    
    # 4. 运行一段时间
    await asyncio.sleep(10)
    
    # 5. 停止采集
    await tdx_l2_local.disconnect()


if __name__ == "__main__":
    asyncio.run(example_usage())
