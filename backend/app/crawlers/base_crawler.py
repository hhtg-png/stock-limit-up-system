"""
爬虫基类
"""
import httpx
import asyncio
from typing import Optional, Dict, Any, List
from abc import ABC, abstractmethod
from datetime import datetime
import random
from loguru import logger

from app.config import settings


class BaseCrawler(ABC):
    """爬虫基类"""
    
    # User-Agent列表，用于轮换
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    ]
    
    def __init__(self, name: str = "BaseCrawler"):
        self.name = name
        self.client: Optional[httpx.AsyncClient] = None
        self._request_count = 0
        self._last_request_time: Optional[datetime] = None
        self._min_interval = 1.0  # 最小请求间隔（秒）
    
    async def init_client(self):
        """初始化HTTP客户端"""
        if self.client is None:
            self.client = httpx.AsyncClient(
                timeout=settings.CRAWLER_REQUEST_TIMEOUT,
                follow_redirects=True,
                verify=False  # 忽略SSL验证（某些网站需要）
            )
    
    async def close_client(self):
        """关闭HTTP客户端"""
        if self.client:
            await self.client.aclose()
            self.client = None
    
    def get_random_user_agent(self) -> str:
        """获取随机User-Agent"""
        return random.choice(self.USER_AGENTS)
    
    def get_headers(self) -> Dict[str, str]:
        """获取请求头"""
        return {
            "User-Agent": self.get_random_user_agent(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Cache-Control": "max-age=0",
        }
    
    async def _rate_limit(self):
        """请求频率限制"""
        if self._last_request_time:
            elapsed = (datetime.now() - self._last_request_time).total_seconds()
            if elapsed < self._min_interval:
                await asyncio.sleep(self._min_interval - elapsed)
        self._last_request_time = datetime.now()
        self._request_count += 1
    
    async def fetch(self, url: str, method: str = "GET", 
                    params: Optional[Dict] = None,
                    data: Optional[Dict] = None,
                    headers: Optional[Dict] = None,
                    retry: int = 3) -> Optional[httpx.Response]:
        """
        发送HTTP请求
        
        Args:
            url: 请求URL
            method: 请求方法
            params: URL参数
            data: POST数据
            headers: 自定义请求头
            retry: 重试次数
        
        Returns:
            响应对象或None
        """
        await self.init_client()
        await self._rate_limit()
        
        request_headers = self.get_headers()
        if headers:
            request_headers.update(headers)
        
        for attempt in range(retry):
            try:
                if method.upper() == "GET":
                    response = await self.client.get(
                        url, params=params, headers=request_headers
                    )
                else:
                    response = await self.client.post(
                        url, params=params, data=data, headers=request_headers
                    )
                
                response.raise_for_status()
                return response
            
            except httpx.HTTPStatusError as e:
                logger.warning(f"[{self.name}] HTTP error {e.response.status_code} for {url}")
                if e.response.status_code == 403:
                    # 可能被封，增加等待时间
                    await asyncio.sleep(5 * (attempt + 1))
            except httpx.TimeoutException:
                logger.warning(f"[{self.name}] Timeout for {url}, attempt {attempt + 1}/{retry}")
            except Exception as e:
                logger.error(f"[{self.name}] Request error for {url}: {e}")
            
            if attempt < retry - 1:
                await asyncio.sleep(2 ** attempt)  # 指数退避
        
        return None
    
    async def fetch_json(self, url: str, **kwargs) -> Optional[Dict]:
        """获取JSON响应"""
        response = await self.fetch(url, **kwargs)
        if response:
            try:
                return response.json()
            except Exception as e:
                logger.error(f"[{self.name}] JSON parse error: {e}")
        return None
    
    async def fetch_html(self, url: str, **kwargs) -> Optional[str]:
        """获取HTML响应"""
        response = await self.fetch(url, **kwargs)
        if response:
            return response.text
        return None
    
    @abstractmethod
    async def crawl(self) -> List[Dict]:
        """
        执行爬取任务
        
        Returns:
            爬取的数据列表
        """
        pass
    
    @abstractmethod
    def parse(self, content: Any) -> List[Dict]:
        """
        解析响应内容
        
        Args:
            content: 响应内容
        
        Returns:
            解析后的数据列表
        """
        pass
