"""
开盘啦数据爬虫
获取涨停原因和涨停股票数据

注意：开盘啦Web API (2026年) 已不再提供涨停池数据，
涨停页面返回404，API返回空数据。
本爬虫保留代码以备将来API恢复使用，目前返回空数据。
系统将自动回退使用同花顺和东方财富数据。
"""
from typing import List, Dict, Optional, Any
from datetime import datetime, date
import re
import json
from bs4 import BeautifulSoup
from loguru import logger

from app.crawlers.base_crawler import BaseCrawler


class KaiPanLaCrawler(BaseCrawler):
    """
    开盘啦涨停数据爬虫
    
    状态：API已停用 (2026-02)
    - 涨停页面 /stock/limitup 返回 404
    - API pchq.kaipanla.com 返回空数据
    - 建议使用东方财富(EM)作为主数据源
    """
    
    # 开盘啦涨停页面 (已停用)
    LIMIT_UP_URL = "https://www.kaipanla.com/stock/limitup"
    # 开盘啦涨停API (已停用)
    LIMIT_UP_API = "https://pchq.kaipanla.com/w1/api/index.php"
    # 备用：开盘啦APP API (需要token)
    APP_API = "https://apphq.kaipanla.com/api/v1/stock/limit_up_pool"
    
    # API状态标记
    API_AVAILABLE = False  # API已停用
    
    def __init__(self):
        super().__init__("KaiPanLa")
        self._min_interval = 2.0
    
    def get_headers(self) -> Dict[str, str]:
        """获取开盘啦专用请求头"""
        headers = super().get_headers()
        headers.update({
            "Referer": "https://www.kaipanla.com/",
            "Origin": "https://www.kaipanla.com",
            "Accept": "application/json, text/javascript, */*; q=0.01",
        })
        return headers
    
    async def crawl(self) -> List[Dict]:
        """
        爬取涨停数据
        
        注意：开盘啦API已停用，此方法快速返回空列表。
        系统将自动使用同花顺和东方财富数据作为替代。
        """
        # API已停用，快速返回避免浪费时间
        if not self.API_AVAILABLE:
            logger.info(f"[{self.name}] API已停用，跳过爬取（使用EM/THS数据替代）")
            return []
        
        try:
            # 优先使用API获取（包含涨停原因）
            data = await self._crawl_api()
            if data:
                logger.info(f"[{self.name}] Crawled {len(data)} limit up stocks via API")
                return data
            
            # 备用：页面爬取
            logger.info(f"[{self.name}] API failed, trying HTML...")
            html = await self.fetch_html(self.LIMIT_UP_URL)
            if html:
                data = self._parse_html(html)
                logger.info(f"[{self.name}] Crawled {len(data)} limit up stocks via HTML")
                return data
            
            return []
        except Exception as e:
            logger.error(f"[{self.name}] Crawl error: {e}")
            return []
    
    async def _crawl_api(self) -> Optional[List[Dict]]:
        """通过API获取涨停数据（包含涨停原因）"""
        try:
            # 开盘啦API参数
            params = {
                "c": "StockL2TodayRank",
                "a": "GetZTPool",
                "st": 0,
                "ps": 300,
                "day": date.today().strftime("%Y-%m-%d"),
            }
            
            json_data = await self.fetch_json(self.LIMIT_UP_API, params=params)
            
            if json_data and isinstance(json_data, dict):
                # 检查返回结构
                if "list" in json_data:
                    return self._parse_api_response(json_data["list"])
                elif "data" in json_data:
                    return self._parse_api_response(json_data["data"])
            
            return None
        except Exception as e:
            logger.warning(f"[{self.name}] API error: {e}")
            return None
    
    def _parse_api_response(self, data_list: List) -> List[Dict]:
        """解析API响应"""
        result = []
        
        for item in data_list:
            try:
                code = item.get("code", "") or item.get("stock_code", "")
                name = item.get("name", "") or item.get("stock_name", "")
                
                # 涨停原因 - 开盘啦的字段可能是 reason, zt_reason, concept 等
                reason = (item.get("zt_reason", "") or 
                         item.get("reason", "") or 
                         item.get("concept", "") or 
                         item.get("industry", ""))
                
                # 涨停时间
                zt_time = item.get("zt_time", "") or item.get("first_zt_time", "")
                limit_up_time = None
                if zt_time:
                    try:
                        if len(zt_time) == 8:  # HH:MM:SS
                            today = date.today()
                            limit_up_time = datetime.strptime(
                                f"{today} {zt_time}", "%Y-%m-%d %H:%M:%S"
                            )
                        elif len(zt_time) == 5:  # HH:MM
                            today = date.today()
                            limit_up_time = datetime.strptime(
                                f"{today} {zt_time}:00", "%Y-%m-%d %H:%M:%S"
                            )
                    except:
                        pass
                
                # 流通市值（用于计算实际换手率）
                float_market_value = item.get("lt", 0) or item.get("float_market_value", 0) or item.get("ltsz", 0)
                
                # 成交额
                amount = item.get("cje", 0) or item.get("amount", 0) or item.get("money", 0)
                
                # 计算实际换手率 = 成交额 / 流通市值 * 100
                actual_turnover = 0
                if float_market_value and float_market_value > 0 and amount:
                    actual_turnover = round((amount / float_market_value) * 100, 2)
                else:
                    # 如果没有流通市值，使用数据源提供的换手率
                    actual_turnover = item.get("hs", 0) or item.get("turnover", 0)
                
                result.append({
                    "stock_code": code,
                    "stock_name": name,
                    "limit_up_reason": reason,
                    "reason_category": self._classify_reason(reason),
                    "first_limit_up_time": limit_up_time,
                    "continuous_limit_up_days": item.get("lbc", 1) or item.get("continuous_days", 1),
                    "open_count": item.get("fbt", 0) or item.get("open_times", 0) or item.get("break_times", 0),
                    "is_final_sealed": self._parse_seal_status(item),
                    "turnover_rate": actual_turnover,
                    "float_market_value": float_market_value,  # 流通市值
                    "amount": amount,  # 成交额
                    "seal_amount": item.get("fde", 0) or item.get("seal_money", 0),
                    "data_source": "KPL"
                })
            except Exception as e:
                logger.debug(f"[{self.name}] Parse item error: {e}")
                continue
        
        return result
    
    def _parse_seal_status(self, item: Dict) -> bool:
        """解析封板状态"""
        # 开盘啦的状态字段可能是：status, zt_status, seal_status 等
        status = (item.get("status", "") or 
                 item.get("zt_status", "") or 
                 item.get("seal_status", "") or
                 item.get("state", ""))
        
        if isinstance(status, str):
            # 包含"封"字认为是封板
            if "封" in status or "seal" in status.lower():
                return True
            # 包含"开"或"炸"认为是开板
            if "开" in status or "炸" in status or "open" in status.lower():
                return False
        
        # 数字状态：1=封板 0=开板
        if isinstance(status, (int, float)):
            return status == 1
        
        # 检查开板次数，如果有开板次数且>0，可能是开板状态
        # 但也可能是开板后又封回，所以不能仅靠这个判断
        
        # 默认认为是封板
        return True
    
    def _parse_html(self, html: str) -> List[Dict]:
        """解析HTML页面"""
        result = []
        try:
            soup = BeautifulSoup(html, 'lxml')
            
            # 尝试多种表格选择器
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')[1:]
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 3:
                        try:
                            code_text = cols[0].get_text(strip=True)
                            name_text = cols[1].get_text(strip=True)
                            
                            # 涨停原因可能在第4列或更后
                            reason = ""
                            if len(cols) > 3:
                                reason = cols[3].get_text(strip=True)
                            
                            code_match = re.search(r'\d{6}', code_text)
                            if code_match:
                                result.append({
                                    "stock_code": code_match.group(),
                                    "stock_name": name_text,
                                    "limit_up_reason": reason,
                                    "reason_category": self._classify_reason(reason),
                                    "data_source": "KPL"
                                })
                        except:
                            continue
        except Exception as e:
            logger.error(f"[{self.name}] HTML parse error: {e}")
        
        return result
    
    def _classify_reason(self, reason: str) -> str:
        """分类涨停原因"""
        if not reason:
            return "其他"
        
        category_keywords = {
            "人工智能": ["AI", "人工智能", "算力", "大模型", "机器人", "智能"],
            "半导体": ["半导体", "芯片", "集成电路", "封装", "光刻"],
            "新能源": ["新能源", "锂电", "光伏", "风电", "储能", "电池"],
            "数字经济": ["数字经济", "数据", "云计算", "大数据", "信创"],
            "医药医疗": ["医药", "医疗", "生物", "疫苗", "创新药"],
            "军工": ["军工", "国防", "航空", "航天"],
            "消费": ["消费", "白酒", "食品", "饮料", "零售"],
            "金融": ["金融", "银行", "保险", "证券", "券商"],
            "汽车": ["汽车", "整车", "零部件", "新能源车"],
            "传媒": ["传媒", "游戏", "影视", "短视频"],
        }
        
        for category, keywords in category_keywords.items():
            for keyword in keywords:
                if keyword in reason:
                    return category
        
        return "其他"
    
    def parse(self, content: Any) -> List[Dict]:
        """解析内容（兼容旧接口）"""
        if isinstance(content, str):
            return self._parse_html(content)
        return []


# 创建爬虫实例
kpl_crawler = KaiPanLaCrawler()
