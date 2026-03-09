"""
东方财富数据爬虫 - 获取涨停板数据
"""
from typing import List, Dict, Optional, Any
from datetime import datetime, date
import asyncio
from loguru import logger

from app.crawlers.base_crawler import BaseCrawler


class EastMoneyCrawler(BaseCrawler):
    """东方财富涨停数据爬虫"""
    
    # 东方财富涨停池API
    LIMIT_UP_API = "https://push2ex.eastmoney.com/getTopicZTPool"
    # 东方财富炸板池API
    BROKEN_API = "https://push2ex.eastmoney.com/getTopicZBPool"
    # 东方财富个股行情API（用于获取自由流通市值f183）
    STOCK_API = "https://push2.eastmoney.com/api/qt/stock/get"
    
    def __init__(self):
        super().__init__("EastMoney")
        self._min_interval = 1.0
        # 自由流通市值缓存 {stock_code: free_float_value}，当天有效
        self._free_float_cache: Dict[str, float] = {}
        self._free_float_cache_date: Optional[date] = None
    
    def get_headers(self) -> Dict[str, str]:
        """获取东方财富专用请求头"""
        headers = super().get_headers()
        headers.update({
            "Referer": "https://quote.eastmoney.com/",
            "Host": "push2ex.eastmoney.com",
        })
        return headers
    
    async def crawl(self, trade_date: date = None) -> List[Dict]:
        """爬取涨停数据（包含封板和炸板），使用自由流通市值计算真实换手率"""
        if trade_date is None:
            trade_date = date.today()
        
        try:
            # 格式化日期为 YYYYMMDD (东方财富API要求此格式)
            date_str = trade_date.strftime("%Y%m%d")
            
            params = {
                "ut": "7eea3edcaed734bea9cbfc24409ed989",
                "dpt": "wz.ztzt",
                "Pageindex": 0,
                "pagesize": 200,
                "sort": "fbt:asc",
                "date": date_str,
                "_": int(datetime.now().timestamp() * 1000)
            }
            
            logger.info(f"[{self.name}] 开始获取涨停数据, date={date_str}")
            
            # 获取涨停池数据（封板中的）
            json_data = await self.fetch_json(self.LIMIT_UP_API, params=params)
            sealed_data = []
            if json_data:
                sealed_data = self.parse(json_data, is_sealed=True)
                logger.info(f"[{self.name}] 封板池获取 {len(sealed_data)} 条")
            else:
                logger.warning(f"[{self.name}] 封板池返回空数据")
            
            # 获取炸板池数据（曾涨停后开板的）
            logger.info(f"[{self.name}] 开始获取炸板池数据...")
            broken_data = await self.fetch_json(self.BROKEN_API, params=params)
            opened_data = []
            if broken_data:
                logger.info(f"[{self.name}] 炸板池API返回: rc={broken_data.get('rc')}, tc={broken_data.get('data', {}).get('tc', 0)}")
                opened_data = self.parse(broken_data, is_sealed=False)
                logger.info(f"[{self.name}] 炸板池解析 {len(opened_data)} 条")
            else:
                logger.warning(f"[{self.name}] 炸板池返回空数据")
            
            # 合并数据
            all_data = sealed_data + opened_data
            
            if all_data:
                # 获取自由流通市值并计算真实换手率
                await self._enrich_real_turnover(all_data)
                logger.info(f"[{self.name}] 总计 {len(all_data)} 条 (封板: {len(sealed_data)}, 炸板: {len(opened_data)})")
                return all_data
            
            logger.warning(f"[{self.name}] No data returned for {date_str}")
            return []
            
        except Exception as e:
            logger.error(f"[{self.name}] Crawl error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []
    
    async def _fetch_stock_realtime(self, stock_code: str) -> Optional[Dict]:
        """获取个股实时数据（换手率、自由流通市值）
        
        f168: 换手率（万分比，需除以100）
        f183: 自由流通市值
        """
        try:
            import httpx
            
            # 构造股票代码格式: 0.XXXXXX(深圳) / 1.XXXXXX(上海)
            if stock_code.startswith("6"):
                secid = f"1.{stock_code}"
            else:
                secid = f"0.{stock_code}"
            
            url = "https://push2.eastmoney.com/api/qt/stock/get"
            params = {
                "secid": secid,
                "fields": "f168,f183"  # f168=换手率, f183=自由流通市值
            }
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Referer": "https://quote.eastmoney.com/",
            }
            
            async with httpx.AsyncClient(timeout=5, headers=headers) as client:
                resp = await client.get(url, params=params)
                data = resp.json().get("data", {})
                
                result = {}
                # f168 是万分比，需要除以100得到百分比
                f168 = data.get("f168")
                if f168 and isinstance(f168, (int, float)):
                    result["turnover_rate"] = round(f168 / 100, 2)
                
                f183 = data.get("f183")
                if f183 and isinstance(f183, (int, float)) and f183 > 0:
                    result["free_float"] = float(f183)
                
                return result if result else None
        except Exception:
            pass
        return None
    
    async def _enrich_real_turnover(self, data_list: List[Dict]):
        """批量获取个股实时数据，更新换手率和自由流通市值
        
        从个股API获取更准确的换手率(f168)和自由流通市值(f183)
        """
        today = date.today()
        
        # 缓存日期不是今天则清空
        if self._free_float_cache_date != today:
            self._free_float_cache.clear()
            self._free_float_cache_date = today
        
        # 找出需要获取数据的股票
        codes_to_fetch = []
        for item in data_list:
            code = item.get("stock_code", "")
            if code and code not in self._free_float_cache:
                codes_to_fetch.append(code)
        
        # 并行获取（限制并发数为5，避免请求过快）
        if codes_to_fetch:
            semaphore = asyncio.Semaphore(5)
            success_count = 0
            
            async def fetch_with_limit(code: str):
                nonlocal success_count
                async with semaphore:
                    result = await self._fetch_stock_realtime(code)
                    if result:
                        self._free_float_cache[code] = result
                        success_count += 1
            
            await asyncio.gather(*[fetch_with_limit(c) for c in codes_to_fetch])
            logger.info(f"[{self.name}] 个股实时数据获取: {success_count}/{len(codes_to_fetch)} 只")
        
        # 更新换手率和自由流通市值
        enriched = 0
        for item in data_list:
            code = item.get("stock_code", "")
            cached = self._free_float_cache.get(code)
            
            if cached:
                # 更新换手率（从个股API获取的f168更准确）
                if "turnover_rate" in cached:
                    item["turnover_rate"] = cached["turnover_rate"]
                
                # 记录自由流通市值（万元）
                if "free_float" in cached and cached["free_float"] > 0:
                    item["free_float_value"] = round(cached["free_float"] / 10000, 2)
                
                enriched += 1
        
        if enriched:
            logger.info(f"[{self.name}] 实时数据已更新: {enriched}/{len(data_list)} 只")
    
    def parse(self, content: Any, is_sealed: bool = True) -> List[Dict]:
        """解析API响应"""
        if not isinstance(content, dict):
            return []
        
        data_obj = content.get("data", {})
        if not data_obj:
            return []
        
        pool_data = data_obj.get("pool", [])
        result = []
        
        for item in pool_data:
            try:
                # 股票代码
                code = item.get("c", "")
                if not code:
                    continue
                
                # 股票名称
                name = item.get("n", "")
                
                # 涨停时间 (格式: "093125" -> "09:31:25")
                fbt_str = item.get("fbt", "")
                limit_up_time = None
                if fbt_str:
                    try:
                        fbt_str = str(fbt_str).zfill(6)
                        hour = int(fbt_str[:2])
                        minute = int(fbt_str[2:4])
                        second = int(fbt_str[4:6])
                        today = date.today()
                        limit_up_time = datetime(today.year, today.month, today.day, hour, minute, second)
                    except:
                        pass
                
                # 最后封板时间 (格式同上)
                lbt_str = item.get("lbt", "")
                last_limit_up_time = None
                if lbt_str:
                    try:
                        lbt_str = str(lbt_str).zfill(6)
                        hour = int(lbt_str[:2])
                        minute = int(lbt_str[2:4])
                        second = int(lbt_str[4:6])
                        today = date.today()
                        last_limit_up_time = datetime(today.year, today.month, today.day, hour, minute, second)
                    except:
                        pass
                
                # 涨停原因/题材
                hybk = item.get("hybk", "")  # 行业板块
                
                # 连板数
                lbc = item.get("lbc", 1)  # 连板次数
                
                # 涨停价
                price = item.get("p", 0)
                if price:
                    price = price / 1000  # 东方财富价格需要除以1000
                
                # 流通市值
                float_market_value = item.get("ltsz", 0) or item.get("lt", 0) or item.get("float_market_value", 0)
                
                # 成交额(元)
                amount_raw = item.get("amount", 0)
                amount = amount_raw / 10000 if amount_raw else 0  # 转为万元
                
                # 换手率：直接使用东财原始值
                hs = item.get("hs", 0)
                if isinstance(hs, (int, float)) and hs > 0:
                    hs = round(hs, 2)
                else:
                    hs = 0
                
                # 封单金额
                fund = item.get("fund", 0)
                if fund:
                    fund = fund / 10000  # 转为万元
                
                # 开板次数 (炸板池使用zbc字段)
                oc = item.get("oc", 0) or item.get("zbc", 0)
                
                result.append({
                    "stock_code": code,
                    "stock_name": name,
                    "first_limit_up_time": limit_up_time,
                    "final_seal_time": last_limit_up_time,
                    "limit_up_reason": hybk,
                    "reason_category": self._classify_reason(hybk),
                    "continuous_limit_up_days": lbc,
                    "limit_up_price": price,
                    "turnover_rate": hs,
                    "float_market_value": float_market_value,  # 流通市值
                    "amount": amount,
                    "seal_amount": fund,
                    "open_count": oc,
                    "is_final_sealed": is_sealed,
                    "data_source": "EM"
                })
                
            except Exception as e:
                logger.warning(f"[{self.name}] Parse item error: {e}")
                continue
        
        return result
    
    def _classify_reason(self, reason: str) -> str:
        """分类涨停原因"""
        if not reason:
            return "其他"
        
        category_keywords = {
            "新能源": ["新能源", "锂电", "光伏", "风电", "储能", "充电桩", "电池", "氢能"],
            "人工智能": ["AI", "人工智能", "算力", "大模型", "机器人", "智能", "算力"],
            "半导体": ["半导体", "芯片", "集成电路", "封装", "光刻", "晶圆", "存储"],
            "医药医疗": ["医药", "医疗", "生物", "疫苗", "创新药", "器械", "制药"],
            "军工": ["军工", "国防", "航空", "航天", "舰船", "武器"],
            "消费": ["消费", "白酒", "食品", "饮料", "零售", "电商", "酿酒"],
            "金融": ["金融", "银行", "保险", "证券", "券商"],
            "房地产": ["房地产", "地产", "房企", "物业"],
            "数字经济": ["数字经济", "数据", "云计算", "大数据", "信创", "软件"],
            "汽车": ["汽车", "整车", "零部件", "新能源车"],
        }
        
        for category, keywords in category_keywords.items():
            for keyword in keywords:
                if keyword in reason:
                    return category
        
        return "其他"


# 创建爬虫实例
em_crawler = EastMoneyCrawler()
