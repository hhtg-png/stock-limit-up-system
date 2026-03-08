"""
同花顺数据爬虫
支持多种API和页面解析方式获取涨停数据
"""
from typing import List, Dict, Optional, Any
from datetime import datetime, date
import re
import json
from bs4 import BeautifulSoup
from loguru import logger

from app.crawlers.base_crawler import BaseCrawler


class TongHuaShunCrawler(BaseCrawler):
    """同花顺涨停数据爬虫"""
    
    # 同花顺问财涨停API（更稳定）
    WENCAI_API = "https://www.iwencai.com/gateway/urp/v7/landing/getDataList"
    # 同花顺涨停池API
    LIMIT_UP_API = "https://data.10jqka.com.cn/dataapi/limit_up/limit_up_pool"
    # 同花顺问财搜索API
    WENCAI_SEARCH_API = "https://www.iwencai.com/customized/chart/get-robot-data"
    # 备用：同花顺行情中心涨停板
    HQ_LIMIT_UP_URL = "https://q.10jqka.com.cn/index/index/board/all/field/zdf/order/desc/page/1/ajax/1/"
    
    def __init__(self):
        super().__init__("TongHuaShun")
        self._min_interval = 2.0  # 同花顺需要更长的间隔
    
    def get_headers(self) -> Dict[str, str]:
        """获取同花顺专用请求头"""
        headers = super().get_headers()
        headers.update({
            "Referer": "https://www.10jqka.com.cn/",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        })
        return headers
    
    async def crawl(self) -> List[Dict]:
        """爬取涨停数据"""
        try:
            # 方法1: 尝试涨停池API（最稳定，包含涨停原因）
            data = await self._crawl_api()
            if data:
                logger.info(f"[{self.name}] Crawled {len(data)} limit up stocks via limit_up_pool API")
                return data
            
            # 方法2: 尝试问财搜索API（需要认证，可能失败）
            logger.info(f"[{self.name}] limit_up_pool API failed, trying Wencai Search...")
            data = await self._crawl_wencai_search()
            if data:
                logger.info(f"[{self.name}] Crawled {len(data)} limit up stocks via Wencai Search")
                return data
            
            # 方法3: 尝试问财API
            data = await self._crawl_wencai()
            if data:
                logger.info(f"[{self.name}] Crawled {len(data)} limit up stocks via Wencai")
                return data
            
            # 方法4: 尝试行情中心页面
            logger.warning(f"[{self.name}] All APIs failed, trying HQ page...")
            data = await self._crawl_hq_page()
            if data:
                logger.info(f"[{self.name}] Crawled {len(data)} limit up stocks via HQ page")
                return data
            
            return []
        except Exception as e:
            logger.error(f"[{self.name}] Crawl error: {e}")
            return []
    
    async def _crawl_wencai_search(self) -> Optional[List[Dict]]:
        """通过问财搜索API获取涨停数据（包含涨停原因）"""
        try:
            await self.init_client()
            
            headers = self.get_headers()
            headers["Host"] = "www.iwencai.com"
            headers["Origin"] = "https://www.iwencai.com"
            headers["Content-Type"] = "application/json"
            
            # 问财搜索请求
            payload = {
                "question": "今日涨停 涨停原因",
                "perpage": 100,
                "page": 1,
                "secondary_intent": "stock",
                "log_info": '{"input_type":"typewrite"}',
                "source": "Ths_iwencai_Xuangu",
                "version": "2.0"
            }
            
            response = await self.client.post(
                self.WENCAI_SEARCH_API,
                json=payload,
                headers=headers,
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                return self._parse_wencai_search(data)
            
            return None
        except Exception as e:
            logger.warning(f"[{self.name}] Wencai search error: {e}")
            return None
    
    def _parse_wencai_search(self, content: Dict) -> List[Dict]:
        """解析问财搜索API响应"""
        result = []
        try:
            data = content.get("data", {})
            answer = data.get("answer", [])
            
            if not answer:
                return []
            
            # 找到包含数据的答案
            for ans in answer:
                txt = ans.get("txt", [])
                for item in txt:
                    if item.get("type") == "table":
                        content_data = item.get("content", {})
                        components = content_data.get("components", [])
                        
                        for comp in components:
                            if comp.get("type") == "table":
                                rows = comp.get("data", {}).get("datas", [])
                                for row in rows:
                                    code = row.get("code", "") or row.get("股票代码", "")
                                    name = row.get("股票简称", "") or row.get("name", "")
                                    reason = row.get("涨停原因类别", "") or row.get("涨停原因", "")
                                    
                                    if code:
                                        result.append({
                                            "stock_code": str(code).zfill(6),
                                            "stock_name": name,
                                            "limit_up_reason": reason,
                                            "reason_category": self._classify_reason(reason),
                                            "data_source": "THS"
                                        })
        except Exception as e:
            logger.warning(f"[{self.name}] Parse wencai search error: {e}")
        
        return result
    
    async def _crawl_wencai(self) -> Optional[List[Dict]]:
        """通过问财API获取涨停数据"""
        try:
            headers = self.get_headers()
            headers["Host"] = "www.iwencai.com"
            headers["Origin"] = "https://www.iwencai.com"
            headers["Referer"] = "https://www.iwencai.com/unifiedwap/result?w=%E4%BB%8A%E6%97%A5%E6%B6%A8%E5%81%9C"
            
            # 问财查询参数
            params = {
                "query": "今日涨停",
                "urp_sort_way": "desc",
                "urp_sort_index": "最新涨跌幅",
                "page": 1,
                "perpage": 200,
                "addheaderindexes": "",
                "condition": '[{"indexName":"涨跌幅","indexProperties":["nomark","交易日期"],"source":"new_parser","type":"index","indexPropertiesMap":{"交易日期":"20231201"}}]',
                "codelist": "",
                "indexnamelimit": "",
                "ret": "json_all",
                "source": "Ths_iwencai_Xuangu",
                "urp_use_sort": 1,
                "uuids": "",
                "query_type": "stock",
                "comp_id": 6836372,
                "business_cat": "soniu",
                "uuid": "",
            }
            
            json_data = await self.fetch_json(self.WENCAI_API, params=params, headers=headers)
            
            if json_data and json_data.get("answer"):
                return self._parse_wencai(json_data)
            
            return None
        except Exception as e:
            logger.warning(f"[{self.name}] Wencai API error: {e}")
            return None
    
    def _parse_wencai(self, content: Dict) -> List[Dict]:
        """解析问财API响应"""
        result = []
        try:
            answer = content.get("answer", {})
            components = answer.get("components", [])
            
            for comp in components:
                if comp.get("type") == "table":
                    data = comp.get("data", {})
                    datas = data.get("datas", [])
                    
                    for item in datas:
                        try:
                            code = item.get("code", "")
                            name = item.get("股票简称", "") or item.get("name", "")
                            
                            # 涨停原因字段可能有多种名称
                            reason = (item.get("涨停原因类别", "") or 
                                     item.get("涨停原因", "") or 
                                     item.get("所属概念", ""))
                            
                            result.append({
                                "stock_code": code,
                                "stock_name": name,
                                "limit_up_reason": reason,
                                "reason_category": self._classify_reason(reason),
                                "continuous_limit_up_days": item.get("连续涨停天数", 1),
                                "turnover_rate": item.get("换手率", 0),
                                "data_source": "THS"
                            })
                        except Exception as e:
                            logger.debug(f"Parse wencai item error: {e}")
                            continue
        except Exception as e:
            logger.error(f"[{self.name}] Parse wencai error: {e}")
        
        return result
    
    async def _crawl_api(self) -> Optional[List[Dict]]:
        """通过涨停池API获取涨停数据"""
        try:
            # 使用独立的client确保请求头干净
            import httpx
            
            headers = {
                "User-Agent": self.get_random_user_agent(),
                "Referer": "https://data.10jqka.com.cn/",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "zh-CN,zh;q=0.9",
            }
            
            # 注意：limit最大200，超过会返回-1
            params = {
                "page": 1,
                "limit": 200,
                "field": "199112,10,9001,330323,330324,330325,9002,330329,133,330326,330327,330328",
                "filter": "HS,GEM2STAR",
                "order_field": "330324",
                "order_type": 0,
            }
            
            async with httpx.AsyncClient(verify=False, timeout=15) as client:
                response = await client.get(
                    self.LIMIT_UP_API, 
                    params=params, 
                    headers=headers
                )
                
                if response.status_code == 200:
                    json_data = response.json()
                    if json_data and json_data.get("status_code") == 0:
                        result = self.parse(json_data)
                        if result:
                            return result
                        else:
                            logger.warning(f"[{self.name}] Limit up pool API parse returned empty")
                    else:
                        logger.warning(f"[{self.name}] Limit up pool API status_code: {json_data.get('status_code') if json_data else 'None'}")
                else:
                    logger.warning(f"[{self.name}] Limit up pool API HTTP status {response.status_code}")
            
            return None
        except Exception as e:
            logger.warning(f"[{self.name}] Limit up pool API error: {e}")
            return None
    
    async def _crawl_hq_page(self) -> Optional[List[Dict]]:
        """通过行情中心页面获取涨停股票"""
        try:
            headers = self.get_headers()
            headers["Host"] = "q.10jqka.com.cn"
            
            html = await self.fetch_html(self.HQ_LIMIT_UP_URL, headers=headers)
            if html:
                return self._parse_hq_html(html)
            return None
        except Exception as e:
            logger.warning(f"[{self.name}] HQ page error: {e}")
            return None
    
    def _parse_hq_html(self, html: str) -> List[Dict]:
        """解析行情中心HTML页面"""
        result = []
        try:
            soup = BeautifulSoup(html, 'lxml')
            # 查找股票表格
            table = soup.find('table', class_='m-table')
            if not table:
                # 尝试其他表格格式
                table = soup.find('table')
            
            if not table:
                return []
            
            rows = table.find_all('tr')[1:]  # 跳过表头
            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 3:
                    continue
                
                try:
                    # 提取股票代码和名称
                    code_cell = cols[1] if len(cols) > 1 else cols[0]
                    name_cell = cols[2] if len(cols) > 2 else cols[1]
                    
                    code = code_cell.get_text(strip=True)
                    name = name_cell.get_text(strip=True)
                    
                    # 涨幅检查（确认是涨停）
                    change_cell = cols[5] if len(cols) > 5 else None
                    if change_cell:
                        change_text = change_cell.get_text(strip=True).replace('%', '')
                        try:
                            change = float(change_text)
                            # 涨幅>=9.9%认为是涨停
                            if change < 9.9:
                                continue
                        except:
                            pass
                    
                    result.append({
                        "stock_code": code,
                        "stock_name": name,
                        "limit_up_reason": "",  # 行情页面没有涨停原因
                        "reason_category": "其他",
                        "data_source": "THS"
                    })
                except Exception as e:
                    logger.debug(f"Parse HQ row error: {e}")
                    continue
        except Exception as e:
            logger.error(f"[{self.name}] HQ HTML parse error: {e}")
        
        return result
    
    def parse(self, content: Any) -> List[Dict]:
        """解析API响应"""
        if not isinstance(content, dict):
            return []
        
        data_list = content.get("data", {}).get("info", [])
        result = []
        
        for item in data_list:
            try:
                # 解析涨停时间（API返回时间戳格式）
                limit_up_time_str = item.get("first_limit_up_time", "")
                limit_up_time = None
                if limit_up_time_str:
                    try:
                        # 格式可能是 "09:31:25" 或时间戳
                        if ":" in str(limit_up_time_str):
                            today = date.today()
                            limit_up_time = datetime.strptime(
                                f"{today} {limit_up_time_str}", "%Y-%m-%d %H:%M:%S"
                            )
                        else:
                            limit_up_time = datetime.fromtimestamp(int(limit_up_time_str))
                    except:
                        pass
                
                # 解析最后封板时间
                last_limit_up_time_str = item.get("last_limit_up_time", "")
                last_limit_up_time = None
                if last_limit_up_time_str:
                    try:
                        if ":" in str(last_limit_up_time_str):
                            today = date.today()
                            last_limit_up_time = datetime.strptime(
                                f"{today} {last_limit_up_time_str}", "%Y-%m-%d %H:%M:%S"
                            )
                        else:
                            last_limit_up_time = datetime.fromtimestamp(int(last_limit_up_time_str))
                    except:
                        pass
                
                # 解析涨停原因 - 新API字段是 reason_type
                reason = item.get("reason_type", "") or item.get("limit_up_reason", "")
                reason_category = self._classify_reason(reason)
                
                # 解析连板天数 - 从 high_days 解析（如"首板"=1, "2连板"=2）
                continuous_days = 1
                high_days = item.get("high_days", "")
                if high_days:
                    if "首板" in high_days:
                        continuous_days = 1
                    else:
                        # 尝试提取数字（如"2连板" -> 2）
                        import re
                        match = re.search(r'(\d+)', high_days)
                        if match:
                            continuous_days = int(match.group(1))
                
                # 开板次数 - 新API字段是 open_num
                open_count = item.get("open_num", 0) or item.get("break_times", 0)
                
                # 价格 - 新API字段是 latest
                price = item.get("latest", 0) or item.get("price", 0)
                
                # 判断是否封板 - change_tag 包含 LIMIT_BACK 表示炸板回封
                change_tag = item.get("change_tag", "")
                is_sealed = "LIMIT" in change_tag or open_count == 0
                
                result.append({
                    "stock_code": item.get("code", ""),
                    "stock_name": item.get("name", ""),
                    "first_limit_up_time": limit_up_time,
                    "final_seal_time": last_limit_up_time,
                    "limit_up_reason": reason,
                    "reason_category": reason_category,
                    "continuous_limit_up_days": continuous_days,
                    "limit_up_price": float(price) if price else 0,
                    "turnover_rate": float(item.get("turnover_rate", 0) or 0),
                    "amount": float(item.get("amount", 0) or 0) / 10000,  # 转换为万元
                    "open_count": open_count,
                    "is_final_sealed": is_sealed,
                    "seal_amount": float(item.get("order_amount", 0) or 0) / 10000,  # 封单金额(万元)
                    "data_source": "THS"
                })
            except Exception as e:
                logger.warning(f"[{self.name}] Parse item error: {e}")
                continue
        
        return result
    
    def _classify_reason(self, reason: str) -> str:
        """
        分类涨停原因
        
        基于关键词进行分类，扩展更多题材
        """
        if not reason:
            return "其他"
        
        # 题材分类关键词映射（扩展版）
        category_keywords = {
            "人工智能": ["AI", "人工智能", "算力", "大模型", "ChatGPT", "机器人", "智能", 
                       "智能体", "深度学习", "机器学习", "自动驾驶", "无人驾驶", "人形机器人"],
            "半导体": ["半导体", "芯片", "集成电路", "封装", "光刻", "晶圆", "存储", 
                      "先进封装", "EDA", "GPU", "CPU", "MCU", "IGBT", "碳化硅", "氮化镓"],
            "新能源": ["新能源", "锂电", "光伏", "风电", "储能", "充电桩", "电池", 
                      "氢能", "钠电池", "固态电池", "光伏组件", "逆变器", "特高压"],
            "数字经济": ["数字经济", "数据", "云计算", "大数据", "信创", "软件", 
                       "数据要素", "数字中国", "数字政府", "数字货币", "区块链", "Web3"],
            "医药医疗": ["医药", "医疗", "生物", "疫苗", "创新药", "CXO", "器械", 
                       "中药", "医美", "基因", "细胞治疗", "体外诊断", "仿制药"],
            "军工": ["军工", "国防", "航空", "航天", "军民融合", "舰船", "卫星", 
                    "导弹", "雷达", "军机", "无人机", "北斗"],
            "消费": ["消费", "白酒", "食品", "饮料", "零售", "电商", "酿酒", 
                    "乳业", "调味品", "预制菜", "免税", "旅游", "酒店", "餐饮"],
            "金融": ["金融", "银行", "保险", "证券", "券商", "信托", "期货", "资管"],
            "房地产": ["房地产", "地产", "房企", "物业", "城投", "土地", "住宅"],
            "汽车": ["汽车", "整车", "零部件", "新能源车", "电动车", "智能汽车", 
                    "汽车电子", "轮胎", "座椅", "电机", "电控"],
            "通信": ["通信", "5G", "6G", "光模块", "光纤", "基站", "天线", "卫星通信"],
            "传媒": ["传媒", "游戏", "影视", "短视频", "直播", "元宇宙", "虚拟现实", 
                    "VR", "AR", "MCN", "网红经济"],
            "重组": ["重组", "并购", "借壳", "资产注入", "整合", "收购", "增发"],
            "业绩": ["业绩", "预增", "净利润", "营收", "扭亏", "高增长", "超预期"],
            "次新股": ["次新", "上市", "新股", "IPO"],
        }
        
        for category, keywords in category_keywords.items():
            for keyword in keywords:
                if keyword in reason:
                    return category
        
        return "其他"


# 创建爬虫实例
ths_crawler = TongHuaShunCrawler()
