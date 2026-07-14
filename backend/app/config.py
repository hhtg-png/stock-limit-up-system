"""
应用配置管理
"""
from pydantic_settings import BaseSettings
from typing import Optional
import os


class Settings(BaseSettings):
    """应用配置"""
    
    # 应用基础配置
    APP_NAME: str = "股票涨停统计分析系统"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = True
    
    # 数据库配置
    DATABASE_URL: str = "sqlite+aiosqlite:///./data/stock_limit_up.db"
    
    # 通达信配置
    TDX_HOST: str = "119.147.212.81"  # 通达信服务器
    TDX_PORT: int = 7709
    TDX_L2_ENABLED: bool = True  # 是否启用L2数据采集
    TDX_L2_MODE: str = "tradex"  # L2采集模式: tradex(COM组件) | memory(内存读取)
    TDX_HEARTBEAT_INTERVAL: int = 30  # 心跳间隔(秒)
    TDX_RECONNECT_ATTEMPTS: int = 5  # 最大重连尝试次数
    TDX_RECONNECT_BASE_DELAY: float = 1.0  # 重连基础延迟(秒)，指数退避
    
    # 爬虫配置
    CRAWLER_INTERVAL_THS: int = 300  # 同花顺爬虫间隔(秒)
    CRAWLER_INTERVAL_KPL: int = 600  # 开盘啦爬虫间隔(秒)
    CRAWLER_USER_AGENT: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    CRAWLER_REQUEST_TIMEOUT: int = 30
    
    # Level-2数据采集配置
    L2_COLLECT_ENABLED: bool = False  # 是否注册后台L2轮询任务；线上默认关闭，避免TDX连接阻塞
    L2_COLLECT_INTERVAL: int = 3  # Level-2采集间隔(秒)
    
    # 大单阈值配置（默认值，用户可自定义）
    DEFAULT_BIG_ORDER_VOLUME: int = 300  # 主板默认300手
    DEFAULT_BIG_ORDER_VOLUME_20CM: int = 200  # 20cm板块(科创/创业)默认200手
    
    # 涨停板配置
    LIMIT_UP_RATIO_MAIN: float = 0.10  # 主板涨停比例
    LIMIT_UP_RATIO_KC_CY: float = 0.20  # 科创板/创业板涨停比例
    LIMIT_UP_RATIO_ST: float = 0.05  # ST股票涨停比例
    
    # WebSocket配置
    WS_HEARTBEAT_INTERVAL: int = 30  # 心跳间隔(秒)
    WS_MAX_CONNECTIONS: int = 100  # 最大连接数
    WS_SEND_TIMEOUT_SECONDS: float = 1.0  # 单客户端广播发送超时

    # Tushare 配置
    TUSHARE_TOKEN: Optional[str] = None
    TUSHARE_API_URL: str = "http://api.tushare.pro"
    
    # 播报配置
    ALERT_DEDUP_INTERVAL: int = 300  # 去重间隔(秒)
    
    # 交易时间配置
    MARKET_OPEN_TIME: str = "09:30"
    MARKET_CLOSE_TIME: str = "15:00"
    MARKET_LUNCH_START: str = "11:30"
    MARKET_LUNCH_END: str = "13:00"

    # 市场复盘配置
    MARKET_REVIEW_ENABLED: bool = True
    MARKET_REVIEW_BUILD_HOUR: int = 15
    MARKET_REVIEW_BUILD_MINUTE: int = 5
    MARKET_REVIEW_REPAIR_HOUR: int = 20
    MARKET_REVIEW_REPAIR_MINUTE: int = 15
    MARKET_REVIEW_REPAIR_ENABLED: bool = True
    DAILY_ANALYSIS_INTRADAY_HOUR: int = 14
    DAILY_ANALYSIS_INTRADAY_MINUTE: int = 50

    # 交易作战手册配置
    TRADING_PLAYBOOK_ENABLED: bool = True
    TRADING_PLAYBOOK_TRANSCRIPT_ROOT: Optional[str] = None
    TRADING_PLAYBOOK_MONITOR_INTERVAL_SECONDS: int = 3
    TRADING_PLAYBOOK_CALENDAR_REFRESH_TIMEOUT_SECONDS: float = 5.0
    TRADING_PLAYBOOK_CALENDAR_RETRY_INTERVAL_SECONDS: float = 30.0
    TRADING_PLAYBOOK_KLINE_STAGE_TIMEOUT_SECONDS: float = 25.0
    TRADING_PLAYBOOK_KLINE_CANCEL_GRACE_SECONDS: float = 0.25
    TRADING_PLAYBOOK_AUCTION_QUOTE_MAX_AGE_SECONDS: float = 120.0
    TRADING_PLAYBOOK_JOB_LEASE_SECONDS: int = 300
    TRADING_PLAYBOOK_TRIAL_POSITION_PCT: float = 10.0
    TRADING_PLAYBOOK_CONFIRMED_POSITION_PCT: float = 30.0
    TRADING_PLAYBOOK_HARD_STOP_PCT: float = 5.0
    TRADING_PLAYBOOK_MAX_ACTION_CANDIDATES: int = 3

    # DeepSeek 总结配置
    DEEPSEEK_API_KEY: Optional[str] = None
    DEEPSEEK_BASE_URL: str = "https://api.deepseek.com"
    DEEPSEEK_MODEL: str = "deepseek-v4-pro"
    DEEPSEEK_REQUEST_TIMEOUT: int = 60

    # ima 知识库同步配置
    INTELLIGENCE_ENABLED: bool = True
    IMA_DAILY_REVIEW_SHARE_ID: str = "12d27469b8ae3e0fae5d6ce8242cff813010a6a5b8bf3248155c1487f099bace"
    IMA_JIEGE_SHARE_ID: str = "8fbac3ae9fb49b7d2d76a8e22f6bf866d83963ed7a9534f4d1057886fc13d161"
    IMA_SYNC_PAGE_SIZE: int = 20
    IMA_SYNC_MAX_PAGES: int = 20
    INTELLIGENCE_PROBE_INTERVAL_SECONDS: int = 60
    INTELLIGENCE_PROBE_LIMIT: int = 20

    # Obsidian 自生长知识库配置
    OBSIDIAN_ENABLED: bool = False
    OBSIDIAN_VAULT_PATH: str = ""
    OBSIDIAN_AUTO_GIT_ENABLED: bool = False
    WEB_RESEARCH_ENABLED: bool = False
    WEB_RESEARCH_ALLOWLIST: str = ""
    
    # 日志配置
    LOG_LEVEL: str = "INFO"
    LOG_FILE: str = "./logs/app.log"
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()


def get_settings() -> Settings:
    """获取配置实例"""
    return settings
