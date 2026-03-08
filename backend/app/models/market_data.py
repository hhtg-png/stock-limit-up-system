"""
统计数据和用户配置模型
"""
from sqlalchemy import Column, Integer, String, DateTime, Float, Date, JSON, Boolean, Text
from datetime import datetime, date

from app.database import Base


class DailyStatistics(Base):
    """每日统计数据表"""
    __tablename__ = "daily_statistics"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(Date, unique=True, nullable=False, comment="交易日期")
    
    # 涨停统计
    total_limit_up = Column(Integer, default=0, comment="涨停总数")
    new_limit_up = Column(Integer, default=0, comment="首板数量")
    continuous_2 = Column(Integer, default=0, comment="2连板数量")
    continuous_3 = Column(Integer, default=0, comment="3连板数量")
    continuous_4_plus = Column(Integer, default=0, comment="4连板及以上")
    
    # 炸板统计
    break_count = Column(Integer, default=0, comment="炸板数量")
    break_rate = Column(Float, default=0, comment="炸板率(%)")
    
    # 封板统计
    average_seal_time = Column(String(10), comment="平均封板时间")
    early_seal_count = Column(Integer, default=0, comment="早盘封板数(10:00前)")
    
    # 板块统计
    strongest_sector = Column(String(50), comment="最强板块")
    sector_statistics = Column(JSON, comment="板块统计详情")
    
    # 市场情绪
    total_stocks = Column(Integer, comment="全市场股票数")
    up_count = Column(Integer, comment="上涨家数")
    down_count = Column(Integer, comment="下跌家数")
    limit_down_count = Column(Integer, default=0, comment="跌停数量")
    
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")
    
    def __repr__(self):
        return f"<DailyStatistics {self.trade_date}>"


class UserConfig(Base):
    """用户配置表"""
    __tablename__ = "user_configs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # 大单配置
    big_order_threshold = Column(Float, default=500000, comment="大单金额阈值(元)-已废弃")
    big_order_volume = Column(Integer, default=300, comment="主板大单手数阈值(手)")
    big_order_volume_20cm = Column(Integer, default=200, comment="20cm板块大单手数阈值(手)")
    
    # 播报开关
    alert_limit_up_enabled = Column(Boolean, default=True, comment="涨停播报开关")
    alert_big_order_enabled = Column(Boolean, default=True, comment="大单播报开关")
    alert_sound_enabled = Column(Boolean, default=True, comment="声音播报开关")
    alert_desktop_enabled = Column(Boolean, default=True, comment="桌面通知开关")
    
    # 自选股
    watch_list = Column(JSON, default=list, comment="自选股列表")
    
    # 过滤配置
    filter_st = Column(Boolean, default=True, comment="是否过滤ST")
    filter_new_stock = Column(Boolean, default=False, comment="是否过滤次新股")
    filter_low_price = Column(Float, default=0, comment="过滤低于此价格的股票")
    filter_high_price = Column(Float, default=0, comment="过滤高于此价格的股票(0不限)")
    
    # 显示配置
    display_columns = Column(JSON, comment="显示列配置")
    chart_theme = Column(String(20), default="light", comment="图表主题")
    
    # 其他自定义配置
    custom_settings = Column(JSON, comment="其他自定义配置")
    
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")
    
    def __repr__(self):
        return f"<UserConfig {self.id}>"


class DataValidation(Base):
    """数据验证记录表"""
    __tablename__ = "data_validations"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(10), nullable=False, comment="股票代码")
    trade_date = Column(Date, nullable=False, comment="交易日期")
    
    source_a = Column(String(20), comment="数据源A")
    source_b = Column(String(20), comment="数据源B")
    field_name = Column(String(50), comment="字段名")
    value_a = Column(Text, comment="源A值")
    value_b = Column(Text, comment="源B值")
    
    diff_type = Column(String(20), comment="差异类型(time/reason/status)")
    diff_seconds = Column(Integer, comment="时间差异(秒)")
    
    is_resolved = Column(Boolean, default=False, comment="是否已解决")
    resolution = Column(Text, comment="解决方案")
    
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    
    def __repr__(self):
        return f"<DataValidation {self.stock_code} {self.trade_date}>"


class CrawlerTask(Base):
    """爬虫任务记录表"""
    __tablename__ = "crawler_tasks"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    task_type = Column(String(20), nullable=False, comment="任务类型(THS/KPL/TDX)")
    
    start_time = Column(DateTime, nullable=False, comment="开始时间")
    end_time = Column(DateTime, comment="结束时间")
    
    status = Column(String(20), default="running", comment="状态(running/success/failed)")
    records_count = Column(Integer, default=0, comment="采集记录数")
    
    error_message = Column(Text, comment="错误信息")
    
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    
    def __repr__(self):
        return f"<CrawlerTask {self.task_type} {self.status}>"
