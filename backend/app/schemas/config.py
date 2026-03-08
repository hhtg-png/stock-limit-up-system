"""
配置相关数据模型
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict


class UserConfigBase(BaseModel):
    """用户配置基础"""
    big_order_volume: int = Field(300, description="主板大单手数阈值(手)")
    big_order_volume_20cm: int = Field(200, description="20cm板块(科创/创业)大单手数阈值(手)")
    alert_limit_up_enabled: bool = Field(True, description="涨停播报开关")
    alert_big_order_enabled: bool = Field(True, description="大单播报开关")
    alert_sound_enabled: bool = Field(True, description="声音播报开关")
    alert_desktop_enabled: bool = Field(True, description="桌面通知开关")
    filter_st: bool = Field(True, description="是否过滤ST")
    filter_new_stock: bool = Field(False, description="是否过滤次新股")
    filter_low_price: float = Field(0, description="过滤低于此价格的股票")
    filter_high_price: float = Field(0, description="过滤高于此价格的股票(0不限)")
    chart_theme: str = Field("light", description="图表主题")


class UserConfigUpdate(UserConfigBase):
    """更新用户配置"""
    watch_list: Optional[List[str]] = None
    display_columns: Optional[List[str]] = None
    custom_settings: Optional[Dict] = None


class UserConfigResponse(UserConfigBase):
    """用户配置响应"""
    id: int
    watch_list: List[str] = Field(default_factory=list)
    display_columns: Optional[List[str]] = None
    
    class Config:
        from_attributes = True


class WatchListItem(BaseModel):
    """自选股项"""
    stock_code: str
    stock_name: Optional[str] = None
    add_time: Optional[str] = None


class BigOrderConfig(BaseModel):
    """大单配置"""
    threshold_amount: float = Field(..., description="金额阈值(元)")
    threshold_volume: int = Field(..., description="手数阈值(手)")
    use_dynamic: bool = Field(False, description="是否使用动态阈值")
    dynamic_ratio: float = Field(0.0005, description="动态阈值比例(流通市值)")
