# 收盘后日级复盘指标仓设计

## 1. 背景

当前项目已经具备以下基础能力：

- 实时涨停池、炸板池聚合能力
- 连板梯队和昨日连板今日表现接口
- 基础统计接口和每日统计表
- 前端基于 ECharts 的轻量统计页

现阶段缺失的是一套面向“收盘后日级复盘”的稳定指标仓。当前页面多数数据仍依赖即时聚合或轻量统计，不适合承载以下目标：

- 稳定输出复盘曲线
- 支持历史区间查询
- 支持回补和重算
- 支持后续扩展到更完整的报表分析

本设计只覆盖“收盘后日级复盘”，不覆盖盘中分时回放、竞价封单精确追踪和全量 tick 仓库建设。

## 2. 目标

构建一套可回补、可重算、口径一致的复盘指标仓，用于支撑以下报表能力：

1. 连板高度
2. 晋级率
3. 昨日涨停平均涨幅
4. 涨跌停趋势
5. 沪深量能与涨跌家数
6. 涨停股与炸板股成交金额

目标要求如下：

- 图表查询不依赖实时重算
- 指标口径在接口、页面、历史回补之间一致
- 支持按交易日幂等重跑
- 数据源异常时具备降级和修正机制

## 3. 非目标

第一版明确不做以下内容：

- 竞价一字涨停封单
- 分钟级或 tick 级全市场行情仓
- 盘中分时复盘回放
- 多数据源逐笔仲裁引擎
- 独立 BI 平台接入实施

说明：

- 后续如需接入 DataEase、Metabase、Superset 或 Datart，应复用本设计产出的聚合表，不改变数据底座。

## 4. 外部参考与选型

### 4.1 图表层

- Apache ECharts
  - 用途：继续承载项目内图表展示
  - 参考仓库：https://github.com/apache/echarts
  - 结论：适合折线图、柱状图、混合图和多序列趋势图

### 4.2 数据回补与日级指标

- AKShare
  - 用途：历史日级数据回补、市场量能、宽度类指标补数
  - 参考仓库：https://github.com/akfamily/akshare
  - 结论：适合历史补数，不适合作为高实时唯一主链路

- easyquotation
  - 用途：轻量行情字段补充
  - 参考仓库：https://github.com/shidenggui/easyquotation
  - 结论：可作为辅源，不作为第一版复盘主口径来源

- adata
  - 用途：历史或行情字段补充
  - 参考仓库：https://github.com/1nchaos/adata
  - 结论：可作为补数辅源

### 4.3 不作为第一版主体方案的项目

- XtQuant
  - 参考仓库：https://github.com/ai4trade/XtQuant
  - 原因：适合更高实时性场景，对“收盘后日级复盘”属于过配

- vn.py
  - 参考仓库：https://github.com/vnpy/vnpy
  - 原因：偏量化交易平台，不适合作为当前报表底座

- QUANTAXIS
  - 参考仓库：https://github.com/yutiansut/QUANTAXIS
  - 原因：体系较重，接入成本高于当前目标收益

## 5. 总体方案

### 5.1 核心原则

- 不建设“全量行情仓”，只建设“复盘指标仓”
- 日级报表只认收盘后最终结果
- 页面查询优先读取聚合表，不依赖外部接口实时计算
- 历史回补与当日生成使用同一套口径和状态定义
- 所有任务必须支持幂等执行

### 5.2 分层结构

系统分为四层：

1. 采集层
   - 现有实时涨停池/炸板池聚合
   - 历史回补数据源（AKShare 为主）

2. 事实层
   - 按股票按交易日沉淀复盘事实
   - 按事件沉淀涨停、炸板、回封、收盘状态

3. 聚合层
   - 将事实层计算为日级复盘指标

4. 展示层
   - 现有前端页面通过 API 查询聚合结果
   - 后续可接 BI 平台

## 6. 指标定义

### 6.1 连板高度

- `max_board_height`
  - 定义：当日最高连板数
- `second_board_height`
  - 定义：当日次高连板数
- `gem_board_height`
  - 定义：创业板/科创板最高连板数

口径规则：

- 连板高度按“当日曾达成”的连板数计算
- 收盘是否封住只作为附加状态，不改变高度值

### 6.2 晋级率

- `first_to_second_rate`
  - 定义：昨日首板股中，今日晋级 2 板的比例
- `continuous_promotion_rate`
  - 定义：昨日 2 板及以上股票中，今日继续晋级的比例
- `seal_rate`
  - 定义：当日涨停股中，收盘封住的比例

状态机定义：

- `touch_limit`：当日曾涨停
- `sealed_close`：收盘封住
- `opened_close`：收盘未封住，但当日曾触板
- `broken`：未触及目标晋级状态或最终断板

说明：

- 所有晋级率、昨日连板今日表现、报表统计必须复用同一状态机

### 6.3 昨日涨停平均涨幅

- `yesterday_limit_up_avg_change`
  - 定义：昨日涨停股票在今日收盘的平均涨跌幅
- `yesterday_continuous_avg_change`
  - 定义：昨日连板股票在今日收盘的平均涨跌幅

说明：

- 第一版只统计收盘涨跌幅，不引入“含当日分时”口径

### 6.4 涨跌停趋势

- `continuous_count`
  - 定义：当日连板股数量
- `limit_up_count`
  - 定义：当日涨停股数量
- `limit_down_count`
  - 定义：当日跌停股数量

### 6.5 沪深量能与涨跌家数

- `market_turnover`
  - 定义：沪深两市成交额
- `up_count_ex_st`
  - 定义：不含 ST 的上涨家数
- `down_count_ex_st`
  - 定义：不含 ST 的下跌家数

口径规则：

- 第一版默认不含 ST
- 如后续需要支持是否包含 ST，应作为维度扩展，不覆盖现有字段

### 6.6 涨停股与炸板股成交金额

- `limit_up_amount`
  - 定义：当日涨停股成交总额
- `broken_amount`
  - 定义：当日炸板股成交总额

口径规则：

- 炸板股定义为“当日曾触板但收盘未封住”

## 7. 数据模型设计

### 7.1 `market_review_daily_metric`

用途：

- 一天一行，作为所有日级复盘图表的主查询表

建议字段：

- `id`
- `trade_date`
- `limit_up_count`
- `limit_down_count`
- `continuous_count`
- `max_board_height`
- `second_board_height`
- `gem_board_height`
- `first_to_second_rate`
- `continuous_promotion_rate`
- `seal_rate`
- `yesterday_limit_up_avg_change`
- `yesterday_continuous_avg_change`
- `market_turnover`
- `up_count_ex_st`
- `down_count_ex_st`
- `limit_up_amount`
- `broken_amount`
- `calc_version`
- `source_status`
- `created_at`
- `updated_at`

约束：

- `trade_date` 唯一

### 7.2 `market_review_stock_daily`

用途：

- 一天一股一行，保存复盘相关个股事实

建议字段：

- `id`
- `trade_date`
- `stock_code`
- `stock_name`
- `board_type`
- `is_st`
- `yesterday_limit_up`
- `yesterday_continuous_days`
- `today_touched_limit_up`
- `today_sealed_close`
- `today_opened_close`
- `today_broken`
- `today_continuous_days`
- `first_limit_time`
- `final_seal_time`
- `open_count`
- `close_price`
- `pre_close`
- `change_pct`
- `amount`
- `turnover_rate`
- `tradable_market_value`
- `limit_up_reason`
- `data_quality_flag`
- `created_at`
- `updated_at`

约束：

- `(trade_date, stock_code)` 唯一

### 7.3 `market_review_limitup_event`

用途：

- 保存个股关键事件轨迹，支持复盘解释和后续扩展

建议字段：

- `id`
- `trade_date`
- `stock_code`
- `event_type`
- `event_time`
- `event_seq`
- `source_name`
- `payload_json`
- `created_at`

事件类型固定枚举：

- `first_seal`
- `open_board`
- `re_seal`
- `close_sealed`
- `close_opened`
- `close_broken`

索引建议：

- `(trade_date, stock_code)`
- `(trade_date, event_type)`

## 8. 数据流与调度

### 8.1 收盘后首轮任务

时间建议：`15:01 - 15:10`

执行内容：

- 拉取当日涨停池、炸板池、跌停池
- 获取当日收盘价、昨收价、成交额、换手率等字段
- 生成并写入 `market_review_stock_daily`
- 生成并写入 `market_review_limitup_event`

要求：

- 以交易日为单位幂等执行
- 使用唯一键加 upsert

### 8.2 聚合任务

时间建议：`15:10 - 15:20`

执行内容：

- 从 `market_review_stock_daily` 聚合生成 `market_review_daily_metric`
- 不在该阶段直接依赖外部实时接口

### 8.3 晚间修正任务

时间建议：`20:00 - 21:00`

执行内容：

- 重跑当日事实层和聚合层
- 修正收盘后源站字段延迟或补录

要求：

- 使用同一计算逻辑
- 更新 `calc_version` 和 `updated_at`

### 8.4 历史回补任务

执行内容：

- 按日期区间回补事实数据
- 重算对应交易日聚合结果

策略：

- 先补 `market_review_stock_daily`
- 再算 `market_review_daily_metric`
- 按月或按周批量处理，避免一次性回补全历史

## 9. 接口设计原则

第一版建议新增以下接口，而不是直接在原始统计接口中堆逻辑：

- `/statistics/review/daily`
  - 查询日级复盘趋势数据
- `/statistics/review/detail`
  - 查询单日复盘明细
- `/statistics/review/ladder`
  - 查询单日连板高度和对应股票

接口原则：

- 图表趋势接口只查 `market_review_daily_metric`
- 单日钻取接口查 `market_review_stock_daily`
- 事件解释接口查 `market_review_limitup_event`

## 10. 异常与数据质量策略

### 10.1 收盘后源数据晚到

问题：

- 收盘后部分字段可能晚于首次任务可用

策略：

- 保留晚间修正任务
- 使用 `calc_version` 标记重算版本

### 10.2 多源字段冲突

问题：

- 不同源可能对成交额、状态、原因等字段存在差异

策略：

- 固定主源
- 辅源仅补缺，不覆盖主源主口径
- 原始差异保留在 `payload_json` 或诊断日志中

### 10.3 口径漂移

问题：

- 页面、接口、历史回补若各自计算，易产生同名指标不同值

策略：

- 所有指标统一从聚合表读取
- 所有聚合逻辑集中在单一服务层

### 10.4 重跑脏数据

问题：

- 重跑任务可能重复写入或产生旧数据残留

策略：

- 使用唯一键 + upsert
- 避免“先删后插”作为默认方案

### 10.5 历史回补与当日口径不一致

问题：

- 历史回补源和盘中实时源可能字段不完全一致

策略：

- 报表只认“收盘后最终版本”
- 历史回补使用同一状态机和聚合服务

## 11. 性能与存储优化

- 报表页只查 `market_review_daily_metric`
- 钻取某日个股时才查 `market_review_stock_daily`
- `market_review_limitup_event` 仅保存复盘相关股票，不保存全市场
- 不为两三条日级曲线去建设全市场分钟仓或 tick 仓
- 历史回补按月分批执行

## 12. 实施阶段建议

### 阶段 1：指标仓基础建设

- 新建三张核心表
- 实现收盘后事实写入和日聚合
- 打通基础趋势接口

### 阶段 2：报表页改造

- 基于聚合表重构统计页
- 新增连板高度、晋级率、昨日涨停平均涨幅等图表

### 阶段 3：回补与修正

- 增加历史回补任务
- 增加晚间修正任务
- 增加数据质量监控

### 阶段 4：可选扩展

- 接入 DataEase、Metabase 或其他 BI 平台
- 增加更多市场宽度指标
- 为第二阶段的盘中复盘预留快照能力

## 13. 测试与验证策略

### 13.1 单元测试

- 状态机判断测试
- 晋级率计算测试
- 昨日涨停平均涨幅计算测试
- 聚合服务幂等测试

### 13.2 集成测试

- 收盘事实写入测试
- 日聚合任务测试
- 历史回补任务测试

### 13.3 数据验收

- 随机抽取交易日，与人工复盘结果比对
- 对比现有页面关键指标，定位历史口径差异
- 对高波动交易日做专项校验

## 14. 决策结论

第一版采用以下路线：

- 目标限定为“收盘后日级复盘”
- 继续使用现有项目作为主承载系统
- 建设复盘指标仓，而非全量行情仓
- 图表继续使用 ECharts
- 历史回补以 AKShare 为主
- 第一版不纳入竞价封单和分时回放

该方案在实现成本、口径稳定性、历史查询能力和后续扩展性之间取得平衡，适合作为当前项目的正式落地方案。
