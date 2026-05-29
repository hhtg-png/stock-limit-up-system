# 通达信轻量独立插件设计

## 背景

当前通达信插件已经有独立路由：

- `/tdx/ztlive/dark`
- `/tdx/news/dark`
- `/tdx/strong/dark`
- `/tdx/yidong/:code?/dark`
- `/tdx/thsyd/:code?/dark`

用户实际使用方式是在通达信里把多个插件窗口分别嵌入不同区域，自行排版。因此不能把所有插件强制合并成一个大工作台。优化目标是在保留独立插件入口和布局自由度的前提下，降低多窗口同时运行时的内存、连接数、定时器和语音播报重复成本。

## 目标

1. 保留现有插件 URL，不改变用户在通达信里的排版方式。
2. 为 `/tdx/*` 提供独立轻量运行时，避免加载主系统外壳、Element Plus 全量组件、全量图标和无关页面资源。
3. 新增聚合资讯语音专用入口 `/tdx/news-voice/dark`，用于只听快讯，不渲染大列表。
4. 多个插件窗口同时打开时，尽量共享实时数据通道，避免每个窗口都独立建立 WebSocket。
5. 语音播报只允许一个窗口主控，避免重复播报、音频抢占和重复 TTS 请求。
6. 聚合快讯播报继续保留 1 分钟内标题相似度 80% 的去重规则。

## 非目标

1. 不移除现有插件页面。
2. 不强制用户使用单窗口工作台。
3. 不在第一阶段重写所有插件 UI。
4. 不把通达信外的主系统页面迁入轻量运行时。

## 方案选择

### 方案 A：单窗口工作台

把所有插件放在一个 `/tdx/workbench/black` 页面里。资源最省，但不符合用户在通达信中自由排版多个区域的使用方式。

结论：不作为主方案。

### 方案 B：保留独立插件，但只做局部性能优化

保留现有入口，只减少列表数量和刷新频率。实现简单，但每个窗口仍会加载主系统入口、创建独立 WebSocket、独立语音队列和独立定时器，无法解决多窗口卡顿的根因。

结论：收益不足。

### 方案 C：独立插件 + TDX 轻量运行时 + 共享实时/语音主控

保留每个插件独立 URL，同时把 `/tdx/*` 从主系统入口拆出。独立插件继续支持通达信布局；运行时、WebSocket、语音播报和缓存做轻量化与共享。

结论：采用该方案。

## 架构设计

### 1. TDX 轻量入口

新增轻量入口文件：

- `frontend/src/tdx-main.ts`
- `frontend/src/TdxApp.vue`
- `frontend/src/router/tdx.ts`

主入口 `frontend/src/main.ts` 只负责判断当前路径：

- `/tdx/*` 加载 `tdx-main.ts`
- 其他路径加载现有主系统入口

TDX 入口不使用主系统 `App.vue`，不初始化主系统侧边栏、抽屉、移动端导航和主系统全局 WebSocket。TDX 入口只安装 Pinia、TDX 路由和必要的轻量样式。

Element Plus 处理原则：

- 第一阶段不在 TDX 轻量入口全量 `app.use(ElementPlus)`。
- TDX 插件页面优先使用原生 HTML/CSS。
- 如果个别控件确实需要 Element Plus，改为局部导入组件，不注册全量库和全量图标。

### 2. 保留现有插件路由

TDX 路由保留：

- `/tdx`
- `/tdx/ztlive/dark`
- `/tdx/news/dark`
- `/tdx/news-voice/dark`
- `/tdx/strong/dark`
- `/tdx/yidong/:code?/dark`
- `/tdx/thsyd/:code?/dark`

现有主路由中的 `/tdx/*` 可保留为兼容声明，但实际入口由路径判断进入 TDX 轻量运行时。

### 3. 聚合资讯语音插件

新增 `/tdx/news-voice/dark`。它是语音专用插件，不展示大列表，只展示最小必要状态：

- 播报开关
- WebSocket/共享通道连接状态
- 最近播报标题
- 今日已播报数量
- 最近一次更新时间
- 错误提示

该插件默认成为聚合快讯语音主控窗口。它负责：

- 接收聚合快讯事件
- 标题相似度去重
- 统一调用后端神经 TTS
- 防止多个窗口重复播报

如果该窗口没有打开，`/tdx/news/dark` 可以接管播报；其他插件默认不播报聚合快讯。

### 4. 多窗口实时共享

新增 `tdxRealtimeHub`，提供统一实时数据接口：

- `subscribe(topic, handler)`
- `publish(message)`
- `connect(topics)`
- `disconnect()`
- `status`

共享优先级：

1. `SharedWorker`
2. `BroadcastChannel`
3. `localStorage` leader 选举
4. 每窗口独立 WebSocket 兜底

第一阶段可以先完成轻量入口和语音插件，第二阶段再实现完整共享通道。

### 5. WebSocket Topic 订阅

后端 WebSocket 增加插件 topic 概念，减少无关消息：

- `limitup`
- `news`
- `strong`
- `stock_move`

示例：

```text
/ws/tdx-plugins?topics=limitup,news
```

独立插件只订阅自己需要的 topic。共享通道开启时，leader 根据当前已打开窗口的需求合并 topic。

### 6. 语音主控

新增 `tdxVoiceHub`，只允许一个 voice leader 播报：

- 优先级最高：`/tdx/news-voice/dark`
- 其次：`/tdx/news/dark`
- 兜底：第一个开启语音的插件窗口

所有聚合快讯播报请求进入 voice hub。voice hub 负责：

- 稳定 key 去重
- 1 分钟内相似度 80% 去重
- TTS 请求节流
- 当前播报状态广播
- 页面关闭时释放 leader

### 7. 插件资源预算

每个插件默认执行以下限制：

- 列表最多保留 80 到 120 条。
- 页面不可见时降低刷新频率。
- 可见且连接正常时优先使用 WebSocket。
- 定时轮询只作为兜底。
- 不使用 ECharts 展示 TDX 插件图形。
- 不在独立插件中加载主系统 AlertPanel、主布局、主导航。
- 每个窗口最多一个 audio 元素。

## 数据流

### 聚合资讯语音

1. 后端聚合资讯 watcher 发现新快讯。
2. WebSocket 推送 `tdx_news_event`。
3. `tdxRealtimeHub` 收到事件并广播给所有 TDX 插件窗口。
4. `/tdx/news-voice/dark` 或当前 voice leader 接管播报。
5. voice leader 执行 key 去重和标题相似度去重。
6. 通过 `/api/v1/tts/speech` 获取神经 TTS 音频并播放。
7. 播报状态同步给其他窗口。

### 独立插件展示

1. 插件页面启动时向 `tdxRealtimeHub` 订阅需要的 topic。
2. 初始数据通过 HTTP API 加载。
3. 后续数据优先由共享实时通道增量更新。
4. HTTP 定时刷新作为兜底，不作为主实时链路。

## 错误处理

1. SharedWorker 不可用时降级到 BroadcastChannel。
2. BroadcastChannel 不可用时降级到 localStorage leader。
3. leader 超过 5 秒无心跳，其他窗口重新选举。
4. WebSocket 断开时指数退避重连。
5. TTS 获取失败时，降级浏览器 Web Speech。
6. 语音主控窗口关闭后，其他可播报窗口接管。

## 测试计划

### 单元/静态测试

- TDX 路由仍包含所有现有插件入口和 `/tdx/news-voice/dark`。
- TDX 轻量入口不导入主系统 `App.vue`。
- TDX 轻量入口不全量注册 Element Plus 和所有图标。
- 聚合资讯语音插件使用 voice hub。
- 语音相似度去重只应用于聚合快讯。

### 前端构建验证

- `npm run build`
- 检查主系统和 TDX 入口都能被构建。
- 检查 TDX chunk 没有包含主系统布局和大型图表依赖。

### 浏览器验证

- 打开 `/tdx/ztlive/dark`、`/tdx/news/dark`、`/tdx/news-voice/dark`、`/tdx/strong/dark`。
- 验证页面黑底布局不变。
- 验证通达信个股联动仍使用 `http://www.treeid/CODE_xxxxxx`。
- 验证聚合资讯语音主控只播报一次。

### 多窗口验证

- 同时打开 3 个 TDX 插件。
- 检查真实 WebSocket 连接数量尽量为 1。
- 打开 `/tdx/news-voice/dark` 后，其他窗口不重复播报聚合快讯。
- 关闭语音主控窗口后，其他窗口能接管或明确显示未启用语音。

## 分阶段实施

### 第一阶段：轻量入口和语音插件

1. 拆分主系统入口和 TDX 轻量入口。
2. 新增 TDX 路由文件。
3. 迁移现有 `/tdx/*` 到 TDX 轻量运行时。
4. 新增 `/tdx/news-voice/dark`。
5. 聚合资讯语音开关只放到语音插件和聚合资讯页。
6. 保留现有插件 URL。

### 第二阶段：共享实时通道

1. 新增 `tdxRealtimeHub`。
2. 实现 SharedWorker 优先的共享 WebSocket。
3. 增加 BroadcastChannel/localStorage 降级。
4. 插件页面改为订阅 hub，不直接各自连接 WebSocket。

### 第三阶段：语音主控和资源预算

1. 新增 `tdxVoiceHub`。
2. 实现 voice leader。
3. 统一聚合快讯相似度去重。
4. 页面隐藏降频。
5. 列表上限和缓存上限统一配置。

## 验收标准

1. 现有 TDX 插件 URL 全部可用。
2. `/tdx/news-voice/dark` 可作为极小语音播报窗口使用。
3. TDX 插件不加载主系统侧边栏和主系统抽屉。
4. 多开插件时不重复播报聚合资讯。
5. 聚合快讯 1 分钟内相似度 80% 以上不重复播报。
6. 通达信个股点击联动不回退到项目内股票详情页。
7. 构建通过，现有主系统页面不受影响。
