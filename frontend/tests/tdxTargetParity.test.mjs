import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')

function read(path) {
  return readFileSync(resolve(root, path), 'utf8')
}

const ztlive = read('src/views/tdx/TdxLimitUpLive.vue')
const composite = read('src/views/tdx/TdxCompositeWatch.vue')
assert.match(ztlive, /id="plates"/, '涨停播报 should expose target-style plate filter bar')
for (const label of ['名称', '代码', '涨幅', '状态', '首封', '封单', '板块']) {
  assert.match(ztlive, new RegExp(label), `涨停播报 should use target column ${label}`)
}
assert.doesNotMatch(ztlive, /title="涨停播报"/, '涨停播报 should not use the generic shell title chrome')
assert.match(ztlive, /target-ztlive/, '涨停播报 should use a dedicated target parity class')
assert.match(ztlive, /dates-container/, '涨停播报 plate filter should keep the target draggable dates-container wrapper')
assert.doesNotMatch(ztlive, /embedded-move-body|move-panel-resizer|getTdxStockMove/, '涨停播报 should stay as a pure broadcast table')
assert.match(composite, /embedded-move-body/, '复合看盘 should include an independently scrollable stock move analysis body')
assert.match(composite, /overflow:\s*auto/, '复合看盘 table and embedded analysis should scroll independently')
assert.doesNotMatch(ztlive, />\s*聚合快讯语音\s*</, '涨停播报 should not display the aggregate voice label text')
assert.doesNotMatch(ztlive, /待开启|已播|等待新快讯/, '涨停播报 should not display aggregate voice status placeholder text')
assert.match(composite, /move-panel-resizer/, '复合看盘 should allow vertical resizing between table and stock move analysis')
assert.doesNotMatch(composite, />\s*异动解析\s*</, '复合看盘 embedded stock move panel should not add a redundant title label')

const news = read('src/views/tdx/TdxNewsFeed.vue')
assert.match(news, /语音资讯/, '聚合快讯 should expose target top speech title')
assert.match(news, /聚合快讯/, '聚合快讯 should keep the main news panel')
assert.match(news, /韭研社\s*\|\s*识别区/, '聚合快讯 should keep the Jiuyan identify panel')
assert.match(news, /题材库/, '聚合快讯 should include the target topic library panel')
assert.match(news, /topicItems/, '聚合快讯 should derive a topic-library feed from available news items')
assert.match(news, /layui-timeline/, '聚合快讯 should render target-style timeline markup')
assert.match(news, /展开/, '聚合快讯 should expose target-style expand action')
assert.match(news, /target-news/, '聚合快讯 should use a dedicated target parity class')
assert.match(news, /height:\s*100dvh/, '聚合快讯 should own viewport height so its content can scroll inside the TDX iframe/window')
assert.match(news, /overflow-y:\s*auto/, '聚合快讯 should allow vertical scrolling when body overflow is hidden')

const strong = read('src/views/tdx/TdxPlateStrength.vue')
assert.match(strong, /target-strong/, '实时板块强度 should use a dedicated target black layout')
assert.match(strong, /板块轮动/, '实时板块强度 should render the target plate rotation title')
assert.match(strong, /开盘啦板块/, '实时板块强度 should expose the target Kaipanla plate tab')
assert.match(strong, /同花顺板块/, '实时板块强度 should expose the target THS plate tab')
for (const chartId of ['main1', 'main2', 'main3']) {
  assert.match(strong, new RegExp(`id="${chartId}"`), `实时板块强度 should expose target chart container ${chartId}`)
}
assert.doesNotMatch(strong, /TdxPluginShell/, '实时板块强度 should not use generic shell chrome')

const yidong = read('src/views/tdx/TdxStockMove.vue')
assert.match(yidong, /最近涨停/, '股票异动解析 should show latest limit-up date in the heading')
assert.match(yidong, /&nbsp;&nbsp;&nbsp;/, '股票异动解析 heading should keep target spacing before latest limit-up date')
assert.match(yidong, /target-yidong/, '股票异动解析 should use target-style plain text layout')
assert.match(yidong, /numberedParagraphs/, '股票异动解析 should preserve source numbered paragraphs')
assert.match(yidong, /<p v-for="line in reasonLines/, '股票异动解析 should render target-style paragraph rows')
assert.doesNotMatch(yidong, /<ol/, '股票异动解析 should not render ordered-list markers')
assert.match(yidong, /font-size:\s*12px/, '股票异动解析 body text should match target compact text size')
assert.match(yidong, /#F0BE83/i, '股票异动解析 title color should match target black theme')
assert.match(yidong, /#111219/i, '股票异动解析 background should match target black theme variable')
assert.doesNotMatch(yidong, /query-bar/, '股票异动解析 should not show the generic query bar in target layout')
assert.doesNotMatch(yidong, /limit-meta/, '股票异动解析 should not show extra generic metadata pills')

const thsyd = read('src/views/tdx/TdxThsMove.vue')
assert.match(thsyd, /同花顺异动解析/, '同花顺异动解析 should show THS heading')
assert.match(thsyd, /target-yidong/, '同花顺异动解析 should share the target plain text layout')
assert.match(thsyd, /&nbsp;&nbsp;&nbsp;/, '同花顺异动解析 heading should keep target spacing before latest limit-up date')
assert.match(thsyd, /numberedParagraphs/, '同花顺异动解析 should preserve source numbered paragraphs')
assert.match(thsyd, /<p v-for="line in reasonLines/, '同花顺异动解析 should render target-style paragraph rows')
assert.doesNotMatch(thsyd, /<ol/, '同花顺异动解析 should not render ordered-list markers')
assert.match(thsyd, /#F0BE83/i, '同花顺异动解析 title color should match target black theme')
assert.match(thsyd, /#111219/i, '同花顺异动解析 background should match target black theme variable')

console.log('tdx target parity structure checks passed')
