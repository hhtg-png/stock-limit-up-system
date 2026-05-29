import { readFileSync } from 'node:fs'
import test from 'node:test'
import assert from 'node:assert/strict'

function read(path) {
  return readFileSync(path, 'utf8').replace(/\r\n/g, '\n')
}

function escaped(text) {
  return text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

function toolbarTitleBlock(source) {
  const match = source.match(/<div class="toolbar-title">([\s\S]*?)<\/div>/)
  return match?.[1] ?? ''
}

test('main data pages do not render explanatory page descriptions', () => {
  const sources = {
    dailyInfo: read('src/views/DailyInfo.vue'),
    dailyAnalysis: read('src/views/DailyAnalysis.vue'),
    jiegeMode: read('src/views/JiegeMode.vue'),
    statistics: read('src/views/Statistics.vue')
  }

  for (const [name, source] of Object.entries(sources)) {
    assert.doesNotMatch(
      toolbarTitleBlock(source),
      /<span>/,
      `${name} toolbar title should not include a subtitle span`
    )
  }

  for (const text of [
    '知识库增量同步后保存每日摘要',
    '近10日涨停/触板池自动识别',
    '从核心知识库规则和项目复盘数据生成',
    '用复盘指标跟踪连板高度',
    '龙头高度、次高板高度与创业板高度',
    '首板进二板、连板晋级与封板率联动观察',
    '昨日涨停与昨日连板次日反馈对比',
    '连板家数、涨停与跌停数量的情绪脉冲',
    '量能与非ST涨跌家数的市场广度对照',
    '封板成交额与炸板成交额的资金去向',
    '当前复盘日的高标结构与封板状态',
    '按连板高度与成交额排序的个股复盘列表'
  ]) {
    const combined = Object.values(sources).join('\n')
    assert.doesNotMatch(combined, new RegExp(escaped(text)), `page copy should not include "${text}"`)
  }
})

test('headers and chart cards use compact spacing after descriptions are removed', () => {
  const dailyInfo = read('src/views/DailyInfo.vue')
  const dailyAnalysis = read('src/views/DailyAnalysis.vue')
  const jiegeMode = read('src/views/JiegeMode.vue')
  const statistics = read('src/views/Statistics.vue')

  for (const [name, source] of Object.entries({ dailyInfo, dailyAnalysis, jiegeMode })) {
    assert.match(source, /min-height:\s*52px/, `${name} toolbar should use a compact height`)
    assert.match(source, /padding:\s*12px 14px/, `${name} toolbar should use compact padding`)
  }

  assert.doesNotMatch(statistics, /\.summary-copy p/, 'statistics summary should not reserve styles for removed copy')
  assert.doesNotMatch(statistics, /\.card-header p/, 'statistics cards should not reserve styles for removed copy')
  assert.match(statistics, /\.card-header\s*\{[\s\S]*margin-bottom:\s*12px/, 'statistics card headers should be compact')
})
