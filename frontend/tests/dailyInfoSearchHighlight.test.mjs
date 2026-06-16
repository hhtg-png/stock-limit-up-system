import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'

const viewSource = readFileSync('src/views/DailyInfo.vue', 'utf8')

function expectPattern(pattern, message) {
  assert.match(viewSource, pattern, message)
}

expectPattern(/const activeSearchKeyword = computed\(\(\) => searchKeyword\.value\.trim\(\)\)/, 'daily info should expose a trimmed active search keyword')
expectPattern(/const emptyDescription = computed\(\(\) => hasActiveSearch\.value \? '没有匹配的每日资讯' : '暂无资讯摘要，可点击同步知识库后再查看'\)/, 'search should use a filtered-result empty state')
expectPattern(/function highlightText\(/, 'daily info should render searchable text through one highlight helper')
expectPattern(/function escapeHtml\(/, 'highlight helper should escape source text before v-html rendering')
expectPattern(/function escapeRegExp\(/, 'highlight helper should escape user search text before building a regex')
expectPattern(/<mark class="search-highlight">/, 'highlight helper should wrap matched search words in a mark element')

for (const snippet of [
  'v-html="highlightText(item.summary.overview ||',
  'v-html="highlightText(dailyInfo.summary.overview ||',
  'v-html="highlightText(row.summary || row.reason ||',
  'v-html="highlightText(source.title)"',
  'v-html="highlightText(sourceBody)"',
]) {
  assert.ok(viewSource.includes(snippet), `daily info should highlight search text for ${snippet}`)
}

expectPattern(/\.search-highlight/, 'daily info should style highlighted search words')

console.log('daily info search highlight checks passed')
