import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import test from 'node:test'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')

function read(path) {
  return readFileSync(resolve(root, path), 'utf8').replace(/\r\n/g, '\n')
}

test('Jiege mode is presented as generic trading mode in user-visible UI', () => {
  const app = read('src/App.vue')
  const router = read('src/router/index.ts')
  const view = read('src/views/JiegeMode.vue')
  const combined = [app, router, view].join('\n')

  assert.match(app, /<span>交易模式<\/span>/, 'desktop navigation should use 交易模式')
  assert.match(app, /path:\s*'\/jiege-mode'[\s\S]*label:\s*'交易'/, 'mobile navigation should use 交易')
  assert.match(router, /meta:\s*\{\s*title:\s*'交易模式'\s*\}/, 'route title should use 交易模式')
  assert.match(view, /<h3>交易模式<\/h3>/, 'page title should use 交易模式')
  assert.doesNotMatch(combined, /杰哥交易模式|label:\s*'杰哥'/, 'user-visible UI should not expose the old name')
})

test('rules system panel is collapsed by default', () => {
  const view = read('src/views/JiegeMode.vue')

  assert.match(view, /const rulesExpanded = ref\(false\)/, 'rules panel should default to collapsed')
  assert.match(
    view,
    /v-if="rulesExpanded && signalData\.rules\.length"/,
    'rule grid should render only after the panel is expanded'
  )
})
