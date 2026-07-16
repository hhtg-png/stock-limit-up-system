import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'
import test from 'node:test'

const root = resolve(import.meta.dirname, '..')
const read = path => readFileSync(resolve(root, path), 'utf8').replace(/\r\n/g, '\n')

function indexOfOrThrow(source, pattern, label) {
  const index = source.search(pattern)
  assert.notEqual(index, -1, `${label} should exist`)
  return index
}

test('router exposes an independent trading playbook page', () => {
  const router = read('src/router/index.ts')

  assert.match(router, /path:\s*['"]\/trading-playbook['"]/)
  assert.match(router, /name:\s*['"]TradingPlaybook['"]/)
  assert.match(router, /TradingPlaybook\.vue/)
  assert.match(router, /meta:\s*\{\s*title:\s*['"]交易预案['"]\s*\}/)
})

test('desktop and mobile navigation place trading playbook next to trading mode', () => {
  const app = read('src/App.vue')
  const desktopMode = indexOfOrThrow(app, /<el-menu-item index="\/jiege-mode">/, 'desktop trading mode')
  const desktopPlaybook = indexOfOrThrow(app, /<el-menu-item index="\/trading-playbook">/, 'desktop trading playbook')
  const desktopIndustry = indexOfOrThrow(app, /<el-menu-item index="\/industry-trends">/, 'desktop industry trends')
  assert.ok(desktopMode < desktopPlaybook && desktopPlaybook < desktopIndustry)
  assert.match(app, /<el-menu-item index="\/trading-playbook">[\s\S]*?<span>交易预案<\/span>/)

  const mobileMode = indexOfOrThrow(app, /\{\s*path:\s*'\/jiege-mode'/, 'mobile trading mode')
  const mobilePlaybook = indexOfOrThrow(app, /\{\s*path:\s*'\/trading-playbook'/, 'mobile trading playbook')
  const mobileIndustry = indexOfOrThrow(app, /\{\s*path:\s*'\/industry-trends'/, 'mobile industry trends')
  assert.ok(mobileMode < mobilePlaybook && mobilePlaybook < mobileIndustry)
  assert.match(app, /path:\s*'\/trading-playbook'[\s\S]*?label:\s*'预案'/)
})
