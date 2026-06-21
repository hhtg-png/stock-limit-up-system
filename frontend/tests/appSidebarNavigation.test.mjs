import { readFileSync } from 'node:fs'
import test from 'node:test'
import assert from 'node:assert/strict'

function read(path) {
  return readFileSync(path, 'utf8').replace(/\r\n/g, '\n')
}

test('main sidebar navigates only from explicit menu selection', () => {
  const app = read('src/App.vue')
  const menuTag = app.match(/<el-menu[\s\S]*?>/)?.[0] ?? ''

  assert.doesNotMatch(
    menuTag,
    /\srouter(?:\s|>|$)/,
    'sidebar should not use Element Plus implicit router mode'
  )
  assert.match(app, /@select="handleMenuSelect"/, 'sidebar should handle menu selection explicitly')
  assert.match(app, /const router = useRouter\(\)/, 'sidebar should use the app router directly')
  assert.match(app, /if \(path === route\.path\) return/, 'sidebar should ignore duplicate route selections')
  assert.match(app, /router\.push\(path\)/, 'sidebar should navigate only after a menu select event')
})
