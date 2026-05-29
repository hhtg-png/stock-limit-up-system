# TDX Lite Independent Plugins Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a lightweight `/tdx/*` runtime that preserves existing independent plugin URLs, adds a tiny aggregate-news voice-only plugin, and keeps aggregate-news speech ownership out of unrelated plugin windows.

**Architecture:** Split the frontend boot path into a normal full app runtime and a TDX-only runtime selected by `window.location.pathname`. The TDX runtime mounts a minimal app shell with the existing TDX plugin views plus a new `/tdx/news-voice/dark` view, while reusing current API, stock-link, speech, and WebSocket composables. Aggregate news speech is owned by `TdxNewsFeed` and `TdxNewsVoice`, not by the global WebSocket handler.

**Tech Stack:** Vue 3, Vue Router, Pinia, Vite, TypeScript, existing static Node test scripts, FastAPI backend unchanged for this phase.

---

## Scope

This plan implements the first phase from `docs/superpowers/specs/2026-05-29-tdx-lite-independent-plugins-design.md`:

- TDX light entry and app shell.
- Existing TDX plugin routes preserved.
- New `/tdx/news-voice/dark` voice-only route.
- Settings/plugin center expose the voice-only URL.
- Non-news TDX plugin windows do not auto-speak aggregate news events.

The shared multi-window WebSocket hub and full voice-leader election are separate follow-up work. This phase keeps the current per-window WebSocket behavior but removes the biggest runtime load by avoiding the main system shell and Element Plus full registration for `/tdx/*`.

## Existing Dirty Worktree Guard

Before implementation, run:

```powershell
git status --short --branch
```

Expected current dirty files may include:

- `backend/app/services/edge_tts_service.py`
- `backend/tests/test_edge_tts_service.py`
- `frontend/src/composables/useSpeech.ts`
- `frontend/tests/tdxPluginSpeechLink.test.mjs`

Keep those changes. Do not revert or overwrite them. The implementation tasks below touch `frontend/src/composables/useSpeech.ts` and `frontend/tests/tdxPluginSpeechLink.test.mjs`; merge with the existing changes in place.

## File Map

- Create `frontend/src/main-full.ts`: current full app bootstrap moved out of `main.ts`.
- Modify `frontend/src/main.ts`: lightweight dispatcher that dynamically imports full or TDX runtime.
- Create `frontend/src/TdxApp.vue`: minimal TDX shell that connects WebSocket and renders `router-view`.
- Create `frontend/src/router/tdx.ts`: TDX-only router containing all TDX plugin routes.
- Create `frontend/src/tdx-main.ts`: TDX bootstrap without Element Plus full install or global icon registration.
- Create `frontend/src/views/tdx/TdxNewsVoice.vue`: voice-only aggregate-news plugin.
- Modify `frontend/src/composables/useWebSocket.ts`: stop direct speech enqueue for `tdx_news_event`; keep realtime state update.
- Modify `frontend/src/views/tdx/TdxNewsFeed.vue`: it remains the visible-list owner for aggregate-news speech.
- Modify `frontend/src/views/tdx/TdxPluginCenter.vue`: add voice-only plugin URL.
- Modify `frontend/src/views/Settings.vue`: add voice-only plugin URL to modal.
- Create or modify tests:
  - `frontend/tests/tdxLiteEntry.test.mjs`
  - `frontend/tests/tdxNewsVoiceRoute.test.mjs`
  - `frontend/tests/tdxPluginSpeechLink.test.mjs`
  - `frontend/tests/tdxNewsSpeech.test.mjs`

---

### Task 1: Add Static Failing Tests for the TDX Light Runtime

**Files:**
- Create: `frontend/tests/tdxLiteEntry.test.mjs`
- Test reads: `frontend/src/main.ts`
- Test reads: `frontend/src/main-full.ts`
- Test reads: `frontend/src/tdx-main.ts`
- Test reads: `frontend/src/TdxApp.vue`

- [ ] **Step 1: Write the failing test**

Create `frontend/tests/tdxLiteEntry.test.mjs` with this content:

```js
import { existsSync, readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')

function read(path) {
  return readFileSync(resolve(root, path), 'utf8')
}

assert.ok(existsSync(resolve(root, 'src/main-full.ts')), 'full app bootstrap should live in main-full.ts')
assert.ok(existsSync(resolve(root, 'src/tdx-main.ts')), 'tdx runtime bootstrap should exist')
assert.ok(existsSync(resolve(root, 'src/TdxApp.vue')), 'tdx runtime shell should exist')

const main = read('src/main.ts')
assert.match(main, /window\.location\.pathname\.startsWith\('\/tdx'\)/, 'main.ts should select tdx runtime by path')
assert.match(main, /import\('\.\/tdx-main'\)/, 'main.ts should lazy-load tdx-main for tdx routes')
assert.match(main, /import\('\.\/main-full'\)/, 'main.ts should lazy-load main-full for normal routes')
assert.doesNotMatch(main, /ElementPlus/, 'main.ts dispatcher should not import Element Plus')
assert.doesNotMatch(main, /@element-plus\/icons-vue/, 'main.ts dispatcher should not import icon library')

const full = read('src/main-full.ts')
assert.match(full, /app\.use\(ElementPlus,\s*\{\s*locale:\s*zhCn\s*\}\)/, 'full runtime should keep Element Plus for normal app pages')
assert.match(full, /Object\.entries\(ElementPlusIconsVue\)/, 'full runtime should keep current global icon registration')
assert.match(full, /createApp\(App\)/, 'full runtime should mount the normal App shell')

const tdxMain = read('src/tdx-main.ts')
assert.match(tdxMain, /createApp\(TdxApp\)/, 'tdx runtime should mount TdxApp')
assert.match(tdxMain, /tdxRouter/, 'tdx runtime should use the tdx-only router')
assert.doesNotMatch(tdxMain, /ElementPlus/, 'tdx runtime should not install Element Plus globally')
assert.doesNotMatch(tdxMain, /ElementPlusIconsVue/, 'tdx runtime should not globally register all icons')
assert.doesNotMatch(tdxMain, /App\.vue/, 'tdx runtime should not import the normal app shell')

const tdxApp = read('src/TdxApp.vue')
assert.match(tdxApp, /<router-view\s*\/>/, 'tdx app shell should only render the plugin route')
assert.match(tdxApp, /useWebSocket/, 'tdx app shell should own the websocket connection for plugin pages')
assert.doesNotMatch(tdxApp, /el-container|el-aside|AlertPanel|mobile-bottom-nav/, 'tdx app shell should not include normal app chrome')

console.log('tdx lite entry checks passed')
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```powershell
cd frontend
node tests/tdxLiteEntry.test.mjs
```

Expected: FAIL because `src/main-full.ts`, `src/tdx-main.ts`, and `src/TdxApp.vue` do not exist yet.

- [ ] **Step 3: Create the full app bootstrap**

Create `frontend/src/main-full.ts` by moving the current content of `frontend/src/main.ts` into it:

```ts
import { createApp } from 'vue'
import { createPinia } from 'pinia'
import ElementPlus from 'element-plus'
import 'element-plus/dist/index.css'
import zhCn from 'element-plus/dist/locale/zh-cn.mjs'
import * as ElementPlusIconsVue from '@element-plus/icons-vue'

import App from './App.vue'
import router from './router'
import './styles/main.scss'

const app = createApp(App)

for (const [key, component] of Object.entries(ElementPlusIconsVue)) {
  app.component(key, component)
}

app.use(createPinia())
app.use(router)
app.use(ElementPlus, { locale: zhCn })

app.mount('#app')
```

- [ ] **Step 4: Replace `frontend/src/main.ts` with the runtime dispatcher**

Use this content:

```ts
const isTdxRuntime = window.location.pathname.startsWith('/tdx')

if (isTdxRuntime) {
  import('./tdx-main')
} else {
  import('./main-full')
}
```

- [ ] **Step 5: Create the minimal TDX app shell**

Create `frontend/src/TdxApp.vue`:

```vue
<template>
  <main class="tdx-lite-app">
    <router-view />
  </main>
</template>

<script setup lang="ts">
import { onMounted, onUnmounted } from 'vue'
import { useWebSocket } from '@/composables/useWebSocket'

const { connect, disconnect } = useWebSocket()

onMounted(() => {
  connect()
})

onUnmounted(() => {
  disconnect()
})
</script>

<style scoped>
.tdx-lite-app {
  min-height: 100vh;
  width: 100vw;
  overflow: auto;
  background: #050b12;
}
</style>
```

- [ ] **Step 6: Create a minimal TDX router and bootstrap**

Create `frontend/src/router/tdx.ts` with this minimal content:

```ts
import { createRouter, createWebHistory } from 'vue-router'

const tdxRouter = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: '/tdx',
      name: 'TdxPluginCenter',
      component: () => import('@/views/tdx/TdxPluginCenter.vue'),
      meta: { title: '通达信看盘插件', tdx: true }
    }
  ]
})

tdxRouter.beforeEach((to, _from, next) => {
  document.title = `${to.meta.title || '通达信看盘插件'}`
  next()
})

export default tdxRouter
```

Create `frontend/src/tdx-main.ts`:

```ts
import { createApp } from 'vue'
import { createPinia } from 'pinia'

import TdxApp from './TdxApp.vue'
import tdxRouter from './router/tdx'
import './styles/main.scss'

const app = createApp(TdxApp)

app.use(createPinia())
app.use(tdxRouter)

app.mount('#app')
```

- [ ] **Step 7: Run the test to verify it passes**

Run:

```powershell
cd frontend
node tests/tdxLiteEntry.test.mjs
```

Expected: PASS with `tdx lite entry checks passed`.

- [ ] **Step 8: Commit**

```powershell
git add frontend/src/main.ts frontend/src/main-full.ts frontend/src/TdxApp.vue frontend/src/tdx-main.ts frontend/src/router/tdx.ts frontend/tests/tdxLiteEntry.test.mjs
git commit -m "feat: add tdx lite runtime entry"
```

---

### Task 2: Add TDX-Only Routes Including the Voice Plugin Route

**Files:**
- Create or modify: `frontend/src/router/tdx.ts`
- Create: `frontend/tests/tdxNewsVoiceRoute.test.mjs`

- [ ] **Step 1: Write the failing route test**

Create `frontend/tests/tdxNewsVoiceRoute.test.mjs`:

```js
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')
const router = readFileSync(resolve(root, 'src/router/tdx.ts'), 'utf8')

for (const path of [
  '/tdx',
  '/tdx/ztlive/dark',
  '/tdx/yidong/:code?/dark',
  '/tdx/strong/dark',
  '/tdx/news/dark',
  '/tdx/news-voice/dark',
  '/tdx/thsyd/:code?/dark'
]) {
  assert.match(router, new RegExp(`path:\\s*'${path.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}'`), `tdx router should include ${path}`)
}

assert.match(router, /name:\s*'TdxNewsVoice'/, 'tdx router should name the voice-only news route')
assert.match(router, /TdxNewsVoice\.vue/, 'tdx router should lazy-load TdxNewsVoice')
assert.match(router, /document\.title/, 'tdx router should set document title')

console.log('tdx news voice route checks passed')
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```powershell
cd frontend
node tests/tdxNewsVoiceRoute.test.mjs
```

Expected: FAIL because `/tdx/news-voice/dark` and `TdxNewsVoice.vue` are not wired yet.

- [ ] **Step 3: Implement `frontend/src/router/tdx.ts`**

Use this content:

```ts
import { createRouter, createWebHistory } from 'vue-router'

const tdxRouter = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: '/tdx',
      name: 'TdxPluginCenter',
      component: () => import('@/views/tdx/TdxPluginCenter.vue'),
      meta: { title: '通达信看盘插件', tdx: true }
    },
    {
      path: '/tdx/ztlive/dark',
      name: 'TdxLimitUpLive',
      component: () => import('@/views/tdx/TdxLimitUpLive.vue'),
      meta: { title: '涨停播报', tdx: true }
    },
    {
      path: '/tdx/yidong/:code?/dark',
      name: 'TdxStockMove',
      component: () => import('@/views/tdx/TdxStockMove.vue'),
      meta: { title: '股票异动解析联动', tdx: true }
    },
    {
      path: '/tdx/strong/dark',
      name: 'TdxPlateStrength',
      component: () => import('@/views/tdx/TdxPlateStrength.vue'),
      meta: { title: '实时板块强度', tdx: true }
    },
    {
      path: '/tdx/news/dark',
      name: 'TdxNewsFeed',
      component: () => import('@/views/tdx/TdxNewsFeed.vue'),
      meta: { title: '聚合快讯', tdx: true }
    },
    {
      path: '/tdx/news-voice/dark',
      name: 'TdxNewsVoice',
      component: () => import('@/views/tdx/TdxNewsVoice.vue'),
      meta: { title: '聚合快讯语音播报', tdx: true }
    },
    {
      path: '/tdx/thsyd/:code?/dark',
      name: 'TdxThsMove',
      component: () => import('@/views/tdx/TdxThsMove.vue'),
      meta: { title: '异动解析（同花顺版）', tdx: true }
    }
  ]
})

tdxRouter.beforeEach((to, _from, next) => {
  document.title = `${to.meta.title || '通达信看盘插件'}`
  next()
})

export default tdxRouter
```

- [ ] **Step 4: Create a temporary voice view so route build can resolve**

Create `frontend/src/views/tdx/TdxNewsVoice.vue`:

```vue
<template>
  <section class="tdx-news-voice">
    <strong>聚合快讯语音播报</strong>
  </section>
</template>

<style scoped>
.tdx-news-voice {
  min-height: 100vh;
  padding: 10px;
  background: #050b12;
  color: #d6e4ff;
  font-size: 12px;
}
</style>
```

- [ ] **Step 5: Run the route test**

Run:

```powershell
cd frontend
node tests/tdxNewsVoiceRoute.test.mjs
```

Expected: PASS with `tdx news voice route checks passed`.

- [ ] **Step 6: Commit**

```powershell
git add frontend/src/router/tdx.ts frontend/src/views/tdx/TdxNewsVoice.vue frontend/tests/tdxNewsVoiceRoute.test.mjs
git commit -m "feat: add tdx news voice route"
```

---

### Task 3: Build the Voice-Only Aggregate News Plugin

**Files:**
- Modify: `frontend/src/views/tdx/TdxNewsVoice.vue`
- Modify: `frontend/tests/tdxNewsSpeech.test.mjs`

- [ ] **Step 1: Add failing static checks for the voice plugin**

Append these assertions to `frontend/tests/tdxNewsSpeech.test.mjs` after the existing `const speech = ...` line:

```js
const voice = readFileSync(resolve(root, 'src/views/tdx/TdxNewsVoice.vue'), 'utf8')

assert.match(voice, /useTdxPluginRealtime/, 'voice-only plugin should listen to realtime aggregate news')
assert.match(voice, /useSpeech/, 'voice-only plugin should use the shared speech queue')
assert.match(voice, /getTdxNews\(\{\s*limit:\s*20\s*\}\)/, 'voice-only plugin should load a small initial news snapshot')
assert.match(voice, /handleSpeechToggle/, 'voice-only plugin should expose a user gesture speech toggle')
assert.match(voice, /recentTitle/, 'voice-only plugin should show the latest spoken title')
assert.match(voice, /spokenCount/, 'voice-only plugin should show the spoken count')
assert.doesNotMatch(voice, /v-for="item in items"/, 'voice-only plugin should not render a large news list')
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```powershell
cd frontend
node tests/tdxNewsSpeech.test.mjs
```

Expected: FAIL because `TdxNewsVoice.vue` is still the temporary stub.

- [ ] **Step 3: Replace `TdxNewsVoice.vue` with the working voice-only plugin**

Use this content:

```vue
<template>
  <section class="tdx-news-voice">
    <header class="voice-header">
      <div>
        <h1>聚合快讯语音</h1>
        <span :class="['status-dot', statusClass]">{{ statusText }}</span>
      </div>
      <label class="voice-switch">
        <input type="checkbox" :checked="speechUnlocked" @change="handleSpeechToggle" />
        <span>{{ speechUnlocked ? '播报中' : '开启播报' }}</span>
      </label>
    </header>

    <main class="voice-main">
      <div class="metric">
        <span>已播报</span>
        <strong>{{ spokenCount }}</strong>
      </div>
      <div class="metric">
        <span>最新时间</span>
        <strong>{{ latestTime || '--' }}</strong>
      </div>
      <p class="latest-title">{{ recentTitle || '等待聚合快讯' }}</p>
      <p v-if="errorMessage" class="voice-error">{{ errorMessage }}</p>
    </main>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { getTdxNews } from '@/api/tdx-plugins'
import { useSpeech } from '@/composables/useSpeech'
import { useTdxPluginRealtime } from '@/composables/useWebSocket'
import type { TdxNewsItem } from '@/types/tdx-plugins'

const { enqueuePluginSpeech, unlockSpeech, speechUnlocked } = useSpeech()
const { realtimeNewsItems } = useTdxPluginRealtime()

const knownNewsKeys = new Set<string>()
const spokenNewsKeys = new Set<string>()
const recentTitle = ref('')
const latestTime = ref('')
const spokenCount = ref(0)
const loaded = ref(false)
const errorMessage = ref('')

const statusText = computed(() => {
  if (errorMessage.value) return '异常'
  if (!loaded.value) return '连接中'
  return speechUnlocked.value ? '语音已启用' : '待开启'
})

const statusClass = computed(() => {
  if (errorMessage.value) return 'error'
  if (!loaded.value) return 'pending'
  return speechUnlocked.value ? 'ok' : 'idle'
})

function newsKey(item: TdxNewsItem) {
  return `news-${item.news_id || `${item.time}-${item.title}`}`
}

function normalizeSpeechPart(value?: string) {
  return (value || '').replace(/\s+/g, ' ').trim()
}

function newsSpeechText(item: TdxNewsItem) {
  const source = normalizeSpeechPart(item.source)
  const title = normalizeSpeechPart(item.title)
  if (item.source === '韭研公社') {
    return `${source ? `${source}新帖，` : '新帖，'}${title}`.slice(0, 120)
  }
  return title.slice(0, 120)
}

function rememberKnown(items: readonly TdxNewsItem[]) {
  for (const item of items) {
    if (item.news_id && item.title) knownNewsKeys.add(newsKey(item))
  }
}

function speakNews(item: TdxNewsItem) {
  if (!item.news_id || !item.title || !speechUnlocked.value) return false
  const key = newsKey(item)
  if (spokenNewsKeys.has(key)) return false

  const text = newsSpeechText(item)
  const queued = enqueuePluginSpeech(text, key, { force: true })
  if (!queued) return false

  spokenNewsKeys.add(key)
  knownNewsKeys.add(key)
  recentTitle.value = normalizeSpeechPart(item.title)
  latestTime.value = item.time || ''
  spokenCount.value += 1
  return true
}

function handleSpeechToggle(event: Event) {
  const input = event.target as HTMLInputElement | null
  if (input && !input.checked) return
  unlockSpeech({ silent: true })
}

async function loadInitialSnapshot() {
  try {
    const payload = await getTdxNews({ limit: 20 })
    rememberKnown(payload.items || [])
    loaded.value = true
    errorMessage.value = ''
  } catch {
    loaded.value = true
    errorMessage.value = '快讯连接异常'
  }
}

onMounted(() => {
  loadInitialSnapshot()
})

watch(realtimeNewsItems, (nextItems, previousItems) => {
  const previousKeys = new Set(previousItems.map(newsKey))
  for (const item of nextItems) {
    const key = newsKey(item)
    const wasKnown = knownNewsKeys.has(key) || previousKeys.has(key)
    knownNewsKeys.add(key)
    if (!wasKnown) speakNews(item)
  }
})
</script>

<style scoped>
.tdx-news-voice {
  min-height: 100vh;
  padding: 8px;
  background: #050b12;
  color: #d6e4ff;
  font-size: 12px;
}

.voice-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  border-bottom: 1px solid #26364f;
  padding-bottom: 7px;
}

.voice-header h1 {
  margin: 0 0 3px;
  color: #f8fafc;
  font-size: 14px;
  line-height: 1.2;
}

.status-dot {
  color: #94a3b8;
}

.status-dot.ok {
  color: #22c55e;
}

.status-dot.error {
  color: #ef4444;
}

.status-dot.pending {
  color: #f59e0b;
}

.voice-switch {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  white-space: nowrap;
  color: #e2e8f0;
  cursor: pointer;
}

.voice-main {
  display: grid;
  gap: 8px;
  padding-top: 9px;
}

.metric {
  display: flex;
  align-items: center;
  justify-content: space-between;
  border: 1px solid #1e293b;
  background: #0b1220;
  padding: 6px 8px;
}

.metric span {
  color: #94a3b8;
}

.metric strong {
  color: #facc15;
  font-size: 13px;
}

.latest-title {
  margin: 0;
  color: #f8fafc;
  line-height: 1.55;
  word-break: break-word;
}

.voice-error {
  margin: 0;
  color: #f87171;
}
</style>
```

- [ ] **Step 4: Run the speech test**

Run:

```powershell
cd frontend
node tests/tdxNewsSpeech.test.mjs
```

Expected: PASS with `tdx news speech checks passed`.

- [ ] **Step 5: Commit**

```powershell
git add frontend/src/views/tdx/TdxNewsVoice.vue frontend/tests/tdxNewsSpeech.test.mjs
git commit -m "feat: add tdx news voice plugin"
```

---

### Task 4: Keep Aggregate News Speech Out of Non-News Plugin Windows

**Files:**
- Modify: `frontend/src/composables/useWebSocket.ts`
- Modify: `frontend/tests/tdxPluginSpeechLink.test.mjs`
- Modify: `frontend/tests/tdxRealtimeHybrid.test.mjs`

- [ ] **Step 1: Update failing tests**

In `frontend/tests/tdxPluginSpeechLink.test.mjs`, replace the current loop:

```js
for (const type of ['tdx_limit_up_event', 'tdx_stock_move_event', 'tdx_news_event', 'tdx_plate_strength_update']) {
  assert.match(websocket, new RegExp(`case '${type}':`), `WebSocket should handle ${type}`)
}
assert.match(websocket, /enqueuePluginSpeech/, 'TDX WebSocket events should enter the plugin speech queue')
assert.match(websocket, /enqueuePluginSpeech\([\s\S]*\{\s*force:\s*true\s*\}/, 'TDX WebSocket plugin events should bypass the original app alert switch after the plugin voice switch is unlocked')
```

with:

```js
for (const type of ['tdx_limit_up_event', 'tdx_stock_move_event', 'tdx_news_event', 'tdx_plate_strength_update']) {
  assert.match(websocket, new RegExp(`case '${type}':`), `WebSocket should handle ${type}`)
}
assert.match(websocket, /case 'tdx_limit_up_event':[\s\S]*enqueuePluginSpeech/, 'limit-up websocket events should enter the speech queue')
assert.match(websocket, /case 'tdx_stock_move_event':[\s\S]*enqueuePluginSpeech/, 'stock-move websocket events should enter the speech queue')
assert.match(websocket, /case 'tdx_plate_strength_update':[\s\S]*enqueuePluginSpeech/, 'plate-strength websocket events should enter the speech queue')
assert.doesNotMatch(websocket, /case 'tdx_news_event':[\s\S]*enqueuePluginSpeech/, 'aggregate news websocket handler should not directly speak in every plugin window')
```

In `frontend/tests/tdxRealtimeHybrid.test.mjs`, replace the assertion:

```js
assert.match(ws, /`news-\$\{item\.news_id \|\| message\.timestamp\}`/, 'tdx_news_event speech keys should match the news page dedupe key')
```

with:

```js
assert.doesNotMatch(ws, /`news-\$\{item\.news_id \|\| message\.timestamp\}`/, 'tdx_news_event speech should be owned by news pages, not the websocket handler')
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
cd frontend
node tests/tdxPluginSpeechLink.test.mjs
node tests/tdxRealtimeHybrid.test.mjs
```

Expected: FAIL because `tdx_news_event` still calls `enqueuePluginSpeech` in `useWebSocket.ts`.

- [ ] **Step 3: Modify `tdx_news_event` handling**

In `frontend/src/composables/useWebSocket.ts`, change:

```ts
      case 'tdx_news_event':
        const item = pushTdxNewsItem(message.data || {}, message.timestamp)
        if (item.title) {
          const { enqueuePluginSpeech } = useSpeech()
          enqueuePluginSpeech(
            message.data.speech_text || item.title,
            `news-${item.news_id || message.timestamp}`,
            { force: true }
          )
        }
        break
```

to:

```ts
      case 'tdx_news_event':
        pushTdxNewsItem(message.data || {}, message.timestamp)
        break
```

This leaves aggregate news data in `useTdxPluginRealtime()` for `TdxNewsFeed` and `TdxNewsVoice` to speak when those pages are open.

- [ ] **Step 4: Run the tests**

Run:

```powershell
cd frontend
node tests/tdxPluginSpeechLink.test.mjs
node tests/tdxRealtimeHybrid.test.mjs
node tests/tdxNewsSpeech.test.mjs
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```powershell
git add frontend/src/composables/useWebSocket.ts frontend/tests/tdxPluginSpeechLink.test.mjs frontend/tests/tdxRealtimeHybrid.test.mjs frontend/tests/tdxNewsSpeech.test.mjs
git commit -m "fix: scope aggregate news speech to news plugins"
```

---

### Task 5: Expose the Voice-Only Plugin in Plugin Center and Settings

**Files:**
- Modify: `frontend/src/views/tdx/TdxPluginCenter.vue`
- Modify: `frontend/src/views/Settings.vue`
- Create: `frontend/tests/tdxNewsVoiceEntry.test.mjs`

- [ ] **Step 1: Write failing static test**

Create `frontend/tests/tdxNewsVoiceEntry.test.mjs`:

```js
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')
const center = readFileSync(resolve(root, 'src/views/tdx/TdxPluginCenter.vue'), 'utf8')
const settings = readFileSync(resolve(root, 'src/views/Settings.vue'), 'utf8')

assert.match(center, /聚合快讯语音/, 'tdx plugin center should show the voice-only news plugin')
assert.match(center, /\/tdx\/news-voice\/dark/, 'tdx plugin center should link to news voice route')
assert.match(settings, /聚合快讯语音/, 'settings plugin modal should show the voice-only news plugin')
assert.match(settings, /\/tdx\/news-voice\/dark/, 'settings plugin modal should copy the news voice route')

console.log('tdx news voice entry checks passed')
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```powershell
cd frontend
node tests/tdxNewsVoiceEntry.test.mjs
```

Expected: FAIL because neither file lists `/tdx/news-voice/dark`.

- [ ] **Step 3: Update `TdxPluginCenter.vue`**

In the `plugins` array, add this item after 聚合快讯:

```ts
{ name: '聚合快讯语音', desc: '极简语音开关，只负责聚合资讯播报', path: '/tdx/news-voice/dark' },
```

- [ ] **Step 4: Update `Settings.vue`**

In the `tdxPlugins` array, add this item after 聚合快讯:

```ts
{ name: '聚合快讯语音', desc: '极简语音开关，只负责聚合资讯播报', path: '/tdx/news-voice/dark' },
```

- [ ] **Step 5: Run the test**

Run:

```powershell
cd frontend
node tests/tdxNewsVoiceEntry.test.mjs
```

Expected: PASS with `tdx news voice entry checks passed`.

- [ ] **Step 6: Commit**

```powershell
git add frontend/src/views/tdx/TdxPluginCenter.vue frontend/src/views/Settings.vue frontend/tests/tdxNewsVoiceEntry.test.mjs
git commit -m "feat: expose tdx news voice plugin"
```

---

### Task 6: Build and Regression Verification

**Files:**
- No new source files.
- Verification covers frontend and the existing dirty backend TTS pitch change.

- [ ] **Step 1: Run all relevant static frontend tests**

Run:

```powershell
cd frontend
node tests/tdxLiteEntry.test.mjs
node tests/tdxNewsVoiceRoute.test.mjs
node tests/tdxNewsVoiceEntry.test.mjs
node tests/tdxPluginSpeechLink.test.mjs
node tests/tdxRealtimeHybrid.test.mjs
node tests/tdxNewsSpeech.test.mjs
node tests/tdxSpeechVoice.test.mjs
```

Expected: each command exits 0 and prints its success message.

- [ ] **Step 2: Run frontend production build**

Run:

```powershell
cd frontend
npm run build
```

Expected: exit 0. Existing Sass legacy API and chunk-size warnings are acceptable if no new errors appear.

- [ ] **Step 3: Run targeted backend TTS test if the pitch change is still dirty**

Run:

```powershell
cd backend
python -m unittest discover tests -p test_edge_tts_service.py -v
```

Expected: PASS and the test asserts Edge TTS pitch is `+0Hz`.

- [ ] **Step 4: Run full backend tests before a deploy or final commit bundle**

Run:

```powershell
cd backend
python -m unittest discover tests -v
```

Expected: all tests pass.

- [ ] **Step 5: Browser smoke check**

Start the local frontend/backend the same way the project normally runs. In the in-app browser, open:

```text
http://127.0.0.1:3000/tdx/ztlive/dark
http://127.0.0.1:3000/tdx/news/dark
http://127.0.0.1:3000/tdx/news-voice/dark
http://127.0.0.1:3000/tdx/strong/dark
```

Expected:

- Each route renders black TDX UI.
- Browser console has no route-load error.
- `/tdx/news-voice/dark` shows the voice switch, waiting latest-title text, spoken count, and status.
- Opening `/tdx/ztlive/dark` does not render the normal app sidebar/header.

- [ ] **Step 6: Final commit if not already committed task-by-task**

If any implementation changes remain unstaged after task commits, inspect them:

```powershell
git status --short
git diff --stat
```

Stage only intended files and commit:

```powershell
git add <intended files>
git commit -m "feat: add tdx lite independent plugins"
```

Expected: working tree contains no unintended changes beyond previously known user edits.

---

## Self-Review Checklist

- Spec goal “保留现有插件 URL” is covered by Task 2.
- Spec goal “TDX 轻量入口” is covered by Task 1.
- Spec goal “新增 `/tdx/news-voice/dark`” is covered by Tasks 2 and 3.
- Spec goal “聚合资讯语音开关只放到语音插件和聚合资讯页” is covered by Task 4.
- Spec goal “设置入口补充新插件 URL” is covered by Task 5.
- Verification is covered by Task 6.
- Shared WebSocket hub and full voice-leader election are intentionally excluded from this first implementation plan and remain separate follow-up work from the same design document.
