import assert from 'node:assert/strict'
import test from 'node:test'
import { resolve } from 'node:path'
import { createServer } from 'vite'
import vue from '@vitejs/plugin-vue'
import { createSSRApp } from 'vue'
import { renderToString } from '@vue/server-renderer'
import { createPinia } from 'pinia'
import ElementPlus, { ID_INJECTION_KEY, ZINDEX_INJECTION_KEY, zIndexContextKey } from 'element-plus'
import { createMemoryHistory, createRouter } from 'vue-router'

const root = resolve(import.meta.dirname, '..')

test('business pages render without retired Obsidian controls', async () => {
  const server = await createServer({
    configFile: false,
    root,
    logLevel: 'silent',
    plugins: [vue()],
    optimizeDeps: { noDiscovery: true, include: [] },
    server: { middlewareMode: true },
    resolve: { alias: { '@': resolve(root, 'src') } }
  })
  try {
    for (const [name, title] of [
      ['DailyInfo', '每日资讯'],
      ['DailyAnalysis', '每日分析'],
      ['JiegeMode', '交易模式'],
      ['IndustryTrends', '产业趋势'],
      ['UltraShortSignals', '超短'],
      ['TradingPlaybook', '交易预案']
    ]) {
      const { default: page } = await server.ssrLoadModule(`/src/views/${name}.vue`)
      const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/', component: page }] })
      await router.push('/')
      await router.isReady()
      const app = createSSRApp(page).use(createPinia()).use(router).use(ElementPlus)
      app.provide(ID_INJECTION_KEY, { prefix: 100, current: 0 })
      app.provide(ZINDEX_INJECTION_KEY, { current: 0 })
      app.provide(zIndexContextKey, { current: 0 })
      const html = await renderToString(app)
      assert.match(html, new RegExp(title), `${name} retains its business content`)
      assert.doesNotMatch(html, /Obsidian|obsidian:\/\/|打开笔记|导出到/, `${name} must not expose retired export controls`)
    }
  } finally {
    await server.close()
  }
})
