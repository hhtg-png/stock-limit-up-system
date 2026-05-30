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
      path: '/tdx/ztlive/:code?/dark',
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
