import { createRouter, createWebHistory } from 'vue-router'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: '/',
      name: 'Dashboard',
      component: () => import('@/views/Dashboard.vue'),
      meta: { title: '主看板' }
    },
    {
      path: '/limit-up',
      name: 'LimitUpList',
      component: () => import('@/views/LimitUpList.vue'),
      meta: { title: '涨停列表' }
    },
    {
      path: '/stock/:code',
      name: 'StockDetail',
      component: () => import('@/views/StockDetail.vue'),
      meta: { title: '个股详情' }
    },
    {
      path: '/statistics',
      name: 'Statistics',
      component: () => import('@/views/Statistics.vue'),
      meta: { title: '统计分析' }
    },
    {
      path: '/daily-analysis',
      name: 'DailyAnalysis',
      component: () => import('@/views/DailyAnalysis.vue'),
      meta: { title: '每日分析' }
    },
    {
      path: '/daily-info',
      name: 'DailyInfo',
      component: () => import('@/views/DailyInfo.vue'),
      meta: { title: '每日资讯' }
    },
    {
      path: '/limit-up-classification',
      name: 'LimitUpClassification',
      component: () => import('@/views/LimitUpClassification.vue'),
      meta: { title: '涨停分类' }
    },
    {
      path: '/jiege-mode',
      name: 'JiegeMode',
      component: () => import('@/views/JiegeMode.vue'),
      meta: { title: '交易模式' }
    },
    {
      path: '/industry-trends',
      name: 'IndustryTrends',
      component: () => import('@/views/IndustryTrends.vue'),
      meta: { title: '产业趋势' }
    },
    {
      path: '/ultra-short-signals',
      name: 'UltraShortSignals',
      component: () => import('@/views/UltraShortSignals.vue'),
      meta: { title: '超短信号' }
    },
    {
      path: '/continuous',
      name: 'ContinuousBoard',
      component: () => import('@/views/ContinuousBoard.vue'),
      meta: { title: '连板梯队' }
    },
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
      path: '/tdx/composite/:code?/dark',
      name: 'TdxCompositeWatch',
      component: () => import('@/views/tdx/TdxCompositeWatch.vue'),
      meta: { title: '复合看盘', tdx: true }
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
      path: '/tdx/thsyd/:code?/dark',
      name: 'TdxThsMove',
      component: () => import('@/views/tdx/TdxThsMove.vue'),
      meta: { title: '异动解析（同花顺版）', tdx: true }
    },

    {
      path: '/settings',
      name: 'Settings',
      component: () => import('@/views/Settings.vue'),
      meta: { title: '系统设置' }
    }
  ]
})

router.beforeEach((to, _from, next) => {
  document.title = `${to.meta.title || '股票涨停分析系统'}`
  next()
})

export default router
