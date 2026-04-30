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
      path: '/continuous',
      name: 'ContinuousBoard',
      component: () => import('@/views/ContinuousBoard.vue'),
      meta: { title: '连板梯队' }
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
