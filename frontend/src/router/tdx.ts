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
