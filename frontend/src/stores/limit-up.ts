import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import type { LimitUpRealtime } from '@/types/limit-up'

export const useLimitUpStore = defineStore('limitUp', () => {
  // 实时涨停列表
  const realtimeList = ref<LimitUpRealtime[]>([])
  const tradeDate = ref('')
  const lastSyncAt = ref('')
  
  // 加载状态
  const loading = ref(false)
  
  // 筛选条件
  const filters = ref({
    reasonCategory: '',
    minContinuousDays: 0,
    sortBy: 'time' as 'time' | 'seal_amount' | 'continuous_days'
  })

  // 筛选后的列表
  const filteredList = computed(() => {
    let list = [...realtimeList.value]
    
    if (filters.value.reasonCategory) {
      list = list.filter(item => item.reason_category === filters.value.reasonCategory)
    }
    
    if (filters.value.minContinuousDays > 0) {
      list = list.filter(item => item.continuous_limit_up_days >= filters.value.minContinuousDays)
    }
    
    // 排序
    switch (filters.value.sortBy) {
      case 'time':
        list.sort((a, b) => (a.first_limit_up_time || '').localeCompare(b.first_limit_up_time || ''))
        break
      case 'seal_amount':
        list.sort((a, b) => (b.seal_amount || 0) - (a.seal_amount || 0))
        break
      case 'continuous_days':
        list.sort((a, b) => b.continuous_limit_up_days - a.continuous_limit_up_days)
        break
    }
    
    return list
  })

  // 统计数据
  const stats = computed(() => {
    const list = realtimeList.value
    return {
      total: list.length,
      sealed: list.filter(item => item.is_sealed).length,
      opened: list.filter(item => !item.is_sealed).length,
      continuous: {
        first: list.filter(item => item.continuous_limit_up_days === 1).length,
        second: list.filter(item => item.continuous_limit_up_days === 2).length,
        third: list.filter(item => item.continuous_limit_up_days === 3).length,
        more: list.filter(item => item.continuous_limit_up_days >= 4).length
      }
    }
  })

  // 设置列表
  function setList(list: LimitUpRealtime[], snapshotTradeDate = '') {
    realtimeList.value = [...list]
    if (snapshotTradeDate) {
      tradeDate.value = snapshotTradeDate
    }
    lastSyncAt.value = new Date().toISOString()
  }

  // 设置完整快照
  function setSnapshot(snapshotTradeDate: string, list: LimitUpRealtime[]) {
    setList(list, snapshotTradeDate)
  }

  // 更新单条记录
  function updateItem(code: string, data: Partial<LimitUpRealtime>) {
    const index = realtimeList.value.findIndex(item => item.stock_code === code)
    if (index !== -1) {
      realtimeList.value[index] = { ...realtimeList.value[index], ...data }
    }
  }

  // 添加新涨停
  function addItem(item: LimitUpRealtime) {
    const exists = realtimeList.value.find(i => i.stock_code === item.stock_code)
    if (!exists) {
      realtimeList.value.push(item)
    }
  }

  // 删除记录
  function removeItems(codes: string[]) {
    if (codes.length === 0) return
    const removed = new Set(codes)
    realtimeList.value = realtimeList.value.filter(item => !removed.has(item.stock_code))
    lastSyncAt.value = new Date().toISOString()
  }

  // 应用 WebSocket 增量更新
  function applyDelta(
    upsert: LimitUpRealtime[],
    remove: string[] = [],
    deltaTradeDate = ''
  ) {
    if (deltaTradeDate && tradeDate.value && tradeDate.value !== deltaTradeDate) {
      setSnapshot(deltaTradeDate, upsert)
      return
    }

    const itemMap = new Map(
      realtimeList.value.map(item => [item.stock_code, item] as const)
    )

    remove.forEach(code => itemMap.delete(code))
    upsert.forEach(item => {
      const previous = itemMap.get(item.stock_code)
      itemMap.set(item.stock_code, previous ? { ...previous, ...item } : item)
    })

    realtimeList.value = Array.from(itemMap.values())
    if (deltaTradeDate) {
      tradeDate.value = deltaTradeDate
    }
    lastSyncAt.value = new Date().toISOString()
  }

  // 设置筛选条件
  function setFilters(newFilters: Partial<typeof filters.value>) {
    Object.assign(filters.value, newFilters)
  }

  return {
    realtimeList,
    tradeDate,
    lastSyncAt,
    loading,
    filters,
    filteredList,
    stats,
    setList,
    setSnapshot,
    updateItem,
    addItem,
    removeItems,
    applyDelta,
    setFilters
  }
})
