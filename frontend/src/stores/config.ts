import { defineStore } from 'pinia'
import { ref, watch } from 'vue'

export interface UserConfig {
  big_order_threshold: number
  big_order_volume: number
  big_order_volume_20cm: number
  alert_limit_up_enabled: boolean
  alert_big_order_enabled: boolean
  alert_sound_enabled: boolean
  alert_desktop_enabled: boolean
  filter_st: boolean
  filter_new_stock: boolean
  filter_low_price: number
  filter_high_price: number
  chart_theme: string
  watch_list: string[]
}

const STORAGE_KEY = 'stock_limit_up_config'

// 从 localStorage 读取配置
function loadFromStorage(): Partial<UserConfig> {
  try {
    const saved = localStorage.getItem(STORAGE_KEY)
    if (saved) {
      return JSON.parse(saved)
    }
  } catch (e) {
    console.error('Load config from localStorage failed:', e)
  }
  return {}
}

// 保存配置到 localStorage
function saveToStorage(config: UserConfig) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(config))
  } catch (e) {
    console.error('Save config to localStorage failed:', e)
  }
}

// 默认配置
const defaultConfig: UserConfig = {
  big_order_threshold: 500000,
  big_order_volume: 300,
  big_order_volume_20cm: 200,
  alert_limit_up_enabled: false, // 默认关闭，避免意外播报
  alert_big_order_enabled: false,
  alert_sound_enabled: true,
  alert_desktop_enabled: true,
  filter_st: true,
  filter_new_stock: false,
  filter_low_price: 0,
  filter_high_price: 0,
  chart_theme: 'light',
  watch_list: []
}

export const useConfigStore = defineStore('config', () => {
  // 初始化时从 localStorage 读取
  const savedConfig = loadFromStorage()
  const config = ref<UserConfig>({
    ...defaultConfig,
    ...savedConfig
  })
  
  // 标记配置是否已从后端初始化
  const initialized = ref(Object.keys(savedConfig).length > 0)

  // 监听配置变化，自动保存到 localStorage
  watch(config, (newConfig) => {
    saveToStorage(newConfig)
  }, { deep: true })

  function setConfig(newConfig: Partial<UserConfig>) {
    Object.assign(config.value, newConfig)
    initialized.value = true
  }

  function addToWatchList(code: string) {
    if (!config.value.watch_list.includes(code)) {
      config.value.watch_list.push(code)
    }
  }

  function removeFromWatchList(code: string) {
    const index = config.value.watch_list.indexOf(code)
    if (index !== -1) {
      config.value.watch_list.splice(index, 1)
    }
  }

  return {
    config,
    initialized,
    setConfig,
    addToWatchList,
    removeFromWatchList
  }
})
