import { defineStore } from 'pinia'
import { ref } from 'vue'

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

export const useConfigStore = defineStore('config', () => {
  const config = ref<UserConfig>({
    big_order_threshold: 500000,
    big_order_volume: 300,
    big_order_volume_20cm: 200,
    alert_limit_up_enabled: true,
    alert_big_order_enabled: true,
    alert_sound_enabled: true,
    alert_desktop_enabled: true,
    filter_st: true,
    filter_new_stock: false,
    filter_low_price: 0,
    filter_high_price: 0,
    chart_theme: 'light',
    watch_list: []
  })

  function setConfig(newConfig: Partial<UserConfig>) {
    Object.assign(config.value, newConfig)
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
    setConfig,
    addToWatchList,
    removeFromWatchList
  }
})
