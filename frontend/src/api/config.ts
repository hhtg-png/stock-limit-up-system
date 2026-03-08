import axios from 'axios'
import type { UserConfig } from '@/stores/config'

const api = axios.create({
  baseURL: '/api/v1',
  timeout: 30000
})

// 获取用户配置
export async function getConfig(): Promise<UserConfig> {
  const { data } = await api.get('/config')
  return data
}

// 更新配置
export async function updateConfig(config: Partial<UserConfig>): Promise<UserConfig> {
  const { data } = await api.put('/config', config)
  return data
}

// 获取自选股
export async function getWatchList(): Promise<{ watchlist: string[] }> {
  const { data } = await api.get('/config/watchlist')
  return data
}

// 添加自选股
export async function addToWatchList(stockCode: string) {
  const { data } = await api.post(`/config/watchlist/${stockCode}`)
  return data
}

// 删除自选股
export async function removeFromWatchList(stockCode: string) {
  const { data } = await api.delete(`/config/watchlist/${stockCode}`)
  return data
}

// 切换播报开关
export async function toggleAlert(alertType: string, enabled: boolean) {
  const { data } = await api.post('/config/alert/toggle', null, {
    params: { alert_type: alertType, enabled }
  })
  return data
}
