import { ref, onUnmounted } from 'vue'
import { useAlertStore } from '@/stores/alert'
import { useLimitUpStore } from '@/stores/limit-up'
import { useSpeech } from '@/composables/useSpeech'
import type { TdxLimitUpEvent, TdxNewsItem } from '@/types/tdx-plugins'

interface WebSocketMessage {
  type: string
  data: any
  timestamp: string
}

const MAX_TDX_REALTIME_ITEMS = 120
const tdxNewsItems = ref<TdxNewsItem[]>([])
const tdxLimitUpEvents = ref<TdxLimitUpEvent[]>([])

function formatWsClock(timestamp: string) {
  const date = new Date(timestamp)
  if (Number.isNaN(date.getTime())) return ''
  return date.toLocaleTimeString('zh-CN', {
    hour12: false,
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  })
}

function normalizeStringList(value: unknown): string[] {
  if (!Array.isArray(value)) return []
  return value
    .map(item => {
      if (typeof item === 'string') return item
      if (item && typeof item === 'object') {
        const record = item as Record<string, unknown>
        return String(record.name || record.stock_name || record.code || '').trim()
      }
      return ''
    })
    .filter(Boolean)
}

function pushUniqueById<T extends Record<string, any>>(
  list: T[],
  item: T,
  idKey: keyof T,
  limit = MAX_TDX_REALTIME_ITEMS
) {
  const id = String(item[idKey] || '')
  const next = id ? list.filter(existing => String(existing[idKey] || '') !== id) : [...list]
  next.unshift(item)
  return next.slice(0, limit)
}

function normalizeTdxNewsItem(data: Record<string, any>, timestamp: string): TdxNewsItem {
  const title = String(data.title || data.msg || data.speech_text || '实时快讯')
  const content = String(data.content || data.digest || data.msg || title)
  const newsId = String(data.news_id || data.event_id || `ws-news-${timestamp}-${title}`)
  return {
    news_id: newsId,
    time: String(data.time || data.event_time || formatWsClock(timestamp)),
    source: String(data.source || data.from || '实时快讯'),
    title,
    content,
    importance: Number(data.importance ?? data.import ?? 70),
    related_stocks: normalizeStringList(data.related_stocks || data.codes || data.stocks),
    related_plates: normalizeStringList(data.related_plates || data.plates || data.concepts),
    jump_url: String(data.jump_url || data.url || data.readurl || '')
  }
}

function normalizeTdxLimitUpEvent(data: Record<string, any>, timestamp: string): TdxLimitUpEvent {
  const stockCode = String(data.stock_code || data.code || '')
  const eventTime = String(data.event_time || data.time || formatWsClock(timestamp))
  const isSealed = data.is_sealed ?? data.event_type !== 'limit_up_opened'
  const eventLabel = String(data.event_label || (isSealed ? '封死涨停' : '涨停打开'))
  const eventType = String(data.event_type || (isSealed ? 'limit_up_sealed' : 'limit_up_opened'))
  return {
    event_id: String(data.event_id || `tdx-ws-${stockCode}-${eventTime}-${eventType}`),
    event_type: eventType,
    event_label: eventLabel,
    event_time: eventTime,
    stock_code: stockCode,
    stock_name: String(data.stock_name || data.name || stockCode),
    board: Number(data.board || data.continuous_days || data.ztnum || 1),
    reason: String(data.reason || data.limit_up_reason || ''),
    reason_category: String(data.reason_category || data.target_plate || '其他'),
    change_pct: Number(data.change_pct || 0),
    seal_amount: Number(data.seal_amount || 0),
    amount: Number(data.amount || 0),
    turnover_rate: Number(data.turnover_rate || 0),
    is_sealed: Boolean(isSealed),
    open_count: Number(data.open_count || 0),
    sources: normalizeStringList(data.sources),
    target_status_label: !isSealed || eventType === 'limit_up_opened' ? '炸板' : String(data.target_status_label || eventLabel),
    target_plate: String(data.target_plate || data.reason_category || ''),
    target_reason_summary: String(data.target_reason_summary || data.reason || ''),
    target_seal_amount: String(data.target_seal_amount || '')
  }
}

function pushTdxNewsItem(data: Record<string, any>, timestamp: string) {
  const item = normalizeTdxNewsItem(data, timestamp)
  tdxNewsItems.value = pushUniqueById(tdxNewsItems.value, item, 'news_id')
  return item
}

function pushTdxLimitUpEvent(data: Record<string, any>, timestamp: string) {
  const item = normalizeTdxLimitUpEvent(data, timestamp)
  tdxLimitUpEvents.value = pushUniqueById(tdxLimitUpEvents.value, item, 'event_id')
  return item
}

export function useTdxPluginRealtime() {
  return {
    realtimeNewsItems: tdxNewsItems,
    realtimeLimitUpEvents: tdxLimitUpEvents
  }
}

export function useWebSocket() {
  const ws = ref<WebSocket | null>(null)
  const isConnected = ref(false)
  const reconnectAttempts = ref(0)
  const maxReconnectAttempts = 10
  
  const alertStore = useAlertStore()
  const limitUpStore = useLimitUpStore()

  let reconnectTimer: number | null = null
  let pingTimer: number | null = null

  function connect() {
    if (ws.value?.readyState === WebSocket.OPEN) return

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const host = window.location.host
    const wsUrl = `${protocol}//${host}/ws/realtime`

    try {
      ws.value = new WebSocket(wsUrl)

      ws.value.onopen = () => {
        console.log('WebSocket connected')
        isConnected.value = true
        reconnectAttempts.value = 0
        startPing()
      }

      ws.value.onmessage = (event) => {
        try {
          const message: WebSocketMessage = JSON.parse(event.data)
          handleMessage(message)
        } catch (e) {
          console.error('Parse message error:', e)
        }
      }

      ws.value.onclose = () => {
        console.log('WebSocket disconnected')
        isConnected.value = false
        stopPing()
        scheduleReconnect()
      }

      ws.value.onerror = (error) => {
        console.error('WebSocket error:', error)
      }
    } catch (e) {
      console.error('WebSocket connect error:', e)
      scheduleReconnect()
    }
  }

  function disconnect() {
    if (reconnectTimer) {
      clearTimeout(reconnectTimer)
      reconnectTimer = null
    }
    stopPing()
    
    if (ws.value) {
      ws.value.close()
      ws.value = null
    }
    isConnected.value = false
  }

  function handleMessage(message: WebSocketMessage) {
    switch (message.type) {
      case 'connected':
        console.log('Connection confirmed:', message.data)
        break

      case 'ping':
        send({ type: 'pong' })
        break

      case 'limit_up_alert':
        alertStore.addMessage({
          type: 'limit_up',
          stock_code: message.data.stock_code,
          stock_name: message.data.stock_name,
          content: `首次涨停 ${message.data.time}${message.data.reason ? ` - ${message.data.reason}` : ''}`,
          time: message.timestamp
        })
        if (message.data.stock_name) {
          const { announceStock } = useSpeech()
          announceStock(message.data.stock_name, message.data.reason)
        }
        break

      case 'limit_up_snapshot':
        limitUpStore.setRealtimeSnapshot(
          message.data.trade_date || '',
          message.data.items || []
        )
        break

      case 'limit_up_delta':
        limitUpStore.applyRealtimeDelta(
          message.data.upsert || [],
          message.data.remove || [],
          message.data.trade_date || ''
        )
        break

      case 'big_order_alert':
        const direction = message.data.direction === 'buy' ? '买入' : '卖出'
        const amount = (message.data.amount / 10000).toFixed(2)
        alertStore.addMessage({
          type: 'big_order',
          stock_code: message.data.stock_code,
          stock_name: message.data.stock_name,
          content: `大单${direction} ${amount}万`,
          time: message.timestamp
        })
        break

      case 'status_change':
        const statusMap: Record<string, string> = {
          'sealed': '封板',
          'opened': '开板',
          'resealed': '回封'
        }
        alertStore.addMessage({
          type: 'status_change',
          stock_code: message.data.stock_code,
          stock_name: message.data.stock_name,
          content: `${statusMap[message.data.status] || message.data.status} ${message.data.time}`,
          time: message.timestamp
        })
        
        // 回封语音播报
        if (message.data.status === 'resealed') {
          const { announceReseal } = useSpeech()
          announceReseal(message.data.stock_name)
        }
        
        // 更新涨停状态
        limitUpStore.updateItem(message.data.stock_code, {
          is_sealed: message.data.status === 'sealed' || message.data.status === 'resealed'
        })
        break

      case 'tdx_limit_up_event':
        pushTdxLimitUpEvent(message.data || {}, message.timestamp)
        break

      case 'tdx_stock_move_event':
        if (message.data.stock_name) {
          const { enqueuePluginSpeech } = useSpeech()
          enqueuePluginSpeech(
            message.data.speech_text || `${message.data.stock_name}出现异动解析更新`,
            message.data.event_id || `stock-move-${message.data.stock_code}-${message.timestamp}`,
            { force: true }
          )
        }
        break

      case 'tdx_news_event':
        pushTdxNewsItem(message.data || {}, message.timestamp)
        break

      case 'tdx_plate_strength_update':
        if (message.data.speech_text) {
          const { enqueuePluginSpeech } = useSpeech()
          enqueuePluginSpeech(
            message.data.speech_text,
            message.data.event_id || `plate-strength-${message.timestamp}`,
            { force: true }
          )
        }
        break

      case 'market_update':
        // 处理行情更新
        break
    }
  }

  function send(data: object) {
    if (ws.value?.readyState === WebSocket.OPEN) {
      ws.value.send(JSON.stringify(data))
    }
  }

  function subscribeStocks(codes: string[]) {
    send({ type: 'subscribe_stocks', data: { stocks: codes } })
  }

  function unsubscribeStocks(codes: string[]) {
    send({ type: 'unsubscribe_stocks', data: { stocks: codes } })
  }

  function startPing() {
    pingTimer = window.setInterval(() => {
      if (ws.value?.readyState === WebSocket.OPEN) {
        send({ type: 'ping' })
      }
    }, 30000)
  }

  function stopPing() {
    if (pingTimer) {
      clearInterval(pingTimer)
      pingTimer = null
    }
  }

  function scheduleReconnect() {
    if (reconnectAttempts.value >= maxReconnectAttempts) {
      console.log('Max reconnect attempts reached')
      return
    }

    const delay = Math.min(1000 * Math.pow(2, reconnectAttempts.value), 30000)
    reconnectTimer = window.setTimeout(() => {
      reconnectAttempts.value++
      connect()
    }, delay)
  }

  onUnmounted(() => {
    disconnect()
  })

  return {
    ws,
    isConnected,
    connect,
    disconnect,
    send,
    subscribeStocks,
    unsubscribeStocks
  }
}
