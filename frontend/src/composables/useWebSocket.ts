import { ref, onUnmounted } from 'vue'
import { useAlertStore } from '@/stores/alert'
import { useLimitUpStore } from '@/stores/limit-up'
import { useSpeech } from '@/composables/useSpeech'

interface WebSocketMessage {
  type: string
  data: any
  timestamp: string
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
