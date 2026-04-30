import { defineStore } from 'pinia'
import { ref, computed } from 'vue'

export interface AlertMessage {
  id: string
  type: 'limit_up' | 'big_order' | 'status_change'
  stock_code: string
  stock_name: string
  content: string
  time: string
  read: boolean
}

export const useAlertStore = defineStore('alert', () => {
  // 播报消息列表
  const messages = ref<AlertMessage[]>([])
  
  // 播报开关
  const enabled = ref(true)
  const soundEnabled = ref(true)
  const desktopEnabled = ref(true)

  // 未读数量
  const unreadCount = computed(() => messages.value.filter(m => !m.read).length)

  // 添加消息
  function addMessage(msg: Omit<AlertMessage, 'id' | 'read'>) {
    const id = `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`
    messages.value.unshift({
      ...msg,
      id,
      read: false
    })

    // 限制消息数量
    if (messages.value.length > 100) {
      messages.value = messages.value.slice(0, 100)
    }

    // 播放声音
    if (soundEnabled.value && enabled.value) {
      playSound(msg.type)
    }

    // 桌面通知
    if (desktopEnabled.value && enabled.value) {
      showDesktopNotification(msg)
    }
  }

  // 标记已读
  function markRead(id: string) {
    const msg = messages.value.find(m => m.id === id)
    if (msg) {
      msg.read = true
    }
  }

  // 全部已读
  function markAllRead() {
    messages.value.forEach(m => m.read = true)
  }

  // 清空消息
  function clearMessages() {
    messages.value = []
  }

  // 设置开关
  function setEnabled(value: boolean) {
    enabled.value = value
  }

  function setSoundEnabled(value: boolean) {
    soundEnabled.value = value
  }

  function setDesktopEnabled(value: boolean) {
    desktopEnabled.value = value
  }

  // 播放声音
  function playSound(_type: string) {
    // 使用Web Audio API或Audio元素播放提示音
    try {
      const audio = new Audio('/sounds/alert.mp3')
      audio.volume = 0.5
      audio.play().catch(() => {})
    } catch (e) {
      console.warn('Sound play failed:', e)
    }
  }

  // 桌面通知
  function showDesktopNotification(msg: Omit<AlertMessage, 'id' | 'read'>) {
    if (!('Notification' in window)) return
    
    if (Notification.permission === 'granted') {
      new Notification(`${msg.stock_name} (${msg.stock_code})`, {
        body: msg.content,
        icon: '/favicon.ico'
      })
    } else if (Notification.permission !== 'denied') {
      Notification.requestPermission()
    }
  }

  return {
    messages,
    enabled,
    soundEnabled,
    desktopEnabled,
    unreadCount,
    addMessage,
    markRead,
    markAllRead,
    clearMessages,
    setEnabled,
    setSoundEnabled,
    setDesktopEnabled
  }
})
