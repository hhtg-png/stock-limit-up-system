import { ref, computed } from 'vue'
import { useConfigStore } from '@/stores/config'

// 语音播报配置 - 从config store获取设置
const speechRate = ref(1.2) // 语速
const speechVolume = ref(0.8) // 音量

// 动态获取开关状态（必须同时检查初始化状态）
function getSpeechEnabled(): boolean {
  try {
    const configStore = useConfigStore()
    // 如果配置未初始化，默认不播报，避免意外播报
    if (!configStore.initialized) {
      return false
    }
    return configStore.config.alert_limit_up_enabled
  } catch {
    return false
  }
}

// 播报队列
const speechQueue: string[] = []
let isSpeaking = false

// 已播报的股票记录（防止重复播报）
const announcedStocks = new Set<string>()

// 播报函数（内部使用，不检查开关）
function speakInternal(text: string) {
  if (!text) return
  
  speechQueue.push(text)
  processQueue()
}

// 播报函数（检查开关）
function speak(text: string) {
  if (!getSpeechEnabled() || !text) return
  
  speechQueue.push(text)
  processQueue()
}

function processQueue() {
  if (isSpeaking || speechQueue.length === 0) return
  
  const text = speechQueue.shift()
  if (!text) return
  
  isSpeaking = true
  
  const utterance = new SpeechSynthesisUtterance(text)
  utterance.lang = 'zh-CN'
  utterance.rate = speechRate.value
  utterance.volume = speechVolume.value
  
  utterance.onend = () => {
    isSpeaking = false
    processQueue()
  }
  
  utterance.onerror = () => {
    isSpeaking = false
    processQueue()
  }
  
  window.speechSynthesis.speak(utterance)
}

// 播报涨停股票
function announceStock(stockName: string, reason?: string) {
  if (!getSpeechEnabled()) return
  
  const key = `limitup-${stockName}-${new Date().toDateString()}`
  if (announcedStocks.has(key)) return
  
  announcedStocks.add(key)
  
  let text = stockName + '涨停'
  if (reason) {
    // 简化原因，只取前面部分
    const shortReason = reason.split(/[,，;；]/)[0].slice(0, 15)
    text += '，' + shortReason
  }
  
  speak(text)
}

// 播报新涨停（批量）
function announceNewStocks(stocks: Array<{ stock_name: string; limit_up_reason?: string }>) {
  if (!getSpeechEnabled()) return
  
  for (const stock of stocks) {
    announceStock(stock.stock_name, stock.limit_up_reason)
  }
}

// 清除今日已播报记录
function clearAnnounced() {
  announcedStocks.clear()
}

// 测试播报
function testSpeech() {
  speakInternal('语音播报功能正常')
}

// 播报回封
function announceReseal(stockName: string) {
  if (!getSpeechEnabled()) return
  speak(stockName + '回封')
}

// 播报状态变化
function announceStatusChange(stockName: string, status: string) {
  if (!getSpeechEnabled()) return
  
  if (status === 'resealed') {
    announceReseal(stockName)
  }
}

// 播报开启提示
function announceEnabled() {
  speakInternal('播报已开启')
}

// 播报关闭提示
function announceDisabled() {
  speakInternal('播报已关闭')
}

export function useSpeech() {
  return {
    speechRate,
    speechVolume,
    speak,
    announceStock,
    announceNewStocks,
    announceReseal,
    announceStatusChange,
    clearAnnounced,
    testSpeech,
    announceEnabled,
    announceDisabled
  }
}
