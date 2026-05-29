import { ref } from 'vue'
import { useConfigStore } from '@/stores/config'
import { useAlertStore } from '@/stores/alert'

const targetSpeechProfile = {
  lang: 'zh-CN',
  rate: 0.95,
  pitch: 1.08,
  volume: 1,
  voiceKeywords: [
    'Microsoft Xiaoxiao',
    'Microsoft Huihui',
    'Google 普通话',
    'Google Mandarin',
    '普通话',
    'Chinese'
  ]
}

// 通达信插件播报配置：目标页使用服务端音频，这里固定成接近的中文女声口径
const speechRate = ref(targetSpeechProfile.rate)
const speechPitch = ref(targetSpeechProfile.pitch)
const speechVolume = ref(targetSpeechProfile.volume)
const speechVoiceName = ref('')
const speechUnlocked = ref(false)
let voicesListenerReady = false

// 动态获取开关状态
function getSpeechEnabled(): boolean {
  try {
    const configStore = useConfigStore()
    const alertStore = useAlertStore()
    return alertStore.enabled && configStore.config.alert_limit_up_enabled
  } catch {
    return false
  }
}

// 播报队列
const speechQueue: string[] = []
let isSpeaking = false

// 已播报的股票记录（防止重复播报）
const announcedStocks = new Set<string>()
const pluginSpeechKeys = new Set<string>()

function hasSpeechSupport(): boolean {
  return typeof window !== 'undefined' &&
    'speechSynthesis' in window &&
    'SpeechSynthesisUtterance' in window
}

function setupSpeechVoices() {
  if (!hasSpeechSupport() || voicesListenerReady) return
  voicesListenerReady = true
  window.speechSynthesis.getVoices()
  window.speechSynthesis.addEventListener?.('voiceschanged', () => {
    selectTargetSpeechVoice()
  })
}

function selectTargetSpeechVoice(): SpeechSynthesisVoice | null {
  if (!hasSpeechSupport()) return null
  const voices = window.speechSynthesis.getVoices()
  const zhVoices = voices.filter(voice => /^zh/i.test(voice.lang) || /Chinese|Mandarin|普通话|中文/i.test(voice.name))
  const matched = targetSpeechProfile.voiceKeywords
    .map(keyword => zhVoices.find(voice => voice.name.includes(keyword)))
    .find(Boolean)
  const selected = matched || zhVoices[0] || null
  speechVoiceName.value = selected?.name || ''
  return selected
}

function applyTargetSpeechProfile(utterance: SpeechSynthesisUtterance) {
  setupSpeechVoices()
  const voice = selectTargetSpeechVoice()
  utterance.lang = voice?.lang || targetSpeechProfile.lang
  utterance.rate = speechRate.value
  utterance.pitch = speechPitch.value
  utterance.volume = speechVolume.value
  if (voice) {
    utterance.voice = voice
  }
}

function requiresSpeechUnlock(): boolean {
  if (typeof window === 'undefined' || typeof navigator === 'undefined') return false
  return navigator.maxTouchPoints > 0 || window.matchMedia?.('(max-width: 767px)').matches
}

function canSpeakNow(): boolean {
  return hasSpeechSupport() && (!requiresSpeechUnlock() || speechUnlocked.value)
}

// 播报函数（内部使用，不检查开关）
function speakInternal(text: string, force = false) {
  if (!text) return
  if (!force && !canSpeakNow()) return
  
  speechQueue.push(text)
  processQueue()
}

// 播报函数（检查开关）
function speak(text: string) {
  if (!getSpeechEnabled() || !text) return
  if (!canSpeakNow()) return
  
  speechQueue.push(text)
  processQueue()
}

function enqueuePluginSpeech(text: string, key?: string) {
  if (!getSpeechEnabled() || !text) return
  const speechKey = key || `plugin-${text}-${new Date().toDateString()}`
  if (pluginSpeechKeys.has(speechKey)) return
  pluginSpeechKeys.add(speechKey)
  speak(text)
}

function processQueue() {
  if (isSpeaking || speechQueue.length === 0 || !canSpeakNow()) return
  
  const text = speechQueue.shift()
  if (!text) return
  
  isSpeaking = true

  try {
    const utterance = new SpeechSynthesisUtterance(text)
    applyTargetSpeechProfile(utterance)

    utterance.onend = () => {
      isSpeaking = false
      processQueue()
    }

    utterance.onerror = () => {
      isSpeaking = false
      processQueue()
    }

    window.speechSynthesis.speak(utterance)
  } catch {
    isSpeaking = false
    speechQueue.length = 0
  }
}

function unlockSpeech(): boolean {
  if (!hasSpeechSupport()) return false

  speechUnlocked.value = true
  setupSpeechVoices()
  window.speechSynthesis.resume()
  speakInternal('语音播报已启用', true)
  return true
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
  pluginSpeechKeys.clear()
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
    speechPitch,
    speechVolume,
    speechVoiceName,
    targetSpeechProfile,
    speechUnlocked,
    speak,
    enqueuePluginSpeech,
    unlockSpeech,
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
