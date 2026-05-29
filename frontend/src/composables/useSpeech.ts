import { ref } from 'vue'
import { useConfigStore } from '@/stores/config'
import { useAlertStore } from '@/stores/alert'

const targetSpeechProfile = {
  lang: 'zh-CN',
  rate: 1.08,
  pitch: 1.05,
  volume: 0.92,
  voiceKeywords: [
    'Microsoft Xiaoxiao',
    'Microsoft Huihui',
    'Google 普通话',
    'Google Mandarin',
    '普通话',
    'Chinese'
  ]
}

const targetTtsAudioId = 'tdx-target-tts-audio'
const targetNeuralTtsEndpoint = '/api/v1/tts/speech'
const targetNeuralTtsVoice = 'zh-CN-XiaoyiNeural'
const targetAudioFallbackVolume = 0.9
const NEWS_SPEECH_SIMILARITY_WINDOW_MS = 60 * 1000
const NEWS_SPEECH_SIMILARITY_THRESHOLD = 0.8

// 通达信插件播报配置：优先播放后端 edge-tts 神经音频，失败时降级为浏览器中文语音
const speechRate = ref(targetSpeechProfile.rate)
const speechPitch = ref(targetSpeechProfile.pitch)
const speechVolume = ref(targetSpeechProfile.volume)
const speechVoiceName = ref('')
const speechUnlocked = ref(false)
let voicesListenerReady = false

type UnlockSpeechOptions = {
  silent?: boolean
}

type PluginSpeechOptions = {
  force?: boolean
}

function normalizeUnlockSpeechOptions(options: UnlockSpeechOptions | Event): UnlockSpeechOptions {
  if (typeof Event !== 'undefined' && options instanceof Event) return {}
  return options as UnlockSpeechOptions
}

// 动态获取开关状态
function getSpeechEnabled(): boolean {
  try {
    const configStore = useConfigStore()
    const alertStore = useAlertStore()
    return Boolean(
      alertStore.enabled &&
      alertStore.soundEnabled &&
      configStore.config.alert_limit_up_enabled &&
      configStore.config.alert_sound_enabled
    )
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
const recentNewsSpeechItems: Array<{ text: string; timestamp: number }> = []

function hasWebSpeechSupport(): boolean {
  return typeof window !== 'undefined' &&
    'speechSynthesis' in window &&
    'SpeechSynthesisUtterance' in window
}

function hasAudioFallbackSupport(): boolean {
  return typeof window !== 'undefined' &&
    typeof document !== 'undefined' &&
    typeof document.createElement === 'function'
}

function hasSpeechSupport(): boolean {
  return hasWebSpeechSupport() || hasAudioFallbackSupport()
}

function setupSpeechVoices() {
  if (!hasWebSpeechSupport() || voicesListenerReady) return
  voicesListenerReady = true
  window.speechSynthesis.getVoices()
  window.speechSynthesis.addEventListener?.('voiceschanged', () => {
    selectTargetSpeechVoice()
  })
}

function selectTargetSpeechVoice(): SpeechSynthesisVoice | null {
  if (!hasWebSpeechSupport()) return null
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
  if (!hasSpeechSupport()) return false
  if (!hasWebSpeechSupport()) return speechUnlocked.value
  return !requiresSpeechUnlock() || speechUnlocked.value
}

function ensureTargetTtsAudio(): HTMLAudioElement | null {
  if (!hasAudioFallbackSupport()) return null
  const existing = document.getElementById(targetTtsAudioId)
  if (existing?.tagName?.toLowerCase() === 'audio') {
    const audio = existing as HTMLAudioElement
    audio.volume = targetAudioFallbackVolume
    return audio
  }

  const audio = document.createElement('audio')
  audio.id = targetTtsAudioId
  audio.hidden = true
  audio.autoplay = true
  audio.preload = 'auto'
  audio.volume = targetAudioFallbackVolume
  document.body.appendChild(audio)
  return audio
}

function shouldUseTargetAudioPlayback(): boolean {
  return speechUnlocked.value && hasAudioFallbackSupport()
}

function buildTargetTtsUrl(text: string) {
  const params = new URLSearchParams({
    text: text.trim().slice(0, 180),
    voice: targetNeuralTtsVoice
  })
  return `${targetNeuralTtsEndpoint}?${params.toString()}`
}

function isNewsSpeechKey(speechKey: string) {
  return speechKey.startsWith('news-')
}

function normalizeNewsSpeechForSimilarity(text: string) {
  return text
    .toLowerCase()
    .replace(/韭研公社新帖|新帖/g, '')
    .replace(/[\s!-/:-@[-`{-~，。！？；：、“”‘’（）()【】[\]《》<>·…—￥]+/g, '')
}

function buildCharacterPairs(text: string) {
  if (text.length <= 1) return text ? [text] : []
  const pairs: string[] = []
  for (let index = 0; index < text.length - 1; index += 1) {
    pairs.push(text.slice(index, index + 2))
  }
  return pairs
}

function calculateTitleSimilarity(first: string, second: string) {
  if (first === second) return first ? 1 : 0
  const firstPairs = buildCharacterPairs(first)
  const secondPairs = buildCharacterPairs(second)
  if (!firstPairs.length || !secondPairs.length) return 0

  const secondCounts = new Map<string, number>()
  for (const pair of secondPairs) {
    secondCounts.set(pair, (secondCounts.get(pair) || 0) + 1)
  }

  let intersection = 0
  for (const pair of firstPairs) {
    const count = secondCounts.get(pair) || 0
    if (count <= 0) continue
    intersection += 1
    if (count === 1) {
      secondCounts.delete(pair)
    } else {
      secondCounts.set(pair, count - 1)
    }
  }

  return (2 * intersection) / (firstPairs.length + secondPairs.length)
}

function pruneRecentNewsSpeech(now: number) {
  for (let index = recentNewsSpeechItems.length - 1; index >= 0; index -= 1) {
    if (now - recentNewsSpeechItems[index].timestamp > NEWS_SPEECH_SIMILARITY_WINDOW_MS) {
      recentNewsSpeechItems.splice(index, 1)
    }
  }
}

function isSimilarRecentNewsSpeech(text: string, speechKey: string, now = Date.now()) {
  if (!isNewsSpeechKey(speechKey)) return false
  pruneRecentNewsSpeech(now)

  const normalizedText = normalizeNewsSpeechForSimilarity(text)
  if (!normalizedText) return false

  const shouldSkip = recentNewsSpeechItems.some(item =>
    calculateTitleSimilarity(normalizedText, item.text) >= NEWS_SPEECH_SIMILARITY_THRESHOLD
  )
  if (!shouldSkip) {
    recentNewsSpeechItems.push({ text: normalizedText, timestamp: now })
  }
  return shouldSkip
}

function finishSpeechItem() {
  isSpeaking = false
  processQueue()
}

function playWithWebSpeech(text: string) {
  if (!hasWebSpeechSupport()) {
    finishSpeechItem()
    return
  }

  try {
    const utterance = new SpeechSynthesisUtterance(text)
    applyTargetSpeechProfile(utterance)
    utterance.onend = () => {
      finishSpeechItem()
    }

    utterance.onerror = () => {
      finishSpeechItem()
    }

    window.speechSynthesis.speak(utterance)
  } catch {
    finishSpeechItem()
  }
}

function playWithAudioFallback(text: string, onFailure: () => void = finishSpeechItem) {
  const audio = ensureTargetTtsAudio()
  if (!audio) {
    onFailure()
    return
  }

  let settled = false
  const finishOnce = () => {
    if (settled) return
    settled = true
    finishSpeechItem()
  }
  const failOnce = () => {
    if (settled) return
    settled = true
    onFailure()
  }

  audio.onended = finishOnce
  audio.onerror = failOnce
  audio.src = buildTargetTtsUrl(text)
  const playPromise = audio.play()
  if (playPromise?.catch) {
    playPromise.catch(failOnce)
  }
}

function playWithNeuralTts(text: string) {
  playWithAudioFallback(text, () => {
    playWithWebSpeech(text)
  })
}

// 播报函数（内部使用，不检查开关）
function speakInternal(text: string, force = false) {
  if (!text) return
  if (!force && !canSpeakNow()) return
  
  speechQueue.push(text)
  processQueue()
}

// 播报函数（检查开关）
function speak(text: string): boolean {
  if (!getSpeechEnabled() || !text) return false
  if (!canSpeakNow()) return false
  
  speechQueue.push(text)
  processQueue()
  return true
}

function enqueuePluginSpeech(text: string, key?: string, options: PluginSpeechOptions = {}): boolean {
  if (!(options.force || getSpeechEnabled()) || !text) return false
  if (!canSpeakNow()) return false
  const speechKey = key || `plugin-${text}-${new Date().toDateString()}`
  if (pluginSpeechKeys.has(speechKey)) return false
  pluginSpeechKeys.add(speechKey)
  if (isSimilarRecentNewsSpeech(text, speechKey)) return false
  speechQueue.push(text)
  processQueue()
  return true
}

function processQueue() {
  if (isSpeaking || speechQueue.length === 0 || !canSpeakNow()) return
  
  const text = speechQueue.shift()
  if (!text) return
  
  isSpeaking = true

  try {
    if (shouldUseTargetAudioPlayback()) {
      playWithNeuralTts(text)
      return
    }

    playWithWebSpeech(text)
  } catch {
    finishSpeechItem()
  }
}

function unlockSpeech(options: UnlockSpeechOptions | Event = {}): boolean {
  if (!hasSpeechSupport()) return false
  normalizeUnlockSpeechOptions(options)

  speechUnlocked.value = true
  setupSpeechVoices()
  ensureTargetTtsAudio()
  if (hasWebSpeechSupport()) {
    window.speechSynthesis.resume()
  }
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
  recentNewsSpeechItems.splice(0)
}

// 测试播报
function testSpeech() {
  speakInternal('语音播报功能正常', true)
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
