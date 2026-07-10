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
const targetSilentAudioUrl = 'data:audio/wav;base64,UklGRrQBAABXQVZFZm10IBAAAAABAAEAQB8AAEAfAAABAAgAZGF0YZABAACAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICA'
const NEURAL_TTS_START_TIMEOUT_MS = 5000
const SPEECH_PLAYBACK_LOG_ENDPOINT = '/api/v1/tts/playback-log'
const SPEECH_PLAYBACK_LOG_TEXT_LIMIT = 120
const SPEECH_UNLOCK_STORAGE_KEY = 'tdx-plugin-speech-unlocked'
const NEWS_SPEECH_SIMILARITY_WINDOW_MS = 60 * 1000
const NEWS_SPEECH_SIMILARITY_THRESHOLD = 0.8

// 通达信插件播报配置：优先播放后端 edge-tts 神经音频，失败时降级为浏览器中文语音
const speechRate = ref(targetSpeechProfile.rate)
const speechPitch = ref(targetSpeechProfile.pitch)
const speechVolume = ref(targetSpeechProfile.volume)
const speechVoiceName = ref('')
const speechPreferenceEnabled = ref(readStoredSpeechUnlocked())
const speechUnlocked = ref(false)
let voicesListenerReady = false
let reportedSessionLock = false

type UnlockSpeechOptions = {
  silent?: boolean
}

type PluginSpeechOptions = {
  force?: boolean
  urgent?: boolean
}

type SpeechPlaybackMode = 'auto' | 'web-speech'

type SpeechQueueItem = {
  text: string
  mode: SpeechPlaybackMode
  urgent?: boolean
}

type SpeechPlaybackLogDetail = Record<string, string | number | boolean | null | undefined>

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
const speechQueue: SpeechQueueItem[] = []
let isSpeaking = false
let currentSpeechToken = 0
let currentSpeechUrgent = false

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

function readStoredSpeechUnlocked(): boolean {
  if (typeof window === 'undefined') return false
  try {
    return window.localStorage.getItem(SPEECH_UNLOCK_STORAGE_KEY) === '1'
  } catch {
    return false
  }
}

function persistSpeechUnlocked(enabled: boolean) {
  if (typeof window === 'undefined') return
  try {
    window.localStorage.setItem(SPEECH_UNLOCK_STORAGE_KEY, enabled ? '1' : '0')
  } catch {
    // 本地存储不可用时只保持当前页面状态。
  }
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

function playbackNow() {
  if (typeof performance !== 'undefined' && typeof performance.now === 'function') {
    return performance.now()
  }
  return Date.now()
}

function reportSpeechPlaybackStatus(
  stage: string,
  text: string,
  detail: SpeechPlaybackLogDetail = {}
) {
  if (typeof window === 'undefined') return

  const payload = {
    stage,
    mode: detail.mode || 'neural-audio',
    text: text.trim().slice(0, SPEECH_PLAYBACK_LOG_TEXT_LIMIT),
    elapsed_ms: typeof detail.elapsed_ms === 'number' ? detail.elapsed_ms : undefined,
    detail
  }
  const body = JSON.stringify(payload)

  try {
    console.debug?.('[tdx-speech]', stage, payload)
  } catch {
    // 控制台不可用不影响播报。
  }

  if (sendSpeechPlaybackXhr(body)) return

  try {
    if (typeof fetch === 'function') {
      fetch(SPEECH_PLAYBACK_LOG_ENDPOINT, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body
      }).catch(() => {
        sendSpeechPlaybackBeacon(body)
      })
      return
    }
  } catch {
    // 老 WebView 对 keepalive fetch 支持不完整时回退到 beacon。
  }

  sendSpeechPlaybackBeacon(body)
}

function sendSpeechPlaybackXhr(body: string) {
  try {
    if (typeof XMLHttpRequest === 'undefined') return false
    const request = new XMLHttpRequest()
    request.open('POST', SPEECH_PLAYBACK_LOG_ENDPOINT, true)
    request.setRequestHeader('Content-Type', 'application/json')
    request.send(body)
    return true
  } catch {
    return false
  }
}

function sendSpeechPlaybackBeacon(body: string) {
  try {
    if (
      typeof navigator !== 'undefined' &&
      typeof navigator.sendBeacon === 'function' &&
      typeof Blob !== 'undefined'
    ) {
      navigator.sendBeacon(
        SPEECH_PLAYBACK_LOG_ENDPOINT,
        new Blob([body], { type: 'application/json' })
      )
    }
  } catch {
    // 播放诊断不能影响真实播报。
  }
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

function finishSpeechItem(token = currentSpeechToken) {
  if (token !== currentSpeechToken) return
  isSpeaking = false
  currentSpeechUrgent = false
  processQueue()
}

function playWithWebSpeech(text: string, token = currentSpeechToken) {
  if (!hasWebSpeechSupport()) {
    reportSpeechPlaybackStatus('web_speech_unavailable', text, { mode: 'web-speech' })
    finishSpeechItem(token)
    return
  }

  try {
    const startedAt = playbackNow()
    const utterance = new SpeechSynthesisUtterance(text)
    applyTargetSpeechProfile(utterance)
    reportSpeechPlaybackStatus('web_speech_start', text, {
      mode: 'web-speech',
      voice: speechVoiceName.value || undefined,
      token
    })
    utterance.onend = () => {
      reportSpeechPlaybackStatus('web_speech_end', text, {
        mode: 'web-speech',
        elapsed_ms: Math.round(playbackNow() - startedAt),
        token
      })
      finishSpeechItem(token)
    }

    utterance.onerror = (event) => {
      reportSpeechPlaybackStatus('web_speech_error', text, {
        mode: 'web-speech',
        elapsed_ms: Math.round(playbackNow() - startedAt),
        error: event.error,
        token
      })
      finishSpeechItem(token)
    }

    window.speechSynthesis.speak(utterance)
  } catch (error) {
    reportSpeechPlaybackStatus('web_speech_exception', text, {
      mode: 'web-speech',
      error: error instanceof Error ? error.message : String(error),
      token
    })
    finishSpeechItem(token)
  }
}

function playWithAudioFallback(text: string, onFailure?: () => void, token = currentSpeechToken) {
  const audio = ensureTargetTtsAudio()
  if (!audio) {
    reportSpeechPlaybackStatus('audio_element_missing', text, { token })
    if (onFailure) onFailure()
    else finishSpeechItem(token)
    return
  }

  let settled = false
  let startTimeoutId: ReturnType<typeof setTimeout> | undefined
  const startedAt = playbackNow()
  const elapsedDetail = () => Math.round(playbackNow() - startedAt)
  const clearStartTimeout = () => {
    if (startTimeoutId === undefined) return
    clearTimeout(startTimeoutId)
    startTimeoutId = undefined
  }
  const cleanupAudioHandlers = () => {
    clearStartTimeout()
    audio.onended = null
    audio.onerror = null
    audio.onplaying = null
    audio.oncanplay = null
  }
  const finishOnce = () => {
    if (settled) return
    settled = true
    reportSpeechPlaybackStatus('audio_ended', text, {
      elapsed_ms: elapsedDetail(),
      token
    })
    cleanupAudioHandlers()
    finishSpeechItem(token)
  }
  const failOnce = (
    reasonOrEvent: string | Event = 'audio_error',
    error?: unknown,
    shouldReport = true
  ) => {
    if (settled) return
    settled = true
    const reason = typeof reasonOrEvent === 'string' ? reasonOrEvent : 'audio_error'
    if (shouldReport) {
      reportSpeechPlaybackStatus(reason, text, {
        elapsed_ms: elapsedDetail(),
        error: error instanceof Error ? error.message : undefined,
        token
      })
    }
    cleanupAudioHandlers()
    if (token !== currentSpeechToken) return
    if (onFailure) onFailure()
    else finishSpeechItem(token)
  }
  const markCanPlay = () => {
    reportSpeechPlaybackStatus('audio_canplay', text, {
      elapsed_ms: elapsedDetail(),
      token
    })
    clearStartTimeout()
  }
  const markStarted = () => {
    reportSpeechPlaybackStatus('audio_playing', text, {
      elapsed_ms: elapsedDetail(),
      token
    })
    clearStartTimeout()
  }

  audio.onended = finishOnce
  audio.onerror = (event) => failOnce('audio_error', event)
  audio.onplaying = markStarted
  audio.oncanplay = markCanPlay
  audio.src = buildTargetTtsUrl(text)
  startTimeoutId = setTimeout(() => {
    if (settled) return
    reportSpeechPlaybackStatus('audio_timeout', text, {
      elapsed_ms: elapsedDetail(),
      timeout_ms: NEURAL_TTS_START_TIMEOUT_MS,
      token
    })
    try {
      audio.pause()
      audio.removeAttribute('src')
      audio.load?.()
    } catch {
      // 仅用于让慢速神经音频降级，清理失败时继续走失败路径。
    }
    failOnce('audio_timeout', undefined, false)
  }, NEURAL_TTS_START_TIMEOUT_MS)

  try {
    reportSpeechPlaybackStatus('audio_play_request', text, {
      timeout_ms: NEURAL_TTS_START_TIMEOUT_MS,
      token
    })
    const playPromise = audio.play()
    if (playPromise?.catch) {
      playPromise.catch((error) => failOnce('audio_play_rejected', error))
    }
  } catch (error) {
    failOnce('audio_play_exception', error)
  }
}

function playWithNeuralTts(text: string, token = currentSpeechToken) {
  playWithAudioFallback(text, () => {
    reportSpeechPlaybackStatus('web_speech_fallback', text, { token })
    playWithWebSpeech(text, token)
  }, token)
}

function resetTargetAudioAfterPrime(audio: HTMLAudioElement) {
  try {
    audio.pause()
    audio.removeAttribute('src')
    audio.load?.()
  } catch {
    // 音频预热失败时只影响当前页面的语音解锁状态。
  }
  audio.volume = targetAudioFallbackVolume
  audio.muted = false
}

function primeTargetTtsAudio() {
  const audio = ensureTargetTtsAudio()
  if (!audio) return false

  try {
    audio.pause()
    audio.muted = false
    audio.volume = 0.01
    audio.src = targetSilentAudioUrl
    const playPromise = audio.play()
    if (playPromise?.then) {
      playPromise
        .then(() => {
          resetTargetAudioAfterPrime(audio)
          reportSpeechPlaybackStatus('speech_unlock_prime_ok', '', { mode: 'neural-audio' })
        })
        .catch((error) => {
          speechUnlocked.value = false
          resetTargetAudioAfterPrime(audio)
          reportSpeechPlaybackStatus('speech_unlock_prime_rejected', '', {
            mode: 'neural-audio',
            error: error instanceof Error ? error.message : String(error)
          })
        })
    } else {
      resetTargetAudioAfterPrime(audio)
      reportSpeechPlaybackStatus('speech_unlock_prime_ok', '', { mode: 'neural-audio' })
    }
    return true
  } catch (error) {
    speechUnlocked.value = false
    resetTargetAudioAfterPrime(audio)
    reportSpeechPlaybackStatus('speech_unlock_prime_exception', '', {
      mode: 'neural-audio',
      error: error instanceof Error ? error.message : String(error)
    })
    return false
  }
}

function stopCurrentSpeechPlayback() {
  currentSpeechToken += 1
  isSpeaking = false
  currentSpeechUrgent = false

  if (hasWebSpeechSupport()) {
    window.speechSynthesis.cancel()
  }

  if (hasAudioFallbackSupport()) {
    const audio = document.getElementById(targetTtsAudioId)
    if (audio?.tagName?.toLowerCase() === 'audio') {
      const target = audio as HTMLAudioElement
      target.pause()
      target.removeAttribute('src')
      target.load?.()
    }
  }
}

function queueSpeechItem(text: string, options: { urgent?: boolean; mode?: SpeechPlaybackMode } = {}) {
  const item: SpeechQueueItem = {
    text,
    mode: options.mode || 'auto',
    urgent: options.urgent
  }

  if (options.urgent) {
    speechQueue.unshift(item)
    if (isSpeaking && !currentSpeechUrgent) {
      stopCurrentSpeechPlayback()
    }
  } else {
    speechQueue.push(item)
  }
  processQueue()
}

// 播报函数（内部使用，不检查开关）
function speakInternal(text: string, force = false) {
  if (!text) return
  if (!force && !canSpeakNow()) return
  
  queueSpeechItem(text)
}

// 播报函数（检查开关）
function speak(text: string): boolean {
  if (!getSpeechEnabled() || !text) return false
  if (!canSpeakNow()) return false
  
  queueSpeechItem(text)
  return true
}

function enqueuePluginSpeech(text: string, key?: string, options: PluginSpeechOptions = {}): boolean {
  if (!(options.force || getSpeechEnabled()) || !text) return false
  if (!speechPreferenceEnabled.value) return false
  if (!speechUnlocked.value) {
    if (!reportedSessionLock) {
      reportedSessionLock = true
      reportSpeechPlaybackStatus('speech_session_locked', text, {
        reason: 'user_activation_required'
      })
    }
    return false
  }
  if (!canSpeakNow()) return false
  const speechKey = key || `plugin-${text}-${new Date().toDateString()}`
  if (pluginSpeechKeys.has(speechKey)) return false
  pluginSpeechKeys.add(speechKey)
  if (isSimilarRecentNewsSpeech(text, speechKey)) return false
  if (options.urgent) {
    queueSpeechItem(text, { urgent: true })
  } else {
    queueSpeechItem(text)
  }
  return true
}

function processQueue() {
  if (isSpeaking || speechQueue.length === 0 || !canSpeakNow()) return
  
  const item = speechQueue.shift()
  if (!item?.text) return
  
  isSpeaking = true
  currentSpeechUrgent = Boolean(item.urgent)
  const token = ++currentSpeechToken

  try {
    if (item.mode !== 'web-speech' && shouldUseTargetAudioPlayback()) {
      playWithNeuralTts(item.text, token)
      return
    }

    playWithWebSpeech(item.text, token)
  } catch {
    finishSpeechItem(token)
  }
}

function unlockSpeech(options: UnlockSpeechOptions | Event = {}): boolean {
  if (!hasSpeechSupport()) return false
  normalizeUnlockSpeechOptions(options)

  speechPreferenceEnabled.value = true
  speechUnlocked.value = true
  reportedSessionLock = false
  persistSpeechUnlocked(true)
  setupSpeechVoices()
  ensureTargetTtsAudio()
  primeTargetTtsAudio()
  if (hasWebSpeechSupport()) {
    window.speechSynthesis.resume()
  }
  return true
}

function lockSpeech() {
  speechPreferenceEnabled.value = false
  speechUnlocked.value = false
  reportedSessionLock = false
  persistSpeechUnlocked(false)
  speechQueue.splice(0)
  stopCurrentSpeechPlayback()
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
    lockSpeech,
    announceStock,
    announceNewStocks,
    announceReseal,
    announceStatusChange,
    clearAnnounced,
    testSpeech,
    announceDisabled
  }
}
