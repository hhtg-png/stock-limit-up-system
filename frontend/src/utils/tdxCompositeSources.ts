export type CompositeSpeechSource = 'limit_up' | 'cls' | 'ths' | 'jygs' | 'stcn' | 'glh' | 'other'

export type CompositeSpeechSources = Record<CompositeSpeechSource, boolean>

export const COMPOSITE_SPEECH_SOURCES: { key: CompositeSpeechSource; label: string }[] = [
  { key: 'limit_up', label: '涨停事件' },
  { key: 'cls', label: '财联社' },
  { key: 'ths', label: '同花顺' },
  { key: 'jygs', label: '韭研公社' },
  { key: 'stcn', label: '时报快讯' },
  { key: 'glh', label: '格隆汇' },
  { key: 'other', label: '其他快讯' }
]

const STORAGE_KEY = 'tdx-composite-speech-sources-v1'

function defaultSources(): CompositeSpeechSources {
  return { limit_up: true, cls: true, ths: true, jygs: true, stcn: true, glh: true, other: true }
}

export function readCompositeSpeechSources(): CompositeSpeechSources {
  const preferences = defaultSources()
  try {
    const stored: unknown = JSON.parse(localStorage.getItem(STORAGE_KEY) || 'null')
    if (!stored || typeof stored !== 'object' || Array.isArray(stored)) return preferences
    const values = stored as Record<string, unknown>
    for (const { key } of COMPOSITE_SPEECH_SOURCES) {
      if (Object.prototype.hasOwnProperty.call(values, key) && typeof values[key] === 'boolean') {
        preferences[key] = values[key] as boolean
      }
    }
  } catch {
    // Embedded browsers can block localStorage or contain an older invalid value.
  }
  return preferences
}

export function writeCompositeSpeechSources(preferences: CompositeSpeechSources): void {
  try {
    const values = defaultSources()
    for (const { key } of COMPOSITE_SPEECH_SOURCES) {
      if (typeof preferences[key] === 'boolean') values[key] = preferences[key]
    }
    localStorage.setItem(STORAGE_KEY, JSON.stringify(values))
  } catch {
    // A storage restriction must not prevent in-memory source selection.
  }
}

export function resolveCompositeNewsSpeechSource(source: string): CompositeSpeechSource {
  const normalized = source.trim().toLowerCase()
  return COMPOSITE_SPEECH_SOURCES.find(item => item.key === normalized || item.label === normalized)?.key || 'other'
}
