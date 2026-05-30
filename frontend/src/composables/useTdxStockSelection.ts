type StockSelectionWindow = Window & {
  tdxSelectStock?: (code: unknown) => void
  onTdxStockChange?: (code: unknown) => void
  stocklink?: (code: unknown) => void
}

type LocationLike = Pick<Location, 'href' | 'pathname' | 'search' | 'hash'>

const TDX_LOCATION_POLL_MS = 500
export const TDX_STOCK_SELECTION_EXAMPLES = ['CODE_000090', 'gpdm=SH600589']
const STOCK_CODE_PARAM_NAMES = [
  'code',
  'stock_code',
  'stockCode',
  'gpdm',
  'symbol',
  'tdxcode',
  'hqCode',
  'secid'
]

export function normalizeTdxSelectionCode(value: unknown): string {
  const text = String(value || '').trim()
  if (!text) return ''
  const match = text.match(/(?:CODE_|SH|SZ|BJ|sh|sz|bj)?(\d{6})(?!\d)/)
  return match ? match[1].padStart(6, '0') : ''
}

export function readTdxStockCodeFromLocation(locationLike: LocationLike = window.location): string {
  const fromSearch = readStockCodeFromSearch(locationLike.search)
  if (fromSearch) return fromSearch

  const fromHash = readStockCodeFromSearch(locationLike.hash.replace(/^#/, '?')) || normalizeTdxSelectionCode(locationLike.hash)
  if (fromHash) return fromHash

  const fromPath = normalizeTdxSelectionCode(locationLike.pathname)
  if (fromPath) return fromPath

  return normalizeTdxSelectionCode(locationLike.href)
}

export function extractTdxStockCodeFromMessage(data: unknown): string {
  if (!data) return ''
  if (typeof data === 'string' || typeof data === 'number') {
    return normalizeTdxSelectionCode(data)
  }
  if (typeof data !== 'object') return ''

  const record = data as Record<string, unknown>
  for (const key of STOCK_CODE_PARAM_NAMES) {
    const code = normalizeTdxSelectionCode(record[key])
    if (code) return code
  }

  return extractTdxStockCodeFromMessage(record.data) || extractTdxStockCodeFromMessage(record.payload)
}

export function installTdxStockSelectionBridge(onSelect: (stockCode: string) => void): () => void {
  let lastHref = window.location.href
  let lastCode = ''

  const emitIfChanged = (rawCode: unknown) => {
    const stockCode = normalizeTdxSelectionCode(rawCode)
    if (!stockCode || stockCode === lastCode) return
    lastCode = stockCode
    onSelect(stockCode)
  }

  const checkLocation = () => {
    const currentHref = window.location.href
    if (currentHref === lastHref) return
    lastHref = currentHref
    emitIfChanged(readTdxStockCodeFromLocation())
  }

  const handleLocationEvent = () => {
    lastHref = ''
    checkLocation()
  }

  const handleMessage = (event: MessageEvent) => {
    emitIfChanged(extractTdxStockCodeFromMessage(event.data))
  }

  const bridgeWindow = window as StockSelectionWindow
  const previousTdxSelectStock = bridgeWindow.tdxSelectStock
  const previousOnTdxStockChange = bridgeWindow.onTdxStockChange
  const previousStocklink = bridgeWindow.stocklink

  bridgeWindow.tdxSelectStock = emitIfChanged
  bridgeWindow.onTdxStockChange = emitIfChanged
  bridgeWindow.stocklink = emitIfChanged

  window.addEventListener('hashchange', handleLocationEvent)
  window.addEventListener('popstate', handleLocationEvent)
  window.addEventListener('message', handleMessage)

  const timer = window.setInterval(checkLocation, TDX_LOCATION_POLL_MS)
  emitIfChanged(readTdxStockCodeFromLocation())

  return () => {
    window.clearInterval(timer)
    window.removeEventListener('hashchange', handleLocationEvent)
    window.removeEventListener('popstate', handleLocationEvent)
    window.removeEventListener('message', handleMessage)
    bridgeWindow.tdxSelectStock = previousTdxSelectStock
    bridgeWindow.onTdxStockChange = previousOnTdxStockChange
    bridgeWindow.stocklink = previousStocklink
  }
}

function readStockCodeFromSearch(search: string): string {
  if (!search) return ''
  const params = new URLSearchParams(search.replace(/^#?\?/, ''))
  for (const key of STOCK_CODE_PARAM_NAMES) {
    const code = normalizeTdxSelectionCode(params.get(key))
    if (code) return code
  }
  return normalizeTdxSelectionCode(search)
}
