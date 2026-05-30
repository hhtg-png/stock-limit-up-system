function isTdxRuntime(): boolean {
  if (typeof navigator === 'undefined') return false
  return /TdxW|hong/i.test(navigator.userAgent)
}

function isDxxRuntime(): boolean {
  if (typeof navigator === 'undefined') return false
  return /dxx/i.test(navigator.userAgent)
}

function normalizeCode(code: string): string {
  const digits = code.replace(/\D/g, '').slice(-6)
  return digits ? digits.padStart(6, '0') : ''
}

const TDX_STOCK_LINK_IFRAME_ID = 'tdx-stock-link-bridge'

export function useTdxStockLink() {
  function openStock(code: string) {
    const stockCode = normalizeCode(code)
    if (!stockCode) return

    if (isDxxRuntime() && callParentStockLink(stockCode)) return
    openTreeIdLink(`http://www.treeid/CODE_${stockCode}`)
  }

  return {
    isTdxRuntime,
    openStock
  }
}

function openTreeIdLink(url: string) {
  if (isTdxRuntime()) {
    window.location.href = url
    return
  }

  if (typeof document === 'undefined') {
    window.open(url, 'tdx-stock-link')
    return
  }

  const iframe = getOrCreateTreeIdIframe()
  iframe.src = url
}

function callParentStockLink(stockCode: string) {
  try {
    if (!window.parent || window.parent === window) return false
    const parentWindow = window.parent as Window & { stocklink?: (code: string) => void }
    if (typeof parentWindow.stocklink !== 'function') return false
    parentWindow.stocklink(stockCode)
    return true
  } catch {
    return false
  }
}

function getOrCreateTreeIdIframe() {
  const existing = document.getElementById(TDX_STOCK_LINK_IFRAME_ID)
  if (existing instanceof HTMLIFrameElement) return existing

  const iframe = document.createElement('iframe')
  iframe.id = TDX_STOCK_LINK_IFRAME_ID
  iframe.name = TDX_STOCK_LINK_IFRAME_ID
  iframe.title = 'tdx stock link bridge'
  iframe.tabIndex = -1
  iframe.setAttribute('aria-hidden', 'true')
  Object.assign(iframe.style, {
    position: 'fixed',
    left: '-1px',
    top: '-1px',
    width: '1px',
    height: '1px',
    border: '0',
    opacity: '0',
    pointerEvents: 'none'
  })
  document.body.appendChild(iframe)
  return iframe
}
