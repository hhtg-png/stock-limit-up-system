function isTdxRuntime(): boolean {
  if (typeof navigator === 'undefined') return false
  return /TdxW|hong/i.test(navigator.userAgent)
}

function normalizeCode(code: string): string {
  const digits = code.replace(/\D/g, '').slice(-6)
  return digits ? digits.padStart(6, '0') : ''
}

export function useTdxStockLink() {
  function openStock(code: string) {
    const stockCode = normalizeCode(code)
    if (!stockCode) return

    window.location.href = `http://www.treeid/CODE_${stockCode}`
  }

  return {
    isTdxRuntime,
    openStock
  }
}
