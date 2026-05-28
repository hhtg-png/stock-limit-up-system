import { useRouter } from 'vue-router'

function isTdxRuntime(): boolean {
  if (typeof navigator === 'undefined') return false
  return /TdxW|hong/i.test(navigator.userAgent)
}

function normalizeCode(code: string): string {
  return code.replace(/\D/g, '').slice(-6)
}

export function useTdxStockLink() {
  const router = useRouter()

  function openStock(code: string) {
    const stockCode = normalizeCode(code)
    if (!stockCode) return

    if (isTdxRuntime()) {
      window.location.href = `http://www.treeid/CODE_${stockCode}`
      return
    }

    router.push({ name: 'StockDetail', params: { code: stockCode } })
  }

  return {
    isTdxRuntime,
    openStock
  }
}
