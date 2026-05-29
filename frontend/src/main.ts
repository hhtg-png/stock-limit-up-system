const isTdxRuntime = window.location.pathname.startsWith('/tdx')

if (isTdxRuntime) {
  import('./tdx-main')
} else {
  import('./main-full')
}
