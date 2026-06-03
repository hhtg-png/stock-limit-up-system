;(function () {
  var pollTimer = null
  var mounted = false
  var lastPayloadText = ''

  function isTdxPath() {
    return /^\/tdx(\/|$)/.test(window.location.pathname)
  }

  function escapeHtml(value) {
    return String(value == null ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;')
  }

  function stockLink(code, name) {
    var safeCode = escapeHtml(code || '')
    var safeName = escapeHtml(name || code || '')
    if (!safeCode) return safeName
    return '<a class="tdx-legacy-stock" href="http://www.treeid/CODE_' + safeCode + '">' + safeName + '<span>' + safeCode + '</span></a>'
  }

  function eventStatus(item) {
    if (!item) return '--'
    if (item.event_type === 'limit_up_opened') return '炸板'
    if (item.event_type === 'limit_up_resealed') return '回封'
    return item.target_status_label || item.event_label || '--'
  }

  function requestJson(url, callback) {
    var xhr = new XMLHttpRequest()
    var sep = url.indexOf('?') === -1 ? '?' : '&'
    xhr.open('GET', url + sep + '_tdx_legacy_ts=' + new Date().getTime(), true)
    xhr.onreadystatechange = function () {
      if (xhr.readyState !== 4) return
      if (xhr.status < 200 || xhr.status >= 300) {
        callback(new Error('HTTP ' + xhr.status))
        return
      }
      try {
        callback(null, JSON.parse(xhr.responseText))
      } catch (error) {
        callback(error)
      }
    }
    xhr.send(null)
  }

  function installStyles() {
    if (document.getElementById('tdx-legacy-style')) return
    var style = document.createElement('style')
    style.id = 'tdx-legacy-style'
    style.type = 'text/css'
    style.appendChild(document.createTextNode(
      'html,body{margin:0;width:100%;height:100%;overflow:hidden;background:#05070d;color:#d6e7ff;font-family:Arial,"Microsoft YaHei",sans-serif;font-size:12px;}' +
      '#app{height:100%;background:#05070d;}' +
      '.tdx-legacy{height:100%;display:flex;flex-direction:column;background:#05070d;color:#d6e7ff;}' +
      '.tdx-legacy-top{height:30px;display:flex;align-items:center;gap:12px;padding:0 8px;border-bottom:1px solid #262c3a;background:#111722;color:#f43f5e;font-weight:700;}' +
      '.tdx-legacy-top small{color:#9aa8c7;font-weight:400;}' +
      '.tdx-legacy-status{margin-left:auto;color:#8ea4c8;font-weight:400;}' +
      '.tdx-legacy-body{flex:1;min-height:0;overflow:auto;}' +
      '.tdx-legacy-table{width:100%;border-collapse:collapse;table-layout:fixed;}' +
      '.tdx-legacy-table th{position:sticky;top:0;background:#141b28;color:#aab8d6;font-weight:400;border-bottom:1px solid #293246;z-index:1;}' +
      '.tdx-legacy-table th,.tdx-legacy-table td{height:24px;padding:2px 6px;border-bottom:1px solid #202838;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}' +
      '.tdx-legacy-table tr:nth-child(even){background:#090d15;}' +
      '.tdx-legacy-table tr:hover{background:#1a2231;}' +
      '.tdx-legacy-time{color:#8ea4c8;width:66px;}' +
      '.tdx-legacy-status-cell{color:#ff4d4f;width:64px;}' +
      '.tdx-legacy-plate{color:#ffd166;width:96px;}' +
      '.tdx-legacy-seal{color:#ff6b6b;width:86px;text-align:right;}' +
      '.tdx-legacy-reason{color:#d7e4ff;}' +
      '.tdx-legacy-stock{display:flex;align-items:center;gap:6px;color:#ff3f54;text-decoration:none;font-weight:700;}' +
      '.tdx-legacy-stock span{color:#ffb4bc;font-weight:400;}' +
      '.tdx-legacy-empty{padding:16px;color:#8896b6;}' +
      '.tdx-legacy-error{padding:16px;color:#ffb86c;}'
    ))
    document.head.appendChild(style)
  }

  function mountShell(title) {
    var app = document.getElementById('app')
    if (!app) return null
    installStyles()
    app.innerHTML =
      '<div class="tdx-legacy">' +
        '<div class="tdx-legacy-top">' +
          '<span>' + escapeHtml(title) + '</span>' +
          '<span id="tdx-legacy-status" class="tdx-legacy-status">连接中</span>' +
        '</div>' +
        '<div id="tdx-legacy-body" class="tdx-legacy-body"><div class="tdx-legacy-empty">加载中</div></div>' +
      '</div>'
    mounted = true
    return document.getElementById('tdx-legacy-body')
  }

  function updateStatus(text) {
    var node = document.getElementById('tdx-legacy-status')
    if (node) node.innerHTML = escapeHtml(text)
  }

  function renderLimitUpItems(items) {
    var body = document.getElementById('tdx-legacy-body') || mountShell('涨停播报')
    var rows = ''
    var i
    var item
    var count = items && items.length ? items.length : 0
    if (!count) {
      body.innerHTML = '<div class="tdx-legacy-empty">暂无涨停数据</div>'
      return
    }
    for (i = 0; i < count; i += 1) {
      item = items[i] || {}
      rows +=
        '<tr>' +
          '<td class="tdx-legacy-time">' + escapeHtml(item.event_time || '--') + '</td>' +
          '<td>' + stockLink(item.stock_code, item.stock_name) + '</td>' +
          '<td class="tdx-legacy-status-cell">' + escapeHtml(eventStatus(item)) + '</td>' +
          '<td class="tdx-legacy-plate">' + escapeHtml(item.target_plate || item.reason_category || '--') + '</td>' +
          '<td class="tdx-legacy-reason">' + escapeHtml(item.target_reason_summary || item.reason || '--') + '</td>' +
          '<td class="tdx-legacy-seal">' + escapeHtml(item.target_seal_amount || '--') + '</td>' +
        '</tr>'
    }
    body.innerHTML =
      '<table class="tdx-legacy-table">' +
        '<thead><tr><th class="tdx-legacy-time">时间</th><th>股票</th><th class="tdx-legacy-status-cell">状态</th><th class="tdx-legacy-plate">板块</th><th>原因</th><th class="tdx-legacy-seal">封单</th></tr></thead>' +
        '<tbody>' + rows + '</tbody>' +
      '</table>'
  }

  function loadLimitUp() {
    requestJson('/api/v1/tdx-plugins/limit-up-live/status', function (error, payload) {
      var text
      if (error) {
        updateStatus('连接失败')
        if (!lastPayloadText) {
          var body = document.getElementById('tdx-legacy-body') || mountShell('涨停播报')
          body.innerHTML = '<div class="tdx-legacy-error">接口请求失败，稍后自动重试</div>'
        }
        return
      }
      text = JSON.stringify(payload && payload.items ? payload.items : [])
      if (text !== lastPayloadText) {
        lastPayloadText = text
        renderLimitUpItems(payload.items || [])
      }
      updateStatus(payload && payload.updated_at ? payload.updated_at.replace('T', ' ').slice(11, 19) : '已更新')
    })
  }

  function renderUnsupported() {
    var body = mountShell('通达信插件')
    if (body) body.innerHTML = '<div class="tdx-legacy-empty">当前内嵌浏览器未能启动完整插件。请先使用涨停播报链接。</div>'
    updateStatus('备用')
  }

  function boot() {
    var path
    if (!isTdxPath()) return
    path = window.location.pathname
    if (pollTimer) window.clearInterval(pollTimer)
    if (path.indexOf('/tdx/ztlive') === 0 || path.indexOf('/tdx/composite') === 0 || path === '/tdx') {
      mountShell(path.indexOf('/tdx/composite') === 0 ? '复合看盘' : '涨停播报')
      loadLimitUp()
      pollTimer = window.setInterval(loadLimitUp, 1200)
      return
    }
    renderUnsupported()
  }

  window.loadTdxLegacyFallback = boot

  if (document.readyState === 'loading') {
    if (document.addEventListener) {
      document.addEventListener('DOMContentLoaded', boot)
    } else {
      window.attachEvent && window.attachEvent('onload', boot)
    }
  } else if (!mounted) {
    boot()
  }
})()
