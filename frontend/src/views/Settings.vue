<template>
  <div class="settings">
    <el-row :gutter="16">
      <el-col :span="24">
        <div class="card">
          <div class="card-title-row">
            <h3>AI 总结设置</h3>
            <el-tag :type="config.deepseek_api_key_configured ? 'success' : 'warning'">
              {{ config.deepseek_api_key_configured ? 'DeepSeek Key 已配置' : 'DeepSeek Key 未配置' }}
            </el-tag>
          </div>
          <el-form label-width="140px">
            <el-form-item label="API 地址">
              <el-input
                v-model="config.deepseek_base_url"
                placeholder="https://api.deepseek.com"
                style="max-width: 520px"
              />
            </el-form-item>
            <el-form-item label="模型">
              <el-input
                v-model="config.deepseek_model"
                placeholder="deepseek-v4-pro"
                style="max-width: 320px"
              />
            </el-form-item>
            <el-form-item label="API Key">
              <div class="secret-input-row">
                <el-input
                  v-model="deepseekApiKey"
                  type="password"
                  autocomplete="new-password"
                  :placeholder="config.deepseek_api_key_configured ? '已保存，重新输入可覆盖' : '输入 DeepSeek API Key'"
                  style="max-width: 520px"
                />
                <el-button type="primary" :loading="savingDeepSeek" @click="saveDeepSeekConfig">
                  保存AI配置
                </el-button>
              </div>
              <div class="form-hint">保存后密钥不会从接口返回，也不会在页面回显。</div>
            </el-form-item>
          </el-form>
        </div>
      </el-col>
    </el-row>

    <el-row :gutter="16">
      <el-col :span="24">
        <div class="card temporary-notebook-card">
          <div class="card-title-row notebook-title-row">
            <h3>临时记录本</h3>
            <div class="notebook-actions">
              <input
                ref="notebookFileInput"
                class="notebook-file-input"
                type="file"
                accept="image/*"
                @change="handleNotebookFileChange"
              >
              <el-button size="small" @click="triggerNotebookImageUpload">添加图片</el-button>
              <el-button size="small" plain @click="clearTemporaryNotebook">清空</el-button>
            </div>
          </div>
          <div
            ref="notebookEditor"
            class="notebook-editor"
            contenteditable="true"
            aria-label="临时记录本"
            @input="handleNotebookInput"
            @paste="handleNotebookPaste"
          ></div>
        </div>
      </el-col>
    </el-row>

    <el-row :gutter="16">
      <!-- 播报设置 -->
      <el-col :xs="24" :md="12">
        <div class="card">
          <h3>播报设置</h3>
          <el-form label-width="120px">
            <el-form-item label="涨停播报">
              <el-switch v-model="config.alert_limit_up_enabled" @change="saveConfig" />
            </el-form-item>
            <el-form-item label="大单播报">
              <el-switch v-model="config.alert_big_order_enabled" @change="saveConfig" />
            </el-form-item>
            <el-form-item label="声音提醒">
              <el-switch v-model="config.alert_sound_enabled" @change="saveConfig" />
            </el-form-item>
            <el-form-item label="桌面通知">
              <el-switch v-model="config.alert_desktop_enabled" @change="saveConfig" />
              <el-button 
                v-if="notificationPermission !== 'granted'" 
                size="small" 
                @click="requestNotification"
                style="margin-left: 10px"
              >授权</el-button>
            </el-form-item>
          </el-form>
        </div>
      </el-col>

      <!-- 通达信插件入口 -->
      <el-col :xs="24" :md="12">
        <div class="card plugin-entry-card">
          <div class="plugin-entry-content">
            <div>
              <h3>通达信看盘插件</h3>
              <p>复制通达信地址后，在通达信自定义面板或网页插件中粘贴使用。</p>
            </div>
            <el-button type="primary" @click="tdxPluginDialogVisible = true">
              通达信地址
            </el-button>
          </div>
        </div>
      </el-col>

      <!-- 大单设置 -->
      <el-col :xs="24" :md="12">
        <div class="card">
          <h3>大单阈值设置</h3>
          <el-form label-width="140px">
            <el-form-item label="主板阈值(10%)">
              <el-input-number 
                v-model="config.big_order_volume" 
                :min="100" 
                :max="100000" 
                :step="100"
                @change="saveConfig"
              />
              <span style="margin-left: 10px; color: #909399;">手</span>
            </el-form-item>
            <el-form-item label="20cm阈值(科创/创业)">
              <el-input-number 
                v-model="config.big_order_volume_20cm" 
                :min="50" 
                :max="100000" 
                :step="50"
                @change="saveConfig"
              />
              <span style="margin-left: 10px; color: #909399;">手</span>
            </el-form-item>
          </el-form>
        </div>
      </el-col>
    </el-row>

    <el-row :gutter="16">
      <!-- 过滤设置 -->
      <el-col :xs="24" :md="12">
        <div class="card">
          <h3>过滤设置</h3>
          <el-form label-width="120px">
            <el-form-item label="过滤ST股票">
              <el-switch v-model="config.filter_st" @change="saveConfig" />
            </el-form-item>
            <el-form-item label="过滤次新股">
              <el-switch v-model="config.filter_new_stock" @change="saveConfig" />
            </el-form-item>
            <el-form-item label="最低价格">
              <el-input-number 
                v-model="config.filter_low_price" 
                :min="0" 
                :precision="2"
                @change="saveConfig"
              />
            </el-form-item>
            <el-form-item label="最高价格">
              <el-input-number 
                v-model="config.filter_high_price" 
                :min="0" 
                :precision="2"
                placeholder="0为不限"
                @change="saveConfig"
              />
            </el-form-item>
          </el-form>
        </div>
      </el-col>

      <!-- 自选股管理 -->
      <el-col :xs="24" :md="12">
        <div class="card">
          <h3>自选股管理</h3>
          <div class="watchlist">
            <el-tag 
              v-for="code in config.watch_list" 
              :key="code"
              closable
              @close="removeWatch(code)"
              style="margin: 4px"
            >{{ code }}</el-tag>
            <el-input 
              v-model="newWatchCode" 
              placeholder="输入股票代码" 
              style="width: 120px; margin: 4px"
              @keyup.enter="addWatch"
            />
            <el-button size="small" @click="addWatch">添加</el-button>
          </div>
        </div>
      </el-col>
    </el-row>

    <el-dialog
      v-model="tdxPluginDialogVisible"
      title="通达信插件地址"
      width="760px"
      class="tdx-plugin-modal"
      destroy-on-close
    >
      <p class="plugin-window-tip">带 xxxxxx 的地址用于通达信当前股票联动，通达信会替换为选中的股票代码。</p>
      <div class="plugin-window">
        <article
          v-for="plugin in tdxPlugins"
          :key="plugin.path"
          class="plugin-window-card"
        >
          <div>
            <strong>{{ plugin.name }}</strong>
            <p>{{ plugin.desc }}</p>
            <div class="plugin-url">{{ buildTdxPluginUrl(plugin.path) }}</div>
          </div>
          <el-button size="small" type="primary" plain @click="copyTdxPluginUrl(plugin.path)">
            复制插件地址
          </el-button>
        </article>
      </div>
      <div v-if="selectedPluginUrl" class="plugin-manual-copy">
        <span>当前地址</span>
        <el-input
          :model-value="selectedPluginUrl"
          readonly
          @focus="selectManualPluginUrl"
        />
      </div>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, onMounted, onBeforeUnmount } from 'vue'
import { ElMessage } from 'element-plus'
import { getConfig, updateConfig, type UserConfigUpdate } from '@/api/config'
import { useConfigStore } from '@/stores/config'
import { useAlertStore } from '@/stores/alert'

const configStore = useConfigStore()
const alertStore = useAlertStore()
const TEMPORARY_NOTEBOOK_KEY = 'temporary_notebook'
const MAX_NOTEBOOK_IMAGE_SIZE = 2 * 1024 * 1024
const NOTEBOOK_SAVE_DELAY = 600

type CustomSettings = Record<string, unknown>

const config = reactive({
  big_order_volume: 300,
  big_order_volume_20cm: 200,
  alert_limit_up_enabled: true,
  alert_big_order_enabled: true,
  alert_sound_enabled: true,
  alert_desktop_enabled: true,
  filter_st: true,
  filter_new_stock: false,
  filter_low_price: 0,
  filter_high_price: 0,
  watch_list: [] as string[],
  custom_settings: {} as CustomSettings,
  deepseek_api_key_configured: false,
  deepseek_base_url: 'https://api.deepseek.com',
  deepseek_model: 'deepseek-v4-pro'
})

const newWatchCode = ref('')
const deepseekApiKey = ref('')
const savingDeepSeek = ref(false)
const notificationPermission = ref(Notification?.permission || 'default')
const tdxPluginDialogVisible = ref(false)
const selectedPluginUrl = ref('')
const notebookEditor = ref<HTMLDivElement | null>(null)
const notebookFileInput = ref<HTMLInputElement | null>(null)
let notebookSaveTimer: ReturnType<typeof setTimeout> | null = null

const tdxPlugins = [
  { name: '涨停播报', desc: '纯涨停、炸板、回封和封单变化播报', path: '/tdx/ztlive/dark' },
  { name: '复合看盘', desc: '涨停播报叠加个股异动解析联动', path: '/tdx/composite/dark#xxxxxx' },
  { name: '股票异动解析联动', desc: '综合口径展示个股最近涨停与异动原因', path: '/tdx/yidong/xxxxxx/dark' },
  { name: '实时板块强度', desc: '板块轮动、强度、量能和核心股入口', path: '/tdx/strong/dark' },
  { name: '聚合快讯', desc: '市场快讯、韭研社识别区和题材库', path: '/tdx/news/dark' },
  { name: '异动解析（同花顺版）', desc: '同花顺口径的概念和异动解析', path: '/tdx/thsyd/xxxxxx/dark' }
]

// 加载配置
async function loadConfig() {
  try {
    const data = await getConfig()
    const customSettings = normalizeCustomSettings(data.custom_settings)
    Object.assign(config, data, { custom_settings: customSettings })
    configStore.setConfig({ ...data, custom_settings: customSettings })
    alertStore.setEnabled(data.alert_limit_up_enabled)
    alertStore.setSoundEnabled(data.alert_sound_enabled)
    alertStore.setDesktopEnabled(data.alert_desktop_enabled)
    renderTemporaryNotebook()
  } catch (e) {
    console.error('Load config error:', e)
  }
}

// 保存配置
async function saveConfig() {
  try {
    const payload = buildUserConfigPayload()
    await updateConfig(payload)
    configStore.setConfig(payload)
    alertStore.setEnabled(Boolean(payload.alert_limit_up_enabled))
    alertStore.setSoundEnabled(payload.alert_sound_enabled as boolean)
    alertStore.setDesktopEnabled(Boolean(payload.alert_desktop_enabled))
    ElMessage.success('保存成功')
  } catch (e) {
    console.error('Save config error:', e)
    ElMessage.error('保存失败')
  }
}

async function saveDeepSeekConfig() {
  savingDeepSeek.value = true
  try {
    const payload: UserConfigUpdate = {
      deepseek_base_url: config.deepseek_base_url,
      deepseek_model: config.deepseek_model
    }
    if (deepseekApiKey.value.trim()) {
      payload.deepseek_api_key = deepseekApiKey.value.trim()
    }
    const data = await updateConfig(payload)
    Object.assign(config, data)
    configStore.setConfig(data)
    deepseekApiKey.value = ''
    ElMessage.success('AI配置已保存')
  } catch (e) {
    console.error('Save DeepSeek config error:', e)
    ElMessage.error('AI配置保存失败')
  } finally {
    savingDeepSeek.value = false
  }
}

function buildUserConfigPayload(): UserConfigUpdate {
  const payload = { ...config } as UserConfigUpdate
  delete payload.deepseek_api_key_configured
  delete payload.deepseek_base_url
  delete payload.deepseek_model
  return payload
}

function normalizeCustomSettings(value: unknown): CustomSettings {
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    return { ...(value as CustomSettings) }
  }
  return {}
}

function getTemporaryNotebookFromConfig() {
  const value = normalizeCustomSettings(config.custom_settings)[TEMPORARY_NOTEBOOK_KEY]
  return typeof value === 'string' ? value : ''
}

function renderTemporaryNotebook() {
  if (!notebookEditor.value) return
  notebookEditor.value.innerHTML = sanitizeNotebookHtml(getTemporaryNotebookFromConfig())
}

function handleNotebookInput() {
  scheduleTemporaryNotebookSave()
}

function triggerNotebookImageUpload() {
  notebookFileInput.value?.click()
}

async function handleNotebookFileChange(event: Event) {
  const input = event.target as HTMLInputElement
  const file = input.files?.[0]
  if (file) {
    await addNotebookImageFile(file)
  }
  input.value = ''
}

async function handleNotebookPaste(event: ClipboardEvent) {
  const items = Array.from(event.clipboardData?.items ?? [])
  const imageItems = items.filter(item => item.kind === 'file' && item.type.startsWith('image/'))
  if (!imageItems.length) return

  event.preventDefault()
  for (const item of imageItems) {
    const file = item.getAsFile()
    if (file) {
      await addNotebookImageFile(file)
    }
  }
}

async function addNotebookImageFile(file: File) {
  if (!validateNotebookImageFile(file)) return

  try {
    const imageUrl = await readNotebookImageFile(file)
    insertNotebookImage(imageUrl)
  } catch (e) {
    console.error('Read notebook image error:', e)
    ElMessage.error('图片添加失败')
  }
}

function validateNotebookImageFile(file: File) {
  if (!file.type.startsWith('image/')) {
    ElMessage.warning('只能添加图片文件')
    return false
  }
  if (file.size > MAX_NOTEBOOK_IMAGE_SIZE) {
    ElMessage.warning('单张图片不能超过 2MB')
    return false
  }
  return true
}

function readNotebookImageFile(file: File) {
  return new Promise<string>((resolve, reject) => {
    const reader = new FileReader()
    reader.onload = () => {
      if (typeof reader.result === 'string') {
        resolve(reader.result)
      } else {
        reject(new Error('Invalid image data'))
      }
    }
    reader.onerror = () => reject(reader.error ?? new Error('Read image failed'))
    reader.readAsDataURL(file)
  })
}

function insertNotebookImage(src: string) {
  const editor = notebookEditor.value
  if (!editor) return

  const image = document.createElement('img')
  image.src = src
  image.alt = '记录图片'
  image.className = 'notebook-image'

  const selection = window.getSelection()
  if (selection?.rangeCount && selection.anchorNode && editor.contains(selection.anchorNode)) {
    const range = selection.getRangeAt(0)
    range.deleteContents()
    range.insertNode(image)
    range.setStartAfter(image)
    range.collapse(true)
    selection.removeAllRanges()
    selection.addRange(range)
  } else {
    editor.appendChild(image)
  }

  image.insertAdjacentText('afterend', ' ')
  editor.focus()
  scheduleTemporaryNotebookSave()
}

function scheduleTemporaryNotebookSave() {
  clearNotebookSaveTimer()
  notebookSaveTimer = setTimeout(() => {
    saveTemporaryNotebook()
  }, NOTEBOOK_SAVE_DELAY)
}

function clearNotebookSaveTimer() {
  if (notebookSaveTimer) {
    clearTimeout(notebookSaveTimer)
    notebookSaveTimer = null
  }
}

async function saveTemporaryNotebook() {
  clearNotebookSaveTimer()
  const editor = notebookEditor.value
  const notebookHtml = sanitizeNotebookHtml(editor?.innerHTML ?? '')
  if (editor && editor.innerHTML !== notebookHtml) {
    editor.innerHTML = notebookHtml
  }

  const customSettings = {
    ...normalizeCustomSettings(config.custom_settings),
    [TEMPORARY_NOTEBOOK_KEY]: notebookHtml
  }
  config.custom_settings = customSettings

  try {
    const data = await updateConfig({ custom_settings: customSettings })
    const updatedCustomSettings = normalizeCustomSettings(data.custom_settings)
    Object.assign(config, data, { custom_settings: updatedCustomSettings })
    configStore.setConfig({ ...data, custom_settings: updatedCustomSettings })
  } catch (e) {
    console.error('Save temporary notebook error:', e)
    ElMessage.error('记录本保存失败')
  }
}

function clearTemporaryNotebook() {
  if (notebookEditor.value) {
    notebookEditor.value.innerHTML = ''
  }
  saveTemporaryNotebook()
}

function sanitizeNotebookHtml(html: string) {
  const source = document.createElement('div')
  const target = document.createElement('div')
  source.innerHTML = html
  appendSanitizedNotebookNodes(source, target)
  return target.innerHTML
}

function appendSanitizedNotebookNodes(source: Node, target: Node) {
  Array.from(source.childNodes).forEach(node => {
    if (node.nodeType === Node.TEXT_NODE) {
      target.appendChild(document.createTextNode(node.textContent ?? ''))
      return
    }

    if (node instanceof HTMLImageElement) {
      if (node.src.startsWith('data:image/')) {
        const image = document.createElement('img')
        image.src = node.src
        image.alt = node.alt || '记录图片'
        image.className = 'notebook-image'
        target.appendChild(image)
      }
      return
    }

    if (node instanceof HTMLBRElement) {
      target.appendChild(document.createElement('br'))
      return
    }

    if (!(node instanceof HTMLElement)) return

    if (node.tagName === 'DIV' || node.tagName === 'P') {
      const block = document.createElement(node.tagName.toLowerCase())
      appendSanitizedNotebookNodes(node, block)
      target.appendChild(block)
      return
    }

    appendSanitizedNotebookNodes(node, target)
  })
}

// 请求通知权限
async function requestNotification() {
  if ('Notification' in window) {
    const permission = await Notification.requestPermission()
    notificationPermission.value = permission
  }
}

// 添加自选
function addWatch() {
  if (!newWatchCode.value) return
  const code = newWatchCode.value.trim()
  if (code && !config.watch_list.includes(code)) {
    config.watch_list.push(code)
    saveConfig()
  }
  newWatchCode.value = ''
}

// 删除自选
function removeWatch(code: string) {
  const index = config.watch_list.indexOf(code)
  if (index !== -1) {
    config.watch_list.splice(index, 1)
    saveConfig()
  }
}

function buildTdxPluginUrl(path: string) {
  return `${window.location.origin}${path}`
}

function fallbackCopyText(text: string) {
  const textarea = document.createElement('textarea')
  textarea.value = text
  textarea.setAttribute('readonly', 'readonly')
  textarea.style.position = 'fixed'
  textarea.style.left = '-9999px'
  textarea.style.top = '0'
  document.body.appendChild(textarea)
  textarea.focus()
  textarea.select()
  textarea.setSelectionRange(0, text.length)
  try {
    return document.execCommand('copy')
  } finally {
    document.body.removeChild(textarea)
  }
}

function selectManualPluginUrl(event: FocusEvent) {
  const target = event.target
  if (target instanceof HTMLInputElement) {
    target.select()
  }
}

async function copyTdxPluginUrl(path: string) {
  const url = buildTdxPluginUrl(path)
  selectedPluginUrl.value = url
  try {
    if (navigator.clipboard?.writeText && window.isSecureContext) {
      await navigator.clipboard.writeText(url)
      ElMessage.success('已复制插件地址')
      return
    }
  } catch (e) {
    console.error('Copy TDX plugin url error:', e)
  }

  if (fallbackCopyText(url)) {
    ElMessage.success('已复制插件地址')
    return
  }

  ElMessage.warning('复制失败，请手动复制下方地址')
}

onMounted(() => {
  loadConfig()
})

onBeforeUnmount(() => {
  clearNotebookSaveTimer()
})
</script>

<style lang="scss" scoped>
.settings {
  .card {
    background: #fff;
    border-radius: 8px;
    padding: 20px;
    margin-bottom: 16px;

    h3 {
      margin: 0 0 20px 0;
      font-size: 16px;
      border-bottom: 1px solid #f0f0f0;
      padding-bottom: 12px;
    }
  }

  .card-title-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    border-bottom: 1px solid #f0f0f0;
    margin-bottom: 20px;
    padding-bottom: 12px;

    h3 {
      margin: 0;
      border-bottom: 0;
      padding-bottom: 0;
    }
  }

  .secret-input-row {
    display: flex;
    align-items: center;
    gap: 8px;
    width: 100%;
  }

  .form-hint {
    margin-top: 6px;
    color: #909399;
    font-size: 12px;
    line-height: 1.5;
  }

  .notebook-title-row {
    align-items: flex-start;
  }

  .notebook-actions {
    display: flex;
    flex-wrap: wrap;
    justify-content: flex-end;
    gap: 8px;
  }

  .notebook-file-input {
    display: none;
  }

  .notebook-editor {
    min-height: 220px;
    max-height: 520px;
    overflow-y: auto;
    padding: 12px;
    border: 1px solid #dcdfe6;
    border-radius: 6px;
    background: #fff;
    color: #303133;
    font-size: 14px;
    line-height: 1.6;
    outline: none;
    white-space: pre-wrap;
    word-break: break-word;

    &:focus {
      border-color: #409eff;
      box-shadow: 0 0 0 2px rgba(64, 158, 255, 0.12);
    }

    :deep(img) {
      display: block;
      max-width: 100%;
      max-height: 360px;
      margin: 8px 0;
      border-radius: 4px;
      object-fit: contain;
    }
  }

  .watchlist {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
  }

  .plugin-entry-card {
    min-height: 156px;
  }

  .plugin-entry-content {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;

    h3 {
      margin-bottom: 10px;
    }

    p {
      margin: 0;
      color: #606266;
      line-height: 1.6;
    }
  }
}

:deep(.tdx-plugin-modal .el-dialog__body) {
  padding-top: 10px;
}

.plugin-window-tip {
  margin: 0 0 10px;
  color: #606266;
  font-size: 13px;
}

.plugin-window {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
  padding: 12px;
  border-radius: 6px;
  background: #111219;
}

.plugin-window-card {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 10px;
  min-height: 74px;
  padding: 12px;
  border: 1px solid #2d3748;
  background: #161922;

  strong {
    color: #f0be83;
    font-size: 14px;
    font-weight: 600;
  }

  p {
    margin: 6px 0 0;
    color: #b0b0b0;
    font-size: 12px;
    line-height: 1.5;
  }

  .plugin-url {
    margin-top: 8px;
    padding: 6px 8px;
    border: 1px solid #263142;
    background: #0b0d12;
    color: #7dd3fc;
    font-size: 12px;
    line-height: 1.4;
    word-break: break-all;
  }
}

.plugin-manual-copy {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr);
  align-items: center;
  gap: 10px;
  margin-top: 12px;
  color: #606266;
  font-size: 13px;

  :deep(.el-input__inner) {
    font-family: Consolas, 'Courier New', monospace;
  }
}

@media (max-width: 767px) {
  .settings {
    :deep(.el-row) {
      margin-left: 0 !important;
      margin-right: 0 !important;
    }

    :deep(.el-col) {
      padding-left: 0 !important;
      padding-right: 0 !important;
    }

    .card {
      padding: 14px;
      margin-bottom: 10px;
    }

    .plugin-entry-content {
      align-items: flex-start;
      flex-direction: column;
    }

    .notebook-title-row {
      align-items: stretch;
      flex-direction: column;
    }

    .notebook-actions {
      justify-content: flex-start;
    }

    .notebook-editor {
      min-height: 180px;
      max-height: 420px;
    }

    :deep(.el-form-item) {
      display: block;
      margin-bottom: 14px;
    }

    :deep(.el-form-item__label) {
      justify-content: flex-start;
      width: auto !important;
      height: auto;
      margin-bottom: 6px;
      line-height: 1.3;
    }

    :deep(.el-input-number),
    :deep(.el-input),
    :deep(.el-button) {
      max-width: 100%;
    }
  }

  :deep(.tdx-plugin-modal) {
    width: calc(100vw - 24px) !important;
  }

  .plugin-window {
    grid-template-columns: 1fr;
  }

  .plugin-window-card {
    grid-template-columns: 1fr;
  }

  .plugin-manual-copy {
    grid-template-columns: 1fr;
  }
}
</style>
