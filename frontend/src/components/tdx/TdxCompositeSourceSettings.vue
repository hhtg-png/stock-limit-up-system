<template>
  <span ref="settingsRoot" class="composite-source-settings">
    <button
      ref="trigger"
      class="source-settings-trigger"
      type="button"
      title="设置复合看盘播报来源"
      aria-label="设置复合看盘播报来源"
      aria-haspopup="dialog"
      :aria-expanded="isOpen"
      :aria-controls="panelId"
      @click="togglePanel"
    >
      <svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true">
        <path d="m9.5 3-.6 2.1-1.5.9-2.2-.5-2.5 4.3 1.5 1.6v1.7l-1.5 1.6 2.5 4.3 2.2-.5 1.5.9.6 2.1h5l.6-2.1 1.5-.9 2.2.5 2.5-4.3-1.5-1.6v-1.7l1.5-1.6-2.5-4.3-2.2.5-1.5-.9-.6-2.1z" />
        <circle cx="12" cy="12" r="3.2" />
      </svg>
    </button>
    <div
      v-if="isOpen"
      :id="panelId"
      ref="panel"
      class="source-settings-panel"
      :style="panelStyle"
      role="dialog"
      :aria-labelledby="`${panelId}-title`"
      @focusout="handleFocusOut"
    >
      <div class="source-settings-heading">
        <strong :id="`${panelId}-title`">播报来源</strong>
        <small>自动保存</small>
      </div>
      <label v-for="source in COMPOSITE_SPEECH_SOURCES" :key="source.key" class="source-settings-option">
        <span>{{ source.label }}</span>
        <input type="checkbox" :checked="modelValue[source.key]" @change="updateSource(source.key, $event)" />
      </label>
    </div>
  </span>
</template>

<script setup lang="ts">
import { getCurrentInstance, nextTick, onBeforeUnmount, onMounted, ref } from 'vue'
import { COMPOSITE_SPEECH_SOURCES } from '@/utils/tdxCompositeSources'
import type { CompositeSpeechSource, CompositeSpeechSources } from '@/utils/tdxCompositeSources'

const props = defineProps<{ modelValue: CompositeSpeechSources }>()
const emit = defineEmits<{ 'update:modelValue': [value: CompositeSpeechSources] }>()

const settingsRoot = ref<HTMLElement | null>(null)
const trigger = ref<HTMLButtonElement | null>(null)
const panel = ref<HTMLElement | null>(null)
const isOpen = ref(false)
const panelId = `tdx-composite-sources-${getCurrentInstance()?.uid}`
const panelStyle = ref({ left: '8px', top: '8px' })

function positionPanel() {
  if (!isOpen.value || !trigger.value || !panel.value) return
  const anchor = trigger.value.getBoundingClientRect()
  const bounds = panel.value.getBoundingClientRect()
  const width = Math.min(bounds.width, window.innerWidth - 16)
  const left = Math.max(8, Math.min(anchor.left, window.innerWidth - width - 8))
  const top = Math.max(8, Math.min(anchor.bottom + 4, window.innerHeight - bounds.height - 8))
  panelStyle.value = { left: `${left}px`, top: `${top}px` }
}

async function togglePanel() {
  if (isOpen.value) {
    closePanel()
    return
  }
  isOpen.value = true
  await nextTick()
  if (!isOpen.value) return
  positionPanel()
  panel.value?.querySelector<HTMLInputElement>('input')?.focus()
}

function closePanel(restoreFocus = false) {
  isOpen.value = false
  if (restoreFocus) trigger.value?.focus()
}

function updateSource(source: CompositeSpeechSource, event: Event) {
  const input = event.target as HTMLInputElement
  emit('update:modelValue', { ...props.modelValue, [source]: input.checked })
}

function handleOutsideClick(event: MouseEvent) {
  if (isOpen.value && !settingsRoot.value?.contains(event.target as Node)) closePanel()
}

function handleKeydown(event: KeyboardEvent) {
  if (!isOpen.value || event.key !== 'Escape') return
  event.preventDefault()
  closePanel(true)
}

function handleFocusOut(event: FocusEvent) {
  if (event.relatedTarget && !settingsRoot.value?.contains(event.relatedTarget as Node)) closePanel()
}

onMounted(() => {
  document.addEventListener('click', handleOutsideClick)
  document.addEventListener('keydown', handleKeydown)
  window.addEventListener('resize', positionPanel)
  window.addEventListener('scroll', positionPanel, true)
})

onBeforeUnmount(() => {
  document.removeEventListener('click', handleOutsideClick)
  document.removeEventListener('keydown', handleKeydown)
  window.removeEventListener('resize', positionPanel)
  window.removeEventListener('scroll', positionPanel, true)
})
</script>

<style scoped>
.composite-source-settings {
  display: inline-flex;
  flex: 0 0 24px;
  width: 24px;
  height: 24px;
  vertical-align: middle;
  text-align: left;
  font-weight: 400;
}

.source-settings-trigger {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 24px;
  height: 24px;
  padding: 0;
  border: 1px solid #4a5568;
  border-radius: 3px;
  background: #202432;
  color: #b6c4d7;
  cursor: pointer;
}

.source-settings-trigger:hover,
.source-settings-trigger[aria-expanded="true"] {
  border-color: #96cdfa;
  color: #96cdfa;
}

.source-settings-trigger:focus,
.source-settings-option input:focus {
  outline: 2px solid #96cdfa;
  outline-offset: 2px;
}

.source-settings-panel {
  position: fixed;
  z-index: 100;
  box-sizing: border-box;
  width: 176px;
  max-width: calc(100vw - 16px);
  max-height: calc(100vh - 16px);
  padding: 9px 10px;
  overflow: auto;
  border: 1px solid #4a5568;
  border-radius: 4px;
  background: #202432;
  color: #dbe4f0;
  box-shadow: 0 5px 18px #0008;
  font-size: 12px;
  line-height: 1.4;
  white-space: normal;
}

.source-settings-heading {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 5px;
  padding-bottom: 6px;
  border-bottom: 1px solid #3b4353;
}

.source-settings-heading strong {
  font-weight: 500;
}

.source-settings-heading small {
  color: #92a1b5;
  font-size: 10px;
}

.source-settings-option {
  display: flex;
  align-items: center;
  justify-content: space-between;
  min-height: 27px;
  cursor: pointer;
}

.source-settings-option input {
  width: 14px;
  height: 14px;
  margin: 0 1px 0 8px;
  cursor: pointer;
}
</style>
