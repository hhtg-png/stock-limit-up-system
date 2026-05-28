<template>
  <section class="tdx-plugin-shell">
    <header class="tdx-plugin-header">
      <div>
        <h1>{{ title }}</h1>
        <p v-if="subtitle">{{ subtitle }}</p>
      </div>
      <div class="tdx-plugin-actions">
        <span v-if="updatedAt" class="tdx-updated">更新 {{ formatTime(updatedAt) }}</span>
        <span v-if="isCache" class="tdx-cache">缓存</span>
        <button type="button" @click="$emit('unlock-speech')">播报</button>
        <button type="button" @click="$emit('refresh')">刷新</button>
      </div>
    </header>

    <div v-if="warnings.length" class="tdx-warnings">
      <span v-for="warning in warnings" :key="warning">{{ warning }}</span>
    </div>

    <div v-if="sourceStatusEntries.length" class="tdx-source-status">
      <span
        v-for="[name, status] in sourceStatusEntries"
        :key="name"
        :class="`status-${status}`"
      >
        {{ name }} {{ status }}
      </span>
    </div>

    <main class="tdx-plugin-body" :class="{ loading }">
      <slot />
    </main>
  </section>
</template>

<script setup lang="ts">
import { computed } from 'vue'

const props = withDefaults(defineProps<{
  title: string
  subtitle?: string
  updatedAt?: string
  sourceStatus?: Record<string, string>
  isCache?: boolean
  warnings?: string[]
  loading?: boolean
}>(), {
  subtitle: '',
  updatedAt: '',
  sourceStatus: () => ({}),
  isCache: false,
  warnings: () => [],
  loading: false
})

defineEmits<{
  refresh: []
  'unlock-speech': []
}>()

const sourceStatusEntries = computed(() => Object.entries(props.sourceStatus))

function formatTime(value: string) {
  if (!value) return ''
  return value.replace('T', ' ').slice(5, 19)
}
</script>

<style scoped>
.tdx-plugin-shell {
  min-height: 100vh;
  background: #050b12;
  color: #d7e3f4;
  font-size: 12px;
  line-height: 1.42;
  overflow: auto;
}

.tdx-plugin-header {
  position: sticky;
  top: 0;
  z-index: 5;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 8px 10px;
  border-bottom: 1px solid #16324f;
  background: #07111d;
}

.tdx-plugin-header h1 {
  margin: 0;
  color: #f3f8ff;
  font-size: 15px;
  font-weight: 700;
}

.tdx-plugin-header p {
  margin: 2px 0 0;
  color: #7f93ad;
}

.tdx-plugin-actions {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 6px;
  white-space: nowrap;
}

.tdx-plugin-actions button {
  height: 24px;
  padding: 0 9px;
  border: 1px solid #245b8f;
  border-radius: 2px;
  background: #102238;
  color: #b9dcff;
  cursor: pointer;
}

.tdx-updated,
.tdx-cache {
  color: #7f93ad;
}

.tdx-cache {
  color: #ffd36d;
}

.tdx-warnings,
.tdx-source-status {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  padding: 6px 10px 0;
}

.tdx-warnings span,
.tdx-source-status span {
  padding: 2px 6px;
  border: 1px solid #27415d;
  border-radius: 2px;
  color: #9eb3cd;
  background: #0b1725;
}

.tdx-warnings span {
  color: #ffd36d;
  border-color: #654b13;
}

.tdx-source-status .status-ok {
  color: #52e0a3;
}

.tdx-source-status .status-error {
  color: #ff6b6b;
}

.tdx-source-status .status-empty {
  color: #ffd36d;
}

.tdx-plugin-body {
  padding: 8px 10px 12px;
}

.tdx-plugin-body.loading {
  opacity: 0.68;
}

:deep(.tdx-table) {
  width: 100%;
  border-collapse: collapse;
  table-layout: fixed;
}

:deep(.tdx-table th),
:deep(.tdx-table td) {
  padding: 5px 6px;
  border: 1px solid #132b44;
  overflow: hidden;
  color: #d7e3f4;
  text-overflow: ellipsis;
  white-space: nowrap;
}

:deep(.tdx-table th) {
  color: #89a7c9;
  background: #091827;
  font-weight: 500;
}

:deep(.tdx-stock-link) {
  border: 0;
  background: transparent;
  color: #55a7ff;
  cursor: pointer;
  padding: 0;
}

:deep(.tdx-tag) {
  display: inline-flex;
  align-items: center;
  height: 18px;
  margin-right: 4px;
  padding: 0 5px;
  border: 1px solid #244b75;
  border-radius: 2px;
  color: #9fd0ff;
  background: #0d2034;
}

:deep(.tdx-red) {
  color: #ff4d5e;
}

:deep(.tdx-green) {
  color: #42d392;
}

:deep(.tdx-yellow) {
  color: #ffd36d;
}

@media (max-width: 640px) {
  .tdx-plugin-shell {
    font-size: 11px;
  }

  .tdx-plugin-header {
    align-items: flex-start;
    flex-direction: column;
  }

  .tdx-plugin-actions {
    width: 100%;
    justify-content: flex-start;
    overflow-x: auto;
  }

  .tdx-plugin-body {
    padding: 6px;
  }
}
</style>
