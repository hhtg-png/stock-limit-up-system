<template>
  <TdxPluginShell
    title="通达信看盘插件"
    subtitle="黑底嵌入版，首版提供涨停、异动、板块和快讯插件"
    @refresh="refresh"
    @unlock-speech="unlockSpeech"
  >
    <div class="tdx-plugin-grid">
      <router-link v-for="plugin in plugins" :key="plugin.path" :to="plugin.path" class="plugin-card">
        <strong>{{ plugin.name }}</strong>
        <span>{{ plugin.desc }}</span>
        <small>{{ plugin.path }}</small>
      </router-link>
    </div>
  </TdxPluginShell>
</template>

<script setup lang="ts">
import TdxPluginShell from '@/components/tdx/TdxPluginShell.vue'
import { useSpeech } from '@/composables/useSpeech'

const { unlockSpeech } = useSpeech()

const plugins = [
  { name: '涨停播报', desc: '实时封板、开板、回封事件', path: '/tdx/ztlive/dark#xxxxxx' },
  { name: '股票异动解析联动', desc: '综合口径解析个股异动原因', path: '/tdx/yidong/xxxxxx/dark' },
  { name: '实时板块强度', desc: '按涨停、封板率、核心股计算强度', path: '/tdx/strong/dark' },
  { name: '聚合快讯', desc: '市场快讯聚合与语音播报', path: '/tdx/news/dark' },
  { name: '异动解析（同花顺版）', desc: '同花顺口径个股解析', path: '/tdx/thsyd/xxxxxx/dark' }
]

function refresh() {
  window.location.reload()
}
</script>

<style scoped>
.tdx-plugin-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 8px;
}

.plugin-card {
  display: grid;
  gap: 5px;
  min-height: 86px;
  padding: 10px;
  border: 1px solid #173858;
  border-radius: 4px;
  background: #081827;
  color: #d7e3f4;
  text-decoration: none;
}

.plugin-card strong {
  color: #f4f8ff;
  font-size: 14px;
}

.plugin-card span {
  color: #9cb1ca;
}

.plugin-card small {
  color: #55a7ff;
}
</style>
