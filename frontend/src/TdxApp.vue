<template>
  <main class="tdx-lite-app">
    <router-view />
  </main>
</template>

<script setup lang="ts">
import { onMounted, onUnmounted } from 'vue'
import { useWebSocket } from '@/composables/useWebSocket'

const { connect, disconnect } = useWebSocket()

onMounted(() => {
  connect()
})

onUnmounted(() => {
  disconnect()
})
</script>

<style scoped>
.tdx-lite-app {
  min-height: 100vh;
  width: 100vw;
  overflow: auto;
  background: #050b12;
}
</style>
