<template>
  <el-config-provider :locale="zhCn">
    <div class="app-container">
      <el-container class="layout-container">
        <!-- 侧边栏 -->
        <el-aside :width="isCollapsed ? '64px' : '200px'" class="aside">
          <div class="logo">
            <el-icon :size="24"><DataBoard /></el-icon>
            <span v-show="!isCollapsed">数据中心</span>
          </div>
          <el-menu
            :default-active="currentRoute"
            :collapse="isCollapsed"
            router
            class="sidebar-menu"
          >
            <el-menu-item index="/">
              <el-icon><DataBoard /></el-icon>
              <span>涨停监控</span>
            </el-menu-item>
            <el-menu-item index="/statistics">
              <el-icon><PieChart /></el-icon>
              <span>报表分析</span>
            </el-menu-item>
            <el-menu-item index="/continuous">
              <el-icon><TrendCharts /></el-icon>
              <span>连板梯队</span>
            </el-menu-item>

            <el-menu-item index="/settings">
              <el-icon><Setting /></el-icon>
              <span>系统设置</span>
            </el-menu-item>
          </el-menu>
          <div class="collapse-btn" @click="isCollapsed = !isCollapsed">
            <el-icon :size="16">
              <Fold v-if="!isCollapsed" />
              <Expand v-else />
            </el-icon>
          </div>
        </el-aside>

        <!-- 主内容区 -->
        <el-container>
          <!-- 顶部栏 -->
          <el-header class="header">
            <div class="header-left">
              <span class="market-status" :class="marketStatus">
                {{ marketStatusText }}
              </span>
              <span class="current-time">{{ currentTime }}</span>
            </div>
            <div class="header-right">
              <el-badge :value="alertCount" :hidden="alertCount === 0" class="alert-badge">
                <el-button :icon="Bell" circle @click="showAlertPanel = true" />
              </el-badge>
              <el-switch
                v-model="alertEnabled"
                active-text="播报"
                inactive-text="静音"
                @change="toggleAlert"
              />
            </div>
          </el-header>

          <!-- 内容 -->
          <el-main class="main">
            <router-view />
          </el-main>
        </el-container>
      </el-container>

      <!-- 播报面板抽屉 -->
      <el-drawer
        v-model="showAlertPanel"
        title="实时播报"
        direction="rtl"
        size="400px"
      >
        <AlertPanel />
      </el-drawer>
    </div>
  </el-config-provider>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { useRoute } from 'vue-router'
import {
  DataBoard, PieChart, Setting,
  Fold, Expand, Bell, TrendCharts
} from '@element-plus/icons-vue'
import zhCn from 'element-plus/dist/locale/zh-cn.mjs'
import AlertPanel from '@/components/alert/AlertPanel.vue'
import { useWebSocket } from '@/composables/useWebSocket'
import { useSpeech } from '@/composables/useSpeech'
import { useAlertStore } from '@/stores/alert'
import dayjs from 'dayjs'

const route = useRoute()
const alertStore = useAlertStore()
const { connect } = useWebSocket()

const isCollapsed = ref(false)
const showAlertPanel = ref(false)
const alertEnabled = ref(true)
const currentTime = ref(dayjs().format('HH:mm:ss'))

// 当前路由
const currentRoute = computed(() => route.path)

// 播报数量
const alertCount = computed(() => alertStore.unreadCount)

// 市场状态
const marketStatus = computed(() => {
  const hour = dayjs().hour()
  const minute = dayjs().minute()
  const time = hour * 60 + minute

  if (time < 9 * 60 + 15) return 'pre-market'
  if (time < 9 * 60 + 30) return 'auction'
  if (time < 11 * 60 + 30) return 'trading'
  if (time < 13 * 60) return 'lunch'
  if (time < 15 * 60) return 'trading'
  return 'closed'
})

const marketStatusText = computed(() => {
  const statusMap: Record<string, string> = {
    'pre-market': '盘前',
    'auction': '集合竞价',
    'trading': '交易中',
    'lunch': '午间休市',
    'closed': '已收盘'
  }
  return statusMap[marketStatus.value] || '未知'
})

// 切换播报
const toggleAlert = (enabled: boolean) => {
  alertStore.setEnabled(enabled)
  const { announceEnabled, announceDisabled } = useSpeech()
  if (enabled) {
    announceEnabled()
  } else {
    announceDisabled()
  }
}

// 更新时间
let timeInterval: number
onMounted(() => {
  connect()
  timeInterval = window.setInterval(() => {
    currentTime.value = dayjs().format('HH:mm:ss')
  }, 1000)
})

onUnmounted(() => {
  clearInterval(timeInterval)
})
</script>

<style lang="scss" scoped>
.app-container {
  height: 100vh;
  width: 100vw;
  overflow: hidden;
}

.layout-container {
  height: 100%;
}

.aside {
  background: #001529;
  transition: width 0.3s;
  display: flex;
  flex-direction: column;

  .logo {
    height: 64px;
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 8px;
    color: #fff;
    font-size: 16px;
    font-weight: bold;
    border-bottom: 1px solid #002140;
  }

  .sidebar-menu {
    flex: 1;
    border-right: none;
    background: transparent;

    :deep(.el-menu-item) {
      color: rgba(255, 255, 255, 0.65);

      &:hover {
        color: #fff;
        background: #002140;
      }

      &.is-active {
        color: #fff;
        background: #1890ff;
      }
    }
  }

  .collapse-btn {
    height: 48px;
    display: flex;
    align-items: center;
    justify-content: center;
    color: rgba(255, 255, 255, 0.65);
    cursor: pointer;
    border-top: 1px solid #002140;

    &:hover {
      color: #fff;
    }
  }
}

.header {
  background: #fff;
  border-bottom: 1px solid #f0f0f0;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 20px;

  .header-left {
    display: flex;
    align-items: center;
    gap: 16px;

    .market-status {
      padding: 4px 12px;
      border-radius: 4px;
      font-size: 13px;

      &.trading {
        background: #f6ffed;
        color: #52c41a;
      }

      &.auction {
        background: #fff7e6;
        color: #fa8c16;
      }

      &.closed, &.lunch, &.pre-market {
        background: #f5f5f5;
        color: #8c8c8c;
      }
    }

    .current-time {
      font-size: 14px;
      color: #666;
    }
  }

  .header-right {
    display: flex;
    align-items: center;
    gap: 16px;

    .alert-badge {
      margin-right: 8px;
    }
  }
}

.main {
  background: #f0f2f5;
  padding: 16px;
  overflow-y: auto;
}
</style>
