<template>
  <el-config-provider :locale="zhCn">
    <div v-if="isTdxRoute" class="tdx-standalone">
      <router-view />
    </div>
    <div v-else class="app-container">
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
            class="sidebar-menu"
            @select="handleMenuSelect"
          >
            <el-menu-item index="/">
              <el-icon><DataBoard /></el-icon>
              <span>涨停监控</span>
            </el-menu-item>
            <el-menu-item index="/statistics">
              <el-icon><PieChart /></el-icon>
              <span>报表分析</span>
            </el-menu-item>
            <el-menu-item index="/daily-analysis">
              <el-icon><Calendar /></el-icon>
              <span>每日分析</span>
            </el-menu-item>
            <el-menu-item index="/daily-info">
              <el-icon><Document /></el-icon>
              <span>每日资讯</span>
            </el-menu-item>
            <el-menu-item index="/limit-up-classification">
              <el-icon><Grid /></el-icon>
              <span>涨停分类</span>
            </el-menu-item>
            <el-menu-item index="/jiege-mode">
              <el-icon><TrendCharts /></el-icon>
              <span>交易模式</span>
            </el-menu-item>
            <el-menu-item index="/industry-trends">
              <el-icon><TrendCharts /></el-icon>
              <span>产业趋势</span>
            </el-menu-item>
            <el-menu-item index="/ultra-short-signals">
              <el-icon><Bell /></el-icon>
              <span>超短信号</span>
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
        <el-container class="content-container">
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
              <el-button
                class="mobile-speech-unlock"
                size="small"
                type="primary"
                text
                @click="enableMobileSpeech"
              >
                {{ speechUnlocked ? '语音已启用' : '启用语音' }}
              </el-button>
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

      <nav class="mobile-bottom-nav" aria-label="移动端主导航">
        <router-link
          v-for="item in mobileNavItems"
          :key="item.path"
          :to="item.path"
          class="mobile-nav-item"
          :class="{ active: isMobileNavActive(item.path) }"
        >
          <el-icon>
            <component :is="item.icon" />
          </el-icon>
          <span>{{ item.label }}</span>
        </router-link>
      </nav>
    </div>
  </el-config-provider>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import {
  DataBoard, PieChart, Setting,
  Fold, Expand, Bell, TrendCharts, Calendar, Document, Grid
} from '@element-plus/icons-vue'
import zhCn from 'element-plus/dist/locale/zh-cn.mjs'
import AlertPanel from '@/components/alert/AlertPanel.vue'
import { useWebSocket } from '@/composables/useWebSocket'
import { useSpeech } from '@/composables/useSpeech'
import { useAlertStore } from '@/stores/alert'
import { useConfigStore } from '@/stores/config'
import { getConfig, toggleAlert as toggleAlertConfig } from '@/api/config'
import dayjs from 'dayjs'

const route = useRoute()
const router = useRouter()
const alertStore = useAlertStore()
const configStore = useConfigStore()
const { connect } = useWebSocket()
const { unlockSpeech, speechUnlocked } = useSpeech()

const isCollapsed = ref(false)
const showAlertPanel = ref(false)
const alertEnabled = ref(true)
const currentTime = ref(dayjs().format('HH:mm:ss'))
const mobileNavItems = [
  { path: '/', label: '监控', icon: DataBoard },
  { path: '/statistics', label: '报表', icon: PieChart },
  { path: '/daily-analysis', label: '分析', icon: Calendar },
  { path: '/daily-info', label: '资讯', icon: Document },
  { path: '/limit-up-classification', label: '分类', icon: Grid },
  { path: '/jiege-mode', label: '交易', icon: TrendCharts },
  { path: '/industry-trends', label: '趋势', icon: TrendCharts },
  { path: '/ultra-short-signals', label: '超短', icon: Bell },
  { path: '/continuous', label: '连板', icon: TrendCharts },
  { path: '/settings', label: '设置', icon: Setting }
]

// 当前路由
const currentRoute = computed(() => route.path)
const isTdxRoute = computed(() => route.path === '/tdx' || route.path.startsWith('/tdx/'))

const isMobileNavActive = (path: string) => {
  if (path === '/') {
    return route.path === '/' || route.path === '/limit-up' || route.path.startsWith('/stock/')
  }
  return route.path.startsWith(path)
}

const handleMenuSelect = (path: string) => {
  if (path === route.path) return
  router.push(path)
}

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
const loadAlertConfig = async () => {
  try {
    const config = await getConfig()
    configStore.setConfig(config)
    alertEnabled.value = config.alert_limit_up_enabled
    alertStore.setEnabled(config.alert_limit_up_enabled)
    alertStore.setSoundEnabled(config.alert_sound_enabled)
    alertStore.setDesktopEnabled(config.alert_desktop_enabled)
  } catch (e) {
    console.error('Load alert config error:', e)
  }
}

const toggleAlert = async (enabled: boolean) => {
  alertStore.setEnabled(enabled)
  configStore.setConfig({ alert_limit_up_enabled: enabled })
  const { announceDisabled } = useSpeech()
  if (enabled) {
    unlockSpeech({ silent: true })
  } else {
    announceDisabled()
  }

  try {
    await toggleAlertConfig('limit_up', enabled)
  } catch (e) {
    console.error('Toggle alert config error:', e)
  }
}

const enableMobileSpeech = () => {
  unlockSpeech({ silent: true })
}

// 更新时间
let timeInterval: number
onMounted(() => {
  loadAlertConfig()
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

.tdx-standalone {
  min-height: 100vh;
  width: 100vw;
  overflow: auto;
  background: #050b12;
}

.layout-container {
  height: 100%;
}

.content-container {
  min-width: 0;
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
  min-width: 0;
  overflow-y: auto;
}

.mobile-bottom-nav {
  display: none;
}

.mobile-speech-unlock {
  display: none;
}

@media (max-width: 767px) {
  .app-container {
    height: 100dvh;
  }

  .aside {
    display: none;
  }

  .layout-container {
    min-width: 0;
  }

  .header {
    height: 52px !important;
    padding: 0 12px;
    position: sticky;
    top: 0;
    z-index: 20;

    .header-left {
      gap: 8px;

      .market-status {
        padding: 3px 8px;
        font-size: 12px;
      }

      .current-time {
        font-size: 13px;
      }
    }

    .header-right {
      gap: 8px;

      :deep(.el-switch) {
        display: none;
      }

      .alert-badge {
        margin-right: 0;
      }

      .mobile-speech-unlock {
        display: inline-flex;
        padding: 0 6px;
        font-size: 12px;
      }
    }
  }

  .main {
    padding: 10px 10px 74px;
  }

  .mobile-bottom-nav {
    position: fixed;
    left: 0;
    right: 0;
    bottom: 0;
    z-index: 30;
    display: flex;
    gap: 2px;
    overflow-x: auto;
    overflow-y: hidden;
    -webkit-overflow-scrolling: touch;
    scrollbar-width: none;
    scroll-snap-type: x proximity;
    height: calc(58px + env(safe-area-inset-bottom));
    padding: 6px 4px calc(6px + env(safe-area-inset-bottom));
    border-top: 1px solid #e5e7eb;
    background: rgba(255, 255, 255, 0.96);
    box-shadow: 0 -8px 22px rgba(15, 23, 42, 0.08);

    &::-webkit-scrollbar {
      display: none;
    }
  }

  .mobile-nav-item {
    display: flex;
    flex: 0 0 64px;
    min-width: 64px;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 3px;
    color: #64748b;
    font-size: 11px;
    line-height: 1;
    text-decoration: none;
    scroll-snap-align: center;

    .el-icon {
      font-size: 18px;
    }

    &.active {
      color: #1677ff;
      font-weight: 600;
    }
  }

  :deep(.el-drawer.rtl) {
    width: min(400px, 92vw) !important;
  }
}
</style>
