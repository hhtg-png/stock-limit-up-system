<template>
  <div class="alert-panel">
    <div class="panel-header">
      <el-button size="small" @click="markAllRead">全部已读</el-button>
      <el-button size="small" @click="clearAll">清空</el-button>
    </div>
    
    <div class="message-list">
      <div 
        v-for="msg in messages" 
        :key="msg.id" 
        class="message-item"
        :class="{ unread: !msg.read, [msg.type]: true }"
        @click="handleClick(msg)"
      >
        <div class="message-icon">
          <el-icon v-if="msg.type === 'limit_up'"><TrendCharts /></el-icon>
          <el-icon v-else-if="msg.type === 'big_order'"><Coin /></el-icon>
          <el-icon v-else><Bell /></el-icon>
        </div>
        <div class="message-content">
          <div class="message-title">
            {{ msg.stock_name }} 
            <span class="code">{{ msg.stock_code }}</span>
          </div>
          <div class="message-text">{{ msg.content }}</div>
        </div>
        <div class="message-time">
          {{ formatTime(msg.time) }}
        </div>
      </div>
      
      <div v-if="messages.length === 0" class="empty">
        暂无消息
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useRouter } from 'vue-router'
import { TrendCharts, Coin, Bell } from '@element-plus/icons-vue'
import { useAlertStore, type AlertMessage } from '@/stores/alert'
import dayjs from 'dayjs'

const router = useRouter()
const alertStore = useAlertStore()

const messages = computed(() => alertStore.messages)

function markAllRead() {
  alertStore.markAllRead()
}

function clearAll() {
  alertStore.clearMessages()
}

function handleClick(msg: AlertMessage) {
  alertStore.markRead(msg.id)
  router.push(`/stock/${msg.stock_code}`)
}

function formatTime(time: string) {
  return dayjs(time).format('HH:mm:ss')
}
</script>

<style lang="scss" scoped>
.alert-panel {
  height: 100%;
  display: flex;
  flex-direction: column;

  .panel-header {
    display: flex;
    justify-content: flex-end;
    gap: 8px;
    padding-bottom: 16px;
    border-bottom: 1px solid #f0f0f0;
    margin-bottom: 16px;
  }

  .message-list {
    flex: 1;
    overflow-y: auto;

    .message-item {
      display: flex;
      padding: 12px;
      border-radius: 8px;
      margin-bottom: 8px;
      cursor: pointer;
      transition: background 0.2s;

      &:hover {
        background: #f5f5f5;
      }

      &.unread {
        background: #f6ffed;
      }

      &.limit_up .message-icon {
        color: #f5222d;
      }

      &.big_order .message-icon {
        color: #1890ff;
      }

      &.status_change .message-icon {
        color: #faad14;
      }

      .message-icon {
        width: 40px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 20px;
      }

      .message-content {
        flex: 1;

        .message-title {
          font-weight: 500;
          margin-bottom: 4px;

          .code {
            font-size: 12px;
            color: #8c8c8c;
          }
        }

        .message-text {
          font-size: 13px;
          color: #666;
        }
      }

      .message-time {
        font-size: 12px;
        color: #8c8c8c;
        white-space: nowrap;
      }
    }

    .empty {
      text-align: center;
      color: #8c8c8c;
      padding: 40px;
    }
  }
}
</style>
