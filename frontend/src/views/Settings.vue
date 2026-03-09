<template>
  <div class="settings">
    <el-row :gutter="16">
      <!-- 播报设置 -->
      <el-col :span="12">
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

      <!-- 大单设置 -->
      <el-col :span="12">
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
      <el-col :span="12">
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
      <el-col :span="12">
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
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { getConfig, updateConfig } from '@/api/config'
import { useConfigStore } from '@/stores/config'

const configStore = useConfigStore()

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
  watch_list: [] as string[]
})

const newWatchCode = ref('')
const notificationPermission = ref(Notification?.permission || 'default')

// 加载配置
async function loadConfig() {
  try {
    const data = await getConfig()
    Object.assign(config, data)
    configStore.setConfig(data)
  } catch (e) {
    console.error('Load config error:', e)
  }
}

// 保存配置
async function saveConfig() {
  try {
    await updateConfig(config)
    configStore.setConfig(config)
    ElMessage.success('保存成功')
  } catch (e) {
    console.error('Save config error:', e)
    ElMessage.error('保存失败')
  }
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

onMounted(() => {
  loadConfig()
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

  .watchlist {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
  }
}
</style>
