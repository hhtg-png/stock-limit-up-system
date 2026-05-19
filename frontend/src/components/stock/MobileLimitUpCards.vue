<template>
  <div class="mobile-stock-list" v-loading="loading">
    <button
      v-for="item in items"
      :key="item.stock_code"
      type="button"
      class="mobile-stock-card"
      @click="$emit('select', item)"
    >
      <div class="mobile-stock-main">
        <div>
          <strong>{{ item.stock_name }}</strong>
          <span>{{ item.stock_code }}</span>
        </div>
        <div class="mobile-stock-tags">
          <el-tag v-if="item.continuous_limit_up_days > 1" type="danger" size="small">
            {{ item.continuous_limit_up_days }}板
          </el-tag>
          <el-tag :type="item.is_sealed ? 'danger' : 'warning'" size="small">
            {{ item.is_sealed ? '封板' : '炸板' }}
          </el-tag>
        </div>
      </div>

      <div class="mobile-stock-metrics">
        <div>
          <span>首封</span>
          <strong>{{ item.first_limit_up_time || '-' }}</strong>
        </div>
        <div>
          <span>回封</span>
          <strong>{{ item.final_seal_time || '-' }}</strong>
        </div>
        <div>
          <span>封单</span>
          <strong>{{ formatWan(item.seal_amount) }}</strong>
        </div>
        <div>
          <span>换手</span>
          <strong>{{ formatPercent(item.turnover_rate) }}</strong>
        </div>
      </div>

      <div class="mobile-stock-reason">
        <span>{{ item.reason_category || '未分类' }}</span>
        <p>{{ item.limit_up_reason || '暂无涨停原因' }}</p>
      </div>
    </button>

    <el-empty v-if="!loading && items.length === 0" description="暂无涨停数据" />
  </div>
</template>

<script setup lang="ts">
import type { LimitUpRealtime } from '@/types/limit-up'

defineProps<{
  items: LimitUpRealtime[]
  loading?: boolean
}>()

defineEmits<{
  select: [row: LimitUpRealtime]
}>()

function formatWan(value: number | undefined | null): string {
  if (value == null || value === 0) return '-'
  return `${value.toFixed(0)}万`
}

function formatPercent(value: number | undefined | null): string {
  if (value == null || value === 0) return '-'
  return `${value.toFixed(2)}%`
}
</script>

<style lang="scss" scoped>
.mobile-stock-list {
  display: none;
}

@media (max-width: 767px) {
  .mobile-stock-list {
    display: flex;
    flex-direction: column;
    gap: 10px;
    min-height: 160px;
  }

  .mobile-stock-card {
    width: 100%;
    border: 1px solid #e5e7eb;
    border-radius: 8px;
    background: #fff;
    padding: 12px;
    text-align: left;
    box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
  }

  .mobile-stock-main {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 10px;

    strong {
      display: block;
      color: #111827;
      font-size: 16px;
      line-height: 1.25;
    }

    span {
      display: block;
      margin-top: 3px;
      color: #1677ff;
      font-family: monospace;
      font-size: 12px;
    }
  }

  .mobile-stock-tags {
    display: flex;
    flex-shrink: 0;
    gap: 6px;
  }

  .mobile-stock-metrics {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 6px;
    margin-top: 10px;

    div {
      min-width: 0;
      border-radius: 6px;
      background: #f8fafc;
      padding: 7px 5px;
      text-align: center;
    }

    span {
      display: block;
      color: #64748b;
      font-size: 11px;
      line-height: 1;
    }

    strong {
      display: block;
      margin-top: 5px;
      color: #111827;
      font-size: 12px;
      line-height: 1.1;
      white-space: nowrap;
    }
  }

  .mobile-stock-reason {
    margin-top: 10px;
    border-top: 1px solid #f1f5f9;
    padding-top: 10px;

    span {
      display: inline-flex;
      border-radius: 999px;
      background: #eff6ff;
      padding: 3px 8px;
      color: #1d4ed8;
      font-size: 11px;
      font-weight: 600;
    }

    p {
      margin: 7px 0 0;
      color: #475569;
      font-size: 13px;
      line-height: 1.5;
    }
  }
}
</style>
