<template>
  <section class="directory" aria-label="动态板块目录">
    <header><div><strong>我的板块</strong><span>{{ total }} 个</span></div><el-button type="primary" size="small" @click="$emit('create')">新建板块</el-button></header>
    <el-input :model-value="query.q" clearable placeholder="搜索板块名称或描述" maxlength="100" aria-label="搜索板块" @update:model-value="change('q', $event)" />
    <div class="filters">
      <el-select :model-value="query.folder" clearable placeholder="全部分类" aria-label="板块分类" @update:model-value="change('folder', $event)"><el-option v-for="folder in folders" :key="folder" :label="folder || '未分类'" :value="folder || '__unfiled__'" /></el-select>
      <el-select :model-value="query.state" aria-label="板块状态" @update:model-value="change('state', $event)"><el-option label="未归档" value="all" /><el-option label="运行中" value="active" /><el-option label="已暂停" value="paused" /><el-option label="已归档" value="archived" /></el-select>
    </div>
    <div class="list-caption"><span>分类管理 · 永久保留入选档案</span><el-select :model-value="query.sort" size="small" aria-label="目录排序" @update:model-value="change('sort', $event)"><el-option label="置顶优先" value="pinned" /><el-option label="最近更新" value="updated" /><el-option label="名称排序" value="name" /></el-select></div>
    <p v-if="error" class="load-error" role="alert">{{ error }} <el-button link type="primary" @click="$emit('retry')">重试</el-button></p>
    <div v-loading="loading" class="groups">
      <button v-for="group in groups" :key="group.id" type="button" class="group" :class="{ selected: group.id === selectedId }" :aria-pressed="group.id === selectedId" @click="$emit('select', group)">
        <div class="group-title"><i :style="{ background: group.color || '#409eff' }" /><strong>{{ group.name }}</strong><span v-if="group.pinned" class="pin">置顶</span></div>
        <p class="summary">{{ ruleSummary(group.rules, fields, group.match_mode) }}<span v-if="group.strategy?.ranking">；{{ rankingSummary(group, fields) }}</span></p>
        <div class="group-meta"><span>{{ group.folder || '未分类' }} · {{ group.member_count }} 只</span><span>{{ group.archived ? '已归档' : !group.enabled ? '已暂停' : statusLabel(group.data_status) }}</span></div>
        <div class="group-meta"><span>{{ group.membership_mode === 'retain' ? '持续跟踪' : '动态进出' }}</span><time>{{ timeText(group.last_evaluated_at || group.updated_at) }}</time></div>
      </button>
      <el-empty v-if="!loading && !error && !groups.length" :image-size="64" description="没有符合条件的板块"><el-button @click="$emit('create')">创建第一个板块</el-button></el-empty>
    </div>
    <el-pagination size="small" background layout="prev, pager, next" :pager-count="5" :current-page="query.page" :page-size="20" :total="total" @current-change="change('page', $event)" />
  </section>
</template>

<script setup lang="ts">
import type { DynamicField, DynamicGroup } from '@/types/dynamic-groups'
import { ruleSummary, rankingSummary, statusLabel } from '@/utils/dynamicGroups'
export interface DirectoryQuery { q: string; folder: string; state: string; sort: string; page: number }
const props = defineProps<{ groups: DynamicGroup[]; total: number; folders: string[]; fields: DynamicField[]; query: DirectoryQuery; selectedId: number | null; loading: boolean; error: string }>()
const emit = defineEmits<{ select: [group: DynamicGroup]; create: []; query: [query: DirectoryQuery]; retry: [] }>()
function change(key: keyof DirectoryQuery, value: string | number) { emit('query', { ...props.query, [key]: value, ...(key === 'page' ? {} : { page: 1 }) }) }
function timeText(value: string | null) { return value ? value.replace('T', ' ').slice(5, 16) : '待更新' }
</script>

<style scoped>
.directory{display:flex;flex-direction:column;gap:14px;min-width:0}header,header>div,.list-caption,.group-meta,.group-title{display:flex;align-items:center;justify-content:space-between;gap:8px}header strong{font-size:17px}header span,.list-caption,.group-meta{font-size:12px;color:#8491a3}.filters{display:grid;grid-template-columns:1fr 1fr;gap:8px}.list-caption>.el-select{width:108px;flex-shrink:0}.list-caption>span{font-size:11px}.groups{min-height:180px;display:flex;flex-direction:column;gap:8px}.group{background:#fff;border:1px solid #e7ebf1;border-radius:10px;text-align:left;padding:13px 12px;cursor:pointer;display:flex;flex-direction:column;gap:9px;transition:border-color .15s;width:100%;font:inherit;color:#26364b}.group:hover{border-color:#9fc7ff}.group.selected{border-color:#409eff;background:#f1f7ff;box-shadow:0 0 0 1px #409eff20}.group-title{justify-content:flex-start;min-width:0}.group-title strong{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1;font-size:14px}.group-title i{width:8px;height:8px;border-radius:50%;flex-shrink:0}.pin{font-size:10px;color:#d99a22;background:#fff4d9;padding:2px 5px;border-radius:4px}.summary{margin:0;font-size:12px;color:#6c7b8f;line-height:1.6;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}.group-meta{font-size:11px}.el-pagination{justify-content:center;margin-top:4px}.load-error{font-size:13px;color:#c45656}
</style>
