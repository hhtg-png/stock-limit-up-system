<template>
  <el-drawer :model-value="modelValue" :title="group ? '编辑动态板块' : '创建动态板块'" size="min(720px, 100vw)" :close-on-click-modal="!saving" :close-on-press-escape="!saving" :show-close="!saving" @update:model-value="$emit('update:modelValue', $event)">
    <div class="editor">
      <section v-if="!group" class="template-box"><label>从常用条件开始</label><div class="template-options"><el-button v-for="item in availableTemplates" :key="item.name" size="small" :disabled="saving" @click="applyTemplate(item)">{{ item.name }}</el-button></div><p>模板只填写条件，保存后才会创建板块。</p></section>
      <el-form :disabled="saving" label-position="top" @submit.prevent="save">
        <div class="form-row"><el-form-item label="板块名称" required><el-input v-model="form.name" maxlength="80" show-word-limit placeholder="例如：二板竞价观察" /></el-form-item><el-form-item label="分类"><el-select v-model="form.folder" filterable allow-create default-first-option clearable placeholder="选择或输入分类"><el-option v-for="folder in folders" :key="folder" :label="folder" :value="folder" /></el-select></el-form-item></div>
        <el-form-item label="说明"><el-input v-model="form.description" type="textarea" :rows="2" maxlength="500" placeholder="记录观察目的（选填）" /></el-form-item>
        <div class="form-row"><el-form-item label="选股范围"><el-select v-model="form.universe"><el-option label="全沪深 A 股" value="all_a" /><el-option label="今日触板及昨日涨停" value="limit_up" /></el-select></el-form-item><el-form-item label="成员方式"><el-select v-model="form.membership_mode"><el-option label="持续跟踪 · 命中后保留" value="retain" /><el-option label="动态进出 · 随条件调整" value="dynamic" /></el-select></el-form-item></div>
        <p class="hint">{{ form.membership_mode === 'retain' ? '股票命中后持续保留，可手动移出；以后再次满足条件仍可重新入选。' : '不再满足条件时退出当前名单；原入选批次及历史走势仍会保留。' }} 输入数据暂缺时，保留原成员并等待更新。</p>
        <p v-if="group" class="hint">修改条件、选股范围或成员方式将重新筛选当前名单，旧批次仍可查询。</p>
        <div class="rules-title"><strong>选股条件</strong><el-radio-group v-model="form.match_mode" size="small"><el-radio-button value="all">全部满足</el-radio-button><el-radio-button value="any">任一满足</el-radio-button></el-radio-group></div>
        <div v-for="(rule, index) in form.rules" :key="index" class="rule-row">
          <el-select v-model="rule.field" filterable :aria-label="`第${index + 1}条条件字段`" placeholder="选择条件" @change="resetRule(rule)"><el-option v-for="field in fields" :key="field.key" :label="`${field.label}${field.unit ? ` (${field.unit})` : ''}`" :value="field.key" /></el-select>
          <el-select v-model="rule.op" :aria-label="`第${index + 1}条运算符`" @change="resetOperator(rule)"><el-option v-for="op in fieldFor(rule)?.operators || []" :key="op" :label="operatorLabels[op] || op" :value="op" /></el-select>
          <div class="rule-values">
            <el-select v-if="fieldFor(rule)?.kind === 'boolean'" v-model="rule.value" :aria-label="`第${index + 1}条条件值`"><el-option label="是" :value="true" /><el-option label="否" :value="false" /></el-select>
            <el-select v-else-if="fieldFor(rule)?.options?.length" v-model="rule.value" :multiple="rule.op === 'in'" :aria-label="`第${index + 1}条条件值`"><el-option v-for="option in fieldFor(rule)?.options" :key="String(option.value)" :label="option.label" :value="option.value" /></el-select>
            <el-time-select v-else-if="fieldFor(rule)?.kind === 'time'" v-model="rule.value as string" start="09:15" step="00:01" end="15:00" placeholder="时:分" :aria-label="`第${index + 1}条时间`" />
            <el-input v-else v-model="rule.value as string" :type="fieldFor(rule)?.kind === 'number' ? 'number' : 'text'" :placeholder="rule.op === 'in' ? '多个值用逗号分隔' : '条件值'" :aria-label="`第${index + 1}条条件值`" />
            <template v-if="rule.op === 'between'"><span class="to">至</span><el-time-select v-if="fieldFor(rule)?.kind === 'time'" v-model="rule.value2 as string" start="09:15" step="00:01" end="15:00" placeholder="上限" aria-label="区间上限" /><el-input v-else v-model="rule.value2" type="number" placeholder="上限" aria-label="区间上限" /></template>
          </div>
          <el-button text type="danger" :aria-label="`删除第${index + 1}条条件`" @click="form.rules.splice(index, 1)">删除</el-button>
        </div>
        <el-button size="small" :disabled="form.rules.length >= 30 || !fields.length" @click="addRule">＋ 添加条件</el-button>
        <div class="form-options"><el-form-item label="标识颜色"><el-color-picker v-model="form.color" :predefine="['#409eff', '#67c23a', '#e6a23c', '#f56c6c', '#9b6dff', '#18a999']" /></el-form-item><el-checkbox v-model="form.pinned">置顶板块</el-checkbox><el-checkbox v-model="form.enabled">启用自动选股</el-checkbox></div>
      </el-form>
      <section class="preview" aria-live="polite"><div class="preview-title"><strong>条件预览</strong><span>{{ previewLoading ? '正在预览…' : preview ? statusLabel(preview.status) : '填写条件后自动预览' }}</span></div><p v-if="previewError" class="error">{{ previewError }}</p><template v-else-if="preview"><p>匹配 <b>{{ preview.matched_count ?? preview.total ?? preview.items.length }}</b> 只<span v-if="preview.unknown_count">，{{ preview.unknown_count }} 只数据待确认</span><span v-if="preview.trade_date"> · {{ preview.trade_date }}</span></p><div class="preview-stocks"><span v-for="stock in (preview.items || []).slice(0, 12)" :key="stock.stock_code">{{ stock.stock_name || stock.stock_code }}</span></div><p v-for="warning in preview.warnings || []" :key="warning" class="hint">{{ warning }}</p></template><p v-else class="hint">使用最近一次真实行情快照。行情尚未就绪时，请先保存，后台将自动更新。</p></section>
      <p v-if="error" class="error" role="alert">{{ error }}</p>
    </div>
    <template #footer><div class="editor-footer"><el-button :disabled="saving" @click="$emit('update:modelValue', false)">取消</el-button><el-button type="primary" :loading="saving" :disabled="!fields.length" @click="save">{{ group ? '保存并更新' : '创建并开始跟踪' }}</el-button></div></template>
  </el-drawer>
</template>

<script setup lang="ts">
import { reactive, ref, computed, watch, onBeforeUnmount } from 'vue'
import { createDynamicGroup, updateDynamicGroup, previewDynamicGroup } from '@/api/dynamic-groups'
import type { DynamicDefinition, DynamicField, DynamicGroup, DynamicPreview, DynamicRule, DynamicTemplate } from '@/types/dynamic-groups'
import { createRequestGate, operatorLabels, statusLabel, validateDefinition } from '@/utils/dynamicGroups'
const props = defineProps<{ modelValue: boolean; group: DynamicGroup | null; fields: DynamicField[]; templates: DynamicTemplate[]; folders: string[]; initialTemplateName?: string }>()
const emit = defineEmits<{ 'update:modelValue': [value: boolean]; saved: [group: DynamicGroup] }>()
const defaults = (): DynamicDefinition => ({ name: '', description: '', folder: '', color: '#409eff', pinned: false, enabled: true, universe: 'all_a', membership_mode: 'retain', match_mode: 'all', rules: [] })
const form = reactive<DynamicDefinition>(defaults())
const saving = ref(false), error = ref(''), preview = ref<DynamicPreview | null>(null), previewError = ref(''), previewLoading = ref(false)
const previewGate = createRequestGate()
let previewTimer: ReturnType<typeof setTimeout> | undefined
const starterTemplates: DynamicTemplate[] = [
  { name: '二板竞价', rules: [{ field: 'yesterday_continuous_days', op: 'eq', value: 1 }, { field: 'today_open_change_pct', op: 'between', value: 2, value2: 7 }, { field: 'is_st', op: 'eq', value: false }] },
  { name: '昨日涨停', universe: 'limit_up', rules: [{ field: 'yesterday_limit_up', op: 'eq', value: true }, { field: 'is_st', op: 'eq', value: false }] },
  { name: '高换手活跃', rules: [{ field: 'turnover_rate', op: 'gte', value: 10 }, { field: 'amount_yi', op: 'gte', value: 3 }, { field: 'is_st', op: 'eq', value: false }] }
]
const availableTemplates = computed(() => [...props.templates, ...starterTemplates.filter(template => !props.templates.some(item => item.name === template.name))].filter(template => template.rules?.every(rule => props.fields.some(field => field.key === rule.field && field.operators.includes(rule.op)))))
function fieldFor(rule: DynamicRule) { return props.fields.find(field => field.key === rule.field) }
function resetRule(rule: DynamicRule) { const field = fieldFor(rule); rule.op = field?.operators[0] || 'eq'; rule.value = field?.kind === 'boolean' ? true : ''; delete rule.value2 }
function resetOperator(rule: DynamicRule) { if (rule.op === 'in' && !Array.isArray(rule.value)) rule.value = rule.value === '' ? [] : [String(rule.value)]; else if (rule.op !== 'in' && Array.isArray(rule.value)) rule.value = rule.value[0] || ''; if (rule.op !== 'between') delete rule.value2 }
function addRule() { const field = props.fields.find(item => item.key === 'change_pct') || props.fields[0]; if (!field) return; const rule: DynamicRule = { field: field.key, op: field.operators[0] || 'gte', value: '' }; resetRule(rule); form.rules.push(rule) }
function applyTemplate(template: DynamicTemplate) { form.name = template.name; form.description = template.description || ''; form.rules = structuredClone(template.rules); form.universe = template.universe || 'all_a'; form.match_mode = template.match_mode || 'all'; form.membership_mode = template.membership_mode || 'retain' }
function definition(): DynamicDefinition { return { ...form, name: form.name.trim(), folder: form.folder.trim(), color: form.color || '#409eff', rules: form.rules.map(rule => { const field = fieldFor(rule); return { field: rule.field, op: rule.op, value: rule.op === 'in' && !Array.isArray(rule.value) ? String(rule.value).split(/[,，]/).map(value => value.trim()).filter(Boolean) : field?.kind === 'number' ? Number(rule.value) : rule.value, ...(rule.op === 'between' ? { value2: field?.kind === 'number' ? Number(rule.value2) : rule.value2 } : {}) } }) } }
watch(() => props.modelValue, open => {
  previewGate.invalidate(); clearTimeout(previewTimer); preview.value = null; previewError.value = ''; error.value = ''; previewLoading.value = false
  if (open) { Object.assign(form, defaults(), props.group ? JSON.parse(JSON.stringify(props.group)) : {}); const template = availableTemplates.value.find(item => item.name === props.initialTemplateName); if (!props.group && template) applyTemplate(template); if (!form.rules.length && props.fields.length) addRule() }
})
watch(form, () => {
  previewGate.invalidate(); clearTimeout(previewTimer); previewLoading.value = false
  if (!props.modelValue) return
  const invalid = validateDefinition({ ...form, name: form.name || '未命名' }, props.fields)
  if (invalid) { preview.value = null; previewError.value = invalid; return }
  const request = previewGate.begin()
  const runPreview = async () => {
    if (!previewGate.isCurrent(request) || !props.modelValue) return
    previewLoading.value = true; previewError.value = ''
    try { const data = await previewDynamicGroup({ ...definition(), name: form.name.trim() || '未命名' }); if (previewGate.isCurrent(request)) { preview.value = data; if (['pending', 'queued', 'loading', 'building'].includes(data.status)) previewTimer = setTimeout(runPreview, 5000) } }
    catch (e) { if (previewGate.isCurrent(request)) previewError.value = errorText(e, '暂时无法预览，可以保存后等待更新') }
    finally { if (previewGate.isCurrent(request)) previewLoading.value = false }
  }
  previewTimer = setTimeout(runPreview, 650)
}, { deep: true })
function errorText(e: unknown, fallback: string) { const detail = (e as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail; return typeof detail === 'string' ? detail : fallback }
async function save() {
  error.value = validateDefinition(form, props.fields); if (error.value || saving.value) return
  saving.value = true
  try { const data = props.group ? await updateDynamicGroup(props.group.id, definition()) : await createDynamicGroup(definition()); emit('saved', data); emit('update:modelValue', false) }
  catch (e) { error.value = errorText(e, '保存失败，请重试') }
  finally { saving.value = false }
}
onBeforeUnmount(() => { clearTimeout(previewTimer); previewGate.invalidate() })
</script>

<style scoped>
.editor{min-width:0}.template-box{background:#f3f7fe;border-radius:10px;padding:15px;margin-bottom:22px}.template-box label{font-size:13px;font-weight:600}.template-options{display:flex;flex-wrap:wrap;gap:8px;margin-top:10px}.template-options .el-button+.el-button{margin-left:0}.template-box p,.hint{font-size:12px;color:#8190a5;line-height:1.7;margin:8px 0}.form-row{display:grid;grid-template-columns:1fr 1fr;gap:16px}.form-row .el-select{width:100%}.rules-title{display:flex;justify-content:space-between;align-items:center;margin:24px 0 14px;gap:12px}.rules-title strong{font-size:14px}.rule-row{display:grid;grid-template-columns:minmax(120px,1.3fr) minmax(100px,.8fr) minmax(140px,1.5fr) 44px;gap:8px;align-items:start;margin-bottom:12px}.rule-values{display:flex;gap:6px;align-items:center;min-width:0}.rule-values>*{min-width:0}.rule-values .el-select,.rule-values .el-input{width:100%}.to{color:#9aa6b6;font-size:12px;flex-shrink:0}.form-options{display:flex;align-items:center;gap:22px;margin-top:22px}.form-options .el-form-item{margin-bottom:0}.preview{border:1px solid #e6ecf5;border-radius:10px;padding:16px;margin-top:22px;font-size:13px}.preview-title{display:flex;justify-content:space-between;gap:10px}.preview-title span{color:#8a98ab;font-size:12px}.preview-stocks{display:flex;gap:6px;flex-wrap:wrap}.preview-stocks span{background:#f1f5fb;border-radius:5px;padding:4px 7px;font-size:12px}.error{color:#d45656;font-size:13px;line-height:1.6}.editor-footer{display:flex;justify-content:flex-end;gap:10px}@media(max-width:600px){.form-row{grid-template-columns:1fr;gap:0}.rule-row{grid-template-columns:1fr 104px 44px;background:#f6f8fc;padding:10px;border-radius:8px;gap:8px}.rule-values{grid-column:1/3;grid-row:2}.rule-row>.el-button{grid-column:3;grid-row:1}.form-options{gap:12px;flex-wrap:wrap}.rules-title{flex-wrap:wrap}.editor-footer .el-button{margin:0}.template-options{gap:6px}}
</style>
