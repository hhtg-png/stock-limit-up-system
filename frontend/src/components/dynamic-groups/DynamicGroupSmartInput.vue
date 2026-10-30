<template>
  <section class="smart-input">
    <header><div><span class="smart-label">智能选股</span><h3>描述你的选股想法</h3></div><span class="step-label">理解 → 预览 → 保存</span></header>
    <el-input :model-value="prompt" :disabled="disabled" type="textarea" :rows="5" maxlength="2000" show-word-limit aria-label="选股想法" placeholder="例如：最近5个已完成交易日有两次涨停，今天涨幅大于3%，排除ST，按换手率从高到低选前20只。" @update:model-value="$emit('update:prompt', $event)" @keydown.ctrl.enter.prevent="$emit('interpret')" />
    <div class="examples"><span>试试</span><button v-for="example in examples" :key="example.label" type="button" :disabled="disabled" @click="$emit('example', example.prompt)">{{ example.label }}</button></div>
    <div class="smart-actions"><p>{{ hasContext ? '继续补充描述，将结合下方已有条件调整。' : '先查看理解结果，再确认是否创建。' }}</p><el-button type="primary" :loading="busy" :disabled="disabled || interpretDisabled || !prompt.trim()" @click="$emit('interpret')">{{ busy ? '正在理解' : hasContext ? '理解并调整条件' : '理解选股想法' }}</el-button></div>
    <p v-if="interpretBlockedReason && !error" class="smart-notice" role="status">{{ interpretBlockedReason }}</p>
    <p class="capabilities">支持条件组合、涨停 / 触板历史统计、排序与数量。历史统计使用最近 1–60 个已完成交易日，盘中不含今日，收盘归档后可包含当日。</p>
    <p class="capabilities">均线、历史放量、新闻情绪等未支持条件会明确列出。</p>
    <p v-if="error" class="smart-error" role="alert">{{ error }}</p>
    <div v-if="stale && !busy" class="smart-notice" role="status">描述已修改，请重新理解后预览和保存。</div>
    <template v-if="result && !stale && !busy">
      <div v-if="result.status !== 'ready'" class="feedback" aria-live="polite"><strong>{{ result.status === 'needs_clarification' ? '还需要你补充' : result.status === 'unsupported' ? '这些条件暂时无法执行' : '暂时无法完成理解' }}</strong><p>{{ result.message }}</p><ul v-if="result.questions.length"><li v-for="question in result.questions" :key="question">{{ question }}</li></ul><ul v-if="result.unsupported.length"><li v-for="item in result.unsupported" :key="item">暂不支持：{{ item }}</li></ul><p class="feedback-hint">补充或调整上方描述后，重新理解。未解决的条件不会被省略后保存。</p></div>
      <section v-else class="understanding" aria-label="选股理解结果"><div class="understanding-title"><strong>将按以下条件选股</strong><el-tag type="success" size="small">理解完成</el-tag></div><p v-if="!manuallyAdjusted && summary" class="summary">{{ summary }}</p><p v-if="manuallyAdjusted" class="adjusted-note">已手动调整，以这里显示的当前条件与排序为准。</p><DynamicRuleTree :rules="rules" :fields="fields" :match-mode="matchMode" /><div v-if="assumptions.length" class="assumptions"><strong>理解时采用的约定</strong><ul><li v-for="item in assumptions" :key="item">{{ item }}</li></ul></div></section>
    </template>
  </section>
</template>
<script setup lang="ts">
import DynamicRuleTree from './DynamicRuleTree.vue'
import type { DynamicField, DynamicInterpretation, DynamicRuleNode } from '@/types/dynamic-groups'
defineProps<{ prompt: string; busy: boolean; disabled: boolean; interpretDisabled: boolean; interpretBlockedReason: string; result: DynamicInterpretation | null; stale: boolean; error: string; hasContext: boolean; rules: DynamicRuleNode[]; matchMode: string; fields: DynamicField[]; summary: string; assumptions: string[]; manuallyAdjusted: boolean }>()
defineEmits<{ 'update:prompt': [value: string]; interpret: []; example: [value: string] }>()
const examples = [
  { label: '近期涨停', prompt: '最近5个已完成交易日有至少2次涨停，今天涨幅大于3%，排除ST，按换手率从高到低选前20只。' },
  { label: '竞价强势', prompt: '昨日首板且今日竞价涨幅在2%到7%之间，排除ST，按成交额从高到低选前30只，命中后持续跟踪。' },
  { label: '组合条件', prompt: '排除ST，满足最近10个已完成交易日至少2次触板或者今日涨幅大于5%，成交额至少3亿元。' }
]
</script>
<style scoped>
.smart-input{min-width:0;margin-bottom:22px}.smart-input header{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:14px}.smart-label{display:inline-block;font-size:10px;letter-spacing:1px;color:#658bd0;margin-bottom:6px}.smart-input h3{font-size:19px;margin:0;font-weight:600;color:#344e71}.step-label{font-size:11px;color:#9aa8ba;white-space:nowrap}.smart-input :deep(.el-textarea__inner){line-height:1.9;padding:13px 14px 25px;background:#f9fbff;border-radius:9px;box-shadow:0 0 0 1px #dbe6f7 inset;font-size:13px}.examples{display:flex;gap:7px;align-items:center;flex-wrap:wrap;margin-top:10px}.examples>span{font-size:11px;color:#9aabc0}.examples button{font:inherit;font-size:11px;border:1px solid #e1e9f5;color:#6c87ad;background:white;border-radius:14px;padding:5px 9px;cursor:pointer}.examples button:disabled{cursor:default;opacity:.5}.smart-actions{display:flex;align-items:center;justify-content:space-between;gap:12px;margin:14px 0 10px}.smart-actions p{font-size:11px;color:#8597af;line-height:1.7;margin:0}.smart-actions .el-button{flex-shrink:0}.capabilities{font-size:10px;color:#9aa7ba;line-height:1.7;margin:4px 0}.smart-error{font-size:12px;color:#c75861;line-height:1.8}.smart-notice{font-size:12px;color:#a98a43;background:#fff9eb;padding:11px;border-radius:7px;margin-top:14px}.feedback{margin-top:18px;padding:16px;background:#fff9ee;border:1px solid #f2e6c9;border-radius:9px;color:#9b7b3c;font-size:12px;line-height:1.8;overflow-wrap:anywhere}.feedback strong{font-size:13px}.feedback p{margin:9px 0}.feedback ul{padding-left:18px}.feedback .feedback-hint{font-size:11px;color:#ac9c80}.understanding{border:1px solid #e3ebf7;border-radius:9px;padding:16px;margin-top:20px}.understanding-title{display:flex;justify-content:space-between;align-items:center;gap:10px;margin-bottom:13px;font-size:13px;color:#435f83}.summary{font-size:12px;color:#7e90aa;line-height:1.8;margin:0 0 12px;overflow-wrap:anywhere}.assumptions{font-size:11px;color:#8b9bb0;margin-top:15px;line-height:1.8}.assumptions strong{font-weight:500}.assumptions ul{padding-left:17px;margin:6px 0 0}.adjusted-note{font-size:11px;color:#a5916c;line-height:1.7}@media(max-width:600px){.smart-input h3{font-size:18px}.step-label{font-size:10px}.smart-actions{align-items:flex-start;flex-direction:column}.smart-actions .el-button{width:100%}.smart-input :deep(.el-textarea__inner){font-size:12px}.understanding{padding:12px}.feedback{padding:12px}}
</style>
