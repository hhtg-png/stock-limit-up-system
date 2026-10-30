<template>
  <div class="rule-tree">
    <span class="join-label">{{ matchMode === 'any' ? '任一满足' : '全部满足' }}</span>
    <template v-for="(node, index) in rules" :key="index">
      <p v-if="isRuleLeaf(node)" class="leaf">{{ ruleSummary([node], fields) }}</p>
      <div v-else class="logic-group"><DynamicRuleTree :rules="'all' in node ? node.all : node.any" :fields="fields" :match-mode="'all' in node ? 'all' : 'any'" /></div>
    </template>
  </div>
</template>
<script setup lang="ts">
import type { DynamicField, DynamicRuleNode } from '@/types/dynamic-groups'
import { isRuleLeaf, ruleSummary } from '@/utils/dynamicGroups'
withDefaults(defineProps<{ rules: DynamicRuleNode[]; fields: DynamicField[]; matchMode?: string }>(), { matchMode: 'all' })
</script>
<style scoped>
.rule-tree{display:flex;flex-direction:column;gap:8px;min-width:0}.join-label{font-size:11px;color:#6689b8;font-weight:600}.leaf{font-size:12px;color:#586e89;background:#f5f8fc;margin:0;padding:9px 11px;border-radius:6px;line-height:1.7;overflow-wrap:anywhere}.logic-group{border-left:2px solid #d6e4f8;padding:9px 0 9px 12px;margin-left:3px}.logic-group .join-label{color:#8a78ad}@media(max-width:600px){.logic-group{padding-left:8px}.leaf{font-size:11px;padding:8px}}
</style>
