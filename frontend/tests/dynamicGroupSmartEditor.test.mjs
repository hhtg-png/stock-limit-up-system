import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import ts from 'typescript'
import * as vue from 'vue'
import { createServer } from 'vite'
const root = resolve(import.meta.dirname, '..')
const transpile = source => ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ES2022, target: ts.ScriptTarget.ES2022 } }).outputText
const flush = () => new Promise(resolve => setImmediate(resolve))
const fields = [{ key: 'change_pct', label: '涨幅', kind: 'number', operators: ['gte'], sortable: true }, { key: 'is_st', label: 'ST', kind: 'boolean', operators: ['eq'] }]
const base = { name: '观察', description: '', folder: '', color: '#409eff', pinned: false, enabled: true, universe: 'all_a', membership_mode: 'retain', match_mode: 'all', rules: [{ field: 'change_pct', op: 'gte', value: 5 }] }
async function withEditor(options, run) {
  const server = await createServer({ configFile: false, root, logLevel: 'silent', server: { middlewareMode: true }, resolve: { alias: { '@': resolve(root, 'src') } } })
  const helpers = await server.ssrLoadModule('/src/utils/dynamicGroups.ts'), smartHelpers = await server.ssrLoadModule('/src/utils/dynamicSmartScreening.ts')
  const source = readFileSync(resolve(root, 'src/components/dynamic-groups/DynamicGroupEditor.vue'), 'utf8').match(/<script setup lang="ts">([\s\S]*?)<\/script>/)[1]
  const ast = ts.createSourceFile('DynamicGroupEditor.ts', source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS)
  const code = transpile(ast.statements.filter(node => !ts.isImportDeclaration(node)).map(node => node.getText(ast)).join('\n'))
  const props = vue.reactive({ modelValue: false, group: null, fields, templates: [], folders: [], ...options })
  const timers = new Map(), writes = [], interpretations = [], destroyed = []; let timer = 0
  const scope = { ...vue, ...helpers, ...smartHelpers, defineProps: () => props, defineEmits: () => () => {}, onBeforeUnmount: callback => destroyed.push(callback), setTimeout: callback => { timers.set(++timer, callback); return timer }, clearTimeout: id => timers.delete(id),
    interpretDynamicGroup: (prompt, context) => new Promise(resolve => interpretations.push({ prompt, context, resolve })),
    previewDynamicGroup: async definition => ({ status: 'ready', matched_count: 1, items: [{ stock_code: 'test' }], definition }),
    createDynamicGroup: async definition => { writes.push(definition); return { ...definition, id: 1 } }, updateDynamicGroup: async (_id, definition) => { writes.push(definition); return { ...definition, id: 1 } }
  }
  const effects = vue.effectScope()
  try {
    const state = effects.run(() => new Function(...Object.keys(scope), `${code}\nreturn { form, editorMode, smart, save, changeEditorMode, canSave, interpretPrompt, definition, discardSmartDraft, canInterpret: typeof canInterpret === 'undefined' ? undefined : canInterpret }`)(...Object.values(scope)))
    props.modelValue = true; await vue.nextTick()
    await run({ ...state, props, writes, interpretations, async preview() { const pending = [...timers.values()]; timers.clear(); pending.forEach(callback => callback()); await flush(); await vue.nextTick() } })
  } finally { destroyed.forEach(callback => callback()); effects.stop(); await server.close() }
}

test('new smart editor rejects unparsed and changed prompts and saves only the understood real-previewed tree', async () => {
  await withEditor({}, async editor => {
    assert.equal(editor.editorMode.value, 'smart')
    editor.smart.setPrompt('涨幅大于5%，排除ST'); await editor.save(); assert.equal(editor.writes.length, 0)
    const pending = editor.interpretPrompt()
    const value = { ...base, membership_mode: 'dynamic', rules: [{ all: [base.rules[0], { field: 'is_st', op: 'eq', value: false }] }], strategy: { prompt: '涨幅大于5%，排除ST', summary: '强势非ST', assumptions: [] } }
    editor.interpretations[0].resolve({ status: 'ready', definition: value, message: '', assumptions: [], questions: [], unsupported: [], model: 'test' }); await pending
    assert.equal(editor.canSave.value, false, 'real preview is required before saving a new understood definition')
    await editor.preview(); assert.equal(editor.canSave.value, true)
    await editor.save(); assert.deepEqual(editor.writes[0].rules, value.rules); assert.equal(editor.writes[0].membership_mode, 'dynamic')
    editor.smart.setPrompt('再要求均线金叉'); await editor.save(); assert.equal(editor.writes.length, 1)
  })
})

test('legacy flat groups remain editable and nested manual mode preserves compound conditions', async () => {
  await withEditor({ group: { ...base, id: 5 } }, async editor => {
    assert.equal(editor.editorMode.value, 'manual')
    editor.form.rules[0].value = 8; await editor.save(); assert.equal(editor.writes[0].rules[0].value, 8)
  })
  const nested = { ...base, id: 5, rules: [{ any: [base.rules[0], { field: 'is_st', op: 'eq', value: false }] }] }
  await withEditor({ group: nested }, async editor => {
    await editor.save(); assert.deepEqual(editor.writes[0].rules, nested.rules)
  })
})

test('switching to manual cannot silently save the old plan behind an unparsed new smart description', async () => {
  await withEditor({}, async editor => {
    editor.form.name = '手动观察'; editor.smart.setPrompt('未理解的新要求'); editor.editorMode.value = 'manual'; editor.changeEditorMode('manual')
    editor.form.rules = base.rules; await editor.save(); assert.equal(editor.writes.length, 0)
    editor.discardSmartDraft(); await editor.save(); assert.equal(editor.writes.length, 1)
  })
})

test('refinement waits for the field catalogue and preserves every existing condition once it is ready', async () => {
  const rules = [{ field: 'is_st', op: 'eq', value: false }, base.rules[0]]
  const group = { ...base, id: 5, rules, strategy: { prompt: '排除ST且涨幅至少5%', summary: '旧条件', assumptions: [] } }
  await withEditor({ group, fields: [] }, async editor => {
    editor.smart.setPrompt('再要求涨幅至少8%')
    const blocked = editor.interpretPrompt()
    assert.equal(editor.interpretations.length, 0, 'missing catalogue must not silently turn a refinement into a new screen')
    await blocked
    assert.equal(editor.canInterpret.value, false)
    assert.match(editor.smart.state.error, /字段.*(加载|就绪)/)
    assert.deepEqual(editor.definition().rules, rules)
    editor.props.fields = fields; await vue.nextTick()
    assert.equal(editor.canInterpret.value, true)
    const request = editor.interpretPrompt()
    assert.deepEqual(editor.interpretations[0].context.rules, rules)
    const updated = { ...group, rules: [rules[0], { ...rules[1], value: 8 }] }
    editor.interpretations[0].resolve({ status: 'ready', definition: updated, message: '', assumptions: [], questions: [], unsupported: [], model: 'test' })
    await request; await editor.preview(); await editor.save()
    assert.deepEqual(editor.writes[0].rules, updated.rules)
  })
})

test('an invalid existing definition blocks refinement instead of omitting context', async () => {
  const rules = [{ field: 'old_unavailable_field', op: 'eq', value: 1 }, { field: 'is_st', op: 'eq', value: false }]
  await withEditor({ group: { ...base, id: 5, rules } }, async editor => {
    editor.smart.setPrompt('再要求涨幅至少8%')
    const blocked = editor.interpretPrompt()
    assert.equal(editor.interpretations.length, 0)
    await blocked
    assert.equal(editor.canInterpret.value, false)
    assert.match(editor.smart.state.error, /已有条件.*校验/)
    assert.deepEqual(editor.definition().rules, rules)
    assert.equal(editor.writes.length, 0)
  })
})
