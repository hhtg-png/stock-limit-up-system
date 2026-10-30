import { computed, reactive } from 'vue'
import type { DynamicDefinition, DynamicInterpretation } from '../types/dynamic-groups'

/** One accepted interpretation belongs to one exact prompt revision. */
export function createSmartScreeningController(options: {
  interpret: (prompt: string, context?: DynamicDefinition) => Promise<DynamicInterpretation>
  apply: (definition: DynamicDefinition) => void
  validate?: (definition: DynamicDefinition) => string
  invalidate?: () => void
}) {
  const state = reactive({ prompt: '', result: null as DynamicInterpretation | null, busy: false, error: '', revision: 0, acceptedRevision: -1, acceptedPrompt: '' })
  let disposed = false
  const ready = computed(() => !disposed && !state.busy && state.result?.status === 'ready' && !!state.result.definition && state.acceptedRevision === state.revision && state.acceptedPrompt === state.prompt)
  const stale = computed(() => !!state.result && state.acceptedRevision !== state.revision)
  function invalidate() { state.revision++; state.busy = false; state.error = ''; options.invalidate?.() }
  function setPrompt(prompt: string) { if (prompt === state.prompt) return; state.prompt = prompt; invalidate() }
  function reset(prompt = '', definition?: DynamicDefinition) {
    invalidate(); state.prompt = prompt; state.result = definition ? { status: 'ready', message: '已保存的选股方案', definition, assumptions: definition.strategy?.assumptions || [], questions: [], unsupported: [], model: null } : null
    state.acceptedPrompt = prompt; state.acceptedRevision = definition ? state.revision : -1
  }
  async function interpret(context?: DynamicDefinition) {
    if (disposed || state.busy) return
    if (!state.prompt.trim() || state.prompt.length > 2000) { state.error = '请填写 1 至 2000 字的选股想法'; return }
    const prompt = state.prompt, revision = ++state.revision
    state.busy = true; state.error = ''; options.invalidate?.()
    try {
      const result = await options.interpret(prompt, context)
      if (disposed || revision !== state.revision || prompt !== state.prompt) return
      if (result.status === 'ready') {
        if (!result.definition || result.questions?.length || result.unsupported?.length) throw new Error('理解结果仍有未解决的条件，请补充描述后重试')
        const invalid = options.validate?.(result.definition)
        if (invalid) throw new Error(`条件需要调整：${invalid}`)
        state.result = result; state.acceptedPrompt = prompt; state.acceptedRevision = revision
        options.apply(result.definition)
      } else { state.result = { ...result, definition: null }; state.acceptedRevision = revision; state.acceptedPrompt = prompt }
    } catch (error) {
      if (disposed || revision !== state.revision) return
      const localMessage = error instanceof Error && (error.message.startsWith('理解结果') || error.message.startsWith('条件需要调整')) ? error.message : '暂时无法理解，请保留描述并重试'
      state.error = localMessage; state.acceptedRevision = -1; state.result = null
    } finally { if (!disposed && revision === state.revision) state.busy = false }
  }
  function dispose() { disposed = true; invalidate() }
  return { state, ready, stale, setPrompt, reset, interpret, invalidate, dispose }
}
