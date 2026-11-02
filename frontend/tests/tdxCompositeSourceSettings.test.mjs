import assert from 'node:assert/strict'
import { existsSync, readFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { resolve } from 'node:path'
import test from 'node:test'
import { compileScript, parse } from '@vue/compiler-sfc'
import ts from 'typescript'

const require = createRequire(import.meta.url)
const vue = require('vue')
const componentPath = resolve(import.meta.dirname, '../src/components/tdx/TdxCompositeSourceSettings.vue')
const helperPath = resolve(import.meta.dirname, '../src/utils/tdxCompositeSources.ts')

function evaluate(code, dependencies = {}) {
  const output = ts.transpileModule(code, {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 }
  }).outputText
  const exports = {}
  new Function('require', 'exports', output)(name => dependencies[name] ?? require(name), exports)
  return exports
}

function eventTarget() {
  const listeners = new Map()
  return {
    activeElement: null,
    innerWidth: 220,
    innerHeight: 360,
    addEventListener(type, handler) {
      if (!listeners.has(type)) listeners.set(type, new Set())
      listeners.get(type).add(handler)
    },
    removeEventListener(type, handler) { listeners.get(type)?.delete(handler) },
    dispatch(type, event) { for (const handler of listeners.get(type) ?? []) handler(event) },
    listenerCount() { return [...listeners.values()].reduce((sum, set) => sum + set.size, 0) }
  }
}

function mountSettings(component, model, doc) {
  const makeNode = type => vue.markRaw({
    type, props: {}, children: [], parent: null, text: '',
    focus() { doc.activeElement = this },
    contains(target) { for (let node = target; node; node = node.parent) if (node === this) return true; return false },
    querySelector(selector) { return walk(this, node => node.type === selector)[0] ?? null },
    getBoundingClientRect() { return this.type === 'button' ? { left: 180, right: 204, top: 20, bottom: 44, width: 24, height: 24 } : { width: 176, height: 240 } }
  })
  const renderer = vue.createRenderer({
    createElement: makeNode,
    createText: text => Object.assign(makeNode('#text'), { text }),
    createComment: text => Object.assign(makeNode('#comment'), { text }),
    setText: (node, text) => { node.text = text },
    setElementText: (node, text) => { node.text = text; node.children = [] },
    patchProp: (node, key, _previous, value) => { node.props[key] = value },
    insert(node, parent, anchor = null) {
      if (node.parent) node.parent.children.splice(node.parent.children.indexOf(node), 1)
      const index = anchor ? parent.children.indexOf(anchor) : -1
      parent.children.splice(index < 0 ? parent.children.length : index, 0, node)
      node.parent = parent
    },
    remove(node) { node.parent?.children.splice(node.parent.children.indexOf(node), 1); node.parent = null },
    parentNode: node => node.parent,
    nextSibling: node => node.parent?.children[node.parent.children.indexOf(node) + 1] ?? null
  })
  const updates = []
  const app = renderer.createApp({
    setup() {
      return () => vue.h(component, { modelValue: model.value, 'onUpdate:modelValue': next => { updates.push(next); model.value = next } })
    }
  })
  const root = makeNode('root')
  app.mount(root)
  return { root, app, updates }
}

function walk(root, predicate) {
  return [root, ...root.children.flatMap(child => walk(child, () => true))].filter(predicate)
}

test('settings clicks edit one source immutably, and Escape or outside clicks dismiss the compact panel', async () => {
  assert.ok(existsSync(componentPath), 'compact source settings component should exist')
  const helpers = evaluate(readFileSync(helperPath, 'utf8'))
  const { descriptor } = parse(readFileSync(componentPath, 'utf8'), { filename: componentPath })
  const compiled = compileScript(descriptor, { id: 'composite-source-settings-test', inlineTemplate: true })
  const component = evaluate(compiled.content, { '@/utils/tdxCompositeSources': helpers }).default
  const doc = eventTarget()
  const win = eventTarget()
  const originalDocument = globalThis.document
  const originalWindow = globalThis.window
  globalThis.document = doc
  globalThis.window = win
  const initial = { limit_up: true, cls: true, ths: false, jygs: true, stcn: true, glh: true, other: true }
  const model = vue.ref(initial)
  let mounted
  try {
    mounted = mountSettings(component, model, doc)
    const button = walk(mounted.root, node => node.type === 'button')[0]
    assert.equal(walk(mounted.root, node => node.type === 'input').length, 0)
    assert.equal(button.props['aria-expanded'], false)
    await button.props.onClick()
    await vue.nextTick()
    const inputs = walk(mounted.root, node => node.type === 'input')
    assert.equal(inputs.length, 7)
    assert.equal(inputs[1].props.checked, true)
    assert.equal(inputs[2].props.checked, false)
    assert.equal(button.props['aria-expanded'], true)
    assert.equal(doc.activeElement, inputs[0])
    inputs[1].props.onChange({ target: { checked: false } })
    await vue.nextTick()
    assert.equal(initial.cls, true)
    assert.equal(mounted.updates.length, 1)
    assert.deepEqual(mounted.updates[0], { ...initial, cls: false })
    assert.equal(inputs[1].props.checked, false)
    doc.dispatch('keydown', { key: 'Escape', preventDefault() {} })
    await vue.nextTick()
    assert.equal(walk(mounted.root, node => node.type === 'input').length, 0)
    assert.equal(doc.activeElement, button)
    await button.props.onClick()
    await vue.nextTick()
    doc.dispatch('click', { target: walk(mounted.root, node => node.type === 'input')[0] })
    await vue.nextTick()
    assert.equal(button.props['aria-expanded'], true)
    doc.dispatch('click', { target: {} })
    await vue.nextTick()
    assert.equal(walk(mounted.root, node => node.type === 'input').length, 0)
  } finally {
    mounted?.app.unmount()
    globalThis.document = originalDocument
    globalThis.window = originalWindow
  }
  assert.equal(doc.listenerCount(), 0)
  assert.equal(win.listenerCount(), 0)
})
