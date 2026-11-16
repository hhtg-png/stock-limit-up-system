import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { createRouter, createMemoryHistory } from 'vue-router'
import ts from 'typescript'

const root = resolve(import.meta.dirname, '..')
const source = readFileSync(resolve(root, 'src/router/index.ts'), 'utf8')
const ast = ts.createSourceFile('router.ts', source, ts.ScriptTarget.ES2022, true)
const script = ast.statements
  .filter(node => !ts.isImportDeclaration(node) && !ts.isExportAssignment(node))
  .map(node => node.getText(ast)).join('\n')
const code = ts.transpileModule(script, {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 }
}).outputText.replace(/import\((['"])(@\/views\/[^'"]+)\1\)/g, (_match, _quote, file) => `loadView(${JSON.stringify(file)})`)

test('home menu and direct monitor route load the same calendar-enabled stock monitor', async () => {
  const modules = new Map()
  const loadView = async file => {
    const absolutePath = resolve(root, 'src', file.slice(2))
    if (!modules.has(absolutePath)) {
      modules.set(absolutePath, { default: { __file: absolutePath, source: readFileSync(absolutePath, 'utf8'), render() {} } })
    }
    return modules.get(absolutePath)
  }
  const router = new Function('createRouter', 'createWebHistory', 'loadView', 'document', `${code}\nreturn router`)(
    createRouter, createMemoryHistory, loadView, { title: '' }
  )

  await router.push('/')
  const home = router.currentRoute.value
  const homeComponent = home.matched.at(-1).components.default
  assert.equal(homeComponent.__file, resolve(root, 'src/views/LimitUpList.vue'))
  assert.equal(home.name, 'Dashboard')
  assert.equal(home.path, '/')
  assert.equal(home.meta.title, '主看板')

  await router.push('/limit-up')
  const direct = router.currentRoute.value
  assert.equal(direct.matched.at(-1).components.default, homeComponent)
  assert.equal(direct.name, 'LimitUpList')
  assert.equal(direct.path, '/limit-up')
  assert.match(homeComponent.source, /:disabled-date="isDateDisabled"/)
  assert.match(homeComponent.source, /useTradingCalendar/)
})
