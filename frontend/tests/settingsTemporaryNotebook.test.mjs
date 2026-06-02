import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')
const settings = readFileSync(resolve(root, 'src/views/Settings.vue'), 'utf8')
const configStore = readFileSync(resolve(root, 'src/stores/config.ts'), 'utf8')

assert.match(settings, /临时记录本/, 'Settings should expose a temporary notebook card')
assert.match(settings, /notebook-editor/, 'Settings should render one editable notebook box')
assert.match(settings, /contenteditable="true"/, 'Notebook should allow mixed text and images in one editable box')
assert.match(settings, /handleNotebookPaste/, 'Notebook should handle pasted images')
assert.match(settings, /handleNotebookFileChange/, 'Notebook should handle uploaded images')
assert.match(settings, /insertNotebookImage/, 'Notebook should insert image data URLs into the editor')
assert.match(settings, /temporary_notebook/, 'Notebook should persist under custom_settings.temporary_notebook')
assert.match(settings, /saveTemporaryNotebook/, 'Notebook should save through the backend config API')
assert.match(settings, /notebookSaveTimer/, 'Notebook should debounce backend saves')
assert.match(settings, /MAX_NOTEBOOK_IMAGE_SIZE/, 'Notebook should enforce a per-image size limit')
assert.match(settings, /单张图片不能超过 2MB/, 'Notebook should warn on oversized images')
assert.match(settings, /只能添加图片文件/, 'Notebook should warn on non-image files')
assert.match(settings, /清空/, 'Notebook should provide a small clear action')
assert.doesNotMatch(settings, /localStorage/, 'Notebook should not use localStorage for persistence')

assert.match(configStore, /custom_settings/, 'Config store should type and default custom_settings')

console.log('settings temporary notebook checks passed')
