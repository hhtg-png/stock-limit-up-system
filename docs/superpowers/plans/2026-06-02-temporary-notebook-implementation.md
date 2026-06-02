# Temporary Notebook Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add one backend-persisted shared temporary notebook to the Settings page, with text, pasted images, and uploaded images.

**Architecture:** Reuse the existing single-user `/api/v1/config` API and persist notebook HTML under `user_configs.custom_settings.temporary_notebook`. The frontend renders one `contenteditable` box, converts images to base64 data URLs, and saves the merged `custom_settings` payload through the existing config API.

**Tech Stack:** FastAPI, SQLAlchemy async, Pydantic, Vue 3 `<script setup>`, Element Plus, source-level Node tests, pytest.

---

## File Structure

- Modify `backend/app/schemas/config.py`: expose `custom_settings` on config responses.
- Modify `backend/tests/test_config_secret_api.py`: add an API test for saving and reading `custom_settings.temporary_notebook`.
- Modify `frontend/src/stores/config.ts`: add `custom_settings` to the shared config type and defaults.
- Modify `frontend/src/views/Settings.vue`: add the notebook card, editor, image paste/upload, debounced backend save, clear action, and styles.
- Create `frontend/tests/settingsTemporaryNotebook.test.mjs`: source-level checks for the notebook UI and persistence behavior.

---

### Task 1: Backend Config Persistence

**Files:**
- Modify: `backend/tests/test_config_secret_api.py`
- Modify: `backend/app/schemas/config.py`

- [ ] **Step 1: Write the failing backend test**

Add this test method to `ConfigSecretApiTests`:

```python
    def test_custom_settings_temporary_notebook_is_saved_and_returned(self):
        notebook_html = '<p>盘中备注</p><img src="data:image/png;base64,abc123">'

        response = self.client.put(
            "/config",
            json={"custom_settings": {"temporary_notebook": notebook_html, "other": "kept"}},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["custom_settings"]["temporary_notebook"], notebook_html)
        self.assertEqual(payload["custom_settings"]["other"], "kept")

        loaded = self.client.get("/config")

        self.assertEqual(loaded.status_code, 200)
        loaded_payload = loaded.json()
        self.assertEqual(loaded_payload["custom_settings"]["temporary_notebook"], notebook_html)
        self.assertEqual(loaded_payload["custom_settings"]["other"], "kept")
```

- [ ] **Step 2: Run the backend test and verify it fails**

Run from `backend/`:

```bash
python -m pytest tests/test_config_secret_api.py -q
```

Expected: FAIL because the response payload does not include `custom_settings`.

- [ ] **Step 3: Add the minimal schema field**

In `UserConfigResponse`, add:

```python
    custom_settings: Optional[Dict] = None
```

- [ ] **Step 4: Run the backend test and verify it passes**

Run from `backend/`:

```bash
python -m pytest tests/test_config_secret_api.py -q
```

Expected: PASS.

---

### Task 2: Frontend Notebook UI and Save Flow

**Files:**
- Create: `frontend/tests/settingsTemporaryNotebook.test.mjs`
- Modify: `frontend/src/stores/config.ts`
- Modify: `frontend/src/views/Settings.vue`

- [ ] **Step 1: Write the failing frontend source test**

Create `frontend/tests/settingsTemporaryNotebook.test.mjs`:

```javascript
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
```

- [ ] **Step 2: Run the frontend test and verify it fails**

Run from `frontend/`:

```bash
node tests/settingsTemporaryNotebook.test.mjs
```

Expected: FAIL because the notebook UI and handlers do not exist yet.

- [ ] **Step 3: Add frontend config typing**

In `frontend/src/stores/config.ts`, add to `UserConfig`:

```ts
  custom_settings?: Record<string, unknown>
```

Add to the default config object:

```ts
    custom_settings: {}
```

- [ ] **Step 4: Implement the Settings notebook**

In `frontend/src/views/Settings.vue`:

- Add a "临时记录本" card.
- Add one `contenteditable` editor with class `notebook-editor`.
- Add a hidden image file input and a small "添加图片" button.
- Read and write `config.custom_settings.temporary_notebook`.
- Use `FileReader.readAsDataURL` for pasted and selected image files.
- Reject non-image files and images larger than `MAX_NOTEBOOK_IMAGE_SIZE`.
- Debounce saves with `notebookSaveTimer`.
- Preserve all existing `custom_settings` keys when saving.
- Add compact responsive styles for the editor and inline images.

- [ ] **Step 5: Run the frontend test and verify it passes**

Run from `frontend/`:

```bash
node tests/settingsTemporaryNotebook.test.mjs
```

Expected: PASS.

---

### Task 3: Verification and Completion

**Files:**
- Verify all changed files.

- [ ] **Step 1: Run focused frontend tests**

Run from `frontend/`:

```bash
node tests/settingsTemporaryNotebook.test.mjs
node tests/tdxSettingsEntry.test.mjs
node tests/mobileLayout.test.mjs
```

Expected: PASS for all three tests.

- [ ] **Step 2: Run focused backend tests**

Run from `backend/`:

```bash
python -m pytest tests/test_config_secret_api.py -q
```

Expected: PASS.

- [ ] **Step 3: Run frontend type/build verification**

Run from `frontend/`:

```bash
npm run build
```

Expected: PASS.

- [ ] **Step 4: Review diff**

Run:

```bash
git status --short
git diff -- backend/app/schemas/config.py backend/tests/test_config_secret_api.py frontend/src/stores/config.ts frontend/src/views/Settings.vue frontend/tests/settingsTemporaryNotebook.test.mjs
```

Expected: only planned files changed.

- [ ] **Step 5: Commit implementation**

Run:

```bash
git add backend/app/schemas/config.py backend/tests/test_config_secret_api.py frontend/src/stores/config.ts frontend/src/views/Settings.vue frontend/tests/settingsTemporaryNotebook.test.mjs docs/superpowers/plans/2026-06-02-temporary-notebook-implementation.md
git commit -m "feat: add shared temporary notebook"
```

Expected: commit succeeds on `codex/temporary-notebook`.
