# Temporary Notebook Design

## Goal

Add one shared temporary notebook to the Settings page. It must support text notes and images, and it must persist through the backend so different devices see the same content.

## Scope

- Add a single "临时记录本" card to `frontend/src/views/Settings.vue`.
- The card contains one editable note box.
- Users can type text, paste images with `Ctrl+V`, and add images through one upload button.
- The notebook is shared by the existing single-user backend configuration.
- Keep the feature focused on one shared note, with no note list, history, categories, or per-device local storage.

## Persistence

Use the existing `/api/v1/config` GET and PUT flow. Store the notebook HTML under:

```json
{
  "custom_settings": {
    "temporary_notebook": "<p>盘中备注</p>"
  }
}
```

This reuses the existing `user_configs.custom_settings` JSON column and avoids adding a new table or API route for a one-box feature. Existing custom settings must be preserved when the notebook is saved.

## Frontend Behavior

- On Settings page load, read `config.custom_settings.temporary_notebook` and render it into the editable box.
- The editable area uses `contenteditable` so text and images can coexist in one box.
- Pasted image files are converted to base64 image data URLs and inserted at the cursor.
- Uploaded image files use the same insertion path as pasted images.
- Non-image uploads are rejected with an Element Plus warning.
- Single image files are limited to 2 MB before base64 conversion.
- Changes auto-save to the backend after a short debounce.
- A small "清空" button clears the whole note and saves the empty state.

## Backend Behavior

- Extend the config schema/store typing so `custom_settings` is returned and accepted by the frontend.
- Keep notebook content inside `custom_settings`; no backend file upload or image storage service is added.
- Preserve other existing configuration fields when saving the notebook.

## Error Handling

- If loading config fails, keep the existing Settings behavior and leave the notebook empty.
- If saving fails, show `ElMessage.error('记录本保存失败')`.
- If an image is too large, show `ElMessage.warning('单张图片不能超过 2MB')`.
- If a selected or pasted file is not an image, show `ElMessage.warning('只能添加图片文件')`.

## Testing

- Add a backend config API test proving `custom_settings.temporary_notebook` can be saved and read back.
- Add a frontend source-level test proving Settings exposes the temporary notebook UI, paste/upload handlers, backend save logic, and does not use `localStorage` for notebook persistence.
- Run the focused frontend and backend tests after implementation.
