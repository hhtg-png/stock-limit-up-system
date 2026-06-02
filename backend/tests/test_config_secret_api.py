import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.api.v1.config import router as config_router
from app.config import settings
from app.database import Base, get_db


class ConfigSecretApiTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_async_engine(
            "sqlite+aiosqlite://",
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        self.Session = async_sessionmaker(self.engine, expire_on_commit=False)
        asyncio.run(self._create_schema())
        self.temp_dir = tempfile.TemporaryDirectory()
        self.env_path = Path(self.temp_dir.name) / ".env"
        self.secret_keys = ("DEEPSEEK_API_KEY", "DEEPSEEK_BASE_URL", "DEEPSEEK_MODEL")
        self.original_settings = {key: getattr(settings, key) for key in self.secret_keys}
        self.original_environ = {key: os.environ.get(key) for key in self.secret_keys}

        app = FastAPI()
        app.include_router(config_router, prefix="/config")

        async def override_get_db():
            async with self.Session() as session:
                yield session

        app.dependency_overrides[get_db] = override_get_db
        self.client = TestClient(app)

    def tearDown(self):
        for key, value in self.original_settings.items():
            setattr(settings, key, value)
        for key, value in self.original_environ.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        try:
            from app.services.intelligence_service import intelligence_service

            intelligence_service.summary_client.api_key = settings.DEEPSEEK_API_KEY
            intelligence_service.summary_client.base_url = settings.DEEPSEEK_BASE_URL.rstrip("/")
            intelligence_service.summary_client.model = settings.DEEPSEEK_MODEL
        except Exception:
            pass
        self.client.close()
        self.temp_dir.cleanup()
        asyncio.run(self.engine.dispose())

    async def _create_schema(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    def test_deepseek_key_is_write_only_and_saved_to_env(self):
        with patch("app.api.v1.config.ENV_FILE_PATH", self.env_path):
            response = self.client.put("/config", json={"deepseek_api_key": "test-secret-value"})

            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertTrue(payload["deepseek_api_key_configured"])
            self.assertNotIn("deepseek_api_key", payload)
            self.assertIn("DEEPSEEK_API_KEY=test-secret-value", self.env_path.read_text(encoding="utf-8"))

            loaded = self.client.get("/config")

        self.assertEqual(loaded.status_code, 200)
        loaded_payload = loaded.json()
        self.assertTrue(loaded_payload["deepseek_api_key_configured"])
        self.assertNotIn("deepseek_api_key", loaded_payload)

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


if __name__ == "__main__":
    unittest.main()
