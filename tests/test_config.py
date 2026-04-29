import importlib
import json
import sys
from pathlib import Path


SERVICE_ACCOUNT = {
    "type": "service_account",
    "project_id": "automated-funding",
    "private_key_id": "test-key-id",
    "private_key": "-----BEGIN PRIVATE KEY-----\nTEST\n-----END PRIVATE KEY-----\n",
    "client_email": "svc@example.iam.gserviceaccount.com",
    "client_id": "1234567890",
    "token_uri": "https://oauth2.googleapis.com/token",
}


def _load_config_module(monkeypatch, *, raw_json=None, file_value=None):
    monkeypatch.setenv("GOOGLE_SHEET_ID", "sheet-id")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("GCP_SERVICE_ACCOUNT_JSON", raising=False)
    monkeypatch.delenv("GCP_SERVICE_ACCOUNT_FILE", raising=False)

    if raw_json is not None:
        monkeypatch.setenv("GCP_SERVICE_ACCOUNT_JSON", raw_json)
    if file_value is not None:
        monkeypatch.setenv("GCP_SERVICE_ACCOUNT_FILE", file_value)

    sys.modules.pop("api.config", None)
    module = importlib.import_module("api.config")
    return importlib.reload(module)


def test_load_accepts_inline_json_in_gcp_service_account_file(monkeypatch):
    config = _load_config_module(monkeypatch, file_value=json.dumps(SERVICE_ACCOUNT))
    assert config.settings.gcp_service_account == SERVICE_ACCOUNT


def test_load_accepts_service_account_file_path(monkeypatch):
    credentials_file = Path("tests") / ".service-account-test.json"
    credentials_file.write_text(json.dumps(SERVICE_ACCOUNT), encoding="utf-8")

    try:
        config = _load_config_module(monkeypatch, file_value=str(credentials_file))
        assert config.settings.gcp_service_account == SERVICE_ACCOUNT
    finally:
        credentials_file.unlink(missing_ok=True)
