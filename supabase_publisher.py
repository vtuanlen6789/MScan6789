import json
import os
from typing import Any, Dict, Tuple

import requests


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value


def publish_payload_to_supabase(payload: Dict[str, Any]) -> Tuple[str, str]:
    supabase_url = _required_env("SUPABASE_URL").rstrip("/")
    service_role_key = _required_env("SUPABASE_SERVICE_ROLE_KEY")
    bucket = os.getenv("SUPABASE_BUCKET", "market-data").strip() or "market-data"
    object_path = os.getenv("SUPABASE_OBJECT_PATH", "bizclaw/latest-scan.json").strip() or "bizclaw/latest-scan.json"
    upsert = os.getenv("SUPABASE_UPSERT", "true").strip().lower() in {"1", "true", "yes", "on"}

    upload_url = f"{supabase_url}/storage/v1/object/{bucket}/{object_path}"
    headers = {
        "Authorization": f"Bearer {service_role_key}",
        "apikey": service_role_key,
        "Content-Type": "application/json",
    }
    if upsert:
        headers["x-upsert"] = "true"

    response = requests.post(upload_url, headers=headers, data=json.dumps(payload), timeout=60)
    if response.status_code >= 400:
        raise RuntimeError(f"Supabase upload failed ({response.status_code}): {response.text[:200]}")

    public_url = f"{supabase_url}/storage/v1/object/public/{bucket}/{object_path}"
    return public_url, object_path
