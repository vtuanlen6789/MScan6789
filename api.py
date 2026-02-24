import os
import time
from typing import Any, Dict, List

from fastapi import FastAPI, Query
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware

from config import DATA_SOURCE
from data_layer import initialize_data_source
from main import run_scanner
from payload_builder import build_scan_payload


app = FastAPI(title="BizClaw API", version="1.0.0")

cors_raw = os.getenv("BIZCLAW_CORS_ORIGINS", "*").strip()
if cors_raw == "*":
    allow_origins = ["*"]
else:
    allow_origins = [x.strip() for x in cors_raw.split(",") if x.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

_CACHE: Dict[str, Any] = {
    "at": 0.0,
    "payload": None,
}
CACHE_TTL_SECONDS = int(os.getenv("BIZCLAW_CACHE_TTL", "180"))


def _build_payload() -> Dict[str, Any]:
    initialize_data_source()
    results = run_scanner()
    payload = build_scan_payload(results)
    return jsonable_encoder(payload)


def _get_payload(refresh: bool) -> Dict[str, Any]:
    now = time.time()
    if not refresh and _CACHE["payload"] is not None and (now - _CACHE["at"]) <= CACHE_TTL_SECONDS:
        payload = dict(_CACHE["payload"])
        payload["cached"] = True
        return payload

    payload = _build_payload()
    _CACHE["at"] = now
    _CACHE["payload"] = payload

    payload = dict(payload)
    payload["cached"] = False
    return payload


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "source": DATA_SOURCE,
        "cacheTtlSeconds": CACHE_TTL_SECONDS,
    }


@app.get("/scan")
def scan(refresh: bool = Query(default=False)) -> Dict[str, Any]:
    return _get_payload(refresh=refresh)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=False,
    )
