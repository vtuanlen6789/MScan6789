import os
import time
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware

from config import DATA_SOURCE, MT5_EXPORT_DIR, SUPPORTED_DATA_SOURCES
from data_layer import (
    get_runtime_data_source_context,
    initialize_data_source,
    set_runtime_data_source,
    summarize_mt5_export_dir,
)
from main import run_scanner, run_currency_strength_table, run_smc_scanner, run_opportunity_scanner
from engines.market_focus_engine import run_market_focus_engine
from payload_builder import build_scan_payload
from supabase_publisher import publish_payload_to_supabase


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

_CACHE: Dict[str, Dict[str, Any]] = {}
CACHE_TTL_SECONDS = int(os.getenv("BIZCLAW_CACHE_TTL", "180"))


def _resolve_context(source: Optional[str] = None, export_dir: Optional[str] = None) -> Dict[str, str]:
    return set_runtime_data_source(source=source, export_dir=export_dir)


def _cache_key(context: Dict[str, str]) -> str:
    return f"{context['source']}::{context['exportDir']}"


def _build_payload(source: Optional[str] = None, export_dir: Optional[str] = None) -> Dict[str, Any]:
    context = _resolve_context(source=source, export_dir=export_dir)
    initialize_data_source()
    results = run_scanner()
    opportunity_ranked, opportunity_top3 = run_opportunity_scanner()
    currency_strength_table = run_currency_strength_table()
    smc_analysis = run_smc_scanner()
    focus_ranking, focus_top3 = run_market_focus_engine(
        core_results=results,
        opportunity_ranked=opportunity_ranked,
        currency_strength_table=currency_strength_table,
        smc_analysis=smc_analysis,
    )
    payload = build_scan_payload(
        results,
        opportunity_ranked=opportunity_ranked,
        opportunity_top3=opportunity_top3,
        currency_strength_table=currency_strength_table,
        smc_analysis=smc_analysis,
        focus_ranking=focus_ranking,
        focus_top3=focus_top3,
    )
    payload["source"] = context["source"]
    payload["exportDir"] = context["exportDir"]
    if context["source"] == "mt5_csv":
        payload["mt5ExportStatus"] = summarize_mt5_export_dir(context["exportDir"])
    return jsonable_encoder(payload)


def _get_payload(refresh: bool, source: Optional[str] = None, export_dir: Optional[str] = None) -> Dict[str, Any]:
    context = _resolve_context(source=source, export_dir=export_dir)
    key = _cache_key(context)
    now = time.time()
    cached_entry = _CACHE.get(key)
    if not refresh and cached_entry is not None and (now - cached_entry["at"]) <= CACHE_TTL_SECONDS:
        payload = dict(cached_entry["payload"])
        payload["cached"] = True
        return payload

    payload = _build_payload(source=context["source"], export_dir=context["exportDir"])
    _CACHE[key] = {
        "at": now,
        "payload": payload,
    }

    payload = dict(payload)
    payload["cached"] = False
    return payload


@app.get("/health")
def health() -> Dict[str, Any]:
    runtime = get_runtime_data_source_context()
    return {
        "status": "ok",
        "source": runtime["source"],
        "defaultSource": DATA_SOURCE,
        "defaultMt5ExportDir": MT5_EXPORT_DIR,
        "supportedSources": sorted(SUPPORTED_DATA_SOURCES),
        "cacheTtlSeconds": CACHE_TTL_SECONDS,
    }


@app.get("/scan")
def scan(
    refresh: bool = Query(default=False),
    source: Optional[str] = Query(default=None),
    export_dir: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    return _get_payload(refresh=refresh, source=source, export_dir=export_dir)


@app.get("/data-source/status")
def data_source_status(
    source: Optional[str] = Query(default=None),
    export_dir: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    context = _resolve_context(source=source, export_dir=export_dir)
    response: Dict[str, Any] = {
        "source": context["source"],
        "defaultSource": DATA_SOURCE,
        "exportDir": context["exportDir"],
        "supportedSources": sorted(SUPPORTED_DATA_SOURCES),
    }

    if context["source"] == "mt5_csv":
        response["mt5ExportStatus"] = summarize_mt5_export_dir(context["exportDir"])

    return response


@app.get("/scan/publish")
def scan_and_publish(
    refresh: bool = Query(default=True),
    source: Optional[str] = Query(default=None),
    export_dir: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    payload = _get_payload(refresh=refresh, source=source, export_dir=export_dir)
    payload_to_publish = dict(payload)
    payload_to_publish.pop("cached", None)

    try:
        public_url, object_path = publish_payload_to_supabase(payload_to_publish)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Publish failed: {exc}") from exc

    response = dict(payload)
    response["published"] = True
    response["publicJsonUrl"] = public_url
    response["objectPath"] = object_path
    return response


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=False,
    )
