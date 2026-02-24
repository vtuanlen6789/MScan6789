import streamlit as st
import pandas as pd
import os

from main import run_scanner
from data_layer import initialize_data_source
from payload_builder import build_scan_payload
from supabase_publisher import publish_payload_to_supabase

st.set_page_config(layout="wide")
st.title("🔎 BizClaw – Analytical Edition")

if "last_payload" not in st.session_state:
    st.session_state["last_payload"] = None

if "last_public_json_url" not in st.session_state:
    st.session_state["last_public_json_url"] = None

if st.button("Run Market Scan"):
    initialize_data_source()
    results = run_scanner()
    payload = build_scan_payload(results)
    st.session_state["last_payload"] = payload

    df = pd.DataFrame(results)

    overview_cols = [
        "Pair", "Trust", "Clarity", "Cycle", "CycleState", "Entry",
        "ConflictScore", "ConflictLevel", "Core", "Mode", "Summary", "Focus"
    ]
    tech_cols = [
        "Pair",
        "M5_Dir", "M5_Hist", "M5_Drv", "M5_State", "M5_Score",
        "M30_Dir", "M30_Hist", "M30_Drv", "M30_State", "M30_Score",
        "H4_Dir", "H4_Hist", "H4_Drv", "H4_State", "H4_Score",
        "D1_Dir", "D1_Hist", "D1_Drv", "D1_State", "D1_Score",
        "ConflictScore", "ConflictLevel", "Driver", "M30_Memory", "H4_Memory", "D1_Memory"
    ]

    st.markdown("## Market Overview")
    st.dataframe(df[overview_cols], use_container_width=True)

    st.markdown("## Pine V1_4 Technical Matrix")
    st.dataframe(df[tech_cols], use_container_width=True)

    st.markdown("## Top Analytical Focus")

    top3 = df.head(3)

    for _, row in top3.iterrows():
        st.markdown(f"### {row['Pair']}")
        st.write(f"Trust: {row['Trust']} | Clarity: {row['Clarity']}")
        st.write(f"Core: {row['Core']} | Mode: {row['Mode']}")
        st.write(f"Summary: {row['Summary']}")
        st.write(f"Focus: {row['Focus']}")

    auto_publish = os.getenv("BIZCLAW_AUTO_PUBLISH_SUPABASE", "false").strip().lower() in {"1", "true", "yes", "on"}
    if auto_publish:
        try:
            public_url, object_path = publish_payload_to_supabase(payload)
            st.session_state["last_public_json_url"] = public_url
            st.success(f"Auto published JSON to Supabase: {object_path}")
            st.code(public_url)
        except Exception as exc:
            st.error(f"Auto publish failed: {exc}")

if st.session_state.get("last_payload") is not None:
    st.markdown("---")
    st.markdown("## JSON Publishing")

    col1, col2 = st.columns([1, 2])
    with col1:
        if st.button("Publish latest scan JSON to Supabase"):
            try:
                public_url, object_path = publish_payload_to_supabase(st.session_state["last_payload"])
                st.session_state["last_public_json_url"] = public_url
                st.success(f"Published: {object_path}")
            except Exception as exc:
                st.error(f"Publish failed: {exc}")

    with col2:
        last_url = st.session_state.get("last_public_json_url")
        if last_url:
            st.write("Public JSON URL")
            st.code(last_url)
