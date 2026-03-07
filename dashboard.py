import streamlit as st
import pandas as pd
import os

from main import run_scanner, run_opportunity_scanner, run_currency_strength_table
from data_layer import initialize_data_source
from payload_builder import build_scan_payload
from supabase_publisher import publish_payload_to_supabase

st.set_page_config(layout="wide")
st.title("🔎 BizClaw – Analytical Edition")

if "last_payload" not in st.session_state:
    st.session_state["last_payload"] = None

if "last_public_json_url" not in st.session_state:
    st.session_state["last_public_json_url"] = None

if "last_opportunity_ranked" not in st.session_state:
    st.session_state["last_opportunity_ranked"] = None

if "last_opportunity_top3" not in st.session_state:
    st.session_state["last_opportunity_top3"] = None

if "last_currency_strength_table" not in st.session_state:
    st.session_state["last_currency_strength_table"] = None


def _format_currency_strength_display(rows):
    if not rows:
        return pd.DataFrame()

    currencies = ["USD", "EUR", "JPY", "GBP", "CHF"]
    output = []

    for row in rows:
        display_row = {
            "TimeFrame": row.get("timeframe"),
        }

        currency_pack = row.get("currencies", {})
        for ccy in currencies:
            metrics = currency_pack.get(ccy) or {}
            rsi = metrics.get("rsi")
            pc = metrics.get("pc")
            atr = metrics.get("atr")

            if rsi is None or pc is None or atr is None:
                display_row[ccy] = "N/A"
            else:
                display_row[ccy] = f"{rsi:.1f} | {pc:.2f}% | {atr:.4f}"

        missing_pairs = row.get("missingPairs") or []
        display_row["MissingPairs"] = ", ".join(missing_pairs) if missing_pairs else ""
        output.append(display_row)

    return pd.DataFrame(output)

default_mode = os.getenv("BIZCLAW_TRADING_MODE", "FAST").strip().upper()
if default_mode not in {"FAST", "STABLE"}:
    default_mode = "FAST"

selected_mode = st.selectbox(
    "Trading Mode",
    options=["FAST", "STABLE"],
    index=0 if default_mode == "FAST" else 1,
)

if st.button("Run Market Scan"):
    initialize_data_source()
    results = run_scanner(trading_mode=selected_mode)
    ranked, top3_opportunity = run_opportunity_scanner()
    currency_strength_table = run_currency_strength_table()
    payload = build_scan_payload(results, ranked, top3_opportunity, currency_strength_table)
    st.session_state["last_payload"] = payload
    st.session_state["last_opportunity_ranked"] = ranked
    st.session_state["last_opportunity_top3"] = top3_opportunity
    st.session_state["last_currency_strength_table"] = currency_strength_table

    df = pd.DataFrame(results)

    overview_cols = [
        "Pair", "Trust", "Clarity", "Cycle", "CycleState", "Entry",
        "ConflictScore", "ConflictLevel", "Core", "Mode", "Summary", "Focus"
    ]
    tech_cols = [
        "Pair",
        "AnchorTF", "AnchorDirection", "AnchorPhase", "AnchorRiskZone",
        "FormationTF", "FormationReady", "FormationStatePrevious", "FormationState",
        "FormationBias", "FormationDrive", "SwingCount", "CompressionRatio", "FormationBars",
        "Entry", "EntryReason", "SizeFactor",
        "M5_Dir", "M5_Hist", "M5_Drv", "M5_State", "M5_Score",
        "M30_Dir", "M30_Hist", "M30_Drv", "M30_State", "M30_Score",
        "H4_Dir", "H4_Hist", "H4_Drv", "H4_State", "H4_Score",
        "D1_Dir", "D1_Hist", "D1_Drv", "D1_State", "D1_Score",
        "ConflictScore", "ConflictLevel", "Driver", "M30_Memory", "H4_Memory", "D1_Memory"
    ]

    st.markdown("## Market Overview")
    st.dataframe(df[overview_cols], use_container_width=True)

    st.markdown("## Pine V1_6 Technical Matrix")
    st.dataframe(df[tech_cols], use_container_width=True)

    st.markdown("## Currency Strength (RSI | Price Change | ATR)")
    st.dataframe(_format_currency_strength_display(currency_strength_table), use_container_width=True)

    st.markdown("## Top Analytical Focus")

    top3 = df.head(3)

    for _, row in top3.iterrows():
        st.markdown(f"### {row['Pair']}")
        st.write(f"Trust: {row['Trust']} | Clarity: {row['Clarity']}")
        st.write(f"Core: {row['Core']} | Mode: {row['Mode']}")
        st.write(f"Summary: {row['Summary']}")
        st.write(f"Focus: {row['Focus']}")

    st.markdown("## Opportunity Scanner (CAS/DS)")
    opp_cols = [
        "displaySymbol",
        "state",
        "age",
        "drive",
        "alignment",
        "score",
    ]
    if ranked:
        st.dataframe(pd.DataFrame(ranked)[opp_cols], use_container_width=True)
    else:
        st.info("No opportunity rows available.")

    st.markdown("### Top 3 After Correlation Filter")
    if top3_opportunity:
        st.dataframe(pd.DataFrame(top3_opportunity)[opp_cols], use_container_width=True)
    else:
        st.info("No filtered opportunities available.")

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
