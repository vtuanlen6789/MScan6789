import streamlit as st
import pandas as pd
import os

from main import run_scanner, run_opportunity_scanner, run_currency_strength_table, run_smc_scanner
from data_layer import initialize_data_source, set_runtime_data_source, get_runtime_data_source_context
from engines.indicator_scan_engine import run_indicator_scan_table
from engines.market_focus_engine import run_market_focus_engine
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

if "last_smc_analysis" not in st.session_state:
    st.session_state["last_smc_analysis"] = None

if "last_focus_ranking" not in st.session_state:
    st.session_state["last_focus_ranking"] = None

if "last_focus_top3" not in st.session_state:
    st.session_state["last_focus_top3"] = None

if "last_indicator_scan_table" not in st.session_state:
    st.session_state["last_indicator_scan_table"] = None


SMC_DISPLAY_COLS = [
    "pair",
    "h4_swing_trend",
    "h4_bos_up",
    "h4_bos_down",
    "h4_choch_bull",
    "h4_choch_bear",
    "h4_ob_bull",
    "h4_ob_bear",
    "h4_fvg_bull",
    "h4_fvg_count",
    "h4_poi_score",
    "d1_swing_trend",
    "d1_bos_up",
    "d1_bos_down",
    "d1_choch_bull",
    "d1_choch_bear",
    "entry_signal",
    "entry_dir",
    "entry_confluence",
    "killzone_active",
    "killzone_name",
]

FOCUS_DISPLAY_COLS = [
    "Pair",
    "FocusScore",
    "TrustScore",
    "OpportunityScore",
    "CurrencyStrengthScore",
    "ClarityScore",
    "SmcScore",
    "Cycle",
    "Core",
    "Entry",
    "MacroBias",
    "SmcSignalSummary",
]


def _render_smc_section(smc_analysis):
    st.markdown("## SMC Analysis (BOS / CHoCH / OB / FVG / POI / Killzone)")
    if not smc_analysis:
        st.info("No SMC data available. Run Market Scan first.")
        return
    df_smc = pd.DataFrame(smc_analysis)
    available = [c for c in SMC_DISPLAY_COLS if c in df_smc.columns]
    st.dataframe(df_smc[available], use_container_width=True)

    # Highlight pairs with active entry signal
    signal_rows = [r for r in smc_analysis if r.get("entry_signal")]
    if signal_rows:
        st.markdown("### Entry Signals")
        for row in signal_rows:
            direction = row.get("entry_dir", "")
            pair = row.get("pair", "")
            reason = row.get("entry_reason", "")
            kz = row.get("killzone_name", "—")
            score = row.get("entry_confluence", 0)
            st.success(f"**{pair}** {direction} | KZ: {kz} | Confluence: {score} | {reason}")
    else:
        st.info("No active entry signals in current scan.")


def _render_dataframe_section(title, df: pd.DataFrame, columns, empty_message: str):
    st.markdown(f"## {title}")

    if df is None or df.empty:
        st.info(empty_message)
        return

    available = [col for col in columns if col in df.columns]
    if not available:
        st.info(empty_message)
        return

    st.dataframe(df[available], use_container_width=True)


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


def _format_indicator_scan_display(rows):
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    ordered_cols = [
        "Pair",
        "Timeframe",
        "RSI",
        "RSIwSMA",
        "RSIwWMA",
        "ATR",
        "ATRwSMA",
        "ATRwWMA",
    ]
    available = [col for col in ordered_cols if col in df.columns]
    return df[available]

default_mode = os.getenv("BIZCLAW_TRADING_MODE", "FAST").strip().upper()
if default_mode not in {"FAST", "STABLE"}:
    default_mode = "FAST"

default_source = os.getenv("BIZCLAW_DASHBOARD_DEFAULT_SOURCE", "yahoo").strip().lower()
if default_source not in {"yahoo", "mt5_csv", "mt5"}:
    default_source = "yahoo"

source_labels = {
    "yahoo": "Yahoo",
    "mt5_csv": "MT5 CSV",
    "mt5": "MetaTrader5 Terminal",
}

source_options = list(source_labels.keys())

selected_source = st.selectbox(
    "Data Source",
    options=source_options,
    index=source_options.index(default_source),
    format_func=lambda value: source_labels.get(value, value),
)

default_export_dir = get_runtime_data_source_context().get("exportDir", "")
selected_export_dir = default_export_dir

if selected_source == "mt5_csv":
    selected_export_dir = st.text_input(
        "MT5 CSV Export Folder",
        value=default_export_dir,
        help="Folder containing normalized MT5 CSV files such as EURUSD_M5.csv",
    ).strip() or default_export_dir

selected_mode = st.selectbox(
    "Trading Mode",
    options=["FAST", "STABLE"],
    index=0 if default_mode == "FAST" else 1,
)

st.caption(
    f"Selected source: {source_labels.get(selected_source, selected_source)}"
    + (f" | Export dir: {selected_export_dir}" if selected_source == "mt5_csv" else "")
)

if st.button("Run Market Scan"):
    set_runtime_data_source(
        source=selected_source,
        export_dir=selected_export_dir if selected_source == "mt5_csv" else None,
    )
    initialize_data_source()
    results = run_scanner(trading_mode=selected_mode)
    ranked, top3_opportunity = run_opportunity_scanner()
    currency_strength_table = run_currency_strength_table()
    smc_analysis = run_smc_scanner()
    indicator_scan_table = run_indicator_scan_table()
    focus_ranking, focus_top3 = run_market_focus_engine(
        core_results=results,
        opportunity_ranked=ranked,
        currency_strength_table=currency_strength_table,
        smc_analysis=smc_analysis,
    )
    payload = build_scan_payload(
        results, ranked, top3_opportunity,
        currency_strength_table=currency_strength_table,
        smc_analysis=smc_analysis,
        focus_ranking=focus_ranking,
        focus_top3=focus_top3,
        indicator_scan_table=indicator_scan_table,
    )
    st.session_state["last_payload"] = payload
    st.session_state["last_opportunity_ranked"] = ranked
    st.session_state["last_opportunity_top3"] = top3_opportunity
    st.session_state["last_currency_strength_table"] = currency_strength_table
    st.session_state["last_smc_analysis"] = smc_analysis
    st.session_state["last_focus_ranking"] = focus_ranking
    st.session_state["last_focus_top3"] = focus_top3
    st.session_state["last_indicator_scan_table"] = indicator_scan_table

    df = pd.DataFrame(results)
    focus_df = pd.DataFrame(focus_ranking)

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

    _render_dataframe_section(
        "Market Overview",
        df,
        overview_cols,
        "No scan rows available. Check the data source and try again.",
    )

    _render_dataframe_section(
        "Pine V1_6 Technical Matrix",
        df,
        tech_cols,
        "No technical matrix rows available. Check the data source and try again.",
    )

    _render_dataframe_section(
        "Market Focus Engine Ranking",
        focus_df,
        FOCUS_DISPLAY_COLS,
        "No focus ranking available. Run the scan again.",
    )

    currency_strength_df = _format_currency_strength_display(currency_strength_table)
    _render_dataframe_section(
        "Currency Strength (RSI | Price Change | ATR)",
        currency_strength_df,
        list(currency_strength_df.columns),
        "No currency strength data available.",
    )

    indicator_scan_df = _format_indicator_scan_display(indicator_scan_table)
    _render_dataframe_section(
        "Indicator Scan Matrix (RSI / ATR by Pair & TF)",
        indicator_scan_df,
        list(indicator_scan_df.columns),
        "No indicator scan data available.",
    )

    st.markdown("## Top Analytical Focus")

    top3 = pd.DataFrame(focus_top3) if focus_top3 else pd.DataFrame()

    if top3.empty:
        st.info("No focus candidates available from the current scan.")
    else:
        for _, row in top3.iterrows():
            st.markdown(f"### {row['Pair']} — FocusScore {row['FocusScore']}")
            st.write(f"Trust: {row['Trust']} | Clarity: {row['Clarity']} | Opportunity: {row['OpportunityScore']}")
            st.write(f"Core: {row['Core']} | Cycle: {row['Cycle']} | Entry: {row['Entry']}")
            st.write(f"Macro Bias: {row['MacroBias']} | SMC: {row['SmcSignalSummary']}")
            st.write(f"Actionable: {row['ActionableSummary']}")

    _render_smc_section(smc_analysis)

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
