import streamlit as st
import pandas as pd

from main import run_scanner
from data_layer import initialize_data_source

st.set_page_config(layout="wide")
st.title("🔎 BizClaw – Analytical Edition")

if st.button("Run Market Scan"):
    initialize_data_source()
    results = run_scanner()

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
