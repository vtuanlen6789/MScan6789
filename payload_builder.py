from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from config import DATA_SOURCE


OVERVIEW_COLS = [
    "Pair", "Trust", "Clarity", "Cycle", "CycleState", "Entry",
    "ConflictScore", "ConflictLevel", "Core", "Mode", "Summary", "Focus"
]

TECH_COLS = [
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


def _extract_rows(rows: List[Dict[str, Any]], columns: List[str]) -> List[Dict[str, Any]]:
    extracted: List[Dict[str, Any]] = []
    for row in rows:
        extracted.append({col: row.get(col) for col in columns})
    return extracted


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_scan_payload(
    results: List[Dict[str, Any]],
    opportunity_ranked: Optional[List[Dict[str, Any]]] = None,
    opportunity_top3: Optional[List[Dict[str, Any]]] = None,
    currency_strength_table: Optional[List[Dict[str, Any]]] = None,
    smc_analysis: Optional[List[Dict[str, Any]]] = None,
    focus_ranking: Optional[List[Dict[str, Any]]] = None,
    focus_top3: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    payload = {
        "generatedAt": _now_iso(),
        "source": DATA_SOURCE,
        "count": len(results),
        "results": results,
        "overview": _extract_rows(results, OVERVIEW_COLS),
        "technical": _extract_rows(results, TECH_COLS),
        "top3": results[:3],
    }

    if opportunity_ranked is not None:
        payload["opportunityRanking"] = opportunity_ranked
    if opportunity_top3 is not None:
        payload["opportunityTop3"] = opportunity_top3
    if currency_strength_table is not None:
        payload["currencyStrengthTable"] = currency_strength_table
    if smc_analysis is not None:
        payload["smcAnalysis"] = smc_analysis
    if focus_ranking is not None:
        payload["focusRanking"] = focus_ranking
    if focus_top3 is not None:
        payload["focusTop3"] = focus_top3

    return payload
