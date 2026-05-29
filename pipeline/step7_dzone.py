"""
Step 7 — D-Zone Check
  - Load D-zone reference (postcode prefixes + city names)
  - Flag and remove any orders where ship-postal-code or ship-city is in D-zone
  - Writes dropped D-zone orders to a separate file for review
"""

import os
import pandas as pd
from pipeline.pipeline_state import PipelineState

DZONE_DROPPED_FILE = "output/dropped_dzone_orders.csv"


def _load_dzone_reference(filepath: str) -> tuple[set, set]:
    """
    Load D-zone reference from a two-sheet Excel file:
      Sheet "ALL CITIES"            → column ALL → city names
      Sheet "Postal Codes (Zone D)" → column ALL → outward postcode prefixes

    Returns:
        (dzone_postcodes: set of uppercase outward code prefixes,
         dzone_cities:    set of uppercase city names)
    """
    xl = pd.ExcelFile(filepath)

    def _read_sheet(sheet_name: str) -> set:
        if sheet_name not in xl.sheet_names:
            return set()
        df = xl.parse(sheet_name, dtype=str)
        # Column is named ALL in both sheets
        col = next((c for c in df.columns if str(c).strip().upper() == "ALL"), None)
        if col is None:
            return set()
        values = set(df[col].dropna().str.strip().str.upper())
        values.discard("")
        return values

    dzone_postcodes = _read_sheet("Postal Codes (Zone D)")
    dzone_cities    = _read_sheet("ALL CITIES")

    return dzone_postcodes, dzone_cities


def run(state: PipelineState, dzone_reference_filepath: str = None) -> PipelineState:
    """
    Removes orders shipping to D-zone areas.

    Args:
        state:                    current PipelineState
        dzone_reference_filepath: path to D-zone CSV/XLSX with postcode prefixes
                                  and/or city names. If None, step is skipped.
    """
    print("\n[Step 7] D-zone check")
    rows_in = len(state.df)

    if not dzone_reference_filepath or not os.path.exists(dzone_reference_filepath):
        print("  No D-zone reference file provided — skipping D-zone check.")
        print("  ⚠  WARNING: D-zone orders may be included in output!")
        state.log(
            step="step7_dzone",
            rows_in=rows_in,
            note="Skipped — no D-zone reference file provided",
        )
        return state

    # ── Load reference ────────────────────────────────────────────────────────
    try:
        dzone_postcodes, dzone_cities = _load_dzone_reference(dzone_reference_filepath)
        print(f"  Loaded {len(dzone_postcodes)} D-zone postcode prefixes, "
              f"{len(dzone_cities)} D-zone cities")
    except Exception as e:
        print(f"  WARNING: Could not load D-zone reference ({e}) — skipping.")
        state.log(
            step="step7_dzone",
            rows_in=rows_in,
            note=f"Skipped — failed to load D-zone file: {e}",
        )
        return state

    # ── Build masks ───────────────────────────────────────────────────────────
    postcode_mask = pd.Series(False, index=state.df.index)
    city_mask     = pd.Series(False, index=state.df.index)

    if "ship-postal-code-outward" in state.df.columns and dzone_postcodes:
        postcode_mask = state.df["ship-postal-code-outward"].str.strip().str.upper().isin(dzone_postcodes)

    if "ship-city" in state.df.columns and dzone_cities:
        city_mask = state.df["ship-city"].str.strip().str.upper().isin(dzone_cities)

    dzone_mask = postcode_mask | city_mask
    dropped = state.df[dzone_mask].copy()

    if dropped.empty:
        print("  No D-zone orders found — all orders are deliverable.")
    else:
        print(f"\n  ✗  {len(dropped)} D-zone order(s) removed:")
        for _, row in dropped.iterrows():
            pc   = row.get("ship-postal-code", "")
            city = row.get("ship-city", "")
            oid  = row.get("order-id", "")
            reason = "postcode" if postcode_mask.loc[row.name] else "city"
            print(f"    [{reason}]  order={oid}  postcode={pc}  city={city}")

        # Save dropped orders for review
        os.makedirs("output", exist_ok=True)
        dropped.to_csv(DZONE_DROPPED_FILE, index=False)
        print(f"\n  Dropped D-zone orders saved to: {DZONE_DROPPED_FILE}")

    state.df = state.df[~dzone_mask].copy().reset_index(drop=True)

    state.log(
        step="step7_dzone",
        rows_in=rows_in,
        note=f"Removed {len(dropped)} D-zone orders",
    )
    return state
