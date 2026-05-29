"""
Step 5 — Weight Calculation
  - Drop SDC supplier rows
  - Look up weight from supplier-specific reference files:
      PNC / BTC → PNC___BTC_WEIGHT_FILE.xlsx  (UID col, Weight in Kg col — direct match)
      DFF       → DFF_WEIGHT_FILE.xlsx         (UID col, UOM string col — needs parsing)
      RW / AB / BW → default 1 kg (no weight data provided)
  - exact_quantity  = quantity-purchased * pack
  - total_weight    = weight * exact_quantity
"""

import os
import re
import pandas as pd
from pipeline.pipeline_state import PipelineState

DROP_SUPPLIERS      = {"SDC"}
NO_WEIGHT_SUPPLIERS = {"RW", "AB", "BW"}
DEFAULT_WEIGHT_KG   = 1.0


# ── DFF weight string parser ──────────────────────────────────────────────────

def _unit_to_kg(value: float, unit: str) -> float:
    unit = unit.lower().strip()
    if unit in ("kg",):
        return round(value, 4)
    elif unit in ("g", "gr", "grams"):
        return round(value / 1000, 4)
    elif unit in ("ml", "millilitre", "milliliter"):
        return round(value / 1000, 4)   # density ~1g/ml
    elif unit in ("l", "ltr", "litre", "liter", "liters", "litres"):
        return round(value, 4)
    elif unit in ("cl",):
        return round(value / 100, 4)
    elif unit in ("lb", "lbs"):
        return round(value * 0.453592, 4)
    elif unit in ("oz",):
        return round(value * 0.0283495, 4)
    else:
        return DEFAULT_WEIGHT_KG


def _parse_dff_weight(raw: str) -> float:
    """Parse DFF UOM strings like '100g', '1.5kg', '500ml', '3 x 200ml', '4x440ml'."""
    if not isinstance(raw, str):
        return DEFAULT_WEIGHT_KG
    s = raw.strip()

    # Non-parseable qualitative strings
    qualitative = {"bar", "large", "medium", "1 bottle", "1 box", "1pk",
                   "4 pack", "1 litre", "1ltr", "1l", "1.5l", "10 capsules",
                   "14 pods", "20 tc", "5 treats"}
    if s.lower() in qualitative:
        return DEFAULT_WEIGHT_KG

    # Multiplied format A: '3 x 200ml', '10x0.05g', '12x5g'
    m = re.match(r'^(\d+(?:\.\d+)?)\s*[xX]\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)', s)
    if m:
        return round(float(m.group(1)) * _unit_to_kg(float(m.group(2)), m.group(3)), 4)

    # Multiplied format B: '200ml x 3', '5cl x 3'
    m = re.match(r'^(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*[xX]\s*(\d+)', s)
    if m:
        return round(float(m.group(3)) * _unit_to_kg(float(m.group(1)), m.group(2)), 4)

    # Standard: '100g', '1.5kg', '500ml', '30g pack'
    m = re.match(r'^(\d+(?:\.\d+)?)\s*([a-zA-Z]+)', s)
    if m:
        return _unit_to_kg(float(m.group(1)), m.group(2))

    # Bare number
    m = re.match(r'^(\d+(?:\.\d+)?)$', s)
    if m:
        return round(float(m.group(1)), 4)

    return DEFAULT_WEIGHT_KG


# ── Reference file loaders ────────────────────────────────────────────────────

def _load_pnc_btc_weights(filepath: str) -> dict:
    """Sheet: 'PNC PRODUCT FILE' | Columns: UID, Weight in Kg"""
    df = pd.read_excel(filepath, sheet_name="PNC PRODUCT FILE", dtype=str)
    df.columns = df.columns.str.strip()

    if "UID" not in df.columns or "Weight in Kg" not in df.columns:
        raise ValueError(f"Expected 'UID' and 'Weight in Kg' columns. Got: {list(df.columns)}")

    mapping = {}
    for _, row in df.iterrows():
        uid = str(row["UID"]).strip().upper()
        try:
            w = float(str(row["Weight in Kg"]).strip())
        except (ValueError, TypeError):
            w = DEFAULT_WEIGHT_KG
        if uid and uid != "NAN":
            mapping[uid] = round(w, 4)
    return mapping


def _load_dff_weights(filepath: str) -> dict:
    """Sheet: 'Sheet1' | Columns: UID, UOM size ie 250ml or 300g"""
    df = pd.read_excel(filepath, sheet_name="Sheet1", dtype=str)
    df.columns = df.columns.str.strip()

    if "UID" not in df.columns:
        raise ValueError(f"DFF weight file missing UID column. Got: {list(df.columns)}")

    weight_col = "UOM size ie 250ml or 300g"
    if weight_col not in df.columns:
        weight_col = df.columns[1]

    mapping = {}
    for _, row in df.iterrows():
        uid = str(row["UID"]).strip().upper()
        w   = _parse_dff_weight(str(row[weight_col]).strip())
        if uid and uid != "NAN":
            mapping[uid] = w
    return mapping


# ── Step runner ───────────────────────────────────────────────────────────────

def run(state: PipelineState,
        pnc_btc_weight_filepath: str | None = None,
        dff_weight_filepath: str | None = None) -> PipelineState:
    print("\n[Step 5] Weight calculation & SDC drop")
    rows_in = len(state.df)

    # Drop SDC
    sdc_mask  = state.df["supplier"].str.upper().isin(DROP_SUPPLIERS)
    sdc_count = sdc_mask.sum()
    if sdc_count > 0:
        print(f"  Dropping {sdc_count} SDC row(s):")
        for sku in state.df.loc[sdc_mask, "sku"].tolist():
            print(f"    ✗  {sku}")
        state.df = state.df[~sdc_mask].copy().reset_index(drop=True)

    # Load references
    pnc_btc_map = {}
    dff_map     = {}

    if pnc_btc_weight_filepath and os.path.exists(pnc_btc_weight_filepath):
        try:
            pnc_btc_map = _load_pnc_btc_weights(pnc_btc_weight_filepath)
            print(f"  PNC/BTC weight file: {len(pnc_btc_map):,} UIDs")
        except Exception as e:
            print(f"  WARNING: PNC/BTC weight file error ({e})")
    else:
        print("  No PNC/BTC weight file — defaulting to 1 kg for PNC/BTC")

    if dff_weight_filepath and os.path.exists(dff_weight_filepath):
        try:
            dff_map = _load_dff_weights(dff_weight_filepath)
            print(f"  DFF weight file:     {len(dff_map):,} UIDs")
        except Exception as e:
            print(f"  WARNING: DFF weight file error ({e})")
    else:
        print("  No DFF weight file — defaulting to 1 kg for DFF")

    # Weight lookup
    def get_weight(row) -> float:
        supplier = str(row.get("supplier", "")).strip().upper()
        uid      = str(row.get("uid",      "")).strip().upper()
        if supplier in NO_WEIGHT_SUPPLIERS:
            return DEFAULT_WEIGHT_KG
        if supplier == "DFF":
            return dff_map.get(uid, DEFAULT_WEIGHT_KG)
        if supplier in ("PNC", "BTC"):
            return pnc_btc_map.get(uid, DEFAULT_WEIGHT_KG)
        return DEFAULT_WEIGHT_KG

    state.df["weight"] = state.df.apply(get_weight, axis=1)

    # Quantities
    state.df["quantity-purchased"] = pd.to_numeric(
        state.df["quantity-purchased"], errors="coerce").fillna(1).astype(int)
    state.df["pack"] = pd.to_numeric(
        state.df["pack"], errors="coerce").fillna(1).astype(int)
    state.df["exact_quantity"] = state.df["quantity-purchased"] * state.df["pack"]
    state.df["total_weight"]   = (state.df["weight"] * state.df["exact_quantity"]).round(4)

    # Report
    defaulted = (state.df["weight"] == DEFAULT_WEIGHT_KG).sum()
    print(f"\n  Weight summary:")
    print(f"    Rows defaulted to 1 kg: {defaulted}")
    print(f"    Weight range: {state.df['weight'].min()} – {state.df['weight'].max()} kg")
    print(f"    Total shipment weight: {state.df['total_weight'].sum():.2f} kg")

    # Warn UIDs not found
    not_found = state.df[
        (~state.df["supplier"].str.upper().isin(NO_WEIGHT_SUPPLIERS)) &
        (state.df["weight"] == DEFAULT_WEIGHT_KG)
    ][["supplier", "uid", "sku"]].drop_duplicates()
    if not not_found.empty:
        print(f"\n  ⚠  {len(not_found)} UID(s) not found in weight refs (defaulted 1 kg):")
        for _, r in not_found.iterrows():
            print(f"    [{r['supplier']}]  uid={r['uid']}  sku={r['sku']}")

    state.log("step5_weight", rows_in,
              f"Dropped {sdc_count} SDC, {defaulted} rows defaulted to 1 kg")
    return state
