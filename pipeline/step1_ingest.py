"""
Step 1 — Ingest & Clean
  - Read Amazon unshipped .txt (tab-delimited, Windows line endings)
  - Keep only Confirmed orders (shipment-status is NaN = confirmed, 'Label Cancelled' = skip)
  - Drop the 19 specified columns
"""

import pandas as pd
from pipeline.pipeline_state import PipelineState

# The 19 columns Amazon appends that we never need
COLUMNS_TO_DROP = [
    "default-ship-from-address-name",
    "default-ship-from-address-field-1",
    "default-ship-from-address-field-2",
    "default-ship-from-address-field-3",
    "default-ship-from-city",
    "default-ship-from-state",
    "default-ship-from-country",
    "default-ship-from-postal-code",
    "is-buyer-requested-cancellation",
    "buyer-requested-cancel-reason",
    "is-shipping-settings-automation-enabled",
    "ssa-carrier",
    "ssa-ship-method",
    "verge-of-cancellation",
    "verge-of-lateShipment",
    "amazon-programs",
    "delivery-preferred-time",
    "drop-off-location",
    "pallet-delivery",
]


def run(filepath: str) -> PipelineState:
    """
    Entry point for the pipeline. Reads the raw Amazon .txt file.
    Returns a PipelineState with the cleaned initial DataFrame.
    """
    print(f"\n[Step 1] Ingesting: {filepath}")

    # Amazon exports tab-separated with Windows \r\n line endings
    # encoding: utf-8 covers standard chars; errors='replace' handles
    # the occasional curly-quote or special char in delivery instructions
    df = pd.read_csv(
        filepath,
        sep="\t",
        encoding="utf-8",
        encoding_errors="replace",
        on_bad_lines="skip",   # skip any malformed rows (rare but happens)
        dtype=str,             # load everything as string — we control types
    )

    # Strip Windows carriage return from column names (common in these exports)
    df.columns = df.columns.str.strip().str.replace("\r", "", regex=False)

    rows_raw = len(df)
    print(f"  Raw rows loaded: {rows_raw}")
    print(f"  Columns found:   {len(df.columns)}")

    # ── Filter: keep Confirmed orders only ──────────────────────────────────
    # Confirmed = shipment-status is blank/NaN
    # Cancelled = shipment-status == 'Label Cancelled'
    if "shipment-status" not in df.columns:
        raise ValueError("Column 'shipment-status' not found. Is this the right file?")

    cancelled_mask = df["shipment-status"].str.strip().str.lower() == "label cancelled"
    cancelled_count = cancelled_mask.sum()
    df = df[~cancelled_mask].copy()
    print(f"  Confirmed: {len(df)}  |  Cancelled (dropped): {cancelled_count}")

    # ── Drop the 19 unnecessary columns ────────────────────────────────────
    cols_present = [c for c in COLUMNS_TO_DROP if c in df.columns]
    cols_missing = [c for c in COLUMNS_TO_DROP if c not in df.columns]
    df = df.drop(columns=cols_present)

    if cols_missing:
        print(f"  WARNING: these drop-columns were not in file: {cols_missing}")

    # ── Reset index ─────────────────────────────────────────────────────────
    df = df.reset_index(drop=True)

    # ── Build initial state ─────────────────────────────────────────────────
    state = PipelineState(df=df, source_file=filepath)
    state.log(
        step="step1_ingest",
        rows_in=rows_raw,
        note=f"Dropped {cancelled_count} cancelled, removed {len(cols_present)} columns",
    )
    return state
