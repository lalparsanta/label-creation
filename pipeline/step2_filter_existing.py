"""
Step 2 — Filter Already-Tracked Orders
  - Load the existing unshipped/non-shipment order list (OIDS sheet, OID column)
  - Remove any order-id that already appears in that list
  - If no list file is provided, skip gracefully
"""

import os
import pandas as pd
from pipeline.pipeline_state import PipelineState

# Known sheet/column names from UNSHIPPED_ORDERS_LIST_1.xlsx
PREFERRED_SHEET   = "OIDS"
PREFERRED_ID_COL  = "OID"


def _load_order_id_set(filepath: str) -> set:
    """
    Load the set of order IDs to exclude.
    Tries OIDS sheet / OID column first, then falls back to
    generic column-name detection.
    """
    ext = os.path.splitext(filepath)[1].lower()

    if ext in (".xlsx", ".xlsm", ".xls"):
        xl = pd.ExcelFile(filepath)
        # Try preferred sheet first
        if PREFERRED_SHEET in xl.sheet_names:
            df = pd.read_excel(filepath, sheet_name=PREFERRED_SHEET, dtype=str)
        else:
            df = pd.read_excel(filepath, dtype=str)
    else:
        df = pd.read_csv(filepath, dtype=str)

    df.columns = df.columns.str.strip()

    # Try preferred column name first
    if PREFERRED_ID_COL in df.columns:
        id_col = PREFERRED_ID_COL
    else:
        # Fallback: find any column that looks like an order-id column
        candidates = ["OID", "order-id", "order_id", "orderid",
                      "ORDER-ID", "ORDER ID", "amazon-order-id"]
        id_col = next((c for c in candidates if c in df.columns), None)
        if id_col is None:
            raise ValueError(
                f"Cannot find order-id column in non-shipment list.\n"
                f"Columns: {list(df.columns)}"
            )

    ids = set(df[id_col].dropna().str.strip().str.upper())
    ids.discard("")
    return ids


def run(state: PipelineState, non_shipped_filepath: str | None = None) -> PipelineState:
    print("\n[Step 2] Filtering already-tracked orders")
    rows_in = len(state.df)

    if not non_shipped_filepath or not os.path.exists(non_shipped_filepath):
        print("  No non-shipment list provided — skipping.")
        state.log("step2_filter_existing", rows_in, "Skipped — no file provided")
        return state

    try:
        existing_ids = _load_order_id_set(non_shipped_filepath)
        print(f"  Loaded {len(existing_ids)} existing order IDs from non-shipment list")
    except Exception as e:
        print(f"  WARNING: Could not load non-shipment list ({e}) — skipping.")
        state.log("step2_filter_existing", rows_in, f"Skipped — load error: {e}")
        return state

    current_ids    = state.df["order-id"].str.strip().str.upper()
    mask_existing  = current_ids.isin(existing_ids)
    dropped_orders = state.df.loc[mask_existing, "order-id"].tolist()

    state.df = state.df[~mask_existing].copy().reset_index(drop=True)

    if dropped_orders:
        print(f"  Removed {len(dropped_orders)} already-tracked order(s):")
        for oid in dropped_orders[:10]:
            print(f"    ✗  {oid}")
        if len(dropped_orders) > 10:
            print(f"    ... and {len(dropped_orders) - 10} more")
    else:
        print("  No overlap — all orders are new.")

    state.log("step2_filter_existing", rows_in,
              f"Removed {len(dropped_orders)} already-tracked orders")
    return state
