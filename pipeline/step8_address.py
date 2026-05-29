"""
Step 8 — Address Normalisation
  - Enforce 33-character limit on ship-address-1
    → If address-1 > 33 chars: check address-2
      → If address-2 is empty: move overflow to address-2
      → If address-2 has content: move address-2 to address-3, overflow to address-2
  - Merge orders with same buyer phone + address but different order-ids
    (shipping charges are one, so group them)
"""

import re
import pandas as pd
from pipeline.pipeline_state import PipelineState

MAX_ADDR_LEN = 33


def _truncate_clean(text: str) -> str:
    """Strip and normalise a single address string."""
    if not isinstance(text, str):
        return ""
    return text.strip()


def _cascade_address(row: pd.Series) -> pd.Series:
    """
    Apply the 33-char cascade rule to a single row's address fields.

    Logic:
      addr1 > 33 chars →
        if addr2 empty:
          addr2 = addr1[33:]
          addr1 = addr1[:33]
        else:
          addr3 = addr2           (push addr2 down)
          addr2 = addr1[33:]
          addr1 = addr1[:33]
    """
    a1 = _truncate_clean(row.get("ship-address-1", ""))
    a2 = _truncate_clean(row.get("ship-address-2", ""))
    a3 = _truncate_clean(row.get("ship-address-3", ""))

    if len(a1) <= MAX_ADDR_LEN:
        row["ship-address-1"] = a1
        row["ship-address-2"] = a2
        row["ship-address-3"] = a3
        return row

    # Address 1 overflows
    overflow = a1[MAX_ADDR_LEN:].strip()
    a1_trimmed = a1[:MAX_ADDR_LEN].strip()

    if not a2:
        # addr2 is empty — safe to place overflow there
        row["ship-address-1"] = a1_trimmed
        row["ship-address-2"] = overflow[:MAX_ADDR_LEN]
        row["ship-address-3"] = a3
    else:
        # addr2 has content — push down
        row["ship-address-3"] = a2[:MAX_ADDR_LEN]   # addr2 → addr3 (truncated if needed)
        row["ship-address-2"] = overflow[:MAX_ADDR_LEN]
        row["ship-address-1"] = a1_trimmed

    return row


def _build_address_key(row: pd.Series) -> str:
    """
    Build a normalised key from buyer phone + address for same-buyer detection.
    """
    phone = str(row.get("buyer-phone-number", "")).strip().upper()
    a1    = str(row.get("ship-address-1", "")).strip().upper()
    a2    = str(row.get("ship-address-2", "")).strip().upper()
    city  = str(row.get("ship-city", "")).strip().upper()
    pc    = str(row.get("ship-postal-code", "")).strip().upper()
    return f"{phone}|{a1}|{a2}|{city}|{pc}"


def _merge_same_buyer_orders(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Find rows with identical buyer phone + address but different order-ids.
    Merge them: concatenate order-ids (comma-separated in order-id field),
    sum quantities and weights, keep first row's other fields.

    Returns (merged_df, number_of_merge_groups)
    """
    df = df.copy()
    df["_addr_key"] = df.apply(_build_address_key, axis=1)

    # Only meaningful if phone is actually present
    df["_has_phone"] = df["buyer-phone-number"].astype(str).str.strip().ne("") & \
                       df["buyer-phone-number"].astype(str).str.strip().ne("nan")

    # Only merge groups where phone is known
    mergeable = df[df["_has_phone"]].copy()
    not_mergeable = df[~df["_has_phone"]].copy()

    key_counts = mergeable["_addr_key"].value_counts()
    merge_keys = key_counts[key_counts > 1].index

    if len(merge_keys) == 0:
        df = df.drop(columns=["_addr_key", "_has_phone"])
        return df.reset_index(drop=True), 0

    print(f"\n  Found {len(merge_keys)} same-buyer group(s) to merge:")

    merged_rows = []
    non_merge_df = mergeable[~mergeable["_addr_key"].isin(merge_keys)].copy()

    for key in merge_keys:
        group = mergeable[mergeable["_addr_key"] == key].copy()
        order_ids = ", ".join(group["order-id"].astype(str).tolist())
        print(f"    Merging {len(group)} orders → {order_ids}")

        base = group.iloc[0].copy()
        base["order-id"] = order_ids  # concatenate all order IDs

        for col in ["quantity-purchased", "exact_quantity", "total_weight"]:
            if col in group.columns:
                base[col] = pd.to_numeric(group[col], errors="coerce").fillna(0).sum()

        merged_rows.append(base)

    parts = [non_merge_df, not_mergeable]
    if merged_rows:
        parts.append(pd.DataFrame(merged_rows))

    result = pd.concat(parts, ignore_index=True)
    result = result.drop(columns=["_addr_key", "_has_phone"], errors="ignore")
    return result.reset_index(drop=True), len(merge_keys)


# ── Step runner ───────────────────────────────────────────────────────────────

def run(state: PipelineState) -> PipelineState:
    """
    Normalises addresses to 33-char limit and merges same-buyer orders.
    """
    print("\n[Step 8] Address normalisation & same-buyer merge")
    rows_in = len(state.df)

    # ── 1. Address cascade ───────────────────────────────────────────────────
    addr_cols = ["ship-address-1", "ship-address-2", "ship-address-3"]
    missing_cols = [c for c in addr_cols if c not in state.df.columns]
    if missing_cols:
        print(f"  WARNING: missing address columns: {missing_cols}")
    else:
        overflows_before = (state.df["ship-address-1"].astype(str).str.len() > MAX_ADDR_LEN).sum()
        state.df = state.df.apply(_cascade_address, axis=1)
        overflows_after  = (state.df["ship-address-1"].astype(str).str.len() > MAX_ADDR_LEN).sum()
        print(f"  Address-1 overflow rows: {overflows_before} → {overflows_after} after cascade")

    # ── 2. Same-buyer merge ──────────────────────────────────────────────────
    state.df, merge_count = _merge_same_buyer_orders(state.df)
    if merge_count == 0:
        print("  No same-buyer duplicate addresses found.")

    state.log(
        step="step8_address",
        rows_in=rows_in,
        note=f"Merged {merge_count} same-buyer groups",
    )
    return state
