"""
Step 6 — Validation & Cleaning
  - Deduplicate: if same order-id appears more than once, sum quantity-purchased
    and keep one row (merge exact_quantity and total_weight too)
  - Strip country code from buyer-phone-number (+44... → 0...)
  - Clean ship-postal-code: keep only the outward code (RM9 5UU → RM9)
"""

import re
import pandas as pd
from pipeline.pipeline_state import PipelineState


# ── Phone cleaning ────────────────────────────────────────────────────────────


def _fix_scientific_phone(phone: str) -> str:
    """
    Convert scientific notation phone numbers back to integer strings.
    Cause: Amazon .txt opened/saved in Excel corrupts long numbers.
    e.g. '4.47961E+11' → '447961000000'
    """
    if re.match(r'^[\d.]+[eE][+\-]?\d+$', phone):
        try:
            return str(int(float(phone)))
        except (ValueError, OverflowError):
            pass
    return phone


def _clean_phone(phone: str) -> str:
    """
    1. Fix scientific notation (e.g. 4.47961E+11 → 447961000000)
    2. Strip country code and restore UK leading 0
       +447xxx   → 07xxx
       0044 7xxx → 07xxx
       447xxx    → 07xxx  (bare 44 prefix, 12 digits — from sci notation)
    """
    if not isinstance(phone, str) or not phone.strip():
        return phone

    phone = _fix_scientific_phone(phone.strip())

    # Remove all internal spaces (e.g. '07930 333305' → '07930333305')
    phone = phone.replace(" ", "")

    # +44 → 0
    if re.match(r'^\+44', phone):
        return re.sub(r'^\+44\s*', '0', phone).strip()
    # 0044 → 0
    if re.match(r'^0044', phone):
        return re.sub(r'^0044\s*', '0', phone).strip()
    # bare 44XXXXXXXXXX — 12 digits starting with 44 (scientific notation artifact)
    if re.match(r'^44\d{10}$', phone):
        return '0' + phone[2:]
    # generic: strip any +<country_code>
    if phone.startswith('+'):
        phone = re.sub(r'^\+\d{1,3}\s*', '', phone)
    return phone.strip()


# ── Postal code cleaning ──────────────────────────────────────────────────────

def _clean_postcode(postcode: str) -> str:
    """
    Extract the outward code (first part before the space) from a UK postcode.
    Examples:
      'RM9 5UU'   → 'RM9'
      'SW8 2XR'   → 'SW8'
      'EC1A 1BB'  → 'EC1A'
      'W1A 0AX'   → 'W1A'
      'RM9'       → 'RM9'   (already outward only)
      'TW991UR'   → 'TW99'  (no space — take letters+digits before the last 3 chars)
    """
    if not isinstance(postcode, str) or not postcode.strip():
        return postcode

    pc = postcode.strip().upper()

    # If there's a space, everything before the first space is the outward code
    if " " in pc:
        return pc.split(" ")[0].strip()

    # No space (e.g. TW991UR) — UK inward code is always 3 chars (digit + 2 letters)
    # Outward code = all but last 3 characters
    if len(pc) > 3:
        return pc[:-3].strip()

    return pc   # short code, return as-is


# ── Deduplication ─────────────────────────────────────────────────────────────

def _dedup_orders(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    If the same order-id appears more than once, collapse to one row:
      - quantity-purchased: summed
      - exact_quantity:     summed
      - total_weight:       summed
      - All other fields:   keep first occurrence
    Returns (deduped_df, number_of_collapsed_groups)
    """
    order_counts = df["order-id"].value_counts()
    dup_order_ids = order_counts[order_counts > 1].index

    if len(dup_order_ids) == 0:
        return df, 0

    print(f"\n  Found {len(dup_order_ids)} order-id(s) with multiple rows — collapsing:")

    agg_rows = []
    non_dup_df = df[~df["order-id"].isin(dup_order_ids)].copy()

    for oid in dup_order_ids:
        group = df[df["order-id"] == oid].copy()
        print(f"    order {oid}: {len(group)} rows → 1 row")

        # First row as base
        merged = group.iloc[0].copy()

        # Sum numeric quantity/weight columns
        for col in ["quantity-purchased", "exact_quantity", "total_weight"]:
            if col in group.columns:
                merged[col] = pd.to_numeric(group[col], errors="coerce").fillna(0).sum()

        agg_rows.append(merged)

    if agg_rows:
        collapsed_df = pd.DataFrame(agg_rows)
        result = pd.concat([non_dup_df, collapsed_df], ignore_index=True)
    else:
        result = non_dup_df

    return result.reset_index(drop=True), len(dup_order_ids)


def _dedup_by_address(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    If multiple orders share the same delivery address
    (ship-address-1 + ship-postal-code), collapse to one row so only
    one shipping label is produced:
      - quantity-purchased: summed
      - exact_quantity:     summed
      - total_weight:       summed
      - order-id:           keep first occurrence
      - All other fields:   keep first occurrence
    Returns (deduped_df, number_of_collapsed_address_groups)
    """
    key_cols = ["ship-address-1", "ship-postal-code"]
    available = [c for c in key_cols if c in df.columns]

    if len(available) < 2:
        return df, 0

    key = (
        df["ship-address-1"].fillna("").astype(str).str.strip().str.upper()
        + "|"
        + df["ship-postal-code"].fillna("").astype(str).str.strip().str.upper()
    )

    key_counts = key.value_counts()
    dup_keys = key_counts[key_counts > 1].index

    if len(dup_keys) == 0:
        return df, 0

    print(f"\n  Found {len(dup_keys)} delivery address(es) with multiple orders — merging into one label:")

    df = df.copy()
    df["_addr_key"] = key

    agg_rows = []
    non_dup_df = df[~df["_addr_key"].isin(dup_keys)].copy()

    for addr_key in dup_keys:
        group = df[df["_addr_key"] == addr_key].copy()
        order_ids = group["order-id"].tolist() if "order-id" in group.columns else []
        addr_display = addr_key.replace("|", "  postcode: ")
        print(f"    {addr_display}")
        print(f"      {len(group)} orders → 1 label  (order-ids: {', '.join(str(o) for o in order_ids)})")

        merged = group.iloc[0].copy()

        for col in ["quantity-purchased", "exact_quantity", "total_weight"]:
            if col in group.columns:
                merged[col] = pd.to_numeric(group[col], errors="coerce").fillna(0).sum()

        agg_rows.append(merged)

    if agg_rows:
        collapsed_df = pd.DataFrame(agg_rows)
        result = pd.concat([non_dup_df, collapsed_df], ignore_index=True)
    else:
        result = non_dup_df

    result = result.drop(columns=["_addr_key"], errors="ignore")
    return result.reset_index(drop=True), len(dup_keys)


# ── Step runner ───────────────────────────────────────────────────────────────

def run(state: PipelineState) -> PipelineState:
    """
    Runs deduplication, phone cleaning, and postcode cleaning.
    """
    print("\n[Step 6] Deduplication, phone & postcode cleaning")
    rows_in = len(state.df)

    # ── 1. Deduplicate order IDs ─────────────────────────────────────────────
    state.df, collapsed_count = _dedup_orders(state.df)
    if collapsed_count == 0:
        print("  No duplicate order IDs found.")

    # ── 1b. Deduplicate by delivery address ──────────────────────────────────
    state.df, addr_collapsed_count = _dedup_by_address(state.df)
    if addr_collapsed_count == 0:
        print("  No duplicate delivery addresses found.")

    # ── 2. Clean phone numbers ───────────────────────────────────────────────
    if "buyer-phone-number" in state.df.columns:
        before = state.df["buyer-phone-number"].copy()
        state.df["buyer-phone-number"] = state.df["buyer-phone-number"].apply(_clean_phone)
        changed = (before != state.df["buyer-phone-number"]).sum()
        print(f"  Phone numbers cleaned: {changed} modified")
    else:
        print("  WARNING: 'buyer-phone-number' column not found — skipping phone clean")

    # ── 3. Clean postal codes ────────────────────────────────────────────────
    if "ship-postal-code" in state.df.columns:
        before = state.df["ship-postal-code"].copy()
        full_codes = (
            state.df["ship-postal-code"].astype(str)
                .str.strip()
                .str.upper()
                .replace(r"\s+", " ", regex=True)
        )
        state.df["ship-postal-code-full"] = full_codes
        state.df["ship-postal-code-outward"] = full_codes.apply(_clean_postcode)
        state.df["ship-postal-code"] = state.df["ship-postal-code-full"]
        changed = (before != state.df["ship-postal-code"]).sum()
        print(f"  Postal codes normalized and saved full: {changed} modified")
        print(f"  Sample full codes: {state.df['ship-postal-code-full'].head(5).tolist()}")
        print(f"  Sample outward codes: {state.df['ship-postal-code-outward'].head(5).tolist()}")
    else:
        print("  WARNING: 'ship-postal-code' column not found — skipping postcode clean")

    state.log(
        step="step6_validate",
        rows_in=rows_in,
        note=f"Collapsed {collapsed_count} duplicate order-id groups, {addr_collapsed_count} duplicate address groups",
    )
    return state