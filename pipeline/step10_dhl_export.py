"""
Step 10 — DHL Template Mapping & Export
  - Map processed columns to DHL import template column names
  - Fill alternative-reference with today's date (DD-MMM format)
  - Export as CSV (comma-delimited — DHL standard)
  - Also exports an audit log CSV alongside the main file
"""

import os
from datetime import date
import pandas as pd
from pipeline.pipeline_state import PipelineState

OUTPUT_DIR    = "output"
ACCOUNT_NUMBER = "C989978"
SERVICE_CODE   = "210"

# ── DHL column mapping ────────────────────────────────────────────────────────
# Format: { dhl_column_name: source }
# Sources starting with "__" are computed/static values, not column lookups.

DHL_COLUMN_MAP = {
    "*Account number*":                           "__account_number",
    "Customer reference":                         "order-id",
    "Alternative reference":                      "__today_date",
    "Delivery contact name":                      "recipient-name",
    "Delivery business name":                     "__empty",
    "*Delivery address 1*":                       "ship-address-1",
    "Delivery address 2":                         "ship-address-2",
    "Delivery address 3":                         "ship-address-3",
    "*Delivery town*":                            "ship-city",
    "*Delivery postal code (must have a space)*": "ship-postal-code",
    "Delivery county":                            "ship-state",
    "Country":                                    "__empty",
    "Delivery telephone":                         "buyer-phone-number",
    "*Delivery email*":                           "buyer-email",
    "*No.of items*":                              "__fixed_one",
    "*Weight in kg*":                             "total_weight",
    "*Service code*":                             "__service_code",
    "Special instructions 1st line":              "__special_instr_1",
    "Special instructions 2nd line":              "__special_instr_2",
    "Dispatch date":                              "__empty",
}


def _split_special_instructions(value: str) -> tuple[str, str]:
    """
    DHL has two special-instructions columns, each 33 chars.
    Split the trimmed text across both lines.
    """
    if not isinstance(value, str) or not value.strip():
        return "", ""
    text = value.strip()
    return text[:33].strip(), text[33:66].strip()


def _today_alt_ref() -> str:
    """Build the DHL alternative reference as DD-MMM (e.g. 19-MAY)."""
    return date.today().strftime("%d-%b").upper()


def _map_to_dhl(df: pd.DataFrame) -> pd.DataFrame:
    """Build a new DataFrame with DHL column names from the pipeline DataFrame."""
    out = pd.DataFrame(index=df.index)

    # Pre-split delivery instructions so they can be referenced in the loop
    if "delivery-Instructions" in df.columns:
        splits = df["delivery-Instructions"].apply(_split_special_instructions).tolist()
        instr_line1, instr_line2 = zip(*splits) if splits else ([], [])
    else:
        instr_line1 = [""] * len(df)
        instr_line2 = [""] * len(df)

    for dhl_col, source in DHL_COLUMN_MAP.items():
        if source == "__account_number":
            out[dhl_col] = ACCOUNT_NUMBER
        elif source == "__service_code":
            out[dhl_col] = SERVICE_CODE
        elif source == "__today_date":
            out[dhl_col] = _today_alt_ref()
        elif source == "__fixed_one":
            out[dhl_col] = "1"
        elif source == "__empty":
            out[dhl_col] = ""
        elif source == "__special_instr_1":
            out[dhl_col] = list(instr_line1)
        elif source == "__special_instr_2":
            out[dhl_col] = list(instr_line2)
        elif source in df.columns:
            out[dhl_col] = df[source].fillna("").astype(str).str.strip()
        else:
            out[dhl_col] = ""

    return out


def run(state: PipelineState, output_filename: str | None = None) -> tuple[str, str]:
    """
    Maps data to DHL template and exports.

    Args:
        state:           current PipelineState
        output_filename: optional override for output filename.
                         Defaults to dhl_import_YYYY-MM-DD.csv

    Returns:
        (output_csv_path, audit_log_path)
    """
    print("\n[Step 10] Mapping to DHL template & exporting")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    today_str = date.today().strftime("%Y-%m-%d")
    if not output_filename:
        output_filename = f"dhl_import_{today_str}.csv"

    output_path = os.path.join(OUTPUT_DIR, output_filename)
    audit_path  = os.path.join(OUTPUT_DIR, f"audit_log_{today_str}.csv")

    # ── Map ───────────────────────────────────────────────────────────────────
    dhl_df = _map_to_dhl(state.df)

    # Validate required columns are not all blank
    required = [
        "*Account number*",
        "Customer reference",
        "*Delivery address 1*",
        "*Delivery postal code (must have a space)*",
    ]
    for col in required:
        if col in dhl_df.columns:
            blank_count = (dhl_df[col].astype(str).str.strip() == "").sum()
            if blank_count > 0:
                print(f"  WARNING: {blank_count} rows have blank '{col}'")

    # ── Export main CSV ───────────────────────────────────────────────────────
    dhl_df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"\n  DHL import file: {output_path}")
    print(f"     Rows: {len(dhl_df)}  |  Columns: {len(dhl_df.columns)}")

    # ── Export audit log ──────────────────────────────────────────────────────
    audit_rows = [
        {
            "timestamp":    entry.timestamp,
            "step":         entry.step,
            "rows_in":      entry.rows_in,
            "rows_out":     entry.rows_out,
            "rows_dropped": entry.rows_dropped,
            "note":         entry.note,
        }
        for entry in state.audit_log
    ]
    pd.DataFrame(audit_rows).to_csv(audit_path, index=False, encoding="utf-8")
    print(f"  Audit log:       {audit_path}")

    # ── Final summary ─────────────────────────────────────────────────────────
    state.print_summary()

    return output_path, audit_path
