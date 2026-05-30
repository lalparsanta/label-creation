import sys
import pandas as pd
from datetime import datetime
from pathlib import Path


# ── Amazon template column order (must be preserved exactly) ────────────────
AMAZON_COLUMNS = [
    "order-id",
    "order-item-id",
    "quantity",
    "ship-date",
    "carrier-code",
    "carrier-name",
    "tracking-number",
    "ship-method",
    "transparency_code",
    "ship_from_address_name",
    "ship_from_address_line1",
    "ship_from_address_line2",
    "ship_from_address_line3",
    "ship_from_address_city",
    "ship_from_address_county",
    "ship_from_address_state_or_region",
    "ship_from_address_postalcode",
    "ship_from_address_countrycode",
]

# ── Static values ────────────────────────────────────────────────────────────
CARRIER_CODE = "DHL eCommerce"
SHIP_METHOD  = "Premier 24"


def load_dhl_report(path: str) -> pd.DataFrame:
    """Load DHL CSV and return only the columns we need."""
    df = pd.read_csv(path, dtype={"Shipment number": str, "Alternative reference": str})

    required = {"Customer reference", "Shipment number", "Alternative reference"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"DHL report is missing expected columns: {missing}")

    return df[["Customer reference", "Shipment number", "Alternative reference"]].copy()


def filter_today(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop rows whose Alternative reference is not today.
    Handles both padded (20/05/2026) and unpadded (20/5/2026) D/M/YYYY formats.
    """
    today_dt = datetime.today()

    # Normalise each value by parsing and reformatting — handles zero-padded and unpadded
    def parse_dhl_date(val: str) -> datetime | None:
        val = str(val).strip()
        for fmt in ("%d-%b", "%d-%b-%y", "%d-%b-%Y", "%d/%m/%Y", "%d/%m/%y"):
            try:
                return datetime.strptime(val, fmt)
            except ValueError:
                continue
        return None

    def matches_today(d: datetime | None) -> bool:
        if d is None:
            return False
        if d.year == 1900:  # parsed without year (e.g. "25-May")
            return d.day == today_dt.day and d.month == today_dt.month
        return d.date() == today_dt.date()

    parsed = df["Alternative reference"].apply(parse_dhl_date)
    today_mask = parsed.apply(matches_today)
    dropped = (~today_mask).sum()

    if dropped:
        unique_vals = df.loc[~today_mask, "Alternative reference"].astype(str).str.strip().unique()
        sample = ", ".join(unique_vals[:10])
        print(f"  ⚠  Dropped {dropped} row(s) with dates other than today ({today_dt.strftime('%d-%b')}).")
        print(f"     Sample values found in 'Alternative reference': {sample}")

    filtered = df[today_mask].copy()

    if filtered.empty:
        print(f"  ✗  No rows found for today ({today_dt.strftime('%d-%b')}). Output file will have headers only.")

    return filtered


def build_amazon_df(dhl_df: pd.DataFrame) -> pd.DataFrame:
    """Map DHL columns → Amazon template, filling static values."""
    today_dt = datetime.today()
    today_amz = f"{today_dt.month}/{today_dt.day}/{today_dt.year}"   # Amazon format: 5/20/2026

    out = pd.DataFrame(index=dhl_df.index, columns=AMAZON_COLUMNS)

    # Mapped columns
    out["order-id"]        = dhl_df["Customer reference"].str.strip()
    out["tracking-number"] = dhl_df["Shipment number"].astype(str).str.strip()

    # Static / derived columns
    out["ship-date"]    = today_amz
    out["carrier-code"] = CARRIER_CODE
    out["ship-method"]  = SHIP_METHOD

    # All remaining columns stay blank (empty string, not NaN, for clean TSV)
    out = out.fillna("")

    return out.reset_index(drop=True)


def export_amazon_txt(df: pd.DataFrame, output_path: str) -> None:
    """Write tab-delimited .txt exactly as Amazon expects."""
    df.to_csv(output_path, sep="\t", index=False, lineterminator="\r\n")
    print(f"  ✓  Saved {len(df)} row(s) → {output_path}")


def main():
    # ── Resolve paths ────────────────────────────────────────────────────────
    if len(sys.argv) < 2:
        print("Usage: python dhl_to_amazon_tracking.py <dhl_report.csv> [output.txt]")
        sys.exit(1)

    dhl_path = sys.argv[1]
    if not Path(dhl_path).exists():
        print(f"✗  File not found: {dhl_path}")
        sys.exit(1)

    if len(sys.argv) >= 3:
        out_path = sys.argv[2]
    else:
        date_tag = datetime.today().strftime("%Y-%m-%d")
        out_path = f"Amazon_Trackings_Update_DHL_{date_tag}.txt"

    # ── Pipeline ─────────────────────────────────────────────────────────────
    print(f"\n[1/4] Loading DHL report: {dhl_path}")
    dhl_df = load_dhl_report(dhl_path)
    print(f"      {len(dhl_df)} total row(s) in report.")

    print(f"\n[2/4] Filtering to today's date only ...")
    dhl_df = filter_today(dhl_df)

    print(f"\n[3/4] Mapping columns to Amazon template ...")
    amazon_df = build_amazon_df(dhl_df)

    print(f"\n[4/4] Exporting tab-delimited file ...")
    export_amazon_txt(amazon_df, out_path)

    print("\nDone.\n")


if __name__ == "__main__":
    main()