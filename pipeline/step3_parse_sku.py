"""
Step 3 — SKU Parsing
  - Split SKU into: supplier, uid, brand, color, size
  - Extract pack count (Px = x packs; none = 1 pack)
  - Mark duplicate flag (D1, D2 etc in SKU → set duplicate=True, don't store value)
  - Handle all observed supplier formats:
      BTC  : BTC+UID+PRODUCT+BRAND+COLOR+SIZE_Ppack-Ddupe
      PNC  : PNC+UID.COLOR.SIZE+PRODUCT+BRAND+Ppack-Ddupe
      DFF  : DFF|DFFUID|Ppack
      RW   : RW+UID_with_embedded_color_size+PRODUCT+Ppack
      AB   : AB+UID+PRODUCT+Ppack
      BW   : BW-UID-BRAND-Ppack-CARRIER
      SDC  : SDC+... (will be dropped in step5 but parse anyway)
"""

import re
import pandas as pd
from pipeline.pipeline_state import PipelineState


# ── SKU Cleaning helpers ────────────────────────────────────────────────────

def _clean_sku(sku: str) -> str:
    """Remove trailing/embedded garbage characters from SKU."""
    if not isinstance(sku, str):
        return ""
    return re.sub(r"[\{\}\[\]~`]", "", sku).strip()


def _extract_supplier(raw: str) -> str:
    """Extract supplier prefix — everything before first delimiter (+ | -)."""
    raw = raw.upper().strip()
    for delim in ["+", "|"]:
        if delim in raw:
            return raw.split(delim)[0].strip()
    # BW uses dashes
    parts = raw.split("-")
    if len(parts) >= 2:
        return parts[0].strip()
    return raw


def _extract_pack(sku: str) -> int:
    """
    Find pack count from SKU.
    Rules:
      - Look for P followed immediately by digits (e.g. P1, P3, P36)
      - Must NOT be preceded by a letter (avoids matching inside words like 'SP1')
      - If multiple found, take the FIRST occurrence
      - If none found, default to 1
    """
    clean = _clean_sku(sku)
    # Match P<digits> as a standalone token (after +, |, _, -, space, or start)
    matches = re.findall(r'(?:^|[+|\-_.])P(\d+)(?:[+|\-_.\s]|$)', clean, re.IGNORECASE)
    if matches:
        return int(matches[0])
    # Fallback: looser match — P<digits> not preceded by a letter
    matches2 = re.findall(r'(?<![A-Za-z])P(\d+)(?![A-Za-z])', clean, re.IGNORECASE)
    if matches2:
        return int(matches2[0])
    return 1


def _has_duplicate_flag(sku: str) -> bool:
    """
    Returns True if the SKU contains a D<digit> duplicate marker (D1, D2, D7 etc).
    Per spec: we note it but do NOT store the value.
    """
    clean = _clean_sku(sku)
    return bool(re.search(r'(?<![A-Za-z])D\d+(?![A-Za-z])', clean, re.IGNORECASE))


def _parse_btc(parts: list) -> dict:
    """
    BTC+UID+PRODUCT_CODE+BRAND+COLOR+SIZE_Ppack
    parts[0]=BTC, [1]=UID, [2]=product, [3]=brand, [4]=color, [5]=size (may contain _P1)
    """
    uid       = parts[1] if len(parts) > 1 else ""
    brand     = parts[3] if len(parts) > 3 else ""
    color     = parts[4] if len(parts) > 4 else ""
    # size is in parts[5], before the underscore
    size_raw  = parts[5] if len(parts) > 5 else ""
    size      = size_raw.split("_")[0].strip() if "_" in size_raw else size_raw
    return {"uid": uid, "brand": brand, "color": color, "size": size}


def _parse_pnc(parts: list) -> dict:
    """
    PNC+UID.COLOR.SIZE+PRODUCT_CODE+BRAND+Ppack
    UID block encodes all three: retain the full UID.COLOR.SIZE block
    for lookup, while still parsing color and size separately.
    """
    uid_block = parts[1] if len(parts) > 1 else ""
    uid_parts = uid_block.split(".")
    uid   = uid_block
    color = uid_parts[1] if len(uid_parts) > 1 else ""
    size  = uid_parts[2] if len(uid_parts) > 2 else ""
    brand = parts[3] if len(parts) > 3 else (parts[2] if len(parts) > 2 else "")
    # brand is sometimes in position 3, sometimes not present
    # if parts[2] looks like a pack token (starts with P+digit) skip it
    if brand and re.match(r'^P\d+', brand, re.IGNORECASE):
        brand = ""
    return {"uid": uid, "brand": brand, "color": color, "size": size}


def _parse_dff(raw: str) -> dict:
    """
    DFF|DFFUID|Ppack  — pipe-delimited
    No separate color/size/brand in this format.
    """
    parts = raw.split("|")
    uid   = parts[1].strip() if len(parts) > 1 else ""
    return {"uid": uid, "brand": "", "color": "", "size": ""}


def _parse_rw(parts: list) -> dict:
    """
    RW+UID_with_embedded_color_size+PRODUCT_CODE+Ppack
    UID is a composite code; no separate color/size tokens.
    """
    uid   = parts[1] if len(parts) > 1 else ""
    brand = parts[2] if len(parts) > 2 else ""
    return {"uid": uid, "brand": brand, "color": "", "size": ""}


def _parse_ab(parts: list) -> dict:
    """
    AB+UID+PRODUCT_CODE+Ppack
    """
    uid   = parts[1] if len(parts) > 1 else ""
    brand = parts[2] if len(parts) > 2 else ""
    return {"uid": uid, "brand": brand, "color": "", "size": ""}


def _parse_bw(raw: str) -> dict:
    """
    BW-UID-BRAND-Ppack-CARRIER  — dash-delimited
    """
    parts = raw.split("-")
    uid   = parts[1] if len(parts) > 1 else ""
    brand = parts[2] if len(parts) > 2 else ""
    return {"uid": uid, "brand": brand, "color": "", "size": ""}


def _parse_generic(parts: list) -> dict:
    """Fallback for unknown supplier formats."""
    uid = parts[1] if len(parts) > 1 else ""
    return {"uid": uid, "brand": "", "color": "", "size": ""}


def parse_sku(raw_sku: str) -> dict:
    """
    Master SKU parser. Returns dict with keys:
      supplier, uid, brand, color, size, pack, is_duplicate
    """
    if not isinstance(raw_sku, str) or not raw_sku.strip():
        return {
            "supplier": "", "uid": "", "brand": "",
            "color": "", "size": "", "pack": 1, "is_duplicate": False
        }

    clean = _clean_sku(raw_sku)
    supplier = _extract_supplier(clean)
    pack = _extract_pack(clean)
    is_dup = _has_duplicate_flag(clean)

    sup_upper = supplier.upper()

    # Route to supplier-specific parser
    if sup_upper == "DFF" or "|" in clean:
        fields = _parse_dff(clean)
    elif sup_upper == "BW":
        fields = _parse_bw(clean)
    else:
        # + delimited suppliers
        parts = clean.split("+")
        if sup_upper == "BTC":
            fields = _parse_btc(parts)
        elif sup_upper == "PNC":
            fields = _parse_pnc(parts)
        elif sup_upper == "RW":
            fields = _parse_rw(parts)
        elif sup_upper == "AB":
            fields = _parse_ab(parts)
        else:
            fields = _parse_generic(parts)

    # Clean up any residual pack/dupe markers from color/size/brand strings
    for key in ("brand", "color", "size"):
        if fields.get(key):
            # Remove trailing pack/dupe tokens (e.g. _P1, -D1, -B9 suffixes)
            fields[key] = re.sub(r'[-_]?(?:P\d+|D\d+|B\d+).*$', '', fields[key], flags=re.IGNORECASE).strip()
            # Remove garbage chars
            fields[key] = re.sub(r'[\{\}\[\]]', '', fields[key]).strip()
            # Strip trailing punctuation
            fields[key] = fields[key].strip("-_+|")

    return {
        "supplier":     supplier.upper(),
        "uid":          fields.get("uid", "").strip(),
        "brand":        fields.get("brand", "").strip(),
        "color":        fields.get("color", "").strip(),
        "size":         fields.get("size", "").strip(),
        "pack":         pack,
        "is_duplicate": is_dup,
    }


# ── Step runner ─────────────────────────────────────────────────────────────

def run(state: PipelineState) -> PipelineState:
    """
    Parses the SKU column into 5 named columns + pack + is_duplicate.
    Adds columns in-place to state.df.
    """
    print("\n[Step 3] Parsing SKU columns")
    rows_in = len(state.df)

    if "sku" not in state.df.columns:
        raise ValueError("Column 'sku' not found in DataFrame.")

    parsed = state.df["sku"].apply(parse_sku)
    parsed_df = pd.DataFrame(parsed.tolist())

    # Insert new columns right after 'sku'
    sku_idx = state.df.columns.get_loc("sku")
    for i, col in enumerate(["supplier", "uid", "brand", "color", "size", "pack", "is_duplicate"]):
        state.df.insert(sku_idx + 1 + i, col, parsed_df[col])

    # Summary
    supplier_counts = state.df["supplier"].value_counts().to_dict()
    print(f"  Suppliers found: {supplier_counts}")
    print(f"  Pack distribution: {state.df['pack'].value_counts().sort_index().to_dict()}")
    dup_count = state.df["is_duplicate"].sum()
    if dup_count:
        print(f"  Duplicate-flagged rows: {dup_count}")

    state.log(
        step="step3_parse_sku",
        rows_in=rows_in,
        note=f"Suppliers: {supplier_counts}",
    )
    return state
