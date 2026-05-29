"""
Label Creator — Main Orchestrator
===================================
Usage:
    python main.py <amazon_unshipped.txt> [options]

Options:
    --non-shipped      PATH   Unshipped order list (XLSX) - OIDS sheet, OID column
    --pnc-btc-weights  PATH   PNC/BTC weight file (XLSX)
    --dff-weights      PATH   DFF weight file (XLSX)
    --dzone            PATH   D-zone reference file (XLSX)
    --wrong-skus       SKUS   Comma-separated wrong SKUs to auto-remove
    --output           NAME   Output filename override
    --no-ai                   Skip GPT delivery instruction trimming
    --openai-key       KEY    OpenAI API key (or set OPENAI_API_KEY env var)
    --skip-gate               Skip interactive SKU review gate (testing only)

Example:
    python main.py orders.txt \\
        --non-shipped     reference_data/unshipped_orders.xlsx \\
        --pnc-btc-weights reference_data/PNC_BTC_weights.xlsx \\
        --dff-weights     reference_data/DFF_weights.xlsx \\
        --dzone           reference_data/d_zone.xlsx
"""

import sys
import os
import argparse
sys.path.insert(0, os.path.dirname(__file__))

from pipeline.step1_ingest          import run as step1
from pipeline.step2_filter_existing import run as step2
from pipeline.step3_parse_sku       import run as step3
from pipeline.step4_sku_gate        import run as step4
from pipeline.step5_weight          import run as step5
from pipeline.step6_validate        import run as step6
from pipeline.step7_dzone           import run as step7
from pipeline.step8_address         import run as step8
from pipeline.step9_instructions    import run as step9
from pipeline.step10_dhl_export          import run as step10
from pipeline.step11_update_nonshipment  import run as step11


def _find_default_non_shipped() -> str | None:
    candidates = [
        os.path.join("reference data", "non-shipment.xlsx"),
        os.path.join("reference data", "non_shipment.xlsx"),
        os.path.join("reference data", "unshipped_orders.xlsx"),
        os.path.join("reference_data", "non-shipment.xlsx"),
        os.path.join("reference_data", "non_shipment.xlsx"),
        os.path.join("reference_data", "unshipped_orders.xlsx"),
    ]
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    return None


def parse_args():
    p = argparse.ArgumentParser(description="Amazon → DHL Label Creation Pipeline")
    p.add_argument("input_file")
    p.add_argument("--non-shipped",      default=None)
    p.add_argument("--pnc-btc-weights",  default=None)
    p.add_argument("--dff-weights",      default=None)
    p.add_argument("--dzone",            default=None)
    p.add_argument("--wrong-skus",       default="")
    p.add_argument("--output",           default=None)
    p.add_argument("--no-ai",            action="store_true")
    p.add_argument("--openai-key",       default=None)
    p.add_argument("--skip-gate",        action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    print("=" * 60)
    print("  LABEL CREATION PIPELINE")
    print("=" * 60)

    wrong_skus = [s.strip() for s in args.wrong_skus.split(",") if s.strip()] \
                 if args.wrong_skus else []

    non_shipped_path = args.non_shipped or _find_default_non_shipped()
    if non_shipped_path and not args.non_shipped:
        print(f"  Using default non-shipped file: {non_shipped_path}")

    state = step1(args.input_file)
    state = step2(state, non_shipped_filepath=non_shipped_path)
    state = step3(state)

    if args.skip_gate:
        print("\n[Step 4] ══ HUMAN GATE SKIPPED (--skip-gate) ══")
    else:
        state = step4(state, wrong_skus=wrong_skus)

    state = step5(state,
                  pnc_btc_weight_filepath=args.pnc_btc_weights,
                  dff_weight_filepath=args.dff_weights)
    state = step6(state)
    state = step7(state, dzone_reference_filepath=args.dzone)
    state = step8(state)
    state = step9(state, openai_api_key=args.openai_key, use_ai=not args.no_ai)

    output_path, audit_path = step10(state, output_filename=args.output)
    step11(state, non_shipped_filepath=non_shipped_path)

    print(f"\n  Pipeline complete.")
    print(f"  Output: {output_path}")
    print(f"  Audit:  {audit_path}\n")


if __name__ == "__main__":
    main()
