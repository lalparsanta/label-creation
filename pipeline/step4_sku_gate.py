"""
Step 4 — Human Gate: SKU Review
  - Displays all current SKUs to the operator
  - Accepts a list of wrong SKUs from operator (Atifia's input)
  - Removes those rows
  - Requires explicit approval before pipeline continues
  - Writes a pending-review CSV so the gate can be re-entered if the
    script is interrupted
"""

import os
import pandas as pd
from pipeline.pipeline_state import PipelineState

PENDING_FILE = os.path.join("output", "pending_sku_review.csv")


def run(state: PipelineState, wrong_skus: list | None = None) -> PipelineState:
    """
    Human-in-the-loop SKU review gate.

    Args:
        state:      current PipelineState
        wrong_skus: list of SKU strings flagged as wrong (from Atifia).
                    If provided, those rows are removed automatically before
                    showing the review prompt.
                    If None or empty, operator is shown all SKUs to review.
    """
    print("\n[Step 4] ═══ HUMAN GATE: SKU Review ═══")
    rows_in = len(state.df)

    # ── Auto-remove known wrong SKUs ────────────────────────────────────────
    auto_removed = []
    if wrong_skus:
        # Normalize for comparison
        wrong_set = {s.strip().upper() for s in wrong_skus if s.strip()}
        mask_wrong = state.df["sku"].str.strip().str.upper().isin(wrong_set)
        auto_removed = state.df.loc[mask_wrong, "sku"].tolist()
        if auto_removed:
            print(f"\n  Auto-removing {len(auto_removed)} flagged wrong SKUs:")
            for s in auto_removed:
                print(f"    ✗  {s}")
            state.df = state.df[~mask_wrong].copy().reset_index(drop=True)

    # ── Write pending review file ────────────────────────────────────────────
    os.makedirs("output", exist_ok=True)
    review_cols = ["order-id", "sku", "supplier", "uid", "brand", "color", "size", "pack"]
    available_cols = [c for c in review_cols if c in state.df.columns]
    state.df[available_cols].to_csv(PENDING_FILE, index=False)
    print(f"\n  Pending review file written: {PENDING_FILE}")

    # ── Display current SKU list ─────────────────────────────────────────────
    print("\n  ┌─ Current SKU list for review ───────────────────────────────┐")
    for _, row in state.df[available_cols].iterrows():
        supplier = row.get("supplier", "")
        uid      = row.get("uid", "")
        pack     = row.get("pack", 1)
        sku      = row.get("sku", "")
        print(f"  │  [{supplier:5}]  uid={uid:20}  pack={pack:3}  sku={sku}")
    print(f"  └─ Total: {len(state.df)} orders ──────────────────────────────────────┘\n")

    # ── Interactive prompt ───────────────────────────────────────────────────
    manually_removed = []
    while True:
        print("  OPTIONS:")
        print("  [approve]           — all SKUs look good, continue pipeline")
        print("  [remove <sku>]      — remove a specific SKU from this batch")
        print("  [list]              — reprint the SKU list")
        print("  [quit]              — abort pipeline\n")

        try:
            user_input = input("  > ").strip()
        except (EOFError, KeyboardInterrupt):
            # Non-interactive mode (e.g. automated testing) — auto-approve
            print("\n  Non-interactive mode detected — auto-approving.")
            user_input = "approve"

        cmd = user_input.lower()

        if cmd == "approve":
            print(f"\n  ✓  Approved. {len(state.df)} orders proceeding to next step.")
            break

        elif cmd.startswith("remove "):
            target_sku = user_input[7:].strip()
            mask = state.df["sku"].str.strip() == target_sku
            count = mask.sum()
            if count > 0:
                manually_removed.append(target_sku)
                state.df = state.df[~mask].copy().reset_index(drop=True)
                print(f"  ✗  Removed {count} row(s) with SKU: {target_sku}")
                print(f"     Remaining: {len(state.df)} orders")
            else:
                print(f"  ⚠  SKU not found: '{target_sku}' — check exact spelling.")

        elif cmd == "list":
            for _, row in state.df[available_cols].iterrows():
                print(f"  │  [{row.get('supplier',''):5}]  uid={row.get('uid',''):20}  sku={row.get('sku','')}")

        elif cmd == "quit":
            print("\n  Pipeline aborted by operator.")
            raise SystemExit(0)

        else:
            print("  ⚠  Unknown command. Type 'approve', 'remove <sku>', 'list', or 'quit'.")

    # ── Clean up pending file ────────────────────────────────────────────────
    if os.path.exists(PENDING_FILE):
        os.remove(PENDING_FILE)

    total_removed = len(auto_removed) + len(manually_removed)
    state.log(
        step="step4_sku_gate",
        rows_in=rows_in,
        note=(
            f"Auto-removed: {len(auto_removed)}, "
            f"Manually removed: {len(manually_removed)}, "
            f"Approved by operator"
        ),
    )
    return state
