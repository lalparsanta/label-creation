"""
PipelineState — the single object passed between every pipeline step.
Each step receives it, transforms df, appends to audit_log, returns it.
"""

from dataclasses import dataclass, field
from datetime import datetime
import pandas as pd


@dataclass
class AuditEntry:
    step: str
    rows_in: int
    rows_out: int
    rows_dropped: int
    note: str
    timestamp: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


@dataclass
class PipelineState:
    df: pd.DataFrame
    source_file: str = ""
    audit_log: list[AuditEntry] = field(default_factory=list)

    def log(self, step: str, rows_in: int, note: str = ""):
        rows_out = len(self.df)
        rows_dropped = rows_in - rows_out
        entry = AuditEntry(
            step=step,
            rows_in=rows_in,
            rows_out=rows_out,
            rows_dropped=rows_dropped,
            note=note,
        )
        self.audit_log.append(entry)
        status = f"  [{step}] {rows_in} in → {rows_out} out"
        if rows_dropped > 0:
            status += f"  ({rows_dropped} dropped)"
        if note:
            status += f"  | {note}"
        print(status)

    def print_summary(self):
        print("\n" + "=" * 60)
        print("PIPELINE AUDIT SUMMARY")
        print("=" * 60)
        for e in self.audit_log:
            print(f"  {e.timestamp}  [{e.step}]")
            print(f"    in={e.rows_in}  out={e.rows_out}  dropped={e.rows_dropped}")
            if e.note:
                print(f"    note: {e.note}")
        print(f"\n  Final row count: {len(self.df)}")
        print("=" * 60)
