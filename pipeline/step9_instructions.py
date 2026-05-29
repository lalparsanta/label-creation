"""
Step 9 — Delivery Instruction Trim (AI Layer)
  - Trim delivery-Instructions to max 66 characters total
  - Uses GPT to do it meaningfully (preserve intent, not hard-cut)
  - Falls back to deterministic truncation if API unavailable or key not set
  - Processes in batches to avoid rate limits
"""

import os
import re
import time
import pandas as pd
from pipeline.pipeline_state import PipelineState

MAX_INSTR_LINE = 33
MAX_INSTR_TOTAL = MAX_INSTR_LINE * 2
INSTR_COL = "delivery-Instructions"

# ── Deterministic fallback ────────────────────────────────────────────────────

def _deterministic_trim(text: str, max_len: int = MAX_INSTR_TOTAL) -> str:
    """
    Trim delivery instruction to max_len chars without AI.
    Strategy:
      1. If already ≤ max_len, return as-is
      2. Try to cut at last word boundary within max_len
      3. Hard-cut if no word boundary found
    """
    if not isinstance(text, str) or not text.strip():
        return ""
    text = text.strip()
    if len(text) <= max_len:
        return text
    # Try word boundary
    truncated = text[:max_len]
    last_space = truncated.rfind(" ")
    if last_space > max_len // 2:  # only use word boundary if it's not too short
        return truncated[:last_space].strip()
    return truncated.strip()


# ── GPT-powered trim ──────────────────────────────────────────────────────────

def _gpt_trim_batch(instructions: list[str], api_key: str) -> list[str]:
    """
    Send a batch of instructions to GPT-3.5-turbo for intelligent trimming.
    Returns a list of trimmed strings in the same order.

    Prompt design:
      - Give GPT the instruction and max length
      - Ask for a semantically meaningful abbreviation
      - Strict JSON output for reliable parsing
    """
    try:
        import json
        import urllib.request
        import urllib.error

        numbered = "\n".join(
            f'{i+1}. {instr}' for i, instr in enumerate(instructions)
        )

        prompt = f"""You are a parcel delivery assistant. Shorten each delivery instruction to {MAX_INSTR_TOTAL} characters or fewer, preserving as much useful content as possible across two 33-character lines. Keep the most important information: access codes, call numbers, key location details. Remove filler words. Return ONLY a JSON array of strings, same order as input, no other text.

Instructions:
{numbered}"""

        payload = {
            "model": "gpt-3.5-turbo",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0,
            "max_tokens": 500,
        }

        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            "https://api.openai.com/v1/chat/completions",
            data=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode("utf-8"))

        content = result["choices"][0]["message"]["content"].strip()

        # Strip markdown fences if GPT wrapped the JSON
        content = re.sub(r"^```(?:json)?\s*", "", content)
        content = re.sub(r"\s*```$", "", content)

        trimmed_list = json.loads(content)

        # Validate: must be list of same length, each ≤ MAX_INSTR_TOTAL chars
        if not isinstance(trimmed_list, list) or len(trimmed_list) != len(instructions):
            raise ValueError(f"GPT returned {len(trimmed_list)} items, expected {len(instructions)}")

        # Safety: enforce hard limit even on GPT output
        return [str(t)[:MAX_INSTR_TOTAL] for t in trimmed_list]

    except Exception as e:
        print(f"    ⚠  GPT call failed: {e} — using deterministic fallback for this batch")
        return [_deterministic_trim(i) for i in instructions]


# ── Step runner ───────────────────────────────────────────────────────────────

def run(state: PipelineState, openai_api_key: str | None = None, use_ai: bool = True) -> PipelineState:
    """
    Trims delivery instructions to 33 characters.

    Args:
        state:          current PipelineState
        openai_api_key: OpenAI API key string. If None, reads from OPENAI_API_KEY env var.
        use_ai:         if False, skip GPT and use deterministic trimming only.
    """
    print(f"[Step 9] Delivery instruction trim ({MAX_INSTR_TOTAL}-char total limit across 2 lines)")
    rows_in = len(state.df)

    if INSTR_COL not in state.df.columns:
        print(f"  WARNING: '{INSTR_COL}' column not found — skipping.")
        state.log(step="step9_instructions", rows_in=rows_in, note="Column not found — skipped")
        return state

    # Rows that actually have instructions
    has_instr = state.df[INSTR_COL].astype(str).str.strip().ne("") & \
                state.df[INSTR_COL].astype(str).str.strip().ne("nan")
    needs_trim = has_instr & (state.df[INSTR_COL].astype(str).str.len() > MAX_INSTR_TOTAL)

    total_with_instr = has_instr.sum()
    total_needs_trim = needs_trim.sum()
    print(f"  Rows with instructions: {total_with_instr}")
    print(f"  Rows exceeding {MAX_INSTR_TOTAL} chars: {total_needs_trim}")

    if total_needs_trim == 0:
        print("  All instructions within limit — nothing to trim.")
        state.log(step="step9_instructions", rows_in=rows_in, note="No trimming needed")
        return state

    # ── Resolve AI mode ──────────────────────────────────────────────────────
    api_key = openai_api_key or os.getenv("OPENAI_API_KEY", "")
    ai_available = use_ai and bool(api_key)

    if ai_available:
        print(f"  Mode: GPT-assisted trim")
    else:
        reason = "disabled by caller" if not use_ai else "OPENAI_API_KEY not set"
        print(f"  Mode: deterministic fallback ({reason})")

    # ── Process rows needing trim ────────────────────────────────────────────
    indices = state.df[needs_trim].index.tolist()
    instructions = state.df.loc[indices, INSTR_COL].astype(str).tolist()

    if ai_available:
        BATCH_SIZE = 10
        results = []
        for i in range(0, len(instructions), BATCH_SIZE):
            batch = instructions[i : i + BATCH_SIZE]
            print(f"  Processing batch {i // BATCH_SIZE + 1} ({len(batch)} rows)...")
            trimmed = _gpt_trim_batch(batch, api_key)
            results.extend(trimmed)
            if i + BATCH_SIZE < len(instructions):
                time.sleep(0.5)   # gentle rate-limit pause
    else:
        results = [_deterministic_trim(instr) for instr in instructions]

    # Write results back
    for idx, trimmed in zip(indices, results):
        state.df.at[idx, INSTR_COL] = trimmed

    # Verify
    remaining_long = (state.df.loc[has_instr, INSTR_COL].astype(str).str.len() > MAX_INSTR_TOTAL).sum()
    if remaining_long:
        print(f"  ⚠  {remaining_long} rows still exceed {MAX_INSTR_TOTAL} chars after trim — hard-cutting.")
        state.df.loc[has_instr, INSTR_COL] = state.df.loc[has_instr, INSTR_COL].astype(str).str[:MAX_INSTR_TOTAL]

    print(f"  Done. {total_needs_trim} instructions trimmed.")

    state.log(
        step="step9_instructions",
        rows_in=rows_in,
        note=f"Trimmed {total_needs_trim} instructions via {'GPT' if ai_available else 'deterministic'}",
    )
    return state
