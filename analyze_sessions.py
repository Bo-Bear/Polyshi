#!/usr/bin/env python3
"""Cross-session diagnostics analyzer for Polyshi arb bot.

Reads all JSONL log files in the logs/ directory and produces a
pattern-analysis report across sessions.  Designed for post-mortem
debugging — surfaces recurring problems before you need to grep for them.

Usage:
    python analyze_sessions.py                   # all sessions
    python analyze_sessions.py --latest           # most recent session only
    python analyze_sessions.py --last 5          # last 5 sessions only
    python analyze_sessions.py --live             # only live-mode sessions
    python analyze_sessions.py --coin BTC         # filter to BTC trades only
    python analyze_sessions.py --since-update     # sessions since last git pull
    python analyze_sessions.py -n 10              # last 10 sessions (interactive shortcut)
    python analyze_sessions.py --json             # machine-readable output
"""

import argparse
import json
import os
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def pct(v: float) -> str:
    return f"{v * 100:.2f}%"


def usd(v: float) -> str:
    return f"${v:+.4f}"


def ms_fmt(v: float) -> str:
    return f"{v:.0f}ms"


def safe_div(a: float, b: float) -> float:
    return a / b if b else 0.0


def median(vals: List[float]) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    n = len(s)
    if n % 2 == 1:
        return s[n // 2]
    return (s[n // 2 - 1] + s[n // 2]) / 2


def p95(vals: List[float]) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    idx = int(len(s) * 0.95)
    return s[min(idx, len(s) - 1)]


def _utc_to_cst(ts_str: str) -> str:
    """Convert a 19-char ISO UTC timestamp to CST (UTC-6), return 19-char ISO."""
    try:
        dt = datetime.strptime(ts_str[:19], "%Y-%m-%dT%H:%M:%S")
        dt -= timedelta(hours=6)
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except (ValueError, TypeError):
        return ts_str


def bucket_hour(ts_str: str) -> Optional[int]:
    """Extract CST hour from an ISO timestamp string (assumes UTC input)."""
    try:
        utc_h = int(ts_str[11:13])
        return (utc_h - 6) % 24
    except (IndexError, ValueError, TypeError):
        return None


def _session_start_ts(sess: dict) -> Optional[str]:
    """Extract the earliest timestamp from a session (truncated to 19 chars)."""
    diag = sess.get("session_diag") or {}
    ts = diag.get("ts")
    if ts:
        return ts[:19]
    for bucket in ("trades", "skips", "executions", "diagnostics"):
        rows = sess.get(bucket, [])
        if rows and rows[0].get("ts"):
            return rows[0]["ts"][:19]
    return None


def _get_last_update_ts() -> Optional[str]:
    """Get the ISO timestamp of the last code update (git HEAD commit).

    Returns a 19-char ISO string (YYYY-MM-DDTHH:MM:SS) in UTC, or None
    if the git timestamp cannot be determined.
    """
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--format=%ct"],
            capture_output=True, text=True,
            cwd=str(Path(__file__).parent),
        )
        if result.returncode == 0 and result.stdout.strip():
            epoch = int(result.stdout.strip())
            dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except (FileNotFoundError, ValueError, OSError):
        pass
    return None


# ──────────────────────────────────────────────
# Data loading
# ──────────────────────────────────────────────

def load_sessions(log_dir: str) -> List[dict]:
    """Load all JSONL log files, grouping rows by session (file).

    Returns list of session dicts:
      {
        "file": str,
        "trades": [...],
        "skips": [...],
        "diagnostics": [...],
        "outcomes": [...],
        "executions": [...],
        "unwinds": [...],
        "unwind_sweeps": [...],
        "balance_checks": [...],
        "kill_switches": [...],
        "session_diag": dict | None,  # session_diagnostics_dump
      }
    """
    p = Path(log_dir)
    if not p.exists():
        print(f"Error: log directory '{log_dir}' does not exist.", file=sys.stderr)
        sys.exit(1)

    files = sorted(p.glob("arb_logs_market_*.jsonl"))
    if not files:
        print(f"No log files found in '{log_dir}'.", file=sys.stderr)
        sys.exit(1)

    sessions = []
    for f in files:
        sess: dict = {
            "file": str(f),
            "filename": f.name,
            "trades": [],
            "skips": [],
            "diagnostics": [],
            "outcomes": [],
            "executions": [],
            "unwinds": [],
            "unwind_sweeps": [],
            "balance_checks": [],
            "kill_switches": [],
            "session_diag": None,
        }
        with open(f, "r") as fh:
            for line_num, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                lt = row.get("log_type", "")
                if lt == "trade":
                    sess["trades"].append(row)
                elif lt == "skip":
                    sess["skips"].append(row)
                elif lt == "diagnostic":
                    sess["diagnostics"].append(row)
                elif lt == "outcome":
                    sess["outcomes"].append(row)
                elif lt == "execution":
                    sess["executions"].append(row)
                elif lt == "unwind":
                    sess["unwinds"].append(row)
                elif lt == "unwind_sweep":
                    sess["unwind_sweeps"].append(row)
                elif lt == "balance_check":
                    sess["balance_checks"].append(row)
                elif lt == "kill_switch":
                    sess["kill_switches"].append(row)
                elif lt == "session_diagnostics_dump":
                    sess["session_diag"] = row
                elif lt == "session_diagnostics":
                    # Older format — treat as session_diag if we don't already have one
                    if sess["session_diag"] is None:
                        sess["session_diag"] = row
        sessions.append(sess)
    return sessions


def merge_outcomes_into_trades(sess: dict) -> None:
    """Merge outcome rows back into their matching trade rows by timestamp + coin."""
    if not sess["outcomes"]:
        return
    outcome_map: Dict[Tuple[str, str], dict] = {}
    for o in sess["outcomes"]:
        key = (o.get("ts", ""), o.get("coin", ""))
        outcome_map[key] = o
    for t in sess["trades"]:
        key = (t.get("ts", ""), t.get("coin", ""))
        if key in outcome_map:
            o = outcome_map[key]
            for field in ("actual_pnl_total", "hedge_consistent", "poly_won", "kalshi_won",
                          "hedged_pnl", "unhedged_pnl", "hedged_contracts",
                          "unhedged_poly", "unhedged_kalshi", "poly_outcome_source",
                          "payout_per_contract"):
                if field in o and field not in t:
                    t[field] = o[field]


# ──────────────────────────────────────────────
# Analysis sections
# ──────────────────────────────────────────────

def print_header(title: str, char: str = "=", width: int = 72) -> None:
    print(f"\n{char * width}")
    print(f"  {title}")
    print(f"{char * width}")


def print_section(title: str, char: str = "-", width: int = 72) -> None:
    print(f"\n{char * width}")
    print(f"  {title}")
    print(f"{char * width}")


def analyze_session_overview(sessions: List[dict]) -> None:
    """High-level summary of each session."""
    print_header("SESSION OVERVIEW")
    print(f"  Total sessions: {len(sessions)}")
    print()
    fmt = "  {idx:>3}  {ts:19s}  {mode:6s}  {dur:>6s}  {coins:16s}  {trades:>3} trades  {fills:>3} filled  {unwinds:>2} unwinds  {skips:>5} skips"
    print(f"  {'#':>3}  {'Timestamp (CST)':19s}  {'Mode':6s}  {'Dur':>6s}  {'Coins':16s}  {'Trd':>10s}  {'Fill':>10s}  {'Unw':>11s}  {'Skips':>11s}")
    print(f"  {'─' * 3}  {'─' * 19}  {'─' * 6}  {'─' * 6}  {'─' * 16}  {'─' * 10}  {'─' * 10}  {'─' * 11}  {'─' * 11}")

    for i, s in enumerate(sessions, 1):
        diag = s["session_diag"] or {}
        ts = _utc_to_cst(diag.get("ts", s["trades"][0]["ts"] if s["trades"] else "?")[:19])
        mode = diag.get("exec_mode", s["trades"][0].get("exec_mode", "?") if s["trades"] else "?")
        coins = ",".join(diag.get("coins", []))
        if not coins and s["trades"]:
            coins = ",".join(sorted(set(t["coin"] for t in s["trades"])))
        n_trades = len(s["trades"])
        n_filled = sum(1 for t in s["trades"] if t.get("exec_both_filled"))
        n_skips = len(s["skips"])
        n_unwinds = sum(1 for t in s["trades"] if t.get("unwind_attempted"))
        dur_s = diag.get("session_duration_s", 0)
        dur_str = f"{dur_s // 60:.0f}m{dur_s % 60:.0f}s" if dur_s else "?"
        print(fmt.format(idx=i, ts=ts, mode=mode, coins=coins,
                         trades=n_trades, fills=n_filled, skips=n_skips,
                         unwinds=n_unwinds, dur=dur_str))


def analyze_fill_rates(all_trades: List[dict]) -> None:
    """Fill rate analysis: both-filled, one-legged, failed."""
    print_section("FILL RATE ANALYSIS")
    if not all_trades:
        print("  No trades to analyze.")
        return

    both = [t for t in all_trades if t.get("exec_both_filled")]
    one_leg = [t for t in all_trades if not t.get("exec_both_filled") and t.get("unwind_attempted")]
    neither = [t for t in all_trades if not t.get("exec_both_filled") and not t.get("unwind_attempted")]

    n = len(all_trades)
    print(f"  Total trades:     {n}")
    print(f"  Both filled:      {len(both):>4} ({safe_div(len(both), n) * 100:.1f}%)")
    print(f"  One-legged:       {len(one_leg):>4} ({safe_div(len(one_leg), n) * 100:.1f}%)  ← required unwind")
    print(f"  Neither filled:   {len(neither):>4} ({safe_div(len(neither), n) * 100:.1f}%)")

    # Per-exchange leg failure breakdown
    leg1_fails = [t for t in all_trades if t.get("exec_leg1_status") not in ("filled",)]
    leg2_fails = [t for t in all_trades if t.get("exec_leg2_status") not in ("filled",)]
    print(f"\n  Leg failures:")
    print(f"    Leg1 (Poly):    {len(leg1_fails)} failures")
    if leg1_fails:
        statuses = Counter(t.get("exec_leg1_status", "?") for t in leg1_fails)
        for status, cnt in statuses.most_common():
            print(f"      {status}: {cnt}")
        errors = Counter(t.get("exec_leg1_error") for t in leg1_fails if t.get("exec_leg1_error"))
        for err, cnt in errors.most_common(5):
            print(f"      err: {err[:70]} ({cnt}x)")

    print(f"    Leg2 (Kalshi):  {len(leg2_fails)} failures")
    if leg2_fails:
        statuses = Counter(t.get("exec_leg2_status", "?") for t in leg2_fails)
        for status, cnt in statuses.most_common():
            print(f"      {status}: {cnt}")
        errors = Counter(t.get("exec_leg2_error") for t in leg2_fails if t.get("exec_leg2_error"))
        for err, cnt in errors.most_common(5):
            print(f"      err: {err[:70]} ({cnt}x)")


def analyze_edge_distribution(all_trades: List[dict]) -> None:
    """Net edge distribution and edge vs outcome correlation."""
    print_section("EDGE DISTRIBUTION")
    if not all_trades:
        print("  No trades.")
        return

    edges = [t["net_edge"] for t in all_trades if t.get("net_edge") is not None]
    if not edges:
        print("  No edge data.")
        return

    print(f"  Trades:   {len(edges)}")
    print(f"  Min:      {pct(min(edges))}")
    print(f"  Max:      {pct(max(edges))}")
    print(f"  Mean:     {pct(sum(edges) / len(edges))}")
    print(f"  Median:   {pct(median(edges))}")
    print(f"  P95:      {pct(p95(edges))}")

    # Histogram: edge buckets
    buckets = [(0.03, 0.04), (0.04, 0.05), (0.05, 0.06), (0.06, 0.07),
               (0.07, 0.08), (0.08, 0.10), (0.10, 0.15)]
    print(f"\n  Edge buckets:")
    for lo, hi in buckets:
        cnt = sum(1 for e in edges if lo <= e < hi)
        bar = "█" * min(cnt, 50)
        print(f"    {pct(lo):>7s}-{pct(hi):<7s}: {cnt:>4} {bar}")

    # Edge vs P&L correlation
    verified = [t for t in all_trades if t.get("actual_pnl_total") is not None and t.get("net_edge") is not None]
    if verified:
        print(f"\n  Edge → Outcome (n={len(verified)}):")
        # Split into edge quartiles
        sorted_v = sorted(verified, key=lambda t: t["net_edge"])
        q = max(1, len(sorted_v) // 4)
        quartiles = [sorted_v[i:i + q] for i in range(0, len(sorted_v), q)]
        for qi, chunk in enumerate(quartiles[:4], 1):
            avg_edge = sum(t["net_edge"] for t in chunk) / len(chunk)
            avg_pnl = sum(t["actual_pnl_total"] for t in chunk) / len(chunk)
            win_rate = safe_div(sum(1 for t in chunk if t["actual_pnl_total"] > 0), len(chunk))
            print(f"    Q{qi} (avg edge {pct(avg_edge)}): avg P&L {usd(avg_pnl)}, win rate {pct(win_rate)}")


def analyze_exchange_reliability(all_trades: List[dict], sessions: List[dict]) -> None:
    """Per-exchange fill rate, latency, and error patterns."""
    print_section("EXCHANGE RELIABILITY")

    # Aggregate from trade rows
    poly_latencies = [t["exec_leg1_latency_ms"] for t in all_trades
                      if t.get("exec_leg1_latency_ms") is not None and t.get("exec_leg1_latency_ms") > 0]
    kalshi_latencies = [t["exec_leg2_latency_ms"] for t in all_trades
                        if t.get("exec_leg2_latency_ms") is not None and t.get("exec_leg2_latency_ms") > 0]

    poly_fills = sum(1 for t in all_trades if t.get("exec_leg1_status") == "filled")
    kalshi_fills = sum(1 for t in all_trades if t.get("exec_leg2_status") == "filled")
    n = len(all_trades) or 1

    print(f"\n  Polymarket:")
    print(f"    Fill rate:      {poly_fills}/{len(all_trades)} ({safe_div(poly_fills, n) * 100:.1f}%)")
    if poly_latencies:
        print(f"    Latency avg:    {ms_fmt(sum(poly_latencies) / len(poly_latencies))}")
        print(f"    Latency median: {ms_fmt(median(poly_latencies))}")
        print(f"    Latency P95:    {ms_fmt(p95(poly_latencies))}")
        print(f"    Latency max:    {ms_fmt(max(poly_latencies))}")

    print(f"\n  Kalshi:")
    print(f"    Fill rate:      {kalshi_fills}/{len(all_trades)} ({safe_div(kalshi_fills, n) * 100:.1f}%)")
    if kalshi_latencies:
        print(f"    Latency avg:    {ms_fmt(sum(kalshi_latencies) / len(kalshi_latencies))}")
        print(f"    Latency median: {ms_fmt(median(kalshi_latencies))}")
        print(f"    Latency P95:    {ms_fmt(p95(kalshi_latencies))}")
        print(f"    Latency max:    {ms_fmt(max(kalshi_latencies))}")

    # Latency trend across sessions
    if len(sessions) > 1:
        print(f"\n  Latency trend (per session):")
        for i, s in enumerate(sessions, 1):
            pl = [t["exec_leg1_latency_ms"] for t in s["trades"]
                  if t.get("exec_leg1_latency_ms") and t["exec_leg1_latency_ms"] > 0]
            kl = [t["exec_leg2_latency_ms"] for t in s["trades"]
                  if t.get("exec_leg2_latency_ms") and t["exec_leg2_latency_ms"] > 0]
            p_avg = f"{sum(pl) / len(pl):.0f}ms" if pl else "N/A"
            k_avg = f"{sum(kl) / len(kl):.0f}ms" if kl else "N/A"
            print(f"    Session {i}: Poly {p_avg:>8s}  Kalshi {k_avg:>8s}  ({len(s['trades'])} trades)")

    # Error catalog
    poly_errors = Counter(t.get("exec_leg1_error") for t in all_trades if t.get("exec_leg1_error"))
    kalshi_errors = Counter(t.get("exec_leg2_error") for t in all_trades if t.get("exec_leg2_error"))
    if poly_errors or kalshi_errors:
        print(f"\n  Error catalog:")
        if poly_errors:
            print(f"    Poly errors ({sum(poly_errors.values())} total):")
            for err, cnt in poly_errors.most_common(10):
                print(f"      [{cnt:>3}x] {err[:80]}")
        if kalshi_errors:
            print(f"    Kalshi errors ({sum(kalshi_errors.values())} total):")
            for err, cnt in kalshi_errors.most_common(10):
                print(f"      [{cnt:>3}x] {err[:80]}")


def analyze_coin_breakdown(all_trades: List[dict]) -> None:
    """Per-coin performance: trade count, fill rate, edge, P&L, unwinds."""
    print_section("PER-COIN BREAKDOWN")
    if not all_trades:
        print("  No trades.")
        return

    coins = sorted(set(t["coin"] for t in all_trades))
    for coin in coins:
        rows = [t for t in all_trades if t["coin"] == coin]
        filled = [t for t in rows if t.get("exec_both_filled")]
        unwinds = [t for t in rows if t.get("unwind_attempted")]
        verified = [t for t in rows if t.get("actual_pnl_total") is not None]
        edges = [t["net_edge"] for t in rows if t.get("net_edge") is not None]

        print(f"\n  {coin}:")
        print(f"    Trades:     {len(rows)}")
        print(f"    Fill rate:  {len(filled)}/{len(rows)} ({safe_div(len(filled), len(rows)) * 100:.1f}%)")
        print(f"    Unwinds:    {len(unwinds)} ({safe_div(len(unwinds), len(rows)) * 100:.1f}%)")
        if edges:
            print(f"    Avg edge:   {pct(sum(edges) / len(edges))}")
        if verified:
            pnl = sum(t["actual_pnl_total"] for t in verified)
            wins = sum(1 for t in verified if t["actual_pnl_total"] > 0)
            print(f"    P&L:        {usd(pnl)}")
            print(f"    Win rate:   {wins}/{len(verified)} ({safe_div(wins, len(verified)) * 100:.1f}%)")
            mismatches = sum(1 for t in verified if not t.get("hedge_consistent", True))
            if mismatches:
                print(f"    Hedge mismatch: {mismatches} ← BOTH legs lost")

        # Direction bias
        up_trades = [t for t in rows if t.get("poly_side") == "UP"]
        dn_trades = [t for t in rows if t.get("poly_side") == "DOWN"]
        print(f"    Direction:  UP={len(up_trades)}  DOWN={len(dn_trades)}")


def analyze_skip_reasons(sessions: List[dict], coin_filter: Optional[str] = None) -> None:
    """Aggregated skip reasons across all sessions."""
    print_section("SKIP REASON ANALYSIS")

    all_skips: List[dict] = []
    for s in sessions:
        all_skips.extend(s["skips"])

    if coin_filter:
        all_skips = [sk for sk in all_skips if sk.get("coin") == coin_filter]

    if not all_skips:
        print("  No skips recorded.")
        return

    reasons = Counter(sk.get("reason", "?") for sk in all_skips)
    total = sum(reasons.values())
    print(f"  Total skips: {total}")
    print()
    for reason, cnt in reasons.most_common():
        bar = "█" * min(int(cnt / total * 50), 50)
        print(f"    {reason:40s} {cnt:>6} ({cnt / total * 100:5.1f}%)  {bar}")

    # Per-coin skip breakdown
    coins = sorted(set(sk.get("coin", "?") for sk in all_skips))
    if len(coins) > 1:
        print(f"\n  Skip reasons by coin:")
        for coin in coins:
            coin_skips = [sk for sk in all_skips if sk.get("coin") == coin]
            coin_reasons = Counter(sk.get("reason", "?") for sk in coin_skips)
            top3 = coin_reasons.most_common(3)
            top_str = ", ".join(f"{r}={c}" for r, c in top3)
            print(f"    {coin:4s} ({len(coin_skips):>5} skips): {top_str}")

    # Skip trend across sessions
    if len(sessions) > 1:
        print(f"\n  Skip trend (per session):")
        for i, s in enumerate(sessions, 1):
            skips = s["skips"]
            if coin_filter:
                skips = [sk for sk in skips if sk.get("coin") == coin_filter]
            n = len(skips)
            trades = len(s["trades"])
            opp_rate = safe_div(trades, trades + n) * 100
            top_reason = Counter(sk.get("reason", "?") for sk in skips).most_common(1)
            top_str = f" (top: {top_reason[0][0]}={top_reason[0][1]})" if top_reason else ""
            print(f"    Session {i}: {n:>5} skips, {trades:>3} trades → {opp_rate:.1f}% opp rate{top_str}")


def analyze_timing(all_trades: List[dict]) -> None:
    """Time-of-day patterns, window remaining, and scan timing."""
    print_section("TIMING ANALYSIS")
    if not all_trades:
        print("  No trades.")
        return

    # Time-of-day buckets
    hour_trades: Dict[int, List[dict]] = defaultdict(list)
    for t in all_trades:
        h = bucket_hour(t.get("ts", ""))
        if h is not None:
            hour_trades[h].append(t)

    if hour_trades:
        print(f"  Trades by CST hour:")
        for h in sorted(hour_trades.keys()):
            rows = hour_trades[h]
            verified = [t for t in rows if t.get("actual_pnl_total") is not None]
            avg_edge = sum(t["net_edge"] for t in rows) / len(rows) if rows else 0
            pnl_str = ""
            if verified:
                pnl = sum(t["actual_pnl_total"] for t in verified)
                pnl_str = f"  P&L {usd(pnl)}"
            bar = "█" * min(len(rows), 40)
            print(f"    {h:02d}:00  {len(rows):>3} trades  avg_edge {pct(avg_edge)}{pnl_str}  {bar}")

    # Window remaining at trade time
    remaining = [t.get("remaining_s") or t.get("window_remaining_s") for t in all_trades]
    remaining = [r for r in remaining if r is not None and r > 0]
    if remaining:
        print(f"\n  Window remaining at trade time:")
        print(f"    Min:    {min(remaining):.0f}s")
        print(f"    Max:    {max(remaining):.0f}s")
        print(f"    Mean:   {sum(remaining) / len(remaining):.0f}s")
        print(f"    Median: {median(remaining):.0f}s")

    # Scan timing
    scan_times = [t["scan_ms"] for t in all_trades if t.get("scan_ms")]
    if scan_times:
        print(f"\n  Scan timing:")
        print(f"    Avg:    {ms_fmt(sum(scan_times) / len(scan_times))}")
        print(f"    P95:    {ms_fmt(p95(scan_times))}")
        print(f"    Max:    {ms_fmt(max(scan_times))}")


def analyze_slippage(all_trades: List[dict]) -> None:
    """Slippage analysis: per-exchange, edge erosion."""
    print_section("SLIPPAGE ANALYSIS")

    slip_rows = [t for t in all_trades if t.get("exec_slippage_poly") is not None]
    if not slip_rows:
        print("  No slippage data (paper mode or no fills).")
        return

    poly_slips = [t["exec_slippage_poly"] for t in slip_rows]
    kalshi_slips = [t["exec_slippage_kalshi"] for t in slip_rows]
    total_slips = [p + k for p, k in zip(poly_slips, kalshi_slips)]

    print(f"  Trades with slippage data: {len(slip_rows)}")
    print(f"\n  Per-contract slippage:")
    print(f"    Poly:     avg {sum(poly_slips) / len(poly_slips):+.4f}  median {median(poly_slips):+.4f}  max {max(poly_slips):+.4f}")
    print(f"    Kalshi:   avg {sum(kalshi_slips) / len(kalshi_slips):+.4f}  median {median(kalshi_slips):+.4f}  max {max(kalshi_slips):+.4f}")
    print(f"    Combined: avg {sum(total_slips) / len(total_slips):+.4f}  median {median(total_slips):+.4f}  max {max(total_slips):+.4f}")

    # Slippage in bps
    bps = [t.get("exec_slippage_total_bps", 0) for t in slip_rows]
    if any(b != 0 for b in bps):
        print(f"\n  Slippage (bps): avg {sum(bps) / len(bps):.1f}  P95 {p95(bps):.1f}  max {max(bps):.1f}")

    # Edge eaten by slippage
    edge_eaten = [t for t in slip_rows
                  if (t["exec_slippage_poly"] + t["exec_slippage_kalshi"]) > t.get("net_edge", 1)]
    if edge_eaten:
        print(f"\n  ⚠ Slippage exceeded net edge in {len(edge_eaten)}/{len(slip_rows)} trades ({safe_div(len(edge_eaten), len(slip_rows)) * 100:.1f}%)")


def analyze_unwinds(all_trades: List[dict], sessions: List[dict]) -> None:
    """Unwind pattern analysis."""
    print_section("UNWIND ANALYSIS")

    unwind_trades = [t for t in all_trades if t.get("unwind_attempted")]
    if not unwind_trades:
        print("  No unwinds recorded.")
        return

    successful = [t for t in unwind_trades if t.get("unwind_success")]
    failed = [t for t in unwind_trades if not t.get("unwind_success")]
    losses = [t["unwind_loss"] for t in unwind_trades if t.get("unwind_loss") is not None]

    print(f"  Total unwinds:    {len(unwind_trades)}")
    print(f"  Successful:       {len(successful)} ({safe_div(len(successful), len(unwind_trades)) * 100:.1f}%)")
    print(f"  Failed:           {len(failed)} ({safe_div(len(failed), len(unwind_trades)) * 100:.1f}%)")
    if losses:
        print(f"  Total loss:       {usd(sum(losses))}")
        print(f"  Avg loss:         {usd(sum(losses) / len(losses))}")
        print(f"  Worst loss:       {usd(max(losses))}")

    # Per-exchange breakdown
    by_exchange = Counter(t.get("unwind_exchange", "?") for t in unwind_trades)
    if by_exchange:
        print(f"\n  By exchange: {', '.join(f'{e}={c}' for e, c in by_exchange.most_common())}")

    # Per-coin breakdown
    by_coin = Counter(t.get("coin", "?") for t in unwind_trades)
    if by_coin:
        print(f"  By coin:     {', '.join(f'{c}={n}' for c, n in by_coin.most_common())}")

    # Clustering: are unwinds happening in bursts?
    if len(sessions) > 1:
        print(f"\n  Unwind distribution across sessions:")
        for i, s in enumerate(sessions, 1):
            n_unwinds = sum(1 for t in s["trades"] if t.get("unwind_attempted"))
            n_trades = len(s["trades"])
            rate = safe_div(n_unwinds, n_trades) * 100
            bar = "█" * n_unwinds
            print(f"    Session {i}: {n_unwinds:>2}/{n_trades:>3} trades ({rate:5.1f}%)  {bar}")


def analyze_pnl(all_trades: List[dict], sessions: List[dict]) -> None:
    """P&L patterns: running total, win streaks, hedge consistency."""
    print_section("P&L ANALYSIS")

    verified = [t for t in all_trades if t.get("actual_pnl_total") is not None]
    if not verified:
        print("  No verified outcomes yet.")
        return

    wins = [t for t in verified if t["actual_pnl_total"] > 0]
    losses = [t for t in verified if t["actual_pnl_total"] < 0]
    breakevens = [t for t in verified if t["actual_pnl_total"] == 0]

    total_pnl = sum(t["actual_pnl_total"] for t in verified)
    print(f"  Verified trades:  {len(verified)}")
    print(f"  Win/Loss/BE:      {len(wins)}/{len(losses)}/{len(breakevens)}")
    print(f"  Win rate:         {safe_div(len(wins), len(verified)) * 100:.1f}%")
    print(f"  Total P&L:        {usd(total_pnl)}")
    print(f"  Avg P&L/trade:    {usd(safe_div(total_pnl, len(verified)))}")
    if wins:
        print(f"  Best trade:       {usd(max(t['actual_pnl_total'] for t in verified))}")
    if losses:
        print(f"  Worst trade:      {usd(min(t['actual_pnl_total'] for t in verified))}")

    # Running P&L + max drawdown
    running = 0.0
    peak = 0.0
    max_dd = 0.0
    consec_losses = 0
    max_consec_losses = 0
    consec_wins = 0
    max_consec_wins = 0
    for t in verified:
        running += t["actual_pnl_total"]
        peak = max(peak, running)
        dd = peak - running
        max_dd = max(max_dd, dd)
        if t["actual_pnl_total"] < 0:
            consec_losses += 1
            max_consec_losses = max(max_consec_losses, consec_losses)
            consec_wins = 0
        elif t["actual_pnl_total"] > 0:
            consec_wins += 1
            max_consec_wins = max(max_consec_wins, consec_wins)
            consec_losses = 0
        else:
            consec_losses = 0
            consec_wins = 0

    print(f"  Max drawdown:     {usd(-max_dd)}")
    print(f"  Max consec wins:  {max_consec_wins}")
    print(f"  Max consec losses:{max_consec_losses}")

    # Hedged vs unhedged P&L
    hedged_total = sum(t.get("hedged_pnl", 0) for t in verified)
    unhedged_total = sum(t.get("unhedged_pnl", 0) for t in verified)
    if any(t.get("hedged_pnl") is not None for t in verified):
        print(f"\n  P&L breakdown:")
        print(f"    Hedged:         {usd(hedged_total)}")
        print(f"    Unhedged:       {usd(unhedged_total)}")

    # Hedge consistency
    mismatches = [t for t in verified if not t.get("hedge_consistent", True)]
    if mismatches:
        print(f"\n  ⚠ HEDGE MISMATCHES: {len(mismatches)} trades where BOTH legs lost")
        for t in mismatches:
            print(f"    [{_utc_to_cst(t.get('ts', '?'))}] {t.get('coin', '?')} | "
                  f"P&L {usd(t['actual_pnl_total'])} | edge {pct(t.get('net_edge', 0))} | "
                  f"strike_div {t.get('strike_spot_divergence_pct', '?')}%")

    # P&L per session
    if len(sessions) > 1:
        print(f"\n  P&L by session:")
        for i, s in enumerate(sessions, 1):
            sv = [t for t in s["trades"] if t.get("actual_pnl_total") is not None]
            if sv:
                spnl = sum(t["actual_pnl_total"] for t in sv)
                sw = sum(1 for t in sv if t["actual_pnl_total"] > 0)
                print(f"    Session {i}: {usd(spnl):>12s}  ({sw}/{len(sv)} wins)")
            else:
                print(f"    Session {i}: (no verified outcomes)")


def analyze_strike_divergence(all_trades: List[dict]) -> None:
    """Strike-spot divergence patterns — the main both-legs-lose risk factor."""
    print_section("STRIKE-SPOT DIVERGENCE")

    trades_with_div = [t for t in all_trades if t.get("strike_spot_divergence_pct") is not None]
    if not trades_with_div:
        print("  No strike divergence data.")
        return

    divs = [t["strike_spot_divergence_pct"] for t in trades_with_div]
    print(f"  Trades with data: {len(trades_with_div)}")
    print(f"  Min:    {min(divs):.4f}%")
    print(f"  Max:    {max(divs):.4f}%")
    print(f"  Mean:   {sum(divs) / len(divs):.4f}%")
    print(f"  Median: {median(divs):.4f}%")

    # Divergence vs outcome
    verified_div = [t for t in trades_with_div if t.get("actual_pnl_total") is not None]
    if verified_div:
        # Split into high/low divergence
        med = median(divs)
        low_div = [t for t in verified_div if t["strike_spot_divergence_pct"] <= med]
        high_div = [t for t in verified_div if t["strike_spot_divergence_pct"] > med]

        if low_div:
            low_wr = safe_div(sum(1 for t in low_div if t["actual_pnl_total"] > 0), len(low_div))
            low_pnl = sum(t["actual_pnl_total"] for t in low_div)
            print(f"\n  Low divergence (≤{med:.4f}%):  n={len(low_div)}  win_rate={pct(low_wr)}  P&L={usd(low_pnl)}")
        if high_div:
            high_wr = safe_div(sum(1 for t in high_div if t["actual_pnl_total"] > 0), len(high_div))
            high_pnl = sum(t["actual_pnl_total"] for t in high_div)
            print(f"  High divergence (>{med:.4f}%): n={len(high_div)}  win_rate={pct(high_wr)}  P&L={usd(high_pnl)}")


def analyze_book_depth(all_trades: List[dict]) -> None:
    """Poly book depth patterns at trade time."""
    print_section("POLYMARKET BOOK DEPTH")

    depth_trades = [t for t in all_trades if t.get("poly_book_levels") is not None]
    if not depth_trades:
        print("  No depth data (websocket may not have been active).")
        return

    levels = [t["poly_book_levels"] for t in depth_trades]
    sizes = [t.get("poly_book_size", 0) for t in depth_trades if t.get("poly_book_size")]
    notionals = [t.get("poly_book_notional_usd", 0) for t in depth_trades if t.get("poly_book_notional_usd")]

    print(f"  Trades with depth: {len(depth_trades)}")
    if levels:
        print(f"  Book levels:  min={min(levels)}  max={max(levels)}  avg={sum(levels) / len(levels):.1f}")
    if sizes:
        print(f"  Book size:    min={min(sizes):.0f}  max={max(sizes):.0f}  avg={sum(sizes) / len(sizes):.0f} contracts")
    if notionals:
        print(f"  Notional:     min=${min(notionals):.0f}  max=${max(notionals):.0f}  avg=${sum(notionals) / len(notionals):.0f}")

    # Depth vs outcome
    verified_depth = [t for t in depth_trades if t.get("actual_pnl_total") is not None]
    if verified_depth and notionals:
        med_not = median(notionals)
        thin = [t for t in verified_depth if (t.get("poly_book_notional_usd") or 0) <= med_not]
        thick = [t for t in verified_depth if (t.get("poly_book_notional_usd") or 0) > med_not]
        if thin:
            thin_wr = safe_div(sum(1 for t in thin if t["actual_pnl_total"] > 0), len(thin))
            print(f"\n  Thin books (≤${med_not:.0f} notional): n={len(thin)}  win_rate={pct(thin_wr)}")
        if thick:
            thick_wr = safe_div(sum(1 for t in thick if t["actual_pnl_total"] > 0), len(thick))
            print(f"  Thick books (>${med_not:.0f} notional): n={len(thick)}  win_rate={pct(thick_wr)}")

    # Staleness
    stale_trades = [t for t in depth_trades if (t.get("poly_ws_staleness_s") or 0) > 10]
    if stale_trades:
        print(f"\n  ⚠ {len(stale_trades)} trades had stale WS data (>10s)")


def analyze_red_flags(all_trades: List[dict], sessions: List[dict]) -> None:
    """Surface recurring problems that need attention."""
    print_section("RED FLAGS & WARNINGS")
    flags: List[str] = []

    # 1. High unwind rate
    unwind_rate = safe_div(sum(1 for t in all_trades if t.get("unwind_attempted")), len(all_trades))
    if unwind_rate > 0.1:
        flags.append(f"High unwind rate: {pct(unwind_rate)} of trades require unwinding")

    # 2. Failed unwinds
    failed_unwinds = [t for t in all_trades if t.get("unwind_attempted") and not t.get("unwind_success")]
    if failed_unwinds:
        flags.append(f"Failed unwinds: {len(failed_unwinds)} trades left with open exposure")

    # 3. Hedge mismatches
    mismatches = [t for t in all_trades
                  if t.get("actual_pnl_total") is not None and not t.get("hedge_consistent", True)]
    if mismatches:
        flags.append(f"Hedge mismatches: {len(mismatches)} trades where BOTH legs lost (strike-spot issue?)")

    # 4. Slippage eating edge
    slip_eaten = [t for t in all_trades
                  if t.get("exec_slippage_poly") is not None
                  and (t["exec_slippage_poly"] + t.get("exec_slippage_kalshi", 0)) > t.get("net_edge", 1)]
    if slip_eaten:
        flags.append(f"Edge erosion: {len(slip_eaten)} trades had slippage > net edge")

    # 5. Stale data
    stale = [t for t in all_trades if (t.get("poly_ws_staleness_s") or 0) > 30]
    if stale:
        flags.append(f"Stale WS data: {len(stale)} trades had Poly WS data >30s old")

    # 6. High latency events
    high_lat = [t for t in all_trades
                if (t.get("exec_leg1_latency_ms") or 0) > 5000
                or (t.get("exec_leg2_latency_ms") or 0) > 5000]
    if high_lat:
        flags.append(f"High latency: {len(high_lat)} trades had a leg >5s to fill")

    # 7. Kill switches triggered
    total_kills = sum(len(s["kill_switches"]) for s in sessions)
    if total_kills:
        reasons = Counter()
        for s in sessions:
            for k in s["kill_switches"]:
                reasons[k.get("reason", "?")] += 1
        flags.append(f"Kill switches: {total_kills} triggered ({', '.join(f'{r}={c}' for r, c in reasons.most_common(3))})")

    # 8. Negative total P&L
    verified = [t for t in all_trades if t.get("actual_pnl_total") is not None]
    if verified:
        total_pnl = sum(t["actual_pnl_total"] for t in verified)
        if total_pnl < 0:
            flags.append(f"Negative P&L: {usd(total_pnl)} across {len(verified)} verified trades")

    # 9. Low opportunity rate
    total_skips = sum(len(s["skips"]) for s in sessions)
    total_trades = len(all_trades)
    opp_rate = safe_div(total_trades, total_trades + total_skips)
    if opp_rate < 0.01 and total_skips > 100:
        flags.append(f"Very low opportunity rate: {pct(opp_rate)} ({total_trades} trades / {total_skips} skips)")

    # 10. Poly fill rate < Kalshi
    if all_trades:
        poly_fr = safe_div(sum(1 for t in all_trades if t.get("exec_leg1_status") == "filled"), len(all_trades))
        kalshi_fr = safe_div(sum(1 for t in all_trades if t.get("exec_leg2_status") == "filled"), len(all_trades))
        if poly_fr < kalshi_fr - 0.05:
            flags.append(f"Poly fill rate ({pct(poly_fr)}) significantly lower than Kalshi ({pct(kalshi_fr)})")

    if not flags:
        print("  ✓ No red flags detected.")
    else:
        for i, flag in enumerate(flags, 1):
            print(f"  {i}. {flag}")


def output_json(sessions: List[dict], all_trades: List[dict]) -> None:
    """Output machine-readable summary for programmatic consumption."""
    verified = [t for t in all_trades if t.get("actual_pnl_total") is not None]
    filled = [t for t in all_trades if t.get("exec_both_filled")]
    unwinds = [t for t in all_trades if t.get("unwind_attempted")]

    report = {
        "sessions": len(sessions),
        "total_trades": len(all_trades),
        "total_filled": len(filled),
        "total_unwinds": len(unwinds),
        "fill_rate": safe_div(len(filled), len(all_trades)),
        "unwind_rate": safe_div(len(unwinds), len(all_trades)),
    }
    if verified:
        report["verified_trades"] = len(verified)
        report["total_pnl"] = round(sum(t["actual_pnl_total"] for t in verified), 4)
        report["win_rate"] = round(safe_div(sum(1 for t in verified if t["actual_pnl_total"] > 0), len(verified)), 4)

    edges = [t["net_edge"] for t in all_trades if t.get("net_edge") is not None]
    if edges:
        report["avg_edge"] = round(sum(edges) / len(edges), 6)
        report["median_edge"] = round(median(edges), 6)

    coins = sorted(set(t["coin"] for t in all_trades))
    report["per_coin"] = {}
    for coin in coins:
        rows = [t for t in all_trades if t["coin"] == coin]
        cv = [t for t in rows if t.get("actual_pnl_total") is not None]
        report["per_coin"][coin] = {
            "trades": len(rows),
            "filled": sum(1 for t in rows if t.get("exec_both_filled")),
            "unwinds": sum(1 for t in rows if t.get("unwind_attempted")),
            "avg_edge": round(safe_div(sum(t.get("net_edge", 0) for t in rows), len(rows)), 6),
        }
        if cv:
            report["per_coin"][coin]["pnl"] = round(sum(t["actual_pnl_total"] for t in cv), 4)
            report["per_coin"][coin]["win_rate"] = round(safe_div(sum(1 for t in cv if t["actual_pnl_total"] > 0), len(cv)), 4)

    # Skip reason summary
    all_skips: List[dict] = []
    for s in sessions:
        all_skips.extend(s["skips"])
    reasons = Counter(sk.get("reason", "?") for sk in all_skips)
    report["skip_reasons"] = dict(reasons.most_common())

    print(json.dumps(report, indent=2))


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Cross-session diagnostics analyzer for Polyshi")
    parser.add_argument("--log-dir", default="logs", help="Path to logs directory (default: logs)")
    parser.add_argument("-n", type=int, metavar="N", help="Number of recent sessions to analyze (interactive shortcut)")
    parser.add_argument("--last", type=int, help="Only analyze the last N sessions")
    parser.add_argument("--latest", action="store_true", help="Only analyze the most recent session (shortcut for --last 1)")
    parser.add_argument("--live", action="store_true", help="Only analyze live-mode sessions")
    parser.add_argument("--paper", action="store_true", help="Only analyze paper-mode sessions")
    parser.add_argument("--coin", type=str, help="Filter to a specific coin (e.g., BTC)")
    parser.add_argument("--since-update", action="store_true",
                        help="Only analyze sessions run after the last code update (git HEAD)")
    parser.add_argument("--json", action="store_true", help="Output machine-readable JSON instead of report")
    args = parser.parse_args()

    sessions = load_sessions(args.log_dir)

    # -n and --latest are shorthands for --last
    if args.n:
        args.last = args.n
    if args.latest:
        args.last = 1

    # Filter to sessions since last code update
    if args.since_update:
        update_ts = _get_last_update_ts()
        if update_ts:
            before_count = len(sessions)
            sessions = [s for s in sessions
                        if (_session_start_ts(s) or "") >= update_ts]
            print(f"  Filtering to sessions after last update: {update_ts}Z "
                  f"({len(sessions)}/{before_count} sessions)")
        else:
            print("Warning: could not determine last update time from git", file=sys.stderr)

    # Filter by mode
    if args.live:
        sessions = [s for s in sessions if (s["session_diag"] or {}).get("exec_mode") == "live"
                     or any(t.get("exec_mode") == "live" for t in s["trades"])]
    if args.paper:
        sessions = [s for s in sessions if (s["session_diag"] or {}).get("exec_mode") == "paper"
                     or any(t.get("exec_mode") == "paper" for t in s["trades"])]

    # Trim to last N
    if args.last:
        sessions = sessions[-args.last:]

    if not sessions:
        print("No sessions match the filter criteria.", file=sys.stderr)
        sys.exit(1)

    # Merge outcomes into trade rows
    for s in sessions:
        merge_outcomes_into_trades(s)

    # Collect all trade rows across sessions
    all_trades: List[dict] = []
    for s in sessions:
        all_trades.extend(s["trades"])

    # Optional coin filter
    if args.coin:
        all_trades = [t for t in all_trades if t.get("coin") == args.coin.upper()]

    if args.json:
        output_json(sessions, all_trades)
        return

    n_markets = len(set(t.get("poly_ref", "") for t in all_trades if t.get("poly_ref")))

    print(f"\n{'╔' + '═' * 70 + '╗'}")
    print(f"{'║'} {'POLYSHI CROSS-SESSION DIAGNOSTICS':^68s} {'║'}")
    print(f"{'║'} {f'{len(sessions)} sessions · {n_markets} markets · {len(all_trades)} trades':^68s} {'║'}")
    if args.coin:
        print(f"{'║'} {f'Filtered to {args.coin.upper()}':^68s} {'║'}")
    print(f"{'╚' + '═' * 70 + '╝'}")

    analyze_session_overview(sessions)
    analyze_fill_rates(all_trades)
    analyze_edge_distribution(all_trades)
    analyze_exchange_reliability(all_trades, sessions)
    analyze_coin_breakdown(all_trades)
    analyze_skip_reasons(sessions, coin_filter=args.coin.upper() if args.coin else None)
    analyze_timing(all_trades)
    analyze_slippage(all_trades)
    analyze_unwinds(all_trades, sessions)
    analyze_pnl(all_trades, sessions)
    analyze_strike_divergence(all_trades)
    analyze_book_depth(all_trades)
    analyze_red_flags(all_trades, sessions)

    print(f"\n{'═' * 72}")
    print(f"  Run with --json for machine-readable output")
    print(f"  Run with --coin BTC to focus on a single coin")
    print(f"  Run with -n 5 to quickly pick the last N sessions")
    print(f"  Run with --last 3 to analyze only recent sessions")
    print(f"  Run with --latest to analyze only the most recent session")
    print(f"  Run with --since-update for sessions after last git pull")
    print(f"{'═' * 72}\n")


if __name__ == "__main__":
    main()
