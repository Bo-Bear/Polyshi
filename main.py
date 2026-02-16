import base64
import json
import os
import time
import math
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import re

VERSION = "1.1.35"
VERSION_DATE = "2026-02-16 23:15 UTC"

import requests
from dotenv import load_dotenv

try:
    from websocket import WebSocketApp as _WebSocketApp
    _HAS_WS_CLIENT = True
except ImportError:
    _HAS_WS_CLIENT = False

# Kalshi RSA auth (needed for live trading)
try:
    from cryptography.hazmat.primitives import serialization, hashes
    from cryptography.hazmat.primitives.asymmetric import padding as rsa_padding
    _HAS_CRYPTO = True
except ImportError:
    _HAS_CRYPTO = False

# Polymarket CLOB client (needed for live trading)
try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import OrderArgs, OrderType, TradeParams
    from py_clob_client.order_builder.constants import BUY, SELL
    _HAS_CLOB_CLIENT = True
except ImportError:
    _HAS_CLOB_CLIENT = False


# -----------------------------
# Config
# -----------------------------
load_dotenv()

SCAN_SLEEP_SECONDS = float(os.getenv("SCAN_SLEEP_SECONDS", "3"))
MAX_TEST_TRADES = int(os.getenv("MAX_TEST_TRADES", "5"))
WINDOW_ALIGN_TOLERANCE_SECONDS = int(os.getenv("WINDOW_ALIGN_TOLERANCE_SECONDS", "10"))
MIN_LEG_NOTIONAL = float(os.getenv("MIN_LEG_NOTIONAL", "25"))  # $ minimum liquidity per leg
USE_VWAP_DEPTH = os.getenv("USE_VWAP_DEPTH", "true").lower() == "true"

# -----------------------------
# Execution safeguards
# -----------------------------
# Minimum net edge to accept a trade (must exceed expected execution slippage of ~2-3c)
MIN_NET_EDGE = float(os.getenv("MIN_NET_EDGE", "0.06"))  # 6%

# Maximum net edge — skip outliers that are likely stale/bad data, not real opportunities
MAX_NET_EDGE = float(os.getenv("MAX_NET_EDGE", "0.15"))  # 15%

# Session drawdown limit — auto-stop if portfolio drops this much from session start ($)
MAX_SESSION_DRAWDOWN = float(os.getenv("MAX_SESSION_DRAWDOWN", "60.0"))  # $60

# Maximum seconds to remain unhedged — if Poly fills but Kalshi fails, unwind Poly after this
MAX_UNHEDGED_SECONDS = float(os.getenv("MAX_UNHEDGED_SECONDS", "10.0"))  # 10 seconds

# Minimum seconds remaining in the window before we'll trade (need time to fill both legs)
MIN_WINDOW_REMAINING_S = float(os.getenv("MIN_WINDOW_REMAINING_S", "90"))  # 90 seconds (last 1.5 min blocked)

# Maximum spread (ask_up + ask_down - 1) we'll accept; wider means unreliable pricing
MAX_SPREAD = float(os.getenv("MAX_SPREAD", "0.10"))  # 10%

# Reject prices in extreme ranges where outcome is nearly decided (hedging is risky)
PRICE_FLOOR = float(os.getenv("PRICE_FLOOR", "0.10"))   # skip legs below 10c
PRICE_CEILING = float(os.getenv("PRICE_CEILING", "0.98"))  # skip legs priced above 98c

# Session-level circuit breaker: stop scanning after this many consecutive no-trade scans
# (may indicate stale data or broken feeds)
MAX_CONSECUTIVE_SKIPS = int(os.getenv("MAX_CONSECUTIVE_SKIPS", "500"))

# Maximum gross cost we'll accept (tighter than 1.0 to leave room for execution slippage)
MAX_TOTAL_COST = float(os.getenv("MAX_TOTAL_COST", "0.98"))

# Maximum allowed divergence between implied probabilities across exchanges.
# Large divergence signals mismatched strikes (Kalshi uses fixed $, Poly uses relative).
# If |kalshi_up_prob - poly_up_prob| > this, skip the trade.
MAX_PROB_DIVERGENCE = float(os.getenv("MAX_PROB_DIVERGENCE", "0.15"))  # 15 percentage points

# Maximum allowed divergence between Kalshi strike and spot price (as a fraction).
# Kalshi uses a fixed $ strike; Polymarket uses "up/down from window start."
# When strike ≠ spot, both hedge legs can lose. 0.15% = ~$100 on BTC.
MAX_STRIKE_SPOT_DIVERGENCE = float(os.getenv("MAX_STRIKE_SPOT_DIVERGENCE", "0.003"))

# Minimum account balance — stop session if either exchange drops below this ($)
MIN_ACCOUNT_BALANCE = float(os.getenv("MIN_ACCOUNT_BALANCE", "50.0"))  # $50

# -----------------------------
# Fees (paper-trade model)
# -----------------------------
# Size we assume for fee calculations (both venues). Polymarket fee table is for 100 shares. :contentReference[oaicite:3]{index=3}
PAPER_CONTRACTS = float(os.getenv("PAPER_CONTRACTS", "7"))

# Cap trade size to this fraction of Poly book depth (0.5 = use at most 50% of visible depth)
POLY_DEPTH_CAP_RATIO = float(os.getenv("POLY_DEPTH_CAP_RATIO", "0.5"))

# Minimum contracts after depth-capping (skip trade if book can't support this many)
MIN_CONTRACTS = int(os.getenv("MIN_CONTRACTS", "3"))

# Pre-scan minimum: skip coin entirely if Poly book has fewer than this many contracts.
# Should be >= PAPER_CONTRACTS / POLY_DEPTH_CAP_RATIO so the depth-cap won't reduce size.
MIN_POLY_DEPTH_CONTRACTS = int(os.getenv("MIN_POLY_DEPTH_CONTRACTS",
                                          str(int(PAPER_CONTRACTS / POLY_DEPTH_CAP_RATIO))))

# Toggle fee modeling
INCLUDE_POLY_FEES = os.getenv("INCLUDE_POLY_FEES", "true").lower() == "true"
INCLUDE_KALSHI_FEES = os.getenv("INCLUDE_KALSHI_FEES", "true").lower() == "true"

# Optional extras (set to 0 for now; can model later)
EXTRA_WITHDRAW_FEE_USD = float(os.getenv("EXTRA_WITHDRAW_FEE_USD", "0"))  # withdrawal processor fees etc.
EXTRA_GAS_USD = float(os.getenv("EXTRA_GAS_USD", "0"))  # blockchain gas, bridging, etc.

# Amortize one-time extras over N trades (e.g., if you withdraw once after many trades)
AMORTIZE_EXTRAS_OVER_TRADES = int(os.getenv("AMORTIZE_EXTRAS_OVER_TRADES", "1"))

# Expected execution slippage budget per contract (subtracted from net edge).
# Accounts for LIVE_PRICE_BUFFER on both legs + adverse market movement.
# This makes net_edge reflect post-slippage expected profit.
EXECUTION_SLIPPAGE_BUDGET = float(os.getenv("EXECUTION_SLIPPAGE_BUDGET", "0.02"))  # 2 cents/contract


# Kalshi public base + endpoints are documented here:
# https://docs.kalshi.com/getting_started/quick_start_market_data
KALSHI_BASE = os.getenv("KALSHI_BASE", "https://api.elections.kalshi.com/trade-api/v2")

# Polymarket Gamma (public, no auth)
POLY_GAMMA_BASE = os.getenv("POLY_GAMMA_BASE", "https://gamma-api.polymarket.com")
POLY_CLOB_BASE = os.getenv("POLY_CLOB_BASE", "https://clob.polymarket.com")


# These are the (known) Kalshi series tickers for 15-minute crypto markets.
# If Kalshi ever renames them, you can update these without changing logic.
KALSHI_SERIES = {
    "BTC": os.getenv("KALSHI_SERIES_BTC", "KXBTC15M"),
    "ETH": os.getenv("KALSHI_SERIES_ETH", "KXETH15M"),
    "SOL": os.getenv("KALSHI_SERIES_SOL", "KXSOL15M"),
    "XRP": os.getenv("KALSHI_SERIES_XRP", "KXXRP15M"),
}

# Polymarket: We'll discover via tag=crypto + recurrence=15M, then select by title prefix.
AVAILABLE_COINS = ["BTC", "ETH", "SOL", "XRP"]
POLY_TITLE_PREFIX = {
    "BTC": "Bitcoin Up or Down",
    "ETH": "Ethereum Up or Down",
    "SOL": "Solana Up or Down",
    "XRP": "XRP Up or Down",
}

# Output
LOG_DIR = os.getenv("LOG_DIR", "logs")

# Execution mode: "paper" (default) or "live"
EXEC_MODE = os.getenv("EXEC_MODE", "paper").lower()  # "paper" or "live"

# -----------------------------
# Exchange credentials (live trading only)
# -----------------------------
# Kalshi: RSA key-based authentication
#   Generate at https://kalshi.com/account/profile -> API Keys
KALSHI_API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "")      # Key ID string
KALSHI_PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH", "")  # Path to RSA .pem file

# Polymarket: Ethereum wallet + CLOB API
#   Private key of your Polygon wallet that holds USDC.e
POLY_PRIVATE_KEY = os.getenv("POLY_PRIVATE_KEY", "")        # Hex private key (0x...)
# Signature type: 0=EOA (MetaMask/hardware), 1=Magic/email wallet, 2=browser/Gnosis Safe
POLY_SIGNATURE_TYPE = int(os.getenv("POLY_SIGNATURE_TYPE", "0"))
# Funder address (required for signature_type 1 or 2; leave blank for type 0)
POLY_FUNDER_ADDRESS = os.getenv("POLY_FUNDER_ADDRESS", "")

# Order placement config
ORDER_TIMEOUT_S = float(os.getenv("ORDER_TIMEOUT_S", "15"))  # max seconds to wait for fill
ORDER_POLL_INTERVAL_S = float(os.getenv("ORDER_POLL_INTERVAL_S", "0.5"))  # polling interval
# Price buffer added to limit orders in live mode to improve fill rate.
# CLOB gives price improvement, so actual fill price may be lower than limit.
LIVE_PRICE_BUFFER = float(os.getenv("LIVE_PRICE_BUFFER", "0.02"))  # 2 cents
# Maximum slippage allowed above planned price on Polymarket.
# If the best ask has moved more than this above planned_price, skip the fill attempt.
POLY_MAX_SLIPPAGE = float(os.getenv("POLY_MAX_SLIPPAGE", "0.02"))  # 2 cents

# Poly orderbook-aware retry config (used when Kalshi fills first)
POLY_FILL_MAX_RETRIES = int(os.getenv("POLY_FILL_MAX_RETRIES", "5"))
POLY_FILL_RETRY_TIMEOUT_S = float(os.getenv("POLY_FILL_RETRY_TIMEOUT_S", "3"))  # per-attempt timeout

# Per-coin limits
MAX_TRADES_PER_COIN = int(os.getenv("MAX_TRADES_PER_COIN", "5"))
MAX_TRADES_PER_COIN_PER_WINDOW = int(os.getenv("MAX_TRADES_PER_COIN_PER_WINDOW", "6"))
MAX_CONSECUTIVE_UNWINDS = int(os.getenv("MAX_CONSECUTIVE_UNWINDS", "3"))

# Guaranteed trades + avg-edge gating: first N trades per coin per window are guaranteed,
# then subsequent trades require the coin's rolling avg edge >= this threshold.
GUARANTEED_TRADES_PER_COIN = int(os.getenv("GUARANTEED_TRADES_PER_COIN", "3"))
AVG_EDGE_GATE = float(os.getenv("AVG_EDGE_GATE", "0.045"))  # 4.5%

# 2-phase window startup: block trading for the first N seconds of each 15m window
# to let orderbooks settle. Phase 1 (0-90s) = Kalshi connections + strikes load,
# Phase 2 (90-120s) = Poly scrape stabilizes, trading opens at 120s.
WINDOW_STARTUP_DELAY_S = float(os.getenv("WINDOW_STARTUP_DELAY_S", "120"))  # 120 seconds


# -----------------------------
# HTTP session + caches
# -----------------------------
_http_session: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    global _http_session
    if _http_session is None:
        _http_session = requests.Session()
    return _http_session


_clob_working_route_idx: Optional[int] = None

# -----------------------------
# Kalshi authenticated session (live mode)
# -----------------------------
_kalshi_private_key = None  # loaded RSA key object


def _load_kalshi_private_key():
    """Load RSA private key from PEM file for Kalshi API signing."""
    global _kalshi_private_key
    if _kalshi_private_key is not None:
        return _kalshi_private_key
    if not KALSHI_PRIVATE_KEY_PATH:
        raise RuntimeError("KALSHI_PRIVATE_KEY_PATH not set — cannot authenticate with Kalshi")
    if not _HAS_CRYPTO:
        raise RuntimeError("'cryptography' package required for Kalshi live trading: pip install cryptography")
    with open(KALSHI_PRIVATE_KEY_PATH, "rb") as f:
        _kalshi_private_key = serialization.load_pem_private_key(f.read(), password=None)
    return _kalshi_private_key


def _kalshi_sign(method: str, path: str) -> dict:
    """Build Kalshi auth headers with RSA-PSS signature."""
    pk = _load_kalshi_private_key()
    timestamp_ms = str(int(time.time() * 1000))
    # Strip query params for signing
    path_no_query = path.split("?")[0]
    message = (timestamp_ms + method.upper() + path_no_query).encode("utf-8")
    signature = pk.sign(
        message,
        rsa_padding.PSS(
            mgf=rsa_padding.MGF1(hashes.SHA256()),
            salt_length=rsa_padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return {
        "Content-Type": "application/json",
        "KALSHI-ACCESS-KEY": KALSHI_API_KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("utf-8"),
        "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
    }


def _kalshi_auth_get(path: str, timeout: int = 10) -> dict:
    """Authenticated GET to Kalshi API."""
    url = KALSHI_BASE + path
    # Sign with full URL path (Kalshi expects /trade-api/v2/... in signature)
    full_path = "/trade-api/v2" + path
    headers = _kalshi_sign("GET", full_path)
    r = _get_session().get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _kalshi_auth_post(path: str, body: dict, timeout: int = 10) -> dict:
    """Authenticated POST to Kalshi API."""
    url = KALSHI_BASE + path
    # Sign with full URL path (Kalshi expects /trade-api/v2/... in signature)
    full_path = "/trade-api/v2" + path
    headers = _kalshi_sign("POST", full_path)
    r = _get_session().post(url, json=body, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _kalshi_auth_delete(path: str, timeout: int = 10) -> dict:
    """Authenticated DELETE to Kalshi API (used for order cancellation)."""
    url = KALSHI_BASE + path
    full_path = "/trade-api/v2" + path
    headers = _kalshi_sign("DELETE", full_path)
    r = _get_session().delete(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()


# -----------------------------
# Polymarket on-chain approvals + CLOB client (live mode)
# -----------------------------
# Polymarket contract addresses on Polygon
_POLY_USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
_POLY_CTF = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
_POLY_SPENDERS = [
    "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",   # CTF Exchange
    "0xC5d563A36AE78145C45a50134d48A1215220f80a",   # Neg Risk CTF Exchange
    "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296",   # Neg Risk Adapter
]

# ERC-20 approve(address,uint256) selector: 0x095ea7b3
# ERC-1155 setApprovalForAll(address,bool) selector: 0xa22cb465
_POLYGON_RPC = "https://polygon-rpc.com"


def _polygon_rpc(method: str, params: list, _retries: int = 3) -> dict:
    """Raw JSON-RPC call to Polygon with retry on rate limits."""
    for attempt in range(_retries + 1):
        r = _get_session().post(_POLYGON_RPC, json={
            "jsonrpc": "2.0", "id": 1, "method": method, "params": params,
        }, timeout=15)
        data = r.json()
        if "error" in data:
            err_msg = str(data["error"].get("message", ""))
            if "rate limit" in err_msg.lower() and attempt < _retries:
                wait = 2 ** attempt * 5  # 5s, 10s, 20s
                print(f"  [rpc] Rate limited, retrying in {wait}s...")
                time.sleep(wait)
                continue
            raise RuntimeError(f"RPC error: {data['error']}")
        return data.get("result")
    raise RuntimeError("RPC retries exhausted")


def _send_approval_tx(from_addr: str, from_key: str, to_addr: str, calldata: str,
                      nonce: int, gas_price: str) -> str:
    """Sign and send a single approval tx, wait for receipt. Returns tx hash."""
    from eth_account import Account

    # Simulate first to catch revert reasons
    sim = _polygon_rpc("eth_call", [{"from": from_addr, "to": to_addr, "data": calldata}, "latest"])
    # eth_call returns result or throws RPC error (caught above)

    tx = {
        "to": to_addr,
        "value": 0,
        "gas": 100000,
        "gasPrice": int(gas_price, 16),
        "nonce": nonce,
        "chainId": 137,
        "data": calldata,
    }
    signed = Account.sign_transaction(tx, from_key)
    raw = signed.raw_transaction.hex()
    if not raw.startswith("0x"):
        raw = "0x" + raw
    tx_hash = _polygon_rpc("eth_sendRawTransaction", [raw])

    # Poll for receipt (up to 120s)
    for _ in range(120):
        receipt = _polygon_rpc("eth_getTransactionReceipt", [tx_hash])
        if receipt is not None:
            status = int(receipt.get("status", "0x0"), 16)
            gas_used = int(receipt.get("gasUsed", "0x0"), 16)
            if status != 1:
                raise RuntimeError(
                    f"tx reverted (gasUsed={gas_used}): {tx_hash} "
                    f"| to={to_addr} | data={calldata[:20]}...")
            return tx_hash
        time.sleep(1)
    raise RuntimeError(f"tx not confirmed after 120s: {tx_hash}")


def _check_erc20_allowance(owner: str, spender: str, token: str) -> int:
    """Check ERC-20 allowance on-chain. Returns raw allowance as int."""
    owner_padded = owner.lower().replace("0x", "").zfill(64)
    spender_padded = spender.lower().replace("0x", "").zfill(64)
    calldata = "0xdd62ed3e" + owner_padded + spender_padded  # allowance(address,address)
    result = _polygon_rpc("eth_call", [{"to": token, "data": calldata}, "latest"])
    return int(result, 16) if result else 0


def _check_erc1155_approval(owner: str, operator: str, token: str) -> bool:
    """Check ERC-1155 isApprovedForAll on-chain."""
    owner_padded = owner.lower().replace("0x", "").zfill(64)
    operator_padded = operator.lower().replace("0x", "").zfill(64)
    calldata = "0xe985e9c5" + owner_padded + operator_padded  # isApprovedForAll(address,address)
    result = _polygon_rpc("eth_call", [{"to": token, "data": calldata}, "latest"])
    return int(result, 16) != 0 if result else False


def _approve_poly_contracts():
    """Submit on-chain approve() txns for Polymarket exchange contracts.
    Uses eth_account + raw JSON-RPC (no web3 dependency).
    Requires POL for gas on Polygon.
    Skips contracts that already have sufficient allowance."""
    from eth_account import Account

    acct = Account.from_key(POLY_PRIVATE_KEY)
    addr = acct.address

    nonce_hex = _polygon_rpc("eth_getTransactionCount", [addr, "latest"])
    nonce = int(nonce_hex, 16)
    raw_gas = _polygon_rpc("eth_gasPrice", [])
    # Add 20% buffer for faster inclusion during congestion
    gas_price_int = int(int(raw_gas, 16) * 1.2)
    gas_price = hex(gas_price_int)

    max_uint256 = (2**256 - 1)
    max_uint_hex = hex(max_uint256)[2:].zfill(64)
    # Threshold: consider "approved" if allowance >= 1000 USDC.e (1e9 raw)
    MIN_ALLOWANCE = 1_000_000_000

    labels = ["CTF Exchange", "Neg Risk Exchange", "Neg Risk Adapter"]
    tx_count = 0

    for i, spender in enumerate(_POLY_SPENDERS):
        spender_padded = spender.lower().replace("0x", "").zfill(64)

        # ERC-20: check existing allowance before approving
        existing_allowance = _check_erc20_allowance(addr, spender, _POLY_USDC_E)
        if existing_allowance >= MIN_ALLOWANCE:
            print(f"         USDC.e -> {labels[i]}: already approved (skipped)")
        else:
            calldata = "0x095ea7b3" + spender_padded + max_uint_hex
            _send_approval_tx(addr, POLY_PRIVATE_KEY, _POLY_USDC_E, calldata, nonce, gas_price)
            nonce += 1
            tx_count += 1
            print(f"         Approved USDC.e -> {labels[i]}")

        # ERC-1155: check existing approval before approving
        already_approved = _check_erc1155_approval(addr, spender, _POLY_CTF)
        if already_approved:
            print(f"         CTF    -> {labels[i]}: already approved (skipped)")
        else:
            true_padded = "0" * 63 + "1"
            calldata = "0xa22cb465" + spender_padded + true_padded
            _send_approval_tx(addr, POLY_PRIVATE_KEY, _POLY_CTF, calldata, nonce, gas_price)
            nonce += 1
            tx_count += 1
            print(f"         Approved CTF    -> {labels[i]}")

    return tx_count


# NegRiskAdapter address (kept for reference)
_POLY_NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"

# WrappedCollateral (WCOL) — NegRisk markets use this as collateral, not USDC.e directly
_POLY_WCOL = "0x3A3BD7bb9528E159577F7C2e685CC81A765002E2"

# Function selectors (keccak256 of signature, first 4 bytes)
_CTF_REDEEM_SELECTOR = "0x01b7037c"    # redeemPositions(address,bytes32,bytes32,uint256[])
_WCOL_UNWRAP_SELECTOR = "0x39f47693"   # unwrap(address,uint256)
_ERC20_BALANCE_SELECTOR = "0x70a08231" # balanceOf(address)
_PAYOUT_DENOM_SELECTOR = "0xdd34de67"  # payoutDenominator(bytes32)
_BALANCE_OF_SELECTOR = "0x00fdd58e"    # balanceOf(address,uint256) — ERC-1155


def _get_market_info_for_token(token_id: str) -> Tuple[Optional[str], bool]:
    """Get conditionId and negRisk flag for a Polymarket token_id via Gamma API.
    Returns (conditionId, is_neg_risk)."""
    try:
        url = f"https://gamma-api.polymarket.com/markets"
        r = _get_session().get(url, params={"clob_token_ids": str(token_id)}, timeout=10)
        r.raise_for_status()
        markets = r.json()
        if markets and len(markets) > 0:
            m = markets[0]
            cid = m.get("condition_id") or m.get("conditionId")
            neg_risk = bool(m.get("negRisk", False))
            if cid:
                return cid, neg_risk
    except Exception as e:
        print(f"  [redeem] Failed to get market info for token {str(token_id)[:20]}...: {e}")
    return None, False


def _get_condition_id_for_token(token_id: str) -> Optional[str]:
    """Get conditionId for a Polymarket token_id via Gamma API."""
    cid, _ = _get_market_info_for_token(token_id)
    return cid


def _get_ctf_balance(owner: str, token_id: str) -> int:
    """Check ERC-1155 CTF token balance on-chain. Returns raw balance."""
    owner_padded = owner.lower().replace("0x", "").zfill(64)
    id_hex = hex(int(token_id))[2:].zfill(64)
    calldata = _BALANCE_OF_SELECTOR + owner_padded + id_hex
    try:
        result = _polygon_rpc("eth_call", [{"to": _POLY_CTF, "data": calldata}, "latest"])
        return int(result, 16) if result else 0
    except Exception:
        return 0


_BALANCE_OF_BATCH_SELECTOR = "0x4e1273f4"  # balanceOfBatch(address[],uint256[])


def _get_ctf_balances_batch(owner: str, token_ids: List[str], batch_size: int = 40) -> Dict[str, int]:
    """Check multiple ERC-1155 balances in minimal RPC calls using balanceOfBatch.
    Returns {token_id: balance}. Falls back to individual calls on error."""
    results: Dict[str, int] = {}
    owner_hex = owner.lower().replace("0x", "").zfill(64)

    for batch_start in range(0, len(token_ids), batch_size):
        batch = token_ids[batch_start:batch_start + batch_size]
        n = len(batch)

        # ABI encode: balanceOfBatch(address[] accounts, uint256[] ids)
        # Dynamic arrays: offset_accounts(0x40) + offset_ids + accounts_array + ids_array
        accounts_offset = hex(64)[2:].zfill(64)  # 0x40 = 64 bytes
        ids_offset = hex(64 + 32 + n * 32)[2:].zfill(64)  # past accounts array
        accounts_len = hex(n)[2:].zfill(64)
        accounts_data = "".join(owner_hex for _ in batch)
        ids_len = hex(n)[2:].zfill(64)
        ids_data = "".join(hex(int(tid))[2:].zfill(64) for tid in batch)

        calldata = (_BALANCE_OF_BATCH_SELECTOR + accounts_offset + ids_offset +
                    accounts_len + accounts_data + ids_len + ids_data)

        try:
            result = _polygon_rpc("eth_call", [{"to": _POLY_CTF, "data": calldata}, "latest"])
            if result and result != "0x":
                # Decode: offset(32) + length(32) + n × uint256
                raw = result.replace("0x", "")
                # First 64 chars = offset to array, next 64 = array length, then values
                data_start = 128  # skip offset + length (64+64 hex chars)
                for i, tid in enumerate(batch):
                    val_hex = raw[data_start + i * 64: data_start + (i + 1) * 64]
                    results[tid] = int(val_hex, 16) if val_hex else 0
            else:
                for tid in batch:
                    results[tid] = 0
        except Exception as e:
            print(f"  [redeem] Batch balance check failed, falling back to individual: {e}")
            for tid in batch:
                results[tid] = _get_ctf_balance(owner, tid)
                time.sleep(1)

        if batch_start + batch_size < len(token_ids):
            time.sleep(2)  # pace between batches

    return results


def _is_condition_resolved(condition_id: str) -> bool:
    """Check if a condition has been resolved on the CTF contract."""
    cid_hex = condition_id.lower().replace("0x", "").zfill(64)
    calldata = _PAYOUT_DENOM_SELECTOR + cid_hex
    try:
        result = _polygon_rpc("eth_call", [{"to": _POLY_CTF, "data": calldata}, "latest"])
        return int(result, 16) > 0 if result else False
    except Exception:
        return False


def _sign_and_send_tx(addr: str, to: str, calldata: str, nonce: int,
                      gas_price_int: int, gas: int = 200000) -> Optional[str]:
    """Sign, send, and wait for a transaction. Retries with higher gas on
    'replacement transaction underpriced'. Returns tx hash or None."""
    from eth_account import Account

    for attempt in range(3):
        # Bump gas 50% on each retry to replace stuck pending txs
        effective_gas_price = int(gas_price_int * (1.5 ** attempt))

        tx = {
            "to": to,
            "value": 0,
            "gas": gas,
            "gasPrice": effective_gas_price,
            "nonce": nonce,
            "chainId": 137,
            "data": calldata,
        }
        signed = Account.sign_transaction(tx, POLY_PRIVATE_KEY)
        raw = signed.raw_transaction.hex()
        if not raw.startswith("0x"):
            raw = "0x" + raw

        try:
            tx_hash = _polygon_rpc("eth_sendRawTransaction", [raw])
        except RuntimeError as e:
            err = str(e)
            if "replacement transaction underpriced" in err and attempt < 2:
                print(f"  [tx] Pending tx at nonce {nonce}, bumping gas ({attempt+1}/3)...")
                continue
            elif "already known" in err.lower() and attempt < 2:
                # Tx was already submitted — just wait for it
                continue
            raise

        # Poll for receipt (up to 60s — Polygon blocks are ~2s)
        for _ in range(60):
            try:
                receipt = _polygon_rpc("eth_getTransactionReceipt", [tx_hash])
            except Exception:
                receipt = None
            if receipt is not None:
                status = int(receipt.get("status", "0x0"), 16)
                if status != 1:
                    print(f"  [redeem] Tx reverted: {tx_hash}")
                    return None
                return tx_hash
            time.sleep(2)
        print(f"  [redeem] Tx not confirmed after 120s: {tx_hash}")
        return None  # Don't retry timeouts — tx may still confirm later

    return None


def _redeem_positions(condition_id: str, yes_amount: int, no_amount: int,
                      nonce: int = -1, neg_risk: bool = False) -> Tuple[Optional[str], int, int]:
    """Redeem resolved CTF positions. Handles both standard and NegRisk markets.
    Standard: CTF.redeemPositions(USDC.e, 0x0, conditionId, [1,2]) → USDC.e directly
    NegRisk:  CTF.redeemPositions(WCOL, 0x0, conditionId, [1,2]) → WCOL → unwrap to USDC.e
    Returns (tx_hash_or_None, nonces_consumed, payout_micro_usdc)."""
    from eth_account import Account

    acct = Account.from_key(POLY_PRIVATE_KEY)
    addr = acct.address

    # Check if condition is resolved first
    if not _is_condition_resolved(condition_id):
        print(f"  [redeem] Market not yet resolved — skipping")
        return None, 0, 0

    if nonce < 0:
        nonce_hex = _polygon_rpc("eth_getTransactionCount", [addr, "pending"])
        nonce = int(nonce_hex, 16)
    raw_gas = _polygon_rpc("eth_gasPrice", [])
    base_gas = int(raw_gas, 16)
    gas_price_int = int(base_gas * 2)  # 2x multiplier for fast inclusion
    # Cap at 100k gwei — at 300k gas that's only 0.03 POL (~$0.01) per tx
    # Node cap is 1 POL = 3.3M gwei, so 100k is well under
    max_gas_price = int(100_000e9)
    gas_price_int = min(gas_price_int, max_gas_price)
    # Floor at 30 gwei (minimum for Polygon)
    gas_price_int = max(gas_price_int, int(30e9))
    print(f"  [redeem] Gas: base={base_gas/1e9:.0f} gwei, using={gas_price_int/1e9:.0f} gwei, nonce={nonce}")

    # Choose collateral token based on market type
    if neg_risk:
        collateral_addr = _POLY_WCOL
        collateral_label = "WCOL (NegRisk)"
    else:
        collateral_addr = _POLY_USDC_E
        collateral_label = "USDC.e (standard)"
    print(f"  [redeem] Using collateral: {collateral_label}")

    # --- Step 1: CTF.redeemPositions(address collateralToken, bytes32 parentCollectionId,
    #                                  bytes32 conditionId, uint256[] indexSets) ---
    cid_hex = condition_id.lower().replace("0x", "").zfill(64)
    collateral_padded = collateral_addr.lower().replace("0x", "").zfill(64)
    parent_coll_id = "00" * 32  # bytes32(0) — Polymarket doesn't use nested conditions
    # Dynamic array offset: 4 static words × 32 bytes = 128 = 0x80
    array_offset = hex(0x80)[2:].zfill(64)
    array_length = hex(2)[2:].zfill(64)
    index_set_1 = hex(1)[2:].zfill(64)  # outcome 0 (YES/UP)
    index_set_2 = hex(2)[2:].zfill(64)  # outcome 1 (NO/DOWN)

    calldata = (_CTF_REDEEM_SELECTOR + collateral_padded + parent_coll_id +
                cid_hex + array_offset + array_length + index_set_1 + index_set_2)

    # Simulate step 1
    try:
        _polygon_rpc("eth_call", [{"from": addr, "to": _POLY_CTF, "data": calldata}, "latest"])
    except Exception as e:
        print(f"  [redeem] Simulation failed: {e}")
        return None, 0, 0

    # Snapshot balance before redeem to detect payout
    bal_calldata = _ERC20_BALANCE_SELECTOR + addr.lower().replace("0x", "").zfill(64)
    pre_usdc = 0
    pre_wcol = 0
    if not neg_risk:
        try:
            pre_bal_data = _polygon_rpc("eth_call", [{"to": _POLY_USDC_E, "data": bal_calldata}, "latest"])
            pre_usdc = int(pre_bal_data, 16) if pre_bal_data else 0
        except Exception:
            pass
    else:
        try:
            pre_wcol_data = _polygon_rpc("eth_call", [{"to": _POLY_WCOL, "data": bal_calldata}, "latest"])
            pre_wcol = int(pre_wcol_data, 16) if pre_wcol_data else 0
        except Exception:
            pass

    # Send step 1
    tx_hash = _sign_and_send_tx(addr, _POLY_CTF, calldata, nonce, gas_price_int, gas=300000)
    if not tx_hash:
        return None, 0, 0
    nonces_used = 1

    if not neg_risk:
        # Standard market: USDC.e is received directly, no unwrap needed
        # Check USDC.e balance change to report payout
        time.sleep(1)
        try:
            post_bal_data = _polygon_rpc("eth_call", [{"to": _POLY_USDC_E, "data": bal_calldata}, "latest"])
            post_usdc = int(post_bal_data, 16) if post_bal_data else 0
        except Exception:
            post_usdc = pre_usdc
        payout = max(0, post_usdc - pre_usdc)
        if payout > 0:
            print(f"  [redeem] CTF redeem OK → tx={tx_hash} → +${payout / 1e6:.4f} USDC.e")
        else:
            print(f"  [redeem] CTF redeem OK → tx={tx_hash} (losing side burned for $0)")
        return tx_hash, nonces_used, payout

    # --- NegRisk only: Step 2 — WCOL.unwrap(address _to, uint256 _amount) ---
    print(f"  [redeem] Step 1 OK (CTF redeem) → tx={tx_hash}")
    time.sleep(1)
    # Check WCOL balance change to detect winning payout
    try:
        post_wcol_data = _polygon_rpc("eth_call", [{"to": _POLY_WCOL, "data": bal_calldata}, "latest"])
        post_wcol = int(post_wcol_data, 16) if post_wcol_data else 0
    except Exception:
        post_wcol = 0

    wcol_gained = max(0, post_wcol - pre_wcol)
    if wcol_gained <= 0:
        print(f"  [redeem] No WCOL received (losing side tokens burned for $0)")
        return tx_hash, nonces_used, 0

    print(f"  [redeem] WCOL gained: +${wcol_gained / 1e6:.4f} USDC.e equivalent")

    # Unwrap WCOL to USDC.e
    to_padded = addr.lower().replace("0x", "").zfill(64)
    amount_padded = hex(wcol_gained)[2:].zfill(64)
    unwrap_calldata = _WCOL_UNWRAP_SELECTOR + to_padded + amount_padded

    try:
        _polygon_rpc("eth_call", [{"from": addr, "to": _POLY_WCOL, "data": unwrap_calldata}, "latest"])
    except Exception as e:
        print(f"  [redeem] Step 2 simulation failed (unwrap): {e}")
        print(f"  [redeem] WCOL tokens are still in your wallet — can retry later")
        return tx_hash, nonces_used, 0

    unwrap_hash = _sign_and_send_tx(addr, _POLY_WCOL, unwrap_calldata, nonce + 1, gas_price_int)
    if unwrap_hash:
        print(f"  [redeem] Step 2 OK (unwrap) → tx={unwrap_hash} → +${wcol_gained / 1e6:.4f} USDC.e")
        nonces_used += 1
    else:
        print(f"  [redeem] Unwrap tx failed — WCOL still in wallet, retry later")

    return tx_hash, nonces_used, wcol_gained


def redeem_poly_positions(trade_rows: List[dict]) -> int:
    """Redeem resolved Polymarket positions from completed trades.
    Looks up conditionId, checks token balances, and calls NegRiskAdapter.
    Returns number of successful redemptions."""
    from eth_account import Account

    if POLY_SIGNATURE_TYPE != 0:
        return 0  # Only EOA mode needs manual redemption

    if not POLY_PRIVATE_KEY:
        return 0

    acct = Account.from_key(POLY_PRIVATE_KEY)
    addr = acct.address

    # Collect unique (up_token_id, down_token_id) pairs from trades
    seen_tokens: set = set()
    token_pairs: List[Tuple[str, str]] = []
    for row in trade_rows:
        up_tid = row.get("poly_up_token_id") or ""
        down_tid = row.get("poly_down_token_id") or ""
        if up_tid and down_tid and up_tid not in seen_tokens:
            seen_tokens.add(up_tid)
            token_pairs.append((up_tid, down_tid))

    if not token_pairs:
        return 0

    print(f"\n  [redeem] Checking {len(token_pairs)} market(s) for unredeemed positions...")
    redeemed = 0

    for up_tid, down_tid in token_pairs:
        # Check balances
        up_bal = _get_ctf_balance(addr, up_tid)
        down_bal = _get_ctf_balance(addr, down_tid)

        if up_bal == 0 and down_bal == 0:
            continue  # No tokens to redeem

        # Get conditionId and market type from Gamma API
        condition_id, neg_risk = _get_market_info_for_token(up_tid)
        if not condition_id:
            condition_id, neg_risk = _get_market_info_for_token(down_tid)
        if not condition_id:
            print(f"  [redeem] Could not find conditionId — skipping")
            continue

        print(f"  [redeem] Found {up_bal} UP + {down_bal} DOWN tokens, conditionId={condition_id[:16]}...")
        tx_hash, _, _payout = _redeem_positions(condition_id, up_bal, down_bal, neg_risk=neg_risk)
        if tx_hash:
            print(f"  [redeem] Redeemed! tx={tx_hash}")
            redeemed += 1
        else:
            print(f"  [redeem] Redemption failed")

    return redeemed


_poly_clob_client = None  # ClobClient instance


def _get_poly_clob_client():
    """Initialize and return the Polymarket CLOB client for order placement."""
    global _poly_clob_client
    if _poly_clob_client is not None:
        return _poly_clob_client
    if not _HAS_CLOB_CLIENT:
        raise RuntimeError("'py-clob-client' package required for Poly live trading: pip install py-clob-client")
    if not POLY_PRIVATE_KEY:
        raise RuntimeError("POLY_PRIVATE_KEY not set — cannot authenticate with Polymarket")

    kwargs = {
        "host": POLY_CLOB_BASE,
        "key": POLY_PRIVATE_KEY,
        "chain_id": 137,  # Polygon mainnet
        "signature_type": POLY_SIGNATURE_TYPE,
    }
    if POLY_SIGNATURE_TYPE in (1, 2) and POLY_FUNDER_ADDRESS:
        kwargs["funder"] = POLY_FUNDER_ADDRESS

    client = ClobClient(**kwargs)
    # Derive L2 HMAC API credentials from wallet signature
    client.set_api_creds(client.create_or_derive_api_creds())
    _poly_clob_client = client
    return _poly_clob_client


# Spot price cache (one fetch per scan, shared across coins)
_spot_prices: Dict[str, float] = {}
_spot_prices_ts: float = 0.0

COINGECKO_IDS = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana", "XRP": "ripple"}

def fetch_spot_prices() -> Dict[str, float]:
    """Fetch current spot prices from CoinGecko. Returns {coin: usd_price}."""
    global _spot_prices, _spot_prices_ts
    # Cache for 10 seconds to avoid hammering
    if time.monotonic() - _spot_prices_ts < 10 and _spot_prices:
        return _spot_prices
    ids = ",".join(COINGECKO_IDS.values())
    try:
        r = _get_session().get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": ids, "vs_currencies": "usd"},
            timeout=5,
        )
        r.raise_for_status()
        data = r.json()
        result: Dict[str, float] = {}
        for coin, cg_id in COINGECKO_IDS.items():
            if cg_id in data and "usd" in data[cg_id]:
                result[coin] = float(data[cg_id]["usd"])
        _spot_prices = result
        _spot_prices_ts = time.monotonic()
    except Exception:
        pass  # return stale cache on failure
    return _spot_prices


# -----------------------------
# Polymarket CLOB WebSocket (real-time orderbook cache)
# -----------------------------
class _PolyOrderbookWS:
    """Background WebSocket for real-time Polymarket CLOB orderbook data.

    Subscribes to token IDs and maintains an in-memory ask-side book.
    Falls back gracefully if websocket-client is not installed.
    """

    WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def __init__(self):
        self._asks: Dict[str, Dict[float, float]] = {}  # token_id -> {price: size}
        self._best_ask: Dict[str, float] = {}  # token_id -> best ask price (from price_change)
        self._lock = threading.Lock()
        self._ws: Optional[object] = None
        self._thread: Optional[threading.Thread] = None
        self._subscribed_ids: set = set()
        self._ready_ids: set = set()  # IDs that have received at least one book snapshot
        self._connected = threading.Event()
        self._running = False
        self._cache_hits = 0
        self._cache_misses = 0
        self._last_update_ts: Dict[str, float] = {}  # token_id -> monotonic timestamp of last update

    def start(self):
        if not _HAS_WS_CLIENT:
            return
        self._running = True
        self._thread = threading.Thread(target=self._connect_loop, daemon=True)
        self._thread.start()

    def _connect_loop(self):
        while self._running:
            try:
                ws = _WebSocketApp(
                    self.WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws = ws
                ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception:
                pass
            self._connected.clear()
            if self._running:
                time.sleep(0.5)

    def _on_open(self, ws):
        self._connected.set()
        if self._subscribed_ids:
            self._send_initial_subscribe(list(self._subscribed_ids))

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
        except (json.JSONDecodeError, TypeError):
            return

        # Handle list messages (some endpoints send arrays)
        if isinstance(data, list):
            for item in data:
                self._handle_event(item)
        else:
            self._handle_event(data)

    def _handle_event(self, data: dict):
        if not isinstance(data, dict):
            return
        event_type = data.get("event_type")

        if event_type == "book":
            asset_id = data.get("asset_id")
            if not asset_id:
                return
            asks_raw = data.get("asks", [])
            asks_dict: Dict[float, float] = {}
            for lvl in asks_raw:
                try:
                    p = float(lvl["price"])
                    s = float(lvl["size"])
                    if p > 0 and s > 0:
                        asks_dict[p] = s
                except (KeyError, ValueError, TypeError):
                    continue
            with self._lock:
                self._asks[asset_id] = asks_dict
                self._ready_ids.add(asset_id)
                self._last_update_ts[asset_id] = time.monotonic()

        elif event_type == "price_change":
            with self._lock:
                for pc in data.get("price_changes", []):
                    asset_id = pc.get("asset_id")
                    if not asset_id or asset_id not in self._ready_ids:
                        continue
                    # Use best_ask field (Sept 2025+) to update best ask price
                    best_ask_str = pc.get("best_ask")
                    if best_ask_str is not None:
                        try:
                            self._best_ask[asset_id] = float(best_ask_str)
                        except (ValueError, TypeError):
                            pass
                    self._last_update_ts[asset_id] = time.monotonic()
                    # Update ask-side book level from SELL trades
                    side = str(pc.get("side", "")).upper()
                    if side == "SELL" and asset_id in self._asks:
                        try:
                            p = float(pc["price"])
                            s = float(pc["size"])
                        except (KeyError, ValueError, TypeError):
                            continue
                        if s > 0:
                            self._asks[asset_id][p] = s
                        else:
                            self._asks[asset_id].pop(p, None)

        elif event_type == "best_bid_ask":
            # Direct best-ask update (requires custom_feature_enabled)
            asset_id = data.get("asset_id")
            if asset_id:
                try:
                    ba = float(data["best_ask"])
                    with self._lock:
                        self._best_ask[asset_id] = ba
                        self._last_update_ts[asset_id] = time.monotonic()
                        if asset_id not in self._ready_ids:
                            self._ready_ids.add(asset_id)
                            self._asks.setdefault(asset_id, {})
                except (KeyError, ValueError, TypeError):
                    pass

    def _on_error(self, ws, error):
        pass

    def _on_close(self, ws, close_status, close_msg):
        self._connected.clear()

    def subscribe(self, token_ids: List[str]):
        """Subscribe to orderbook updates for given token IDs."""
        new_ids = [tid for tid in token_ids if tid not in self._subscribed_ids]
        if not new_ids:
            return
        self._subscribed_ids.update(new_ids)
        if self._connected.is_set() and self._ws:
            self._send_dynamic_subscribe(new_ids)

    def _send_initial_subscribe(self, token_ids: List[str]):
        """Send subscription on initial connection (uses 'type' key)."""
        if self._ws:
            try:
                self._ws.send(json.dumps({
                    "assets_ids": token_ids,
                    "type": "market",
                    "custom_feature_enabled": True,
                }))
            except Exception:
                pass

    def _send_dynamic_subscribe(self, token_ids: List[str]):
        """Send subscription after connection is already open (uses 'operation' key)."""
        if self._ws:
            try:
                self._ws.send(json.dumps({
                    "assets_ids": token_ids,
                    "operation": "subscribe",
                }))
            except Exception:
                pass

    def get_asks(self, token_id: str) -> Optional[List[Tuple[float, float]]]:
        """Get cached asks for a token. Returns None if not in cache yet."""
        with self._lock:
            if token_id not in self._ready_ids:
                self._cache_misses += 1
                return None
            self._cache_hits += 1
            asks_dict = self._asks.get(token_id, {})
            return sorted([(p, s) for p, s in asks_dict.items() if s > 0], key=lambda x: x[0])

    def get_and_reset_stats(self) -> Tuple[int, int]:
        """Return (hits, misses) since last call and reset counters."""
        with self._lock:
            hits, misses = self._cache_hits, self._cache_misses
            self._cache_hits = self._cache_misses = 0
            return hits, misses

    def get_staleness_s(self, token_id: str) -> Optional[float]:
        """Seconds since last WS update for this token. None if never updated."""
        with self._lock:
            ts = self._last_update_ts.get(token_id)
            if ts is None:
                return None
            return time.monotonic() - ts

    def get_book_depth(self, token_id: str) -> Optional[dict]:
        """Return depth summary for a token's cached ask-side book."""
        with self._lock:
            if token_id not in self._asks:
                return None
            asks = self._asks[token_id]
            if not asks:
                return {"levels": 0, "total_size": 0, "total_notional_usd": 0.0}
            levels = sorted(asks.items())
            total_size = sum(s for _, s in levels)
            total_notional = sum(p * s for p, s in levels)
            return {
                "levels": len(levels),
                "total_size": int(total_size),
                "total_notional_usd": round(total_notional, 2),
                "best_ask": levels[0][0],
                "worst_ask": levels[-1][0],
            }

    def stop(self):
        self._running = False
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass


_poly_ws: Optional[_PolyOrderbookWS] = None


# -----------------------------
# Models
# -----------------------------
@dataclass
class KalshiMarketQuote:
    ticker: str
    title: str
    yes_ask: float  # dollars
    no_ask: float   # dollars
    close_ts: datetime  # window close timestamp (UTC)
    strike: Optional[str] = None  # best-effort



@dataclass
class PolyMarketQuote:
    event_slug: str
    market_slug: str
    title: str
    up_price: float    # dollars
    down_price: float  # dollars
    end_ts: datetime   # window end timestamp (UTC)
    description: str = ""  # resolution criteria text (may contain reference price)
    up_token_id: str = ""   # CLOB token ID for UP outcome
    down_token_id: str = "" # CLOB token ID for DOWN outcome


@dataclass
class HedgeCandidate:
    coin: str
    direction_on_poly: str   # "UP" or "DOWN"
    direction_on_kalshi: str # "UP" or "DOWN"
    poly_price: float
    kalshi_price: float
    total_cost: float
    gross_edge: float  # 1 - total_cost (no fees)
    net_edge: float    # 1 - total_cost - fees - extras
    poly_fee: float
    kalshi_fee: float
    extras: float
    poly_ref: str
    kalshi_ref: str


@dataclass
class LegFill:
    """Result of executing one leg of a hedge on a single exchange."""
    exchange: str           # "poly" or "kalshi"
    side: str               # "UP" or "DOWN"
    planned_price: float
    actual_price: Optional[float]
    planned_contracts: float
    filled_contracts: float
    order_id: Optional[str]
    fill_ts: Optional[str]  # ISO UTC timestamp of fill
    latency_ms: float       # time to place + confirm
    status: str             # "filled", "partial", "rejected", "error"
    error: Optional[str]


@dataclass
class ExecutionResult:
    """Combined result of executing both legs of a hedge."""
    leg1: LegFill           # first leg placed
    leg2: LegFill           # second leg placed
    total_latency_ms: float # total time for both legs
    slippage_poly: float    # actual_price - planned_price (positive = worse)
    slippage_kalshi: float
    both_filled: bool       # True if both legs fully filled
    unwind_failed: bool = False  # True if unwind was needed but failed


# -----------------------------
# Helpers
# -----------------------------
def utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def parse_iso_utc(s: str) -> Optional[datetime]:
    if not s or not isinstance(s, str):
        return None
    try:
        # handles "...Z"
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def prompt_yes_no(question: str, default: Optional[bool] = None) -> bool:
    """
    Prompt user for y/n. If default is provided, empty input returns default.
    """
    while True:
        suffix = " (y/n): "
        if default is True:
            suffix = " (Y/n): "
        elif default is False:
            suffix = " (y/N): "

        ans = input(question + suffix).strip().lower()
        if ans == "" and default is not None:
            return default
        if ans in ("y", "yes"):
            return True
        if ans in ("n", "no"):
            return False
        print("Please enter 'y' or 'n'.")

MARKET_TYPE_LABELS: Dict[str, str] = {
    "CRYPTO_15M_UPDOWN": "Crypto 15m Up/Down",
}

def prompt_market_type() -> str:
    """
    Returns a market type code string. Only CRYPTO_15M_UPDOWN is implemented for now.
    """
    print("\nSELECT MARKET TYPE")
    print("=" * 45)
    print("1) Crypto 15-minute (Up/Down)")
    print("\n(Other market types can be added later.)")

    while True:
        choice = input("\nSelect market type [1]: ").strip()
        if choice == "" or choice == "1":
            return "CRYPTO_15M_UPDOWN"
        print("Invalid selection. Enter 1.")


def redeem_all_old_positions() -> int:
    """Scan log files for all historical token IDs and redeem any unredeemed positions.
    Returns number of successful redemptions."""
    from eth_account import Account
    import glob as glob_mod

    if not POLY_PRIVATE_KEY:
        print("  [redeem] POLY_PRIVATE_KEY not set")
        return 0

    acct = Account.from_key(POLY_PRIVATE_KEY)
    addr = acct.address

    # Collect all unique token pairs from log files
    log_dir = os.getenv("LOG_DIR", "logs")
    log_files = sorted(glob_mod.glob(os.path.join(log_dir, "arb_logs_*.jsonl")))
    print(f"  [redeem] Scanning {len(log_files)} log file(s) for token IDs...")

    seen: set = set()
    token_pairs: List[Tuple[str, str]] = []

    for lf in log_files:
        try:
            with open(lf, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    # Check trade rows
                    up_tid = row.get("poly_up_token_id") or ""
                    down_tid = row.get("poly_down_token_id") or ""

                    # Also check diagnostic dumps (nested in poly quotes)
                    if not up_tid:
                        for coin_key in AVAILABLE_COINS:
                            quotes = (row.get("all_coin_quotes") or {}).get(coin_key, {})
                            poly_q = quotes.get("poly", {})
                            if poly_q.get("up_token_id"):
                                up_tid = str(poly_q["up_token_id"])
                                down_tid = str(poly_q.get("down_token_id", ""))
                                if up_tid and down_tid and up_tid not in seen:
                                    seen.add(up_tid)
                                    token_pairs.append((up_tid, down_tid))

                    if up_tid and down_tid and up_tid not in seen:
                        seen.add(up_tid)
                        token_pairs.append((up_tid, down_tid))
        except Exception:
            continue

    if not token_pairs:
        print("  [redeem] No token IDs found in logs")
        return 0

    print(f"  [redeem] Found {len(token_pairs)} unique market(s) across all sessions")

    # Batch-check all balances using balanceOfBatch (2-3 RPC calls instead of 104)
    all_token_ids = []
    for up_tid, down_tid in token_pairs:
        all_token_ids.append(up_tid)
        all_token_ids.append(down_tid)

    print(f"  [redeem] Checking {len(all_token_ids)} token balances via batch RPC...")
    balances = _get_ctf_balances_batch(addr, all_token_ids)

    # Filter to markets with non-zero holdings
    non_zero = []
    for up_tid, down_tid in token_pairs:
        up_bal = balances.get(up_tid, 0)
        down_bal = balances.get(down_tid, 0)
        if up_bal > 0 or down_bal > 0:
            non_zero.append((up_tid, down_tid, up_bal, down_bal))

    skipped = len(token_pairs) - len(non_zero)
    print(f"  [redeem] {len(non_zero)} market(s) with tokens, {skipped} already empty")

    if not non_zero:
        print(f"\n  [redeem] Done: nothing to redeem")
        return 0

    # Detect and clear stuck pending nonces
    # Nonces are sequential — if nonce 52 is stuck, nonces 53+ can't mine
    confirmed_hex = _polygon_rpc("eth_getTransactionCount", [addr, "latest"])
    confirmed_nonce = int(confirmed_hex, 16)
    pending_hex = _polygon_rpc("eth_getTransactionCount", [addr, "pending"])
    pending_nonce = int(pending_hex, 16)

    if pending_nonce > confirmed_nonce:
        stuck_count = pending_nonce - confirmed_nonce
        print(f"  [redeem] ⚠ {stuck_count} stuck pending tx(s) blocking nonces {confirmed_nonce}-{pending_nonce - 1}")
        print(f"  [redeem] Clearing stuck nonces with self-transfers...")
        raw_gas = _polygon_rpc("eth_gasPrice", [])
        clear_gas = int(int(raw_gas, 16) * 3)  # 3x gas to replace stuck txs

        from eth_account import Account as _Acct
        for stuck_n in range(confirmed_nonce, pending_nonce):
            # Send 0-value tx to self to clear the nonce
            tx = {
                "to": addr,
                "value": 0,
                "gas": 21000,
                "gasPrice": clear_gas,
                "nonce": stuck_n,
                "chainId": 137,
                "data": "0x",
            }
            signed = _Acct.sign_transaction(tx, POLY_PRIVATE_KEY)
            raw = signed.raw_transaction.hex()
            if not raw.startswith("0x"):
                raw = "0x" + raw
            try:
                tx_hash = _polygon_rpc("eth_sendRawTransaction", [raw])
                print(f"  [redeem] Clearing nonce {stuck_n} → {tx_hash[:20]}...")
            except Exception as e:
                print(f"  [redeem] Nonce {stuck_n} clear failed: {e}")

        # Wait for clearance txs to confirm
        print(f"  [redeem] Waiting for stuck nonces to clear...")
        for _ in range(60):
            new_confirmed = int(_polygon_rpc("eth_getTransactionCount", [addr, "latest"]), 16)
            if new_confirmed >= pending_nonce:
                print(f"  [redeem] ✓ All stuck nonces cleared (confirmed: {new_confirmed})")
                break
            time.sleep(2)
        else:
            print(f"  [redeem] ⚠ Some nonces still pending — proceeding anyway")

    # Fetch fresh nonce after clearing
    nonce_hex = _polygon_rpc("eth_getTransactionCount", [addr, "pending"])
    nonce = int(nonce_hex, 16)
    print(f"  [redeem] Starting nonce: {nonce} (pending)")

    # Redeem each non-zero position
    redeemed = 0
    errors = 0
    total_payout = 0  # micro-USDC
    for i, (up_tid, down_tid, up_bal, down_bal) in enumerate(non_zero):
        # Get conditionId AND market type from Gamma API
        condition_id, neg_risk = _get_market_info_for_token(up_tid)
        if not condition_id:
            condition_id, neg_risk = _get_market_info_for_token(down_tid)
        if not condition_id:
            print(f"  [redeem] Could not find conditionId for token {up_tid[:20]}... — skipping")
            errors += 1
            continue

        mtype = "NegRisk" if neg_risk else "standard"
        print(f"  [redeem] [{i+1}/{len(non_zero)}] Market {condition_id[:16]}... ({mtype}): {up_bal} UP + {down_bal} DOWN tokens")

        # Check balance before to verify redemption actually burns tokens
        pre_up = _get_ctf_balance(addr, up_tid) if up_bal > 0 else 0
        pre_down = _get_ctf_balance(addr, down_tid) if down_bal > 0 else 0

        try:
            tx_hash, nonces_used, payout = _redeem_positions(
                condition_id, up_bal, down_bal, nonce=nonce, neg_risk=neg_risk)
            if tx_hash:
                # Verify tokens were actually burned
                time.sleep(1)
                post_up = _get_ctf_balance(addr, up_tid) if pre_up > 0 else 0
                post_down = _get_ctf_balance(addr, down_tid) if pre_down > 0 else 0
                burned = (pre_up - post_up) + (pre_down - post_down)
                if burned > 0:
                    print(f"  [redeem] ✓ Burned {burned} tokens → tx={tx_hash}")
                    redeemed += 1
                    total_payout += payout
                else:
                    print(f"  [redeem] ⚠ Tx succeeded but NO tokens burned — wrong collateral?")
                    print(f"  [redeem]   pre: {pre_up} UP + {pre_down} DOWN → post: {post_up} UP + {post_down} DOWN")
                    errors += 1
                nonce += nonces_used
            else:
                print(f"  [redeem] ✗ Redemption failed")
                errors += 1
        except Exception as e:
            print(f"  [redeem] ✗ Error: {e}")
            errors += 1

    payout_str = f", +${total_payout / 1e6:.2f} USDC.e recovered" if total_payout > 0 else ""
    print(f"\n  [redeem] Done: {redeemed} redeemed{payout_str}, {skipped} already empty, {errors} errors")
    return redeemed


def prompt_execution_mode() -> str:
    """Prompt user to choose between live trading and paper testing."""
    print("\nSELECT EXECUTION MODE")
    print("=" * 45)
    print("1) Paper Testing   — simulated trades, no real money")
    print("2) Live Trading    — real orders on Kalshi & Polymarket")
    print("3) Redeem Positions — collect unredeemed Polymarket winnings")

    while True:
        choice = input("\nSelect mode [1]: ").strip()
        if choice == "" or choice == "1":
            return "paper"
        if choice == "2":
            return "live"
        if choice == "3":
            return "redeem"
        print("Invalid selection. Enter 1, 2, or 3.")


def prompt_coin_selection(available: List[str]) -> List[str]:
    print("\nSELECT CRYPTOCURRENCIES TO TRADE")
    print("=" * 45)
    print("Choose which 15-minute crypto markets to scan:\n")

    selected: List[str] = []
    for c in available:
        if prompt_yes_no(f"Trade {c}?", default=True):
            selected.append(c)

    if not selected:
        print("\nNo coins selected; defaulting to BTC only.")
        selected = ["BTC"]

    print("\nSelected: " + ", ".join(selected))
    return selected


SESSION_DURATION_OPTIONS = {
    "1": ("15 minutes", 15 * 60),
    "2": ("30 minutes", 30 * 60),
    "3": ("1 hour", 60 * 60),
}

def prompt_session_duration() -> int:
    """Prompt user to choose session duration. Returns duration in seconds."""
    print("\nSELECT SESSION DURATION")
    print("=" * 45)
    for key, (label, _) in SESSION_DURATION_OPTIONS.items():
        print(f"{key}) {label}")

    while True:
        choice = input("\nSelect duration [1]: ").strip()
        if choice == "" or choice in SESSION_DURATION_OPTIONS:
            key = choice if choice else "1"
            label, seconds = SESSION_DURATION_OPTIONS[key]
            print(f"\nSession duration: {label}")
            return seconds
        print("Invalid selection. Enter 1, 2, or 3.")


def dollars_from_cents_maybe(x) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x) / 100.0
    except Exception:
        return None


def safe_float(x) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def pct(x: float) -> str:
    return f"{x*100:.2f}%"

def vwap_price_for_notional_asks(levels: List[Tuple[float, float]], target_cost: float) -> Optional[Tuple[float, float, float]]:
    """
    levels: list of (price, size_contracts) on the ASK side
    target_cost: dollars you want to be able to spend (liquidity threshold)

    Returns:
      (avg_price, filled_cost, filled_contracts)

    avg_price = filled_cost / filled_contracts
    """
    if target_cost <= 0:
        return None

    remaining_cost = target_cost
    filled_cost = 0.0
    filled_contracts = 0.0

    for price, size in levels:
        if price <= 0 or size <= 0:
            continue

        # how many contracts can we buy at this level with remaining dollars?
        max_contracts_at_level = remaining_cost / price
        take_contracts = min(size, max_contracts_at_level)

        if take_contracts <= 0:
            continue

        cost_here = take_contracts * price
        filled_cost += cost_here
        filled_contracts += take_contracts
        remaining_cost -= cost_here

        if remaining_cost <= 1e-9:
            break

    # If we couldn't spend target_cost dollars, insufficient liquidity
    if filled_cost + 1e-9 < target_cost:
        return None

    avg_price = filled_cost / filled_contracts
    return (avg_price, filled_cost, filled_contracts)


# -----------------------------
# Fee helpers
# -----------------------------
# Polymarket: 15-minute crypto taker fee table (100 shares) + fee precision rules. :contentReference[oaicite:4]{index=4}
_POLY_FEE_TABLE_100: List[Tuple[float, float]] = [
    (0.01, 0.00),
    (0.05, 0.003),
    (0.10, 0.02),
    (0.15, 0.06),
    (0.20, 0.13),
    (0.25, 0.22),
    (0.30, 0.33),
    (0.35, 0.45),
    (0.40, 0.58),
    (0.45, 0.69),
    (0.50, 0.78),
    (0.55, 0.84),
    (0.60, 0.86),
    (0.65, 0.84),
    (0.70, 0.77),
    (0.75, 0.66),
    (0.80, 0.51),
    (0.85, 0.35),
    (0.90, 0.18),
    (0.95, 0.05),
    (0.99, 0.00),
]

def poly_taker_fee_usdc(price: float, contracts: float) -> float:
    """
    Approx Polymarket taker fee for eligible 15-minute crypto markets by interpolating
    the published "Fee Table (100 shares)". Scales linearly with contracts/100. :contentReference[oaicite:5]{index=5}

    Fee precision: rounded to 4 decimals; smallest non-zero fee is 0.0001 USDC. :contentReference[oaicite:6]{index=6}
    """
    if contracts <= 0 or price <= 0 or price >= 1:
        return 0.0

    pts = _POLY_FEE_TABLE_100

    # clamp
    if price <= pts[0][0]:
        fee_100 = pts[0][1]
    elif price >= pts[-1][0]:
        fee_100 = pts[-1][1]
    else:
        fee_100 = pts[-1][1]
        for (p0, f0), (p1, f1) in zip(pts, pts[1:]):
            if p0 <= price <= p1:
                t = (price - p0) / (p1 - p0)
                fee_100 = f0 + t * (f1 - f0)
                break

    fee = fee_100 * (contracts / 100.0)

    # Polymarket fee precision rules :contentReference[oaicite:7]{index=7}
    fee = round(fee, 4)
    if 0 < fee < 0.0001:
        fee = 0.0001
    return fee


def kalshi_taker_fee_usd(price: float, contracts: float) -> float:
    """
    Kalshi general trading fee formula:
      fee = round up (0.07 * C * P * (1-P)) to the next cent. :contentReference[oaicite:8]{index=8}
    """
    if contracts <= 0 or price <= 0 or price >= 1:
        return 0.0
    raw = 0.07 * contracts * price * (1.0 - price)
    # round up to next cent
    fee = math.ceil(raw * 100.0 - 1e-12) / 100.0
    return fee


def amortized_extras_usd() -> float:
    n = max(1, AMORTIZE_EXTRAS_OVER_TRADES)
    return (EXTRA_WITHDRAW_FEE_USD + EXTRA_GAS_USD) / n


# -----------------------------
# Kalshi client (public market data) (Kalshi helpers)
# -----------------------------
def kalshi_get_markets_for_series(series_ticker: str) -> List[dict]:
    # Docs show /markets?series_ticker=...&status=open
    url = f"{KALSHI_BASE}/markets"
    params = {"series_ticker": series_ticker, "status": "open"}
    r = _get_session().get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    return data.get("markets", [])


def kalshi_get_market(ticker: str) -> dict:
    url = f"{KALSHI_BASE}/markets/{ticker}"
    r = _get_session().get(url, timeout=15)
    r.raise_for_status()
    return r.json().get("market", {})


def pick_current_kalshi_market(series_ticker: str) -> Optional[KalshiMarketQuote]:
    markets = kalshi_get_markets_for_series(series_ticker)
    if not markets:
        return None

    # Prefer the market with the soonest close_time in the future (i.e., "current window").
    best = None
    best_close = None

    now = datetime.now(timezone.utc)

    for m in markets:
        tkr = m.get("ticker")
        if not tkr:
            continue
        close_time = m.get("close_time") or m.get("expiration_time")
        # close_time looks like ISO; parse best-effort
        ct = None
        if isinstance(close_time, str):
            try:
                ct = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
            except Exception:
                ct = None

        # Filter out already-closed if API returns stale items
        if ct is not None and ct < now:
            continue

        if best is None:
            best, best_close = m, ct
        else:
            # choose the earliest close time that is still in the future
            if ct is not None and (best_close is None or ct < best_close):
                best, best_close = m, ct

    if best is None:
        return None

    # Pull richer pricing from Get Market (includes yes_ask/no_ask)
    full = kalshi_get_market(best["ticker"])
    
    # close time: prefer "close_time", fall back to "expiration_time"
    close_time_raw = full.get("close_time") or best.get("close_time") or full.get("expiration_time") or best.get("expiration_time")
    close_ts = None
    if isinstance(close_time_raw, str):
        close_ts = parse_iso_utc(close_time_raw)

    if close_ts is None:
        # If we can't parse close time, don't use this market for alignment-sensitive logic
        return None

    yes_ask = dollars_from_cents_maybe(full.get("yes_ask"))
    no_ask = dollars_from_cents_maybe(full.get("no_ask"))
    title = full.get("title") or best.get("title") or best["ticker"]

    # If asks are missing, fall back to yes_price/no = 1-yes (rough)
    if yes_ask is None:
        yes_price_c = full.get("yes_price") or best.get("yes_price")
        yes_ask = dollars_from_cents_maybe(yes_price_c)
    if no_ask is None and yes_ask is not None:
        no_ask = max(0.0, 1.0 - yes_ask)

    if yes_ask is None or no_ask is None:
        return None

    # Strike extraction: Kalshi 15M crypto markets use a fixed dollar strike.
    # Primary: floor_strike (numeric). Fallback: parse subtitle (e.g., "$96,250 or above").
    strike = full.get("floor_strike") or full.get("cap_strike") or None
    if strike is None:
        subtitle = full.get("subtitle") or full.get("yes_sub_title") or ""
        m = re.search(r'\$([\d,]+(?:\.\d+)?)', subtitle)
        if m:
            try:
                strike = float(m.group(1).replace(',', ''))
            except ValueError:
                pass

    return KalshiMarketQuote(
        ticker=best["ticker"],
        title=title,
        yes_ask=yes_ask,
        no_ask=no_ask,
        close_ts=close_ts,
        strike=str(strike) if strike is not None else None,
    )


def kalshi_get_orderbook(ticker: str) -> dict:
    url = f"{KALSHI_BASE}/markets/{ticker}/orderbook"
    r = _get_session().get(url, timeout=15)
    r.raise_for_status()
    return r.json()


def kalshi_asks_from_orderbook(ob: dict) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
    """
    Convert Kalshi bids-only orderbook into equivalent ASK ladders for:
      - UP (YES asks) derived from NO bids
      - DOWN (NO asks) derived from YES bids

    Returns: (up_yes_asks, down_no_asks) each list of (price_dollars, size_contracts)
    """
    orderbook = ob.get("orderbook", ob)

    yes_bids = orderbook.get("yes") or orderbook.get("yes_bids") or []
    no_bids = orderbook.get("no") or orderbook.get("no_bids") or []

    def parse_levels(levels):
        out = []
        for lvl in levels:
            try:
                # Kalshi prices are usually in cents in orderbook levels
                price_cents = float(lvl.get("price"))
                size = float(lvl.get("count") or lvl.get("quantity") or lvl.get("size") or lvl.get("contracts") or 0)
            except Exception:
                continue
            if size <= 0:
                continue
            out.append((price_cents / 100.0, size))
        return out

    yes = parse_levels(yes_bids)  # YES bids (dollars, size)
    no = parse_levels(no_bids)    # NO bids  (dollars, size)

    # YES ask = 1 - best NO bid (for each NO bid level)
    up_asks = [((1.0 - p), s) for (p, s) in no if 0.0 < p < 1.0]
    # NO ask = 1 - best YES bid
    down_asks = [((1.0 - p), s) for (p, s) in yes if 0.0 < p < 1.0]

    up_asks.sort(key=lambda x: x[0])
    down_asks.sort(key=lambda x: x[0])

    return (up_asks, down_asks)


# -----------------------------
# Polymarket Gamma client (public) (Polymarket helpers)
# -----------------------------
def poly_get_crypto_tag_id() -> Optional[int]:
    # Tag slug "crypto" exists on the site; Gamma supports /tags/slug/{slug}
    url = f"{POLY_GAMMA_BASE}/tags/slug/crypto"
    r = _get_session().get(url, timeout=15)
    if r.status_code != 200:
        return None
    data = r.json()
    tid = data.get("id")
    try:
        return int(tid)
    except Exception:
        return None


def poly_get_active_15m_crypto_events(crypto_tag_id: int, limit: int = 200) -> List[dict]:
    # Gamma /events supports active/closed/tag_id/order/ascending/limit/offset, etc.
    # Some deployments reject unknown/invalid enum values for "recurrence" (422),
    # so we fetch active+open crypto events and filter to 15m locally.
    url = f"{POLY_GAMMA_BASE}/events"
    params = {
        "tag_id": crypto_tag_id,
        "active": "true",
        "closed": "false",
        "order": "endDate",
        "ascending": "true",
        "limit": str(limit),
        "offset": "0",
    }
    r = _get_session().get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()


def poly_clob_best_asks_from_tokens(up_token_id: str, down_token_id: str, target_notional: float) -> Optional[Tuple[float, float, float, float]]:
    """
    Returns (up_price, up_liq, down_price, down_liq) using either best-ask or VWAP-to-fill target_notional.
    """
    # Fetch UP and DOWN orderbooks in parallel
    with ThreadPoolExecutor(max_workers=2) as ex:
        up_future = ex.submit(poly_clob_get_asks, str(up_token_id))
        down_future = ex.submit(poly_clob_get_asks, str(down_token_id))
        up_asks = up_future.result()
        down_asks = down_future.result()

    if not up_asks or not down_asks:
        return None

    if USE_VWAP_DEPTH:
        up_v = vwap_price_for_notional_asks(up_asks, target_notional)
        down_v = vwap_price_for_notional_asks(down_asks, target_notional)
        if up_v is None or down_v is None:
            return None
        up_price, up_liq, _ = up_v
        down_price, down_liq, _ = down_v

    else:
        # Best ask only (still enforce that level notional >= target_notional)
        up_price, up_size = up_asks[0]
        down_price, down_size = down_asks[0]
        up_liq = up_price * up_size
        down_liq = down_price * down_size
        if up_liq + 1e-9 < target_notional or down_liq + 1e-9 < target_notional:
            return None

    return (up_price, up_liq, down_price, down_liq)



def poly_clob_get_asks(token_id: str) -> List[Tuple[float, float]]:
    """
    Returns asks as list of (price, size) from Polymarket CLOB for a token_id.

    Checks WebSocket cache first (near-zero latency), then falls back to HTTP.
    """
    # Try WebSocket cache first
    if _poly_ws is not None:
        cached = _poly_ws.get_asks(token_id)
        if cached is not None:
            return cached

    # Fall back to HTTP
    candidates = [
        (f"{POLY_CLOB_BASE}/book", {"token_id": str(token_id)}),
        (f"{POLY_CLOB_BASE}/book/{token_id}", None),
        (f"{POLY_CLOB_BASE}/orderbook", {"token_id": str(token_id)}),
        (f"{POLY_CLOB_BASE}/orderbook/{token_id}", None),
    ]

    global _clob_working_route_idx
    # Try cached route first to avoid unnecessary 404s
    if _clob_working_route_idx is not None:
        order = [_clob_working_route_idx] + [i for i in range(len(candidates)) if i != _clob_working_route_idx]
    else:
        order = list(range(len(candidates)))

    last_err = None
    for idx in order:
        url, params = candidates[idx]
        try:
            r = _get_session().get(url, params=params, timeout=15)
            if r.status_code == 404:
                continue
            r.raise_for_status()
            data = r.json()

            # Two common shapes:
            # 1) {"asks":[...], "bids":[...]}
            # 2) {"data":{"asks":[...], "bids":[...]}}
            book = data.get("data", data)
            asks_raw = book.get("asks") or []
            asks: List[Tuple[float, float]] = []

            for lvl in asks_raw:
                try:
                    p = float(lvl.get("price"))
                    s = float(lvl.get("size") or lvl.get("quantity") or lvl.get("amount"))
                except Exception:
                    continue
                if p <= 0 or s <= 0:
                    continue
                asks.append((p, s))

            asks.sort(key=lambda x: x[0])
            _clob_working_route_idx = idx
            return asks
        except Exception as e:
            last_err = e
            continue

    return []


def _is_15m_poly_event(e: dict) -> bool:
    """Check if a Polymarket event is a 15-minute window market (not 5m, hourly, etc.)."""
    slug = (e.get("slug") or "").lower()
    # Explicit accept: slug contains "15m" (check first — "15m" also contains "5m")
    if "15m" in slug:
        return True
    # Explicit reject: 5-minute markets have "5m" in slug (e.g., "btc-updown-5m-...")
    if "5m" in slug:
        return False
    # Check recurrence field
    rec = (e.get("recurrence") or "").upper()
    if rec in ("15M", "15MIN", "15MINUTES"):
        return True
    # Check title for a time range and verify the span is ~15 minutes
    title = e.get("title") or ""
    m = re.search(r'(\d{1,2}):(\d{2})\s*([AP]M)\s*-\s*(\d{1,2}):(\d{2})\s*([AP]M)', title, re.IGNORECASE)
    if m:
        def _to_min(h, mm, ap):
            h = int(h) % 12
            if ap.upper() == "PM":
                h += 12
            return h * 60 + int(mm)
        start = _to_min(m.group(1), m.group(2), m.group(3))
        end = _to_min(m.group(4), m.group(5), m.group(6))
        span = (end - start) % (24 * 60)
        if 14 <= span <= 16:  # allow 14-16 min to account for rounding
            return True
    return False


def extract_poly_quote_for_coin(events: List[dict], coin: str) -> Optional[PolyMarketQuote]:
    prefix = POLY_TITLE_PREFIX[coin]
    now = datetime.now(timezone.utc)

    # pick earliest-ending active event matching title prefix
    title_matches = [e for e in events if isinstance(e.get("title"), str) and e["title"].startswith(prefix)]

    # Filter to 15-minute markets only (skip hourly, daily, etc.)
    all_matches = [e for e in title_matches if _is_15m_poly_event(e)]

    # Filter to events whose endDate is still in the future (skip expired windows)
    candidates = []
    skipped = 0
    for e in all_matches:
        end_raw = e.get("endDate") or e.get("end_date") or e.get("end")
        end_ts = parse_iso_utc(end_raw) if isinstance(end_raw, str) else None
        if end_ts is not None and end_ts < now:
            skipped += 1
            continue
        candidates.append(e)

    if not candidates:
        if all_matches:
            print(f"    [{coin}] debug: {len(all_matches)} passed 15m filter but {skipped} expired, {len(all_matches) - skipped} had no parseable endDate")
        return None

    for e in candidates:
        markets = e.get("markets") or []
        if not markets:
            continue

        m0 = markets[0]

        # Gamma market object should include an id; we need it for the CLOB orderbook call
        clob_token_ids = m0.get("clobTokenIds")
        if not clob_token_ids:
            continue

        # Gamma often returns this as a JSON string like '["123","456"]'
        try:
            if isinstance(clob_token_ids, str):
                clob_token_ids = json.loads(clob_token_ids)
        except Exception:
            continue

        if not isinstance(clob_token_ids, list) or len(clob_token_ids) < 2:
            continue

        # We must map token ids to outcomes. Gamma usually provides "outcomes" in the same order.
        outcomes_raw = m0.get("outcomes")
        try:
            if isinstance(outcomes_raw, str):
                outcomes = json.loads(outcomes_raw)
            else:
                outcomes = outcomes_raw
        except Exception:
            continue

        if not isinstance(outcomes, list) or len(outcomes) < 2:
            continue

        # Build outcome->token map
        outcome_to_token = {}
        for o, tid in zip(outcomes, clob_token_ids):
            if isinstance(o, str):
                outcome_to_token[o.lower()] = str(tid)

        if "up" not in outcome_to_token or "down" not in outcome_to_token:
            continue

        # Subscribe tokens to WebSocket for real-time updates on future scans
        if _poly_ws is not None:
            _poly_ws.subscribe([outcome_to_token["up"], outcome_to_token["down"]])

        best = poly_clob_best_asks_from_tokens(
            up_token_id=outcome_to_token["up"],
            down_token_id=outcome_to_token["down"],
            target_notional=MIN_LEG_NOTIONAL,
        )

        if best is None:
            # no real liquidity on one/both sides (or book route not supported)
            continue

        up_ask, up_liq, down_ask, down_liq = best

        end_raw = e.get("endDate") or e.get("end_date") or e.get("end")
        end_ts = parse_iso_utc(end_raw) if isinstance(end_raw, str) else None
        if end_ts is None:
            continue

        # Try to extract description (may contain "price to beat" or reference price)
        desc = m0.get("description") or e.get("description") or ""

        return PolyMarketQuote(
            event_slug=e.get("slug") or "",
            market_slug=m0.get("slug") or "",
            title=e.get("title") or "",
            up_price=up_ask,
            down_price=down_ask,
            end_ts=end_ts,
            description=desc,
            up_token_id=outcome_to_token["up"],
            down_token_id=outcome_to_token["down"],
        )


    return None



# -----------------------------
# Hedge logic
# -----------------------------
def best_hedge_for_coin(coin: str, poly: PolyMarketQuote, kalshi: KalshiMarketQuote) -> Tuple[Optional[HedgeCandidate], List[HedgeCandidate]]:
    # Interpret Kalshi YES as "Up", NO as "Down" for Up/Down markets.
    kalshi_up = kalshi.yes_ask
    kalshi_down = kalshi.no_ask

    extras = amortized_extras_usd()

    def fees_for_leg(poly_price: float, kalshi_price: float) -> Tuple[float, float]:
        poly_fee = poly_taker_fee_usdc(poly_price, PAPER_CONTRACTS) if INCLUDE_POLY_FEES else 0.0
        kalshi_fee = kalshi_taker_fee_usd(kalshi_price, PAPER_CONTRACTS) if INCLUDE_KALSHI_FEES else 0.0
        return poly_fee, kalshi_fee

    cands: List[HedgeCandidate] = []

    # 1) Poly Up + Kalshi Down
    total1 = poly.up_price + kalshi_down
    gross1 = 1.0 - total1
    poly_fee1, kalshi_fee1 = fees_for_leg(poly.up_price, kalshi_down)
    net1 = 1.0 - total1 - (poly_fee1 + kalshi_fee1 + extras) / PAPER_CONTRACTS - EXECUTION_SLIPPAGE_BUDGET

    cands.append(
        HedgeCandidate(
            coin=coin,
            direction_on_poly="UP",
            direction_on_kalshi="DOWN",
            poly_price=poly.up_price,
            kalshi_price=kalshi_down,
            total_cost=total1,
            gross_edge=gross1,
            net_edge=net1,
            poly_fee=poly_fee1,
            kalshi_fee=kalshi_fee1,
            extras=extras,
            poly_ref=f"{poly.event_slug}/{poly.market_slug}",
            kalshi_ref=kalshi.ticker,
        )
    )

    # 2) Poly Down + Kalshi Up
    total2 = poly.down_price + kalshi_up
    gross2 = 1.0 - total2
    poly_fee2, kalshi_fee2 = fees_for_leg(poly.down_price, kalshi_up)
    net2 = 1.0 - total2 - (poly_fee2 + kalshi_fee2 + extras) / PAPER_CONTRACTS - EXECUTION_SLIPPAGE_BUDGET

    cands.append(
        HedgeCandidate(
            coin=coin,
            direction_on_poly="DOWN",
            direction_on_kalshi="UP",
            poly_price=poly.down_price,
            kalshi_price=kalshi_up,
            total_cost=total2,
            gross_edge=gross2,
            net_edge=net2,
            poly_fee=poly_fee2,
            kalshi_fee=kalshi_fee2,
            extras=extras,
            poly_ref=f"{poly.event_slug}/{poly.market_slug}",
            kalshi_ref=kalshi.ticker,
        )
    )

    # Fee-aware viability with execution safeguards:
    viable = [c for c in cands if c.total_cost < MAX_TOTAL_COST
              and c.net_edge >= MIN_NET_EDGE
              and c.net_edge <= MAX_NET_EDGE]  # skip outliers (likely stale data)
    if not viable:
        return None, cands

    viable.sort(key=lambda x: x.net_edge, reverse=True)
    return viable[0], cands



# -----------------------------
# Logging + display
# -----------------------------
# ANSI color codes for terminal output
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Box-drawing helpers
BOX_W = 60  # inner width for boxes

def _box_top(label: str = "", w: int = BOX_W) -> str:
    if label:
        pad = w - len(label) - 2
        return "┌─ " + label + " " + "─" * max(pad, 0) + "┐"
    return "┌" + "─" * (w + 2) + "┐"

def _box_mid(w: int = BOX_W) -> str:
    return "├" + "─" * (w + 2) + "┤"

def _box_bot(w: int = BOX_W) -> str:
    return "└" + "─" * (w + 2) + "┘"

def _box_line(text: str, w: int = BOX_W) -> str:
    return "│ " + text.ljust(w) + " │"


def print_scan_header(scan_i: int) -> None:
    label = f" Scan #{scan_i} "
    print(f"\n{'—' * 3}{label}{'—' * 3}")


def fmt_money(x: float) -> str:
    return f"${x:.4f}"


def fmt_price_pair(up: float, down: float) -> str:
    return f"{up:.3f}/{down:.3f}"


def _parse_price_from_description(desc: str) -> Optional[float]:
    """Try to extract a dollar reference price from Poly market description."""
    # Look for patterns like "$97,250", "$97,250.00", "$2,345.67"
    m = re.search(r'\$([\d,]+(?:\.\d+)?)', desc)
    if m:
        try:
            return float(m.group(1).replace(',', ''))
        except ValueError:
            pass
    return None


def display_coin_box(coin: str, kalshi: Optional[KalshiMarketQuote],
                     poly: Optional[PolyMarketQuote],
                     edge_str: str = "", skip_reason: str = "") -> None:
    """Compact per-coin box matching the screenshot style."""
    print(f"\n{_box_top(coin)}")

    # Kalshi line
    if kalshi is None:
        print(_box_line("KALSHI:     (no market found)"))
    else:
        # Get depth from orderbook if available
        k_yes = f"YES=${kalshi.yes_ask:.2f}"
        k_no = f"NO=${kalshi.no_ask:.2f}"
        print(_box_line(f"KALSHI:     {k_yes}   {k_no}"))

    # Polymarket line
    if poly is None:
        print(_box_line("POLYMARKET: (no market found)"))
    else:
        p_up = f"UP=${poly.up_price:.2f}"
        p_dn = f"DOWN=${poly.down_price:.2f}"
        print(_box_line(f"POLYMARKET: {p_up}   {p_dn}"))

    # Edge line
    if edge_str:
        print(_box_line(f"EDGE: {edge_str}"))

    print(_box_bot())

    # Skip reason below box
    if skip_reason:
        print(f"  → {skip_reason}")


def display_skip_table(skipped_rows: list) -> None:
    """Print a compact table summarising all skipped coins in a scan."""
    if not skipped_rows:
        return

    # If every coin was skipped for the same reason, collapse to one line
    reasons = {r["reason"] for r in skipped_rows}
    if len(reasons) == 1:
        coins = ", ".join(r["coin"] for r in skipped_rows)
        print(f"  Skipped {len(skipped_rows)} coins ({coins}): {next(iter(reasons))}")
        return

    # Column widths
    cw_coin = 6
    cw_kalshi = 13
    cw_poly = 13
    cw_edge = 10
    # Header
    hdr = (f"  {'COIN':<{cw_coin}} {'KALSHI':<{cw_kalshi}} "
           f"{'POLY':<{cw_poly}} {'EDGE':<{cw_edge}} SKIP REASON")
    print(hdr)
    print(f"  {'─' * (len(hdr) - 2)}")
    for r in skipped_rows:
        # Kalshi prices
        if r["kalshi"] is None:
            k_str = "(no market)"
        else:
            k_str = f"{r['kalshi'].yes_ask:.2f}/{r['kalshi'].no_ask:.2f}"
        # Poly prices
        if r["poly"] is None:
            p_str = "(no market)"
        else:
            p_str = f"{r['poly'].up_price:.2f}/{r['poly'].down_price:.2f}"
        # Edge
        e_str = r.get("edge", "—") or "—"
        # Truncate long skip reasons
        reason = r["reason"]
        if len(reason) > 50:
            reason = reason[:47] + "..."
        print(f"  {r['coin']:<{cw_coin}} {k_str:<{cw_kalshi}} "
              f"{p_str:<{cw_poly}} {e_str:<{cw_edge}} {reason}")


_ANSI_RE = re.compile(r'\033\[[0-9;]*m')

def _green_box_top(label: str = "", w: int = BOX_W) -> str:
    if label:
        visible_len = len(_ANSI_RE.sub('', label))
        pad = w - visible_len - 2
        return f"{GREEN}{BOLD}┌─ {label}{GREEN}{BOLD} {'─' * max(pad, 0)}┐{RESET}"
    return f"{GREEN}{BOLD}┌{'─' * (w + 2)}┐{RESET}"

def _green_box_mid(w: int = BOX_W) -> str:
    return f"{GREEN}├{'─' * (w + 2)}┤{RESET}"

def _green_box_bot(w: int = BOX_W) -> str:
    return f"{GREEN}{BOLD}└{'─' * (w + 2)}┘{RESET}"

def _green_box_line(text: str, w: int = BOX_W) -> str:
    visible_len = len(_ANSI_RE.sub('', text))
    padding = max(w - visible_len, 0)
    return f"{GREEN}│{RESET} {text}{' ' * padding} {GREEN}│{RESET}"


def print_trade_complete(candidate, exec_result, contracts: float,
                        kalshi_quote=None, poly_quote=None) -> None:
    """Print a green box-drawn trade summary for easy terminal scanning."""
    leg1 = exec_result.leg1
    leg2 = exec_result.leg2
    slip_poly = exec_result.slippage_poly
    slip_kalshi = exec_result.slippage_kalshi

    strategy = f"K_{candidate.direction_on_kalshi}+P_{candidate.direction_on_poly}"
    actual_p = leg1.actual_price or leg1.planned_price
    actual_k = leg2.actual_price or leg2.planned_price
    actual_total = (actual_p or 0) + (actual_k or 0)
    filled = int(min(leg1.filled_contracts, leg2.filled_contracts))
    total_outlay = actual_total * filled

    # Compute actual edge after fills + fees
    actual_gross = 1.0 - actual_total if actual_total else 0
    total_fees = candidate.poly_fee + candidate.kalshi_fee + candidate.extras
    fees_per_contract = total_fees / filled if filled > 0 else total_fees
    actual_net = actual_gross - fees_per_contract

    # Profit estimate
    profit_per = 1.0 - actual_total - fees_per_contract
    profit_total = profit_per * filled

    G = GREEN
    B = BOLD
    R = RESET

    print(f"\n{_green_box_top(f'{G}{B} ✅  TRADE COMPLETE — {candidate.coin} {R}')}")
    print(_green_box_line(f"{B}{candidate.coin}{R}  {strategy}  |  {filled} contracts  |  {utc_ts()[:19].replace('T', ' ')}"))
    print(_green_box_mid())

    # Side-by-side: Quotes vs Fills
    print(_green_box_line(f"{'':2}{'QUOTES SEEN':<26} {'FILL PRICES':<26}"))
    if kalshi_quote:
        k_quote_str = f"YES=${kalshi_quote.yes_ask:.2f}  NO=${kalshi_quote.no_ask:.2f}"
    else:
        k_quote_str = f"{candidate.direction_on_kalshi}=${candidate.kalshi_price:.2f}"
    k_fill_str = f"${actual_k:.2f}"
    if abs(slip_kalshi) > 0.001:
        k_fill_str += f"  (slip {slip_kalshi:+.3f})"
    print(_green_box_line(f"  Kalshi:  {k_quote_str:<16} {k_fill_str}"))

    if poly_quote:
        p_quote_str = f"UP=${poly_quote.up_price:.2f}   DOWN=${poly_quote.down_price:.2f}"
    else:
        p_quote_str = f"{candidate.direction_on_poly}=${candidate.poly_price:.2f}"
    p_fill_str = f"${actual_p:.2f}"
    if abs(slip_poly) > 0.001:
        p_fill_str += f"  (slip {slip_poly:+.3f})"
    print(_green_box_line(f"  Poly:    {p_quote_str:<16} {p_fill_str}"))
    print(_green_box_mid())

    # Cost + Fees on one section
    fee_detail = f"K=${candidate.kalshi_fee:.2f} + P=${candidate.poly_fee:.2f}"
    if candidate.extras > 0:
        fee_detail += f" + gas=${candidate.extras:.2f}"
    print(_green_box_line(f"  Cost:    ${actual_total:.2f} x {filled} = ${total_outlay:.2f}    Fees: {fee_detail} = ${total_fees:.2f}"))
    print(_green_box_mid())

    # Edge + Profit — the headline numbers
    edge_str = f"Expected {candidate.net_edge * 100:.1f}%  ->  Actual {actual_net * 100:.1f}%"
    profit_color = GREEN if profit_total >= 0 else RED
    print(_green_box_line(f"  Edge:    {edge_str}"))
    print(_green_box_line(f"  {B}PROFIT:  {profit_color}${profit_total:+.2f}  ({actual_net * 100:+.1f}%){R}"))
    print(_green_box_bot())


def append_log(path: str, row: dict) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row) + "\n")


def summarize(log_rows: List[dict], coins: List[str], skip_counts: Optional[Dict[str, int]] = None,
              start_balances: Optional[Dict[str, float]] = None, logfile: Optional[str] = None) -> None:
    if not log_rows:
        print("\nNo trades were logged.")
        if skip_counts:
            print("\nSkip reasons:")
            for reason, count in sorted(skip_counts.items(), key=lambda x: -x[1]):
                print(f"  {reason}: {count}")
        return

    by_coin: Dict[str, List[dict]] = {}
    for r in log_rows:
        by_coin.setdefault(r["coin"], []).append(r)

    title = "TRADE SUMMARY" if EXEC_MODE == "live" else "TEST TRADE SUMMARY"
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print("=" * 60)

    total_edge = sum(r["net_edge"] for r in log_rows)
    avg_edge = total_edge / len(log_rows)

    print(f"\n--- Edge Analysis ---")
    print(f"Logged trades:  {len(log_rows)}")
    print(f"Avg net edge:   {pct(avg_edge)}")
    print(f"Best net edge:  {pct(max(r['net_edge'] for r in log_rows))}")
    print(f"Worst net edge: {pct(min(r['net_edge'] for r in log_rows))}")

    # P&L section (populated after outcome verification)
    verified = [r for r in log_rows if r.get("actual_pnl_total") is not None]
    if verified:
        wins = [r for r in verified if r["actual_pnl_total"] > 0]
        losses = [r for r in verified if r["actual_pnl_total"] < 0]
        breakevens = [r for r in verified if r["actual_pnl_total"] == 0]
        mismatches = [r for r in verified if not r.get("hedge_consistent", True)]

        total_pnl = sum(r["actual_pnl_total"] for r in verified)

        # Running P&L for drawdown calculation
        running = 0.0
        peak = 0.0
        max_dd = 0.0
        consec_losses = 0
        max_consec_losses = 0
        for r in verified:
            running += r["actual_pnl_total"]
            peak = max(peak, running)
            dd = peak - running
            max_dd = max(max_dd, dd)
            if r["actual_pnl_total"] < 0:
                consec_losses += 1
                max_consec_losses = max(max_consec_losses, consec_losses)
            else:
                consec_losses = 0

        print(f"\n--- Outcome Verification ---")
        print(f"Verified:          {len(verified)}/{len(log_rows)} trades")
        win_pct = len(wins) / len(verified) * 100 if verified else 0
        print(f"Win rate:          {len(wins)}/{len(verified)} ({win_pct:.0f}%)")
        print(f"Wins/Losses/BE:    {len(wins)}/{len(losses)}/{len(breakevens)}")
        if mismatches:
            print(f"Hedge mismatches:  {len(mismatches)} (BOTH legs lost — strike mismatch risk!)")
        else:
            print(f"Hedge mismatches:  0 (all hedges consistent)")

        pnl_label = "P&L" if EXEC_MODE == "live" else "Paper P&L"
        print(f"\n--- {pnl_label} ({int(PAPER_CONTRACTS)} contracts/trade) ---")
        print(f"Total P&L:         ${total_pnl:+.4f}")
        print(f"Avg P&L/trade:     ${total_pnl / len(verified):+.4f}")
        if wins:
            print(f"Best trade:        ${max(r['actual_pnl_total'] for r in verified):+.4f}")
        if losses:
            print(f"Worst trade:       ${min(r['actual_pnl_total'] for r in verified):+.4f}")
        print(f"Max drawdown:      ${max_dd:.4f}")
        print(f"Max consec losses: {max_consec_losses}")

    print(f"\n--- By Coin ---")
    for coin in coins:
        rows = by_coin.get(coin, [])
        if not rows:
            continue
        avg = sum(r["net_edge"] for r in rows) / len(rows)
        best = max(r["net_edge"] for r in rows)
        coin_verified = [r for r in rows if r.get("actual_pnl_total") is not None]
        if coin_verified:
            coin_pnl = sum(r["actual_pnl_total"] for r in coin_verified)
            coin_wins = sum(1 for r in coin_verified if r["actual_pnl_total"] > 0)
            print(f"  {coin}: n={len(rows)} | avg_edge={pct(avg)} | best={pct(best)} | P&L=${coin_pnl:+.4f} | wins={coin_wins}/{len(coin_verified)}")
        else:
            print(f"  {coin}: n={len(rows)} | avg_edge={pct(avg)} | best={pct(best)}")

    if skip_counts:
        print(f"\n--- Skip Reasons ---")
        total_skips = sum(skip_counts.values())
        print(f"Total skips: {total_skips}")
        for reason, count in sorted(skip_counts.items(), key=lambda x: -x[1]):
            print(f"  {reason}: {count}")

    # Slippage analysis
    slip_rows = [r for r in log_rows if r.get("exec_slippage_poly") is not None]
    if slip_rows:
        poly_slips = [r["exec_slippage_poly"] for r in slip_rows]
        kalshi_slips = [r["exec_slippage_kalshi"] for r in slip_rows]
        contracts = int(PAPER_CONTRACTS)
        print(f"\n--- Slippage Analysis ({len(slip_rows)} trades) ---")
        print(f"  Poly:    avg {sum(poly_slips)/len(poly_slips):+.4f}/contract"
              f"  max {max(poly_slips):+.4f}  total ${sum(s * contracts for s in poly_slips):+.2f}")
        print(f"  Kalshi:  avg {sum(kalshi_slips)/len(kalshi_slips):+.4f}/contract"
              f"  max {max(kalshi_slips):+.4f}  total ${sum(s * contracts for s in kalshi_slips):+.2f}")
        total_slip_cost = sum((s1 + s2) * contracts for s1, s2 in zip(poly_slips, kalshi_slips))
        print(f"  Combined cost:  ${total_slip_cost:+.2f}")
        # Compare slippage to expected edge
        edge_eaten = [r for r in slip_rows
                      if (r["exec_slippage_poly"] + r["exec_slippage_kalshi"]) > r["net_edge"]]
        if edge_eaten:
            print(f"  ⚠ {len(edge_eaten)}/{len(slip_rows)} trades had slippage > expected edge")

    print(f"\n--- Recent Trades ---")
    for r in log_rows[-5:]:
        pnl_str = ""
        if r.get("actual_pnl_total") is not None:
            pnl_str = f" | P&L=${r['actual_pnl_total']:+.4f}"
        depth_str = ""
        if r.get("poly_book_levels") is not None:
            depth_str = f" | depth={r['poly_book_levels']}lvl/${r.get('poly_book_notional_usd', 0):.0f}$"
        # Show actual fill prices if slippage occurred
        slip_str = ""
        actual_p = r.get("exec_leg1_actual_price")
        actual_k = r.get("exec_leg2_actual_price")
        if actual_p is not None and actual_k is not None:
            actual_total = actual_p + actual_k
            if abs(actual_total - r['total_cost']) > 0.001:
                slip_str = f" (actual {actual_total:.3f})"
        print(
            f"  [{r['ts']}] {r['coin']} | Poly {r['poly_side']} {r['poly_price']:.3f} "
            f"+ Kalshi {r['kalshi_side']} {r['kalshi_price']:.3f} "
            f"= total {r['total_cost']:.3f}{slip_str} | net {pct(r['net_edge'])} (gross {pct(r['gross_edge'])}) "
            f"| fees {r['poly_fee']:.4f}+{r['kalshi_fee']:.4f}+{r['extras']:.4f}"
            f"{depth_str}{pnl_str}"
        )

    # Exchange-level diagnostics: contract counts, $ spent, direction bias
    exec_rows = [r for r in log_rows if r.get("exec_leg1_exchange") is not None]
    if exec_rows:
        kalshi_bought = 0.0
        kalshi_spent = 0.0
        poly_bought = 0.0
        poly_spent = 0.0
        direction_counts: Dict[str, int] = {}
        partial_fills = 0
        zero_qty_fills = 0

        for r in exec_rows:
            direction = f"Kalshi {r.get('kalshi_side','?')} + Poly {r.get('poly_side','?')}"
            direction_counts[direction] = direction_counts.get(direction, 0) + 1

            for leg in (1, 2):
                exch = r.get(f"exec_leg{leg}_exchange", "")
                filled = r.get(f"exec_leg{leg}_filled_qty", 0) or 0
                planned = r.get(f"exec_leg{leg}_planned_qty", 0) or 0
                actual_px = r.get(f"exec_leg{leg}_actual_price") or 0
                status = r.get(f"exec_leg{leg}_status", "")

                if status == "filled" and filled == 0:
                    zero_qty_fills += 1
                elif 0 < filled < planned:
                    partial_fills += 1

                cost = filled * actual_px if actual_px else 0
                if "kalshi" in exch:
                    kalshi_bought += filled
                    kalshi_spent += cost
                elif "poly" in exch:
                    poly_bought += filled
                    poly_spent += cost

        print(f"\n--- Exchange Diagnostics ({len(exec_rows)} trades) ---")
        print(f"  {'':30s} {'KALSHI':>10s} {'POLY':>10s} {'DELTA':>10s}")
        print(f"  {'Contracts filled':30s} {kalshi_bought:10.1f} {poly_bought:10.1f} {kalshi_bought - poly_bought:+10.1f}")
        print(f"  {'$ spent (fills)':30s} ${kalshi_spent:9.2f} ${poly_spent:9.2f} ${kalshi_spent - poly_spent:+9.2f}")
        if kalshi_bought + poly_bought > 0:
            print(f"  {'Avg price/contract':30s} ${kalshi_spent/kalshi_bought if kalshi_bought else 0:9.4f} "
                  f"${poly_spent/poly_bought if poly_bought else 0:9.4f}")

        print(f"\n  Direction bias:")
        for d, cnt in sorted(direction_counts.items(), key=lambda x: -x[1]):
            print(f"    {d}: {cnt} trades ({cnt/len(exec_rows)*100:.0f}%)")

        if zero_qty_fills:
            print(f"\n  WARNING: {zero_qty_fills} leg(s) reported status=filled but filled_qty=0")
        if partial_fills:
            print(f"  NOTE: {partial_fills} partial fill(s) detected")

        # Unhedged exposure
        unhedged = abs(kalshi_bought - poly_bought)
        if unhedged > 0:
            heavier = "Kalshi" if kalshi_bought > poly_bought else "Poly"
            print(f"\n  UNHEDGED EXPOSURE: {unhedged:.1f} contracts ({heavier} side heavy)")

        # Write session diagnostics to log file for post-mortem
        if logfile:
            session_diag = {
                "log_type": "session_diagnostics",
                "ts": utc_ts(),
                "total_trades": len(exec_rows),
                "kalshi_contracts_filled": round(kalshi_bought, 4),
                "poly_contracts_filled": round(poly_bought, 4),
                "kalshi_dollar_spent": round(kalshi_spent, 4),
                "poly_dollar_spent": round(poly_spent, 4),
                "contract_delta": round(kalshi_bought - poly_bought, 4),
                "dollar_delta": round(kalshi_spent - poly_spent, 4),
                "unhedged_contracts": round(unhedged, 4),
                "unhedged_side": ("kalshi" if kalshi_bought > poly_bought else "poly") if unhedged > 0 else None,
                "direction_counts": direction_counts,
                "zero_qty_fills": zero_qty_fills,
                "partial_fills": partial_fills,
                "per_trade": [
                    {
                        "ts": r.get("ts"),
                        "coin": r.get("coin"),
                        "kalshi_side": r.get("kalshi_side"),
                        "poly_side": r.get("poly_side"),
                        "kalshi_planned_px": r.get("kalshi_price"),
                        "kalshi_actual_px": (r.get("exec_leg1_actual_price") if "kalshi" in (r.get("exec_leg1_exchange") or "") else r.get("exec_leg2_actual_price")),
                        "kalshi_filled_qty": (r.get("exec_leg1_filled_qty", 0) if "kalshi" in (r.get("exec_leg1_exchange") or "") else r.get("exec_leg2_filled_qty", 0)) or 0,
                        "kalshi_planned_qty": (r.get("exec_leg1_planned_qty", 0) if "kalshi" in (r.get("exec_leg1_exchange") or "") else r.get("exec_leg2_planned_qty", 0)) or 0,
                        "poly_planned_px": r.get("poly_price"),
                        "poly_actual_px": (r.get("exec_leg1_actual_price") if "poly" in (r.get("exec_leg1_exchange") or "") else r.get("exec_leg2_actual_price")),
                        "poly_filled_qty": (r.get("exec_leg1_filled_qty", 0) if "poly" in (r.get("exec_leg1_exchange") or "") else r.get("exec_leg2_filled_qty", 0)) or 0,
                        "poly_planned_qty": (r.get("exec_leg1_planned_qty", 0) if "poly" in (r.get("exec_leg1_exchange") or "") else r.get("exec_leg2_planned_qty", 0)) or 0,
                        "both_filled": r.get("exec_both_filled"),
                        "net_edge": r.get("net_edge"),
                    }
                    for r in exec_rows
                ],
            }
            append_log(logfile, session_diag)

    # Position & Potential Payout
    # Each hedged contract pair pays $1.00 on resolution (one side wins).
    total_contracts = 0
    total_cost_basis = 0.0
    total_fees_all = 0.0

    if exec_rows:
        # Live mode: use actual fill data
        for r in exec_rows:
            k_filled = p_filled = 0
            k_cost = p_cost = 0.0
            for leg in (1, 2):
                filled = r.get(f"exec_leg{leg}_filled_qty", 0) or 0
                actual_px = r.get(f"exec_leg{leg}_actual_price") or 0
                exch = r.get(f"exec_leg{leg}_exchange", "")
                if "kalshi" in exch:
                    k_filled = filled
                    k_cost = filled * actual_px
                elif "poly" in exch:
                    p_filled = filled
                    p_cost = filled * actual_px
            hedged = min(k_filled, p_filled)
            total_contracts += hedged
            total_cost_basis += k_cost + p_cost
            total_fees_all += r.get("poly_fee", 0) + r.get("kalshi_fee", 0) + r.get("extras", 0)
    else:
        # Paper mode: each trade = PAPER_CONTRACTS hedged pairs
        contracts = int(PAPER_CONTRACTS)
        for r in log_rows:
            total_contracts += contracts
            total_cost_basis += r["total_cost"] * contracts
            total_fees_all += r.get("poly_fee", 0) + r.get("kalshi_fee", 0) + r.get("extras", 0)

    if total_contracts > 0:
        payout = total_contracts * 1.0  # $1 per winning contract
        expected_profit = payout - total_cost_basis - total_fees_all
        max_loss = total_cost_basis + total_fees_all  # if somehow both sides lose (mismatch)
        avg_cost_per_pair = (total_cost_basis + total_fees_all) / total_contracts

        print(f"\n--- Position & Potential Payout ---")
        print(f"  Hedged contract pairs:   {total_contracts}")
        print(f"  Total cost basis:        ${total_cost_basis:.2f}")
        print(f"  Total fees:              ${total_fees_all:.2f}")
        print(f"  Total invested:          ${total_cost_basis + total_fees_all:.2f}")
        print(f"  Avg cost per pair:       ${avg_cost_per_pair:.4f}")
        print(f"  Payout on resolution:    ${payout:.2f}  ($1.00 x {total_contracts})")
        print(f"  Expected profit:         ${expected_profit:+.2f}  ({expected_profit / (total_cost_basis + total_fees_all) * 100:+.1f}% ROI)")
        if max_loss > 0:
            print(f"  Max loss (hedge fail):   ${-max_loss:.2f}")

    # Balance summary (live mode)
    if start_balances:
        end_balances = {}
        if logfile:
            try:
                end_balances = check_balances(logfile)
            except Exception:
                pass

        print(f"\n--- Balances ---")
        start_total = sum(v for v in start_balances.values() if v >= 0)
        end_total = sum(v for v in end_balances.values() if v >= 0) if end_balances else 0
        for exch in ("kalshi", "poly"):
            s = start_balances.get(exch, -1)
            e = end_balances.get(exch, -1)
            s_str = f"${s:.2f}" if s >= 0 else "N/A"
            e_str = f"${e:.2f}" if e >= 0 else "N/A"
            delta = ""
            if s >= 0 and e >= 0:
                d = e - s
                delta = f"  ({d:+.2f})"
            print(f"  {exch.upper():8s} {s_str:>10s} -> {e_str:>10s}{delta}")
        if end_total > 0:
            d_total = end_total - start_total
            print(f"  {'TOTAL':8s} ${start_total:>9.2f} -> ${end_total:>9.2f}  ({d_total:+.2f})")


# -----------------------------
# Parallel fetch helper
# -----------------------------
def _fetch_coin_quotes(coin: str, poly_events: List[dict]) -> dict:
    """Fetch Kalshi and Polymarket quotes for a single coin **in parallel**.

    Both exchange quotes are fetched concurrently so neither price is stale
    relative to the other when the edge is calculated.
    """
    result: dict = {
        "coin": coin, "kalshi": None, "poly": None,
        "kalshi_ms": 0.0, "poly_ms": 0.0,
        "kalshi_err": None, "poly_err": None,
    }

    t0 = time.monotonic()

    def _fetch_kalshi():
        return pick_current_kalshi_market(KALSHI_SERIES[coin])

    def _fetch_poly():
        return extract_poly_quote_for_coin(poly_events, coin)

    with ThreadPoolExecutor(max_workers=2) as ex:
        kalshi_future = ex.submit(_fetch_kalshi)
        poly_future = ex.submit(_fetch_poly)

        try:
            result["kalshi"] = kalshi_future.result(timeout=15)
        except Exception as e:
            result["kalshi_err"] = str(e)
        result["kalshi_ms"] = (time.monotonic() - t0) * 1000

        try:
            result["poly"] = poly_future.result(timeout=15)
        except Exception as e:
            result["poly_err"] = str(e)
        result["poly_ms"] = (time.monotonic() - t0) * 1000

    return result


# -----------------------------
# Execution layer
# -----------------------------
def check_balances(logfile: str) -> Dict[str, float]:
    """Check available balances on both exchanges. Returns {exchange: usd_balance}.

    In paper mode, returns configured PAPER_CONTRACTS * max_price as available.
    In live mode, calls Kalshi authenticated API and Polymarket CLOB client.
    """
    if EXEC_MODE == "paper":
        bal = {"poly": PAPER_CONTRACTS * 1.0, "kalshi": PAPER_CONTRACTS * 1.0}
        return bal

    # --- LIVE MODE ---
    bal: Dict[str, float] = {}

    # Kalshi: authenticated GET /portfolio/balance
    try:
        data = _kalshi_auth_get("/portfolio/balance")
        # Kalshi returns balance in cents
        bal["kalshi"] = float(data.get("balance", 0)) / 100.0
    except Exception as e:
        bal["kalshi"] = -1.0
        print(f"  [balance] Kalshi balance check failed: {e}")

    # Polymarket: CLOB client balance (USDC.e on Polygon)
    try:
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
        client = _get_poly_clob_client()
        resp = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )
        # Balance is in raw USDC units (6 decimals)
        raw_balance = float(resp.get("balance", "0"))
        bal["poly"] = raw_balance / 1e6
    except Exception as e:
        bal["poly"] = -1.0
        print(f"  [balance] Poly balance check failed: {e}")

    append_log(logfile, {"log_type": "balance_check", "ts": utc_ts(), **bal})
    return bal


def execute_leg(exchange: str, side: str, planned_price: float,
                contracts: float, timeout: float = None, **kwargs) -> LegFill:
    """Execute a single leg on an exchange. Returns fill details.

    In paper mode, simulates an instant fill at planned_price.
    In live mode, places a GTC limit order and polls until filled or timeout.
    """
    t0 = time.monotonic()

    if EXEC_MODE == "paper":
        latency = (time.monotonic() - t0) * 1000
        return LegFill(
            exchange=exchange, side=side,
            planned_price=planned_price, actual_price=planned_price,
            planned_contracts=contracts, filled_contracts=contracts,
            order_id=f"paper-{int(time.time()*1000)}", fill_ts=utc_ts(),
            latency_ms=latency, status="filled", error=None,
        )

    # --- LIVE MODE ---
    order_id = None
    actual_price = None
    filled = 0.0
    status = "error"
    error_msg = None

    leg_timeout = timeout or ORDER_TIMEOUT_S

    try:
        if exchange == "kalshi":
            order_id, actual_price, filled, status, error_msg = _execute_kalshi_leg(
                side, planned_price, contracts, kwargs.get("ticker", ""),
                timeout=leg_timeout,
            )
        elif exchange == "poly":
            order_id, actual_price, filled, status, error_msg = _execute_poly_leg(
                side, planned_price, contracts, kwargs.get("token_id", ""),
                timeout=leg_timeout,
            )
    except Exception as e:
        error_msg = str(e)

    latency = (time.monotonic() - t0) * 1000
    return LegFill(
        exchange=exchange, side=side,
        planned_price=planned_price, actual_price=actual_price,
        planned_contracts=contracts, filled_contracts=filled,
        order_id=order_id, fill_ts=utc_ts() if filled > 0 else None,
        latency_ms=latency, status=status, error=error_msg,
    )


def _kalshi_order_is_filled(o: dict, contracts: float, planned_price: float,
                            order_id: str) -> Optional[Tuple[str, Optional[float], float, str, Optional[str]]]:
    """Check if a Kalshi order poll response indicates a terminal state.

    Returns the 5-tuple (order_id, price, filled, status, error) if terminal, else None.
    """
    o_status = (o.get("status") or "").lower()

    if o_status in ("executed", "filled", "complete"):
        fill_count = float(o.get("fill_count", o.get("count", contracts)))
        return order_id, planned_price, fill_count, "filled", None

    if o_status in ("canceled", "cancelled"):
        fill_count = float(o.get("fill_count", 0))
        if fill_count > 0:
            return order_id, planned_price, fill_count, "partial", "order canceled after partial fill"
        return order_id, None, 0.0, "rejected", "order was canceled"

    return None


def _execute_kalshi_leg(side: str, planned_price: float, contracts: float,
                        ticker: str, timeout: float = None) -> Tuple[Optional[str], Optional[float], float, str, Optional[str]]:
    """Place and poll a Kalshi limit order. Returns (order_id, actual_price, filled, status, error)."""
    fill_timeout = timeout or ORDER_TIMEOUT_S
    kalshi_side = "yes" if side == "UP" else "no"
    # Add buffer to limit price for better fill probability
    buffered = min(planned_price + LIVE_PRICE_BUFFER, 0.99)
    price_cents = int(round(buffered * 100))
    client_order_id = f"polyshi-{uuid.uuid4().hex[:12]}"

    # Build order body — use yes_price for YES side, no_price for NO side
    body: dict = {
        "ticker": ticker,
        "action": "buy",
        "side": kalshi_side,
        "type": "limit",
        "count": int(contracts),
        "client_order_id": client_order_id,
    }
    if kalshi_side == "yes":
        body["yes_price"] = price_cents
    else:
        body["no_price"] = price_cents

    # Place the order
    resp = _kalshi_auth_post("/portfolio/orders", body)
    order = resp.get("order", resp)
    order_id = order.get("order_id") or order.get("id")

    if not order_id:
        return None, None, 0.0, "rejected", f"No order_id in response: {resp}"

    # Check if the order already filled in the placement response
    result = _kalshi_order_is_filled(order, contracts, planned_price, order_id)
    if result is not None:
        return result

    # Poll for fill — check immediately, then at intervals
    deadline = time.monotonic() + fill_timeout
    first_poll = True
    while time.monotonic() < deadline:
        try:
            poll = _kalshi_auth_get(f"/portfolio/orders/{order_id}")
            o = poll.get("order", poll)
            result = _kalshi_order_is_filled(o, contracts, planned_price, order_id)
            if result is not None:
                return result
        except Exception as poll_err:
            print(f"  [kalshi-poll] Poll error (will retry): {poll_err}")

        # First iteration: no sleep (order likely fills instantly for crossing limits)
        if first_poll:
            first_poll = False
            time.sleep(0.1)
        else:
            time.sleep(ORDER_POLL_INTERVAL_S)

    # Timeout — do a final status check, then cancel if still resting
    try:
        poll = _kalshi_auth_get(f"/portfolio/orders/{order_id}")
        o = poll.get("order", poll)
        result = _kalshi_order_is_filled(o, contracts, planned_price, order_id)
        if result is not None:
            return result
        # Cancel unfilled remainder via DELETE (Kalshi v2 API)
        _kalshi_auth_delete(f"/portfolio/orders/{order_id}")
        fill_count = float(o.get("fill_count", 0))
        if fill_count > 0:
            return order_id, planned_price, fill_count, "partial", "timeout — canceled remainder"
    except Exception as cancel_err:
        # Cancel failed (e.g. 404) — order may already be in terminal state.
        # Re-check order status before giving up.
        try:
            poll = _kalshi_auth_get(f"/portfolio/orders/{order_id}")
            o = poll.get("order", poll)
            result = _kalshi_order_is_filled(o, contracts, planned_price, order_id)
            if result is not None:
                print(f"  [kalshi] Order filled despite cancel error — recovered")
                return result
        except Exception:
            pass
        return order_id, None, 0.0, "error", f"timeout + cancel failed: {cancel_err}"

    return order_id, None, 0.0, "rejected", "order timed out with no fills"


def _poly_query_recent_fills(client, token_id: str, since_ts: int) -> Tuple[float, float]:
    """Query the CLOB trades ledger for fills on a token since a timestamp.

    Uses ``client.get_trades()`` which returns actual executed trades — the most
    reliable source of fill data, independent of ``get_order()`` status/size_matched.

    Returns (total_size, weighted_avg_price).  Returns (0.0, 0.0) on error or
    if no trades are found.
    """
    try:
        trades = client.get_trades(TradeParams(asset_id=token_id, after=since_ts))
    except Exception:
        return 0.0, 0.0
    if not trades:
        return 0.0, 0.0
    total_size = 0.0
    total_cost = 0.0
    for t in trades:
        try:
            sz = float(t.get("size", 0))
            px = float(t.get("price", 0))
            total_size += sz
            total_cost += sz * px
        except (TypeError, ValueError):
            continue
    avg_price = total_cost / total_size if total_size > 0 else 0.0
    return total_size, avg_price


def _poly_fill_size_from_trades(order: dict) -> float:
    """Extract total filled size from associate_trades (on-chain ground truth).

    The CLOB API's ``size_matched`` field is unreliable (returns 0 for filled
    orders).  ``associate_trades`` contains the actual on-chain fills and is
    the only trustworthy source of fill information.
    """
    trades = order.get("associate_trades") or []
    total = 0.0
    for t in trades:
        try:
            total += float(t.get("size", 0))
        except (TypeError, ValueError):
            continue
    return total


def _poly_avg_fill_price(order: dict, fallback: float) -> float:
    """Extract the true average fill price from associate_trades.

    The order's ``price`` field is the *limit* price, not the execution price.
    ``associate_trades`` contains the individual fills with real prices.
    """
    trades = order.get("associate_trades") or []
    if not trades:
        # No trade data available — fall back to order price (limit), then planned
        return float(order.get("price", fallback))
    total_cost = 0.0
    total_size = 0.0
    for t in trades:
        try:
            tp = float(t.get("price", 0))
            ts = float(t.get("size", 0))
            total_cost += tp * ts
            total_size += ts
        except (TypeError, ValueError):
            continue
    if total_size > 0:
        return total_cost / total_size
    return float(order.get("price", fallback))


def _execute_poly_leg(side: str, planned_price: float, contracts: float,
                      token_id: str, timeout: float = None) -> Tuple[Optional[str], Optional[float], float, str, Optional[str]]:
    """Place and poll a Polymarket CLOB limit order. Returns (order_id, actual_price, filled, status, error)."""
    fill_timeout = timeout or ORDER_TIMEOUT_S
    client = _get_poly_clob_client()

    # Add buffer to limit price for better fill probability.
    # CLOB gives price improvement: if best ask < limit, you fill at best ask.
    # Cap at planned_price + POLY_MAX_SLIPPAGE to prevent runaway slippage.
    buffered = planned_price + LIVE_PRICE_BUFFER
    max_acceptable = planned_price + POLY_MAX_SLIPPAGE
    # Round price to 2 decimal places (Poly CLOB tick size is $0.01), cap at 0.99
    price = min(round(buffered, 2), round(max_acceptable, 2), 0.99)

    # Create and sign order
    order_args = OrderArgs(
        token_id=token_id,
        price=price,
        size=contracts,
        side=BUY,
    )
    signed_order = client.create_order(order_args)
    resp = client.post_order(signed_order, OrderType.GTC)

    # Response: {"success": bool, "orderID": "0x...", ...}
    if not resp.get("success", False):
        error_detail = resp.get("errorMsg") or resp.get("error") or str(resp)
        return None, None, 0.0, "rejected", f"order rejected: {error_detail}"

    order_id = resp.get("orderID") or resp.get("id")
    if not order_id:
        return None, None, 0.0, "rejected", f"no orderID in response: {resp}"

    # Poll for fill status
    deadline = time.monotonic() + fill_timeout
    while time.monotonic() < deadline:
        try:
            o = client.get_order(order_id)
            o_status = (o.get("status") or "").lower()

            if o_status == "matched" or o_status == "filled":
                raw_matched = o.get("size_matched")
                matched_size = float(raw_matched) if raw_matched and float(raw_matched) > 0 else 0.0
                trades_size = _poly_fill_size_from_trades(o)
                filled_size = max(matched_size, trades_size)
                if filled_size > 0:
                    avg_price = _poly_avg_fill_price(o, planned_price)
                    return order_id, avg_price, filled_size, "filled", None
                # Status=filled but fill data not synced yet — keep polling

            if o_status == "canceled" or o_status == "cancelled":
                filled_size = float(o.get("size_matched", 0))
                if filled_size > 0:
                    avg_price = _poly_avg_fill_price(o, planned_price)
                    return order_id, avg_price, filled_size, "partial", "order canceled after partial fill"
                return order_id, None, 0.0, "rejected", "order was canceled"

        except Exception:
            pass
        time.sleep(ORDER_POLL_INTERVAL_S)

    # Timeout — cancel unfilled remainder
    try:
        o = client.get_order(order_id)
        raw_matched = o.get("size_matched")
        filled_size = float(raw_matched) if raw_matched and float(raw_matched) > 0 else 0.0
        trades_size = _poly_fill_size_from_trades(o)
        filled_size = max(filled_size, trades_size)
        if (o.get("status") or "").lower() in ("matched", "filled"):
            if not filled_size:
                filled_size = float(o.get("original_size", contracts))
            avg_price = _poly_avg_fill_price(o, planned_price)
            return order_id, avg_price, filled_size, "filled", None
        client.cancel(order_id)
        if filled_size > 0:
            return order_id, planned_price, filled_size, "partial", "timeout — canceled remainder"
    except Exception as e:
        return order_id, None, 0.0, "error", f"timeout + cancel failed: {e}"

    return order_id, None, 0.0, "rejected", "order timed out with no fills"


def _execute_poly_with_retries(side: str, planned_price: float, contracts: float,
                               token_id: str) -> LegFill:
    """Execute Poly leg with fresh orderbook fetches and retries.

    On each attempt: fetch fresh asks, place limit at best ask + buffer, poll for fill.
    Returns LegFill with aggregate result across all attempts.
    """
    t0 = time.monotonic()
    epoch_before = int(time.time())  # Unix timestamp for get_trades() fallback

    if EXEC_MODE == "paper":
        return LegFill(
            exchange="poly", side=side,
            planned_price=planned_price, actual_price=planned_price,
            planned_contracts=contracts, filled_contracts=contracts,
            order_id=f"paper-{int(time.time()*1000)}", fill_ts=utc_ts(),
            latency_ms=(time.monotonic() - t0) * 1000, status="filled", error=None,
        )

    client = _get_poly_clob_client()
    last_error = None
    placed_order_ids: list = []  # Track all order IDs for final sweep

    # Snapshot on-chain CTF balance before placing any orders.
    # Used as last-resort ground truth to detect fills when all API methods fail.
    try:
        from eth_account import Account as _Acct
        _owner_addr = _Acct.from_key(POLY_PRIVATE_KEY).address
        _ctf_balance_before = _get_ctf_balance(_owner_addr, token_id)
    except Exception:
        _owner_addr = None
        _ctf_balance_before = 0

    for attempt in range(1, POLY_FILL_MAX_RETRIES + 1):
        # Before placing a new order, re-check previous order to prevent duplicate fills
        if placed_order_ids:
            prev_id = placed_order_ids[-1]
            try:
                prev_o = client.get_order(prev_id)
                prev_trades_size = _poly_fill_size_from_trades(prev_o)
                if prev_trades_size > 0:
                    avg_price = _poly_avg_fill_price(prev_o, planned_price)
                    latency = (time.monotonic() - t0) * 1000
                    status = "filled" if prev_trades_size >= contracts else "partial"
                    print(f"  [poly-retry]   Previous order {prev_id[:12]} actually filled! "
                          f"{prev_trades_size} contracts — skipping new attempt")
                    return LegFill(
                        exchange="poly", side=side,
                        planned_price=planned_price, actual_price=avg_price,
                        planned_contracts=contracts, filled_contracts=prev_trades_size,
                        order_id=prev_id, fill_ts=utc_ts(),
                        latency_ms=latency, status=status, error=None,
                    )
            except Exception:
                pass

        # CRITICAL: On-chain duplicate guard — if we already hold shares from a
        # previous retry that the CLOB API failed to report, do NOT place another
        # order.  This prevents the catastrophic "5 retries × N contracts" overspend.
        if _owner_addr and placed_order_ids:
            try:
                current_bal = _get_ctf_balance(_owner_addr, token_id)
                delta_shares = (current_bal - _ctf_balance_before) / 1e6
                if delta_shares >= contracts * 0.9:  # allow small rounding tolerance
                    latency = (time.monotonic() - t0) * 1000
                    status = "filled" if delta_shares >= contracts else "partial"
                    print(f"  [poly-retry]   ON-CHAIN GUARD: balance delta={delta_shares:.2f} "
                          f"shows previous order already filled — aborting retries")
                    return LegFill(
                        exchange="poly", side=side,
                        planned_price=planned_price, actual_price=planned_price,
                        planned_contracts=contracts, filled_contracts=delta_shares,
                        order_id=placed_order_ids[-1], fill_ts=utc_ts(),
                        latency_ms=latency, status=status, error=None,
                    )
            except Exception:
                pass

        # Fetch fresh orderbook
        asks = poly_clob_get_asks(str(token_id))
        if not asks:
            last_error = f"retry {attempt}: empty orderbook"
            print(f"  [poly-retry] Attempt {attempt}/{POLY_FILL_MAX_RETRIES}: no asks available")
            time.sleep(0.5)
            continue

        # Display orderbook levels (up to 3)
        for i, (lvl_price, lvl_size) in enumerate(asks[:3]):
            print(f"  [poly-retry]   Level {i+1}: ${lvl_price:.3f} x {lvl_size:.1f}")

        best_ask = asks[0][0]

        # Enforce slippage cap: reject if market moved too far from planned price
        max_acceptable = planned_price + POLY_MAX_SLIPPAGE
        if best_ask > max_acceptable:
            last_error = (f"retry {attempt}: best ask ${best_ask:.3f} exceeds "
                          f"max acceptable ${max_acceptable:.3f} (planned ${planned_price:.3f} + "
                          f"${POLY_MAX_SLIPPAGE:.3f} cap)")
            print(f"  [poly-retry] Attempt {attempt}/{POLY_FILL_MAX_RETRIES}: "
                  f"SKIP — best ask ${best_ask:.3f} > cap ${max_acceptable:.3f}")
            time.sleep(0.5)
            continue

        # Use best ask + small buffer, but cap at planned_price + max slippage
        limit_price = min(round(best_ask + LIVE_PRICE_BUFFER, 2), round(max_acceptable, 2), 0.99)
        print(f"  [poly-retry] Attempt {attempt}/{POLY_FILL_MAX_RETRIES}: "
              f"{int(contracts)}x @ ${limit_price:.2f} (best ask ${best_ask:.3f})")

        # Place order
        try:
            order_args = OrderArgs(
                token_id=token_id,
                price=limit_price,
                size=contracts,
                side=BUY,
            )
            signed_order = client.create_order(order_args)
            resp = client.post_order(signed_order, OrderType.GTC)
        except Exception as e:
            last_error = f"retry {attempt}: order error: {e}"
            print(f"  [poly-retry]   Order failed: {e}")
            continue

        if not resp.get("success", False):
            last_error = f"retry {attempt}: rejected: {resp.get('errorMsg') or resp}"
            print(f"  [poly-retry]   Rejected: {resp.get('errorMsg') or resp.get('error')}")
            continue

        order_id = resp.get("orderID") or resp.get("id")
        if not order_id:
            last_error = f"retry {attempt}: no orderID"
            continue

        placed_order_ids.append(order_id)

        # Poll for fill with per-attempt timeout
        deadline = time.monotonic() + POLY_FILL_RETRY_TIMEOUT_S
        filled = False
        while time.monotonic() < deadline:
            try:
                o = client.get_order(order_id)
                o_status = (o.get("status") or "").lower()
                if o_status in ("matched", "filled"):
                    raw_matched = o.get("size_matched")
                    matched_size = float(raw_matched) if raw_matched and float(raw_matched) > 0 else 0.0
                    trades_size = _poly_fill_size_from_trades(o)
                    filled_size = max(matched_size, trades_size)
                    if filled_size > 0:
                        avg_price = _poly_avg_fill_price(o, planned_price)
                        latency = (time.monotonic() - t0) * 1000
                        print(f"  [poly-retry]   Filled on attempt {attempt} "
                              f"({filled_size} contracts, matched={matched_size}, trades={trades_size})")
                        return LegFill(
                            exchange="poly", side=side,
                            planned_price=planned_price, actual_price=avg_price,
                            planned_contracts=contracts, filled_contracts=filled_size,
                            order_id=order_id, fill_ts=utc_ts(),
                            latency_ms=latency, status="filled", error=None,
                        )
                    # Status says filled but API hasn't synced fill data yet —
                    # keep polling rather than returning 0 immediately
                if o_status in ("canceled", "cancelled"):
                    # Even canceled orders might have partial fills — check
                    # both size_matched and associate_trades (on-chain truth)
                    cancel_matched = o.get("size_matched")
                    trades_size = _poly_fill_size_from_trades(o)
                    filled_size = (float(cancel_matched) if cancel_matched and float(cancel_matched) > 0
                                   else trades_size)
                    if filled_size > 0:
                        avg_price = _poly_avg_fill_price(o, planned_price)
                        latency = (time.monotonic() - t0) * 1000
                        status = "filled" if filled_size >= contracts else "partial"
                        print(f"  [poly-retry]   Canceled but {filled_size} contracts matched (trades_size={trades_size})")
                        return LegFill(
                            exchange="poly", side=side,
                            planned_price=planned_price, actual_price=avg_price,
                            planned_contracts=contracts, filled_contracts=filled_size,
                            order_id=order_id, fill_ts=utc_ts(),
                            latency_ms=latency, status=status, error=None,
                        )
                    break
            except Exception as poll_err:
                print(f"  [poly-retry]   Poll error: {poll_err}")
            time.sleep(ORDER_POLL_INTERVAL_S)

        # Not filled in time — cancel and check if it filled before cancel took effect
        print(f"  [poly-retry]   Timeout, canceling order {order_id}...")
        try:
            client.cancel(order_id)
        except Exception:
            pass

        # Post-cancel check: order may have matched on-chain before cancel was processed.
        # Add a short delay so the CLOB API has time to reflect on-chain state.
        time.sleep(1.0)
        try:
            o = client.get_order(order_id)
            o_status = (o.get("status") or "").lower()
            raw_matched = o.get("size_matched")
            matched_size = float(raw_matched) if raw_matched and float(raw_matched) > 0 else 0.0
            trades_size = _poly_fill_size_from_trades(o)
            print(f"  [poly-retry]   Post-cancel status={o_status} size_matched={raw_matched} trades_size={trades_size}")

            if o_status in ("matched", "filled") or matched_size > 0 or trades_size > 0:
                filled_size = (matched_size if matched_size > 0
                               else trades_size if trades_size > 0
                               else float(o.get("original_size", contracts)))
                avg_price = _poly_avg_fill_price(o, planned_price)
                latency = (time.monotonic() - t0) * 1000
                status = "filled" if filled_size >= contracts else "partial"
                print(f"  [poly-retry]   Order was actually {status}! {filled_size} contracts @ ~${avg_price:.4f}")
                return LegFill(
                    exchange="poly", side=side,
                    planned_price=planned_price, actual_price=avg_price,
                    planned_contracts=contracts, filled_contracts=filled_size,
                    order_id=order_id, fill_ts=utc_ts(),
                    latency_ms=latency, status=status, error=None,
                )
        except Exception as e:
            print(f"  [poly-retry]   Post-cancel check failed: {e}")

        last_error = f"retry {attempt}: not filled within {POLY_FILL_RETRY_TIMEOUT_S}s"

    # All retries exhausted — final sweep of ALL placed orders.
    # On-chain fills can lag behind the CLOB API by several seconds, so wait
    # and then check every order we placed during this execution.
    if placed_order_ids:
        print(f"  [poly-retry] Final sweep: checking {len(placed_order_ids)} order(s) after 2s delay...")
        time.sleep(2.0)
        for oid in placed_order_ids:
            try:
                o = client.get_order(oid)
                trades_size = _poly_fill_size_from_trades(o)
                raw_matched = o.get("size_matched")
                matched_size = float(raw_matched) if raw_matched and float(raw_matched) > 0 else 0.0
                o_status = (o.get("status") or "").lower()
                effective_size = max(matched_size, trades_size)
                if effective_size > 0 or o_status in ("matched", "filled"):
                    if effective_size <= 0:
                        effective_size = float(o.get("original_size", contracts))
                    avg_price = _poly_avg_fill_price(o, planned_price)
                    latency = (time.monotonic() - t0) * 1000
                    status = "filled" if effective_size >= contracts else "partial"
                    print(f"  [poly-retry]   FINAL SWEEP: order {oid[:12]} was {status}! "
                          f"{effective_size} contracts @ ~${avg_price:.4f} (trades_size={trades_size})")
                    return LegFill(
                        exchange="poly", side=side,
                        planned_price=planned_price, actual_price=avg_price,
                        planned_contracts=contracts, filled_contracts=effective_size,
                        order_id=oid, fill_ts=utc_ts(),
                        latency_ms=latency, status=status, error=None,
                    )
            except Exception as e:
                print(f"  [poly-retry]   Final sweep error for {oid[:12]}: {e}")

    # Last resort: query the CLOB trades ledger directly.  This catches fills
    # that get_order() misses entirely (status="canceled" + empty associate_trades
    # due to API sync lag).  get_trades() returns actual executed trade records
    # independent of order status.
    print(f"  [poly-retry] Trades-ledger fallback: querying get_trades(asset_id={token_id[:12]}..., after={epoch_before})...")
    ledger_size, ledger_price = _poly_query_recent_fills(client, token_id, epoch_before)
    if ledger_size > 0:
        latency = (time.monotonic() - t0) * 1000
        status = "filled" if ledger_size >= contracts else "partial"
        print(f"  [poly-retry]   TRADES LEDGER: found {ledger_size} contracts @ ~${ledger_price:.4f}!")
        return LegFill(
            exchange="poly", side=side,
            planned_price=planned_price, actual_price=ledger_price,
            planned_contracts=contracts, filled_contracts=ledger_size,
            order_id=placed_order_ids[-1] if placed_order_ids else None,
            fill_ts=utc_ts(),
            latency_ms=latency, status=status, error=None,
        )

    # Nuclear fallback: check on-chain CTF token balance delta.
    # The blockchain is the ultimate source of truth — if our balance increased,
    # we definitely received shares regardless of what the CLOB API reports.
    if _owner_addr and _ctf_balance_before is not None:
        try:
            _ctf_balance_after = _get_ctf_balance(_owner_addr, token_id)
            delta_raw = _ctf_balance_after - _ctf_balance_before
            delta_shares = delta_raw / 1e6  # CTF tokens use 10^6 decimals
            print(f"  [poly-retry] On-chain balance fallback: before={_ctf_balance_before} "
                  f"after={_ctf_balance_after} delta={delta_shares:.2f} shares")
            if delta_shares > 0:
                latency = (time.monotonic() - t0) * 1000
                status = "filled" if delta_shares >= contracts else "partial"
                print(f"  [poly-retry]   ON-CHAIN CONFIRMED: {delta_shares:.2f} contracts filled!")
                return LegFill(
                    exchange="poly", side=side,
                    planned_price=planned_price, actual_price=planned_price,
                    planned_contracts=contracts, filled_contracts=delta_shares,
                    order_id=placed_order_ids[-1] if placed_order_ids else None,
                    fill_ts=utc_ts(),
                    latency_ms=latency, status=status, error=None,
                )
        except Exception as e:
            print(f"  [poly-retry]   On-chain balance check failed: {e}")

    # Truly no fills detected
    latency = (time.monotonic() - t0) * 1000
    return LegFill(
        exchange="poly", side=side,
        planned_price=planned_price, actual_price=None,
        planned_contracts=contracts, filled_contracts=0.0,
        order_id=None, fill_ts=None,
        latency_ms=latency, status="rejected",
        error=f"all {POLY_FILL_MAX_RETRIES} retries exhausted: {last_error}",
    )


def execute_hedge(candidate: HedgeCandidate,
                  poly_quote: PolyMarketQuote,
                  kalshi_quote: KalshiMarketQuote,
                  logfile: str,
                  poly_depth: Optional[dict] = None) -> ExecutionResult:
    """Execute both legs of a hedge. Kalshi first (fast, deterministic), then Poly with retries.

    Execution order: Kalshi first, then Poly. If Poly fails after retries, unwind Kalshi.
    Result preserves leg1=Poly, leg2=Kalshi for backward compatibility with logging/P&L.
    """
    # Cap contracts to available Poly book depth to reduce partial fills
    contracts = PAPER_CONTRACTS
    if poly_depth and poly_depth.get("total_size", 0) > 0:
        depth_cap = int(poly_depth["total_size"] * POLY_DEPTH_CAP_RATIO)
        if depth_cap < contracts:
            print(f"  [depth-cap] Poly book has {poly_depth['total_size']} contracts — "
                  f"capping order from {int(contracts)} to {depth_cap} "
                  f"({POLY_DEPTH_CAP_RATIO:.0%} of depth)")
            contracts = depth_cap
    if contracts < MIN_CONTRACTS:
        print(f"  [depth-cap] Effective contracts ({int(contracts)}) below MIN_CONTRACTS ({MIN_CONTRACTS}) — skipping trade")
        skip_fill = LegFill(
            exchange="poly", side=candidate.direction_on_poly,
            planned_price=candidate.poly_price, actual_price=None,
            planned_contracts=0, filled_contracts=0.0,
            order_id=None, fill_ts=None,
            latency_ms=0.0, status="skipped", error="depth too thin",
        )
        kalshi_skip = LegFill(
            exchange="kalshi", side=candidate.direction_on_kalshi,
            planned_price=candidate.kalshi_price, actual_price=None,
            planned_contracts=0, filled_contracts=0.0,
            order_id=None, fill_ts=None,
            latency_ms=0.0, status="skipped", error="depth too thin",
        )
        return ExecutionResult(
            leg1=skip_fill, leg2=kalshi_skip,
            total_latency_ms=0.0,
            slippage_poly=0.0, slippage_kalshi=0.0,
            both_filled=False,
        )

    # Determine Poly token ID for the leg we're buying
    if candidate.direction_on_poly == "UP":
        poly_token = poly_quote.up_token_id
    else:
        poly_token = poly_quote.down_token_id

    # --- STEP 1: Execute Kalshi FIRST (fast, deterministic fills) ---
    t_total = time.monotonic()
    print(f"  [exec] STEP 1: KALSHI — Placing {int(contracts)}x "
          f"{'YES' if candidate.direction_on_kalshi == 'UP' else 'NO'} "
          f"@ ${candidate.kalshi_price:.2f}...")
    kalshi_fill = execute_leg("kalshi", candidate.direction_on_kalshi,
                              candidate.kalshi_price, contracts,
                              ticker=kalshi_quote.ticker)
    kalshi_done = time.monotonic()

    # Guard: if Kalshi failed, do NOT send Poly order
    if kalshi_fill.status in ("error", "rejected") and kalshi_fill.filled_contracts == 0:
        print(f"  [exec] Kalshi leg failed — skipping Poly to avoid unhedged position")
        poly_fill = LegFill(
            exchange="poly", side=candidate.direction_on_poly,
            planned_price=candidate.poly_price, actual_price=None,
            planned_contracts=contracts, filled_contracts=0.0,
            order_id=None, fill_ts=None,
            latency_ms=0.0, status="skipped", error="skipped: kalshi leg failed",
        )
    else:
        # --- STEP 2: Execute Poly with fresh orderbook retries ---
        # Match Poly target to Kalshi's ACTUAL fill count, not planned.
        # This prevents Poly from overfilling when Kalshi partially fills,
        # which is the primary cause of Poly-heavy unhedged exposure.
        poly_target = kalshi_fill.filled_contracts if kalshi_fill.filled_contracts > 0 else contracts
        if poly_target != contracts:
            print(f"  [exec] Adjusting Poly target: {int(contracts)} → {int(poly_target)} "
                  f"(matching Kalshi actual fill)")
        print(f"  [exec] STEP 2: POLYMARKET — Attempting fill with FRESH orderbook...")
        poly_fill = _execute_poly_with_retries(
            candidate.direction_on_poly, candidate.poly_price,
            poly_target, poly_token,
        )

    total_ms = (time.monotonic() - t_total) * 1000

    # Map to leg1=Poly, leg2=Kalshi for backward compatibility
    leg1 = poly_fill
    leg2 = kalshi_fill

    # Compute slippage
    slip_poly = (leg1.actual_price - leg1.planned_price) if leg1.actual_price is not None else 0.0
    slip_kalshi = (leg2.actual_price - leg2.planned_price) if leg2.actual_price is not None else 0.0
    both_filled = (leg1.status == "filled" and leg2.status == "filled")

    result = ExecutionResult(
        leg1=leg1, leg2=leg2,
        total_latency_ms=total_ms,
        slippage_poly=slip_poly, slippage_kalshi=slip_kalshi,
        both_filled=both_filled,
    )

    # Log execution details
    exec_row = {
        "log_type": "execution",
        "ts": utc_ts(),
        "coin": candidate.coin,
        "exec_mode": EXEC_MODE,
        "both_filled": both_filled,
        "total_exec_ms": round(total_ms, 1),
        "leg1_exchange": leg1.exchange,
        "leg1_side": leg1.side,
        "leg1_planned_price": leg1.planned_price,
        "leg1_actual_price": leg1.actual_price,
        "leg1_planned_qty": leg1.planned_contracts,
        "leg1_filled_qty": leg1.filled_contracts,
        "leg1_order_id": leg1.order_id,
        "leg1_status": leg1.status,
        "leg1_latency_ms": round(leg1.latency_ms, 1),
        "leg1_error": leg1.error,
        "leg2_exchange": leg2.exchange,
        "leg2_side": leg2.side,
        "leg2_planned_price": leg2.planned_price,
        "leg2_actual_price": leg2.actual_price,
        "leg2_planned_qty": leg2.planned_contracts,
        "leg2_filled_qty": leg2.filled_contracts,
        "leg2_order_id": leg2.order_id,
        "leg2_status": leg2.status,
        "leg2_latency_ms": round(leg2.latency_ms, 1),
        "leg2_error": leg2.error,
        "slippage_poly": round(slip_poly, 6),
        "slippage_kalshi": round(slip_kalshi, 6),
        "slippage_total_bps": round((slip_poly + slip_kalshi) * 10000, 1),
    }
    append_log(logfile, exec_row)

    # Console output
    s1 = f"Poly {leg1.side}: {leg1.status}"
    if leg1.actual_price is not None and leg1.actual_price != leg1.planned_price:
        s1 += f" (slip {slip_poly:+.4f})"
    s2 = f"Kalshi {leg2.side}: {leg2.status}"
    if leg2.actual_price is not None and leg2.actual_price != leg2.planned_price:
        s2 += f" (slip {slip_kalshi:+.4f})"

    kalshi_ms = (kalshi_done - t_total) * 1000
    print(f"  [exec] {s2} | {s1} | kalshi={kalshi_ms:.0f}ms | total={total_ms:.0f}ms")

    if leg1.status == "partial" or leg2.status == "partial":
        print(f"  [exec] PARTIAL FILL: Poly {leg1.filled_contracts}/{leg1.planned_contracts} "
              f"| Kalshi {leg2.filled_contracts}/{leg2.planned_contracts}")

    if not both_filled:
        print(f"  [exec] *** WARNING: NOT BOTH LEGS FILLED — UNHEDGED EXPOSURE ***")
        if leg1.error:
            print(f"  [exec]   Poly error: {leg1.error}")
        if leg2.error:
            print(f"  [exec]   Kalshi error: {leg2.error}")

        # --- Automatic unwind ---
        # Kalshi has more fills than Poly → unwind excess Kalshi contracts
        # Use filled_contracts instead of status to handle "partial" fills
        kalshi_excess = leg2.filled_contracts - leg1.filled_contracts
        poly_excess = leg1.filled_contracts - leg2.filled_contracts

        if kalshi_excess > 0:
            if EXEC_MODE == "live":
                print(f"  [unwind] Poly under-filled — selling {kalshi_excess:.0f} Kalshi contracts to close exposure...")
                unwind_result = _unwind_kalshi_leg(
                    candidate.direction_on_kalshi, leg2.actual_price or leg2.planned_price,
                    kalshi_excess, kalshi_quote.ticker, logfile,
                )
                if unwind_result:
                    print(f"  [unwind] {unwind_result}")
                if unwind_result and "FAILED" in unwind_result:
                    result.unwind_failed = True
                exec_row["unwind_attempted"] = True
                exec_row["unwind_contracts"] = kalshi_excess
                exec_row["unwind_result"] = unwind_result
            else:
                print(f"  [unwind] Paper mode — would sell {kalshi_excess:.0f} Kalshi contracts")
                exec_row["unwind_attempted"] = False
                exec_row["unwind_contracts"] = kalshi_excess
                exec_row["unwind_result"] = "paper_mode_skip"

        # Poly has more fills than Kalshi → unwind excess Poly contracts
        elif poly_excess > 0:
            if EXEC_MODE == "live":
                print(f"  [unwind] Kalshi under-filled — selling {poly_excess:.0f} Poly contracts to close exposure...")
                unwind_result = _unwind_poly_leg(
                    candidate.direction_on_poly, leg1.actual_price or leg1.planned_price,
                    poly_excess, poly_token, logfile,
                )
                if unwind_result:
                    print(f"  [unwind] {unwind_result}")
                if unwind_result and "FAILED" in unwind_result:
                    result.unwind_failed = True
                exec_row["unwind_attempted"] = True
                exec_row["unwind_contracts"] = poly_excess
                exec_row["unwind_result"] = unwind_result
            else:
                print(f"  [unwind] Paper mode — would sell {poly_excess:.0f} Poly contracts")
                exec_row["unwind_attempted"] = False
                exec_row["unwind_contracts"] = poly_excess
                exec_row["unwind_result"] = "paper_mode_skip"

    return result


def _unwind_poly_leg(side: str, buy_price: float, contracts: float,
                     token_id: str, logfile: str) -> str:
    """Sell back Poly contracts to close unhedged exposure.

    Uses MAX_UNHEDGED_SECONDS as the fill deadline. If the first attempt
    (buy_price - $0.02) times out, retries with aggressive pricing
    (buy_price - $0.05) to force a close.
    """
    discounts = [0.02, 0.05]  # initial discount, then aggressive retry
    last_msg = ""
    for attempt, discount in enumerate(discounts, 1):
        try:
            client = _get_poly_clob_client()
            sell_price = max(0.01, round(buy_price - discount, 2))

            order_args = OrderArgs(
                token_id=token_id,
                price=sell_price,
                size=contracts,
                side=SELL,
            )
            signed_order = client.create_order(order_args)
            resp = client.post_order(signed_order, OrderType.GTC)

            if not resp.get("success", False):
                last_msg = f"unwind rejected: {resp.get('errorMsg') or resp.get('error') or resp}"
                append_log(logfile, {"log_type": "unwind", "ts": utc_ts(), "exchange": "poly",
                                     "side": side, "contracts": contracts, "status": "rejected",
                                     "attempt": attempt, "detail": last_msg})
                continue

            order_id = resp.get("orderID") or resp.get("id")

            # Poll for fill — use MAX_UNHEDGED_SECONDS as deadline
            deadline = time.monotonic() + MAX_UNHEDGED_SECONDS
            while time.monotonic() < deadline:
                try:
                    o = client.get_order(order_id)
                    o_status = (o.get("status") or "").lower()
                    if o_status in ("matched", "filled"):
                        msg = f"unwound {contracts:.0f} Poly contracts at ~${sell_price:.2f} (order {order_id})"
                        append_log(logfile, {"log_type": "unwind", "ts": utc_ts(), "exchange": "poly",
                                             "side": side, "contracts": contracts, "sell_price": sell_price,
                                             "order_id": order_id, "status": "filled", "attempt": attempt})
                        return msg
                except Exception:
                    pass
                time.sleep(ORDER_POLL_INTERVAL_S)

            # Timeout — cancel and retry with deeper discount
            try:
                client.cancel(order_id)
            except Exception:
                pass

            if attempt < len(discounts):
                print(f"  [unwind] Poly unwind attempt {attempt} timed out at -${discount:.2f} — "
                      f"retrying with -${discounts[attempt]:.2f} aggressive pricing")
                last_msg = f"unwind attempt {attempt} timed out"
            else:
                last_msg = f"unwind FAILED after {attempt} attempts (order {order_id}) — NEEDS MANUAL CLOSE"
                append_log(logfile, {"log_type": "unwind", "ts": utc_ts(), "exchange": "poly",
                                     "side": side, "contracts": contracts, "order_id": order_id,
                                     "status": "timeout_final", "attempt": attempt})

        except Exception as e:
            last_msg = f"unwind error (attempt {attempt}): {e}"
            append_log(logfile, {"log_type": "unwind", "ts": utc_ts(), "exchange": "poly",
                                 "side": side, "contracts": contracts, "status": "error",
                                 "attempt": attempt, "detail": str(e)})

    return last_msg


def _unwind_kalshi_leg(side: str, buy_price: float, contracts: float,
                       ticker: str, logfile: str) -> str:
    """Sell back Kalshi contracts to close unhedged exposure.

    Uses MAX_UNHEDGED_SECONDS as the fill deadline. If the first attempt
    (buy_price - $0.02) times out, retries with aggressive pricing
    (buy_price - $0.05) to force a close.
    """
    discounts = [0.02, 0.05]  # initial discount, then aggressive retry
    last_msg = ""
    for attempt, discount in enumerate(discounts, 1):
        try:
            kalshi_side = "yes" if side == "UP" else "no"
            sell_price_cents = max(1, int(round((buy_price - discount) * 100)))

            body: dict = {
                "ticker": ticker,
                "action": "sell",
                "side": kalshi_side,
                "type": "limit",
                "count": int(contracts),
                "client_order_id": f"polyshi-unwind-{uuid.uuid4().hex[:12]}",
            }
            if kalshi_side == "yes":
                body["yes_price"] = sell_price_cents
            else:
                body["no_price"] = sell_price_cents

            resp = _kalshi_auth_post("/portfolio/orders", body)
            order = resp.get("order", resp)
            order_id = order.get("order_id") or order.get("id")

            if not order_id:
                last_msg = f"unwind rejected: no order_id in response"
                append_log(logfile, {"log_type": "unwind", "ts": utc_ts(), "exchange": "kalshi",
                                     "side": side, "contracts": contracts, "status": "rejected",
                                     "attempt": attempt, "detail": last_msg})
                continue

            # Poll for fill — use MAX_UNHEDGED_SECONDS as deadline
            deadline = time.monotonic() + MAX_UNHEDGED_SECONDS
            while time.monotonic() < deadline:
                try:
                    poll = _kalshi_auth_get(f"/portfolio/orders/{order_id}")
                    o = poll.get("order", poll)
                    if o.get("status") == "executed":
                        msg = f"unwound {contracts:.0f} Kalshi contracts at ~${sell_price_cents/100:.2f} (order {order_id})"
                        append_log(logfile, {"log_type": "unwind", "ts": utc_ts(), "exchange": "kalshi",
                                             "side": side, "contracts": contracts,
                                             "sell_price": sell_price_cents / 100.0,
                                             "order_id": order_id, "status": "filled", "attempt": attempt})
                        return msg
                except Exception:
                    pass
                time.sleep(ORDER_POLL_INTERVAL_S)

            # Timeout — cancel via DELETE (Kalshi v2 API)
            try:
                _kalshi_auth_delete(f"/portfolio/orders/{order_id}")
            except Exception:
                # Cancel failed — re-check if order actually filled
                try:
                    poll = _kalshi_auth_get(f"/portfolio/orders/{order_id}")
                    o = poll.get("order", poll)
                    if (o.get("status") or "").lower() in ("executed", "filled", "complete"):
                        msg = f"unwound {contracts:.0f} Kalshi contracts (filled despite cancel error, order {order_id})"
                        append_log(logfile, {"log_type": "unwind", "ts": utc_ts(), "exchange": "kalshi",
                                             "side": side, "contracts": contracts,
                                             "order_id": order_id, "status": "filled", "attempt": attempt})
                        return msg
                except Exception:
                    pass

            if attempt < len(discounts):
                print(f"  [unwind] Kalshi unwind attempt {attempt} timed out at -${discount:.2f} — "
                      f"retrying with -${discounts[attempt]:.2f} aggressive pricing")
                last_msg = f"unwind attempt {attempt} timed out"
            else:
                last_msg = f"unwind FAILED after {attempt} attempts (order {order_id}) — NEEDS MANUAL CLOSE"
                append_log(logfile, {"log_type": "unwind", "ts": utc_ts(), "exchange": "kalshi",
                                     "side": side, "contracts": contracts, "order_id": order_id,
                                     "status": "timeout_final", "attempt": attempt})

        except Exception as e:
            last_msg = f"unwind error (attempt {attempt}): {e}"
            append_log(logfile, {"log_type": "unwind", "ts": utc_ts(), "exchange": "kalshi",
                                 "side": side, "contracts": contracts, "status": "error",
                                 "attempt": attempt, "detail": str(e)})

    return last_msg


def log_skip(logfile: str, skip_counts: Dict[str, int], scan_num: int,
             coin: str, reason: str, poly: Optional[PolyMarketQuote] = None,
             kalshi: Optional[KalshiMarketQuote] = None,
             remaining_s: Optional[float] = None) -> None:
    """Log a trade skip with full context for post-mortem analysis."""
    skip_counts[reason] = skip_counts.get(reason, 0) + 1
    row: dict = {
        "log_type": "skip", "ts": utc_ts(), "scan_num": scan_num,
        "coin": coin, "reason": reason,
    }
    if poly:
        row["poly_up"] = poly.up_price
        row["poly_down"] = poly.down_price
        row["poly_spread"] = round(poly.up_price + poly.down_price - 1.0, 6)
        row["poly_slug"] = poly.event_slug
        row["poly_end_ts"] = poly.end_ts.isoformat()
        row["poly_up_token"] = poly.up_token_id
        row["poly_down_token"] = poly.down_token_id
    if kalshi:
        row["kalshi_up"] = kalshi.yes_ask
        row["kalshi_down"] = kalshi.no_ask
        row["kalshi_spread"] = round(kalshi.yes_ask + kalshi.no_ask - 1.0, 6)
        row["kalshi_ticker"] = kalshi.ticker
        row["kalshi_close_ts"] = kalshi.close_ts.isoformat()
        row["kalshi_strike"] = kalshi.strike
    if poly and kalshi:
        row["total_cost_up_down"] = round(poly.up_price + kalshi.no_ask, 6)
        row["total_cost_down_up"] = round(poly.down_price + kalshi.yes_ask, 6)
        row["prob_div"] = round(abs((1.0 - kalshi.no_ask) - poly.up_price), 6)
    if remaining_s is not None:
        row["remaining_s"] = round(remaining_s, 1)
    append_log(logfile, row)


def log_kill_switch(logfile: str, reason: str, context: dict) -> None:
    """Log exactly why the bot is shutting down."""
    row = {
        "log_type": "kill_switch",
        "ts": utc_ts(),
        "reason": reason,
        **context,
    }
    append_log(logfile, row)
    print(f"\n  [KILL SWITCH] {reason}")
    for k, v in context.items():
        print(f"    {k}: {v}")


# -----------------------------
# Outcome verification (post-trade)
# -----------------------------
def _poll_kalshi_result(ticker: str, max_retries: int = 6, poll_interval: float = 10.0) -> Optional[str]:
    """Poll Kalshi market for settled result. Returns 'yes', 'no', or None if still unsettled."""
    for attempt in range(max_retries):
        try:
            mkt = kalshi_get_market(ticker)
            result = mkt.get("result")
            if result in ("yes", "no"):
                return result
            if attempt < max_retries - 1:
                time.sleep(poll_interval)
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(poll_interval)
    return None


def verify_trade_outcomes(trades: List[dict], logfile: str) -> List[dict]:
    """Wait for windows to close, poll Kalshi for results, compute actual P&L."""
    if not trades:
        return trades

    print("\n" + "=" * 60)
    print("  OUTCOME VERIFICATION")
    print("=" * 60)

    # Find latest window close across all trades
    latest_close: Optional[datetime] = None
    for row in trades:
        close_str = row.get("window_close_ts")
        if close_str:
            ct = parse_iso_utc(close_str)
            if ct and (latest_close is None or ct > latest_close):
                latest_close = ct

    if latest_close:
        now = datetime.now(timezone.utc)
        wait_s = (latest_close - now).total_seconds() + 30  # 30s buffer for settlement
        if wait_s > 0:
            print(f"\n  Waiting {wait_s:.0f}s for last window to close + settle...")
            time.sleep(wait_s)

    print(f"\n  Checking {len(trades)} trade outcomes...\n")

    # Cache Kalshi results per ticker (multiple trades may share same window)
    kalshi_results: Dict[str, Optional[str]] = {}

    for row in trades:
        ticker = row["kalshi_ref"]
        if ticker not in kalshi_results:
            print(f"  Polling Kalshi {ticker}...", end=" ", flush=True)
            result = _poll_kalshi_result(ticker)
            kalshi_results[ticker] = result
            print(f"result={result or 'UNKNOWN'}")

        result = kalshi_results[ticker]
        row["kalshi_result"] = result

        if result in ("yes", "no"):
            price_went_up = (result == "yes")

            poly_won = (row["poly_side"] == "UP" and price_went_up) or \
                       (row["poly_side"] == "DOWN" and not price_went_up)
            kalshi_won = (row["kalshi_side"] == "UP" and price_went_up) or \
                         (row["kalshi_side"] == "DOWN" and not price_went_up)

            payout = (1.0 if poly_won else 0.0) + (1.0 if kalshi_won else 0.0)
            # Use actual fill prices for live trades (accounts for slippage)
            actual_poly = row.get("exec_leg1_actual_price")
            actual_kalshi = row.get("exec_leg2_actual_price")
            if actual_poly is not None and actual_kalshi is not None:
                cost = actual_poly + actual_kalshi
            else:
                cost = row["total_cost"]
            fees_pc = (row["poly_fee"] + row["kalshi_fee"] + row["extras"]) / PAPER_CONTRACTS
            actual_pnl_pc = payout - cost - fees_pc
            actual_pnl_total = actual_pnl_pc * PAPER_CONTRACTS

            row["poly_won"] = poly_won
            row["kalshi_won"] = kalshi_won
            row["payout_per_contract"] = payout
            row["actual_pnl_total"] = round(actual_pnl_total, 4)
            row["hedge_consistent"] = (poly_won != kalshi_won)  # exactly one should win

            tag = "WIN" if actual_pnl_total > 0 else ("LOSS" if actual_pnl_total < 0 else "BREAK-EVEN")
            hedge_tag = "OK" if row["hedge_consistent"] else "BOTH-LOST" if not poly_won and not kalshi_won else "BOTH-WON"

            print(f"  [{row['ts']}] {row['coin']} | Kalshi: {result} | "
                  f"Poly {row['poly_side']}: {'WON' if poly_won else 'LOST'} | "
                  f"Kalshi {row['kalshi_side']}: {'WON' if kalshi_won else 'LOST'} | "
                  f"Hedge: {hedge_tag} | P&L: ${actual_pnl_total:+.4f} ({tag})")
        else:
            row["actual_pnl_total"] = None
            row["hedge_consistent"] = None
            print(f"  [{row['ts']}] {row['coin']} | Result: UNKNOWN (settlement not available)")

        # Append outcome row to logfile
        append_log(logfile, {**row, "log_type": "outcome"})

    return trades


# -----------------------------
# Main loop
# -----------------------------
def main() -> None:
    global EXEC_MODE
    print(f"\n  Polyshi v{VERSION}  ({VERSION_DATE})\n")
    ensure_dir(LOG_DIR)
    logfile = os.path.join(LOG_DIR, f"arb_logs_market_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.jsonl")

    # VPN check: ensure IP is in Ireland
    try:
        r = _get_session().get("https://ipinfo.io/json", timeout=10)
        geo = r.json()
        country = geo.get("country", "??")
        city = geo.get("city", "")
        ip = geo.get("ip", "")
        if country != "IE":
            print(f"\n*** VPN CHECK FAILED ***")
            print(f"  IP: {ip} | Location: {city}, {country}")
            print(f"  Expected: Ireland (IE)")
            print(f"  Please activate your VPN and retry.")
            return
        print(f"VPN check: OK ({city}, {country} | {ip})")
    except Exception as e:
        print(f"\n*** VPN CHECK FAILED — could not determine location: {e}")
        return

    # First choice: execution mode
    EXEC_MODE = prompt_execution_mode()

    # Redeem mode: collect unredeemed Polymarket winnings and exit
    if EXEC_MODE == "redeem":
        print("\n  Scanning logs for unredeemed Polymarket positions...")
        try:
            n = redeem_all_old_positions()
            if n > 0:
                print(f"\n  Done — redeemed {n} position(s).")
            else:
                print("\n  No unredeemed positions found.")
        except Exception as e:
            print(f"\n  Redemption failed: {e}")
        return

    market_type = prompt_market_type()
    if market_type != "CRYPTO_15M_UPDOWN":
        raise RuntimeError(f"Market type not implemented: {market_type}")

    selected_coins = prompt_coin_selection(AVAILABLE_COINS)

    session_duration_s = prompt_session_duration()

    print("\nConfirm settings")
    print("=" * 45)
    mode_label = "*** LIVE TRADING ***" if EXEC_MODE == "live" else "Paper Testing"
    duration_label = f"{session_duration_s // 60} minutes" if session_duration_s < 3600 else "1 hour"
    print(f"Execution:  {mode_label}")
    print(f"Market Type: {market_type}")
    print(f"Coins: {', '.join(selected_coins)}")
    print(f"Duration:   {duration_label}")
    print(f"Contracts:  {int(PAPER_CONTRACTS)}")
    print(f"\nExecution safeguards:")
    print(f"  Min net edge:       {pct(MIN_NET_EDGE)}")
    print(f"  Max net edge:       {pct(MAX_NET_EDGE)} (stale data filter)")
    print(f"  Max total cost:     {MAX_TOTAL_COST:.3f}")
    print(f"  Min window time:    {MIN_WINDOW_REMAINING_S:.0f}s")
    print(f"  Max spread:         {pct(MAX_SPREAD)}")
    print(f"  Price range:        [{PRICE_FLOOR:.2f}, {PRICE_CEILING:.2f}]")
    print(f"  Max prob diverge:   {pct(MAX_PROB_DIVERGENCE)} (strike mismatch detector)")
    print(f"  Max strike-spot Δ:  {MAX_STRIKE_SPOT_DIVERGENCE*100:.2f}% (hedge validity guard)")
    print(f"  Min leg notional:   ${MIN_LEG_NOTIONAL:.0f} (skip thin books)")
    print(f"  Max slippage:       ${POLY_MAX_SLIPPAGE:.2f} (Poly fill rejection)")
    print(f"  Fill price buffer:  ${LIVE_PRICE_BUFFER:.2f} (limit order cushion)")
    print(f"  Session drawdown:   ${MAX_SESSION_DRAWDOWN:.2f} max loss before auto-stop")
    print(f"  Balance floor:      ${MIN_ACCOUNT_BALANCE:.2f} per account (stop if either drops below)")
    print(f"  Consec loss stop:   2 losing 15m windows in a row → stop")
    print(f"  Depth cap ratio:    {POLY_DEPTH_CAP_RATIO:.0%} of Poly book (min {MIN_CONTRACTS} contracts)")
    print(f"  Max unhedged time:  {MAX_UNHEDGED_SECONDS:.0f}s per attempt (2-pass unwind: -$0.02 then -$0.05)")
    print(f"  Circuit breaker:    {MAX_CONSECUTIVE_SKIPS} consecutive skips")
    print(f"  Poly fill retries:  {POLY_FILL_MAX_RETRIES} attempts x {POLY_FILL_RETRY_TIMEOUT_S:.0f}s each")
    print(f"  Per-coin trade cap: {MAX_TRADES_PER_COIN} trades/coin (incl. unwinds)")
    print(f"  Per-window cap:     {MAX_TRADES_PER_COIN_PER_WINDOW} trades/coin/window (resets each 15m)")
    print(f"  Max consec unwinds: {MAX_CONSECUTIVE_UNWINDS}/coin before stopping")

    # Start Polymarket CLOB WebSocket for real-time orderbook data
    global _poly_ws
    if _HAS_WS_CLIENT:
        _poly_ws = _PolyOrderbookWS()
        _poly_ws.start()
        print("WebSocket: Polymarket CLOB connected (cache warms after first scan)")
    else:
        print("WebSocket: websocket-client not installed; using HTTP-only (pip install websocket-client)")

    # Live mode: validate credentials and confirm
    if EXEC_MODE == "live":
        # Pre-flight credential check — fail fast if anything is missing
        missing = []
        if not KALSHI_API_KEY_ID:
            missing.append("KALSHI_API_KEY_ID")
        if not KALSHI_PRIVATE_KEY_PATH:
            missing.append("KALSHI_PRIVATE_KEY_PATH")
        elif not os.path.isfile(KALSHI_PRIVATE_KEY_PATH):
            missing.append(f"KALSHI_PRIVATE_KEY_PATH (file not found: {KALSHI_PRIVATE_KEY_PATH})")
        if not POLY_PRIVATE_KEY:
            missing.append("POLY_PRIVATE_KEY")
        if POLY_SIGNATURE_TYPE in (1, 2) and not POLY_FUNDER_ADDRESS:
            missing.append("POLY_FUNDER_ADDRESS (required for signature_type 1 or 2)")
        if not _HAS_CRYPTO:
            missing.append("'cryptography' package (pip install cryptography)")
        if not _HAS_CLOB_CLIENT:
            missing.append("'py-clob-client' package (pip install py-clob-client)")

        if missing:
            print("\n*** LIVE MODE BLOCKED — missing credentials/dependencies: ***")
            for m in missing:
                print(f"  - {m}")
            print("\nSet these in your .env file and retry.")
            return

        # Test Kalshi key loading
        try:
            _load_kalshi_private_key()
            print("Kalshi:  RSA key loaded OK")
        except Exception as e:
            print(f"\n*** LIVE MODE BLOCKED — Kalshi key error: {e}")
            return

        # Test Poly CLOB client initialization
        try:
            client = _get_poly_clob_client()
            print("Poly:    CLOB client initialized OK")
            # Diagnostic: check what the CLOB API sees for balance/allowance
            try:
                from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
                ba = client.get_balance_allowance(
                    BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
                )
                raw_bal = float(ba.get("balance", "0"))
                raw_allow = float(ba.get("allowance", "0"))
                print(f"         USDC.e balance:   {raw_bal / 1e6:.4f} (raw: {raw_bal})")
                print(f"         USDC.e allowance: {raw_allow / 1e6:.4f} (raw: {raw_allow})")
                if raw_allow < 1e6:
                    if POLY_SIGNATURE_TYPE == 0:
                        print("         *** ALLOWANCE TOO LOW — submitting on-chain approvals ***")
                        try:
                            n = _approve_poly_contracts()
                            print(f"         {n} approval txns confirmed on Polygon")
                            # Re-check allowance
                            ba2 = client.get_balance_allowance(
                                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
                            new_allow = float(ba2.get("allowance", "0"))
                            print(f"         New allowance: {new_allow / 1e6:.4f}")
                        except Exception as e2:
                            print(f"         *** APPROVAL FAILED: {e2}")
                            print("         You may need POL for gas or check your POLY_PRIVATE_KEY")
                            return
                    else:
                        print("         Allowance low — proxy mode: approvals managed by Polymarket website")
                        print("         (Make sure you've deposited USDC.e through polymarket.com)")
            except Exception as e:
                print(f"         Balance/allowance check failed: {e}")
        except Exception as e:
            print(f"\n*** LIVE MODE BLOCKED — Poly CLOB client error: {e}")
            return

        print("\n*** WARNING: LIVE TRADING MODE ***")
        print("Real orders will be placed on Polymarket and Kalshi.")
        print(f"Contracts per trade: {int(PAPER_CONTRACTS)}")
        print(f"Order timeout: {ORDER_TIMEOUT_S}s")
        if not prompt_yes_no("Are you sure you want to continue?", default=False):
            print("Aborted.")
            return

    print("\nPress ENTER to start scanning (Ctrl+C to stop)...")
    input()

    # Pre-flight balance check
    balances = check_balances(logfile)
    session_start_total = 0.0
    print(f"\n{_box_top('BALANCES')}")
    for exch, bal in balances.items():
        if bal < 0:
            print(_box_line(f"{exch.upper():12s} UNAVAILABLE"))
        else:
            print(_box_line(f"{exch.upper():12s} ${bal:.2f}"))
            session_start_total += bal
    if session_start_total > 0:
        print(_box_mid())
        print(_box_line(f"{'TOTAL':12s} ${session_start_total:.2f}"))
    print(_box_bot())

    crypto_tag_id = poly_get_crypto_tag_id()
    if crypto_tag_id is None:
        raise RuntimeError("Could not resolve Polymarket tag id for slug 'crypto' (Gamma /tags/slug/crypto).")

    logged: List[dict] = []
    skip_counts: Dict[str, int] = {}  # reason -> count
    scan_i = 0
    consecutive_skips = 0

    # Per-coin tracking
    coin_trade_counts: Dict[str, int] = {c: 0 for c in AVAILABLE_COINS}
    coin_window_trade_counts: Dict[str, int] = {c: 0 for c in AVAILABLE_COINS}  # resets each 15m window
    coin_consecutive_unwinds: Dict[str, int] = {c: 0 for c in AVAILABLE_COINS}
    coin_window_edges: Dict[str, List[float]] = {c: [] for c in AVAILABLE_COINS}  # per-window edge history
    coin_stopped: Dict[str, str] = {}  # coin -> reason it was stopped

    # Store poly quotes per-coin per-scan for book depth lookups on winning trade
    _scan_poly_quotes: Dict[str, Optional[PolyMarketQuote]] = {}

    # --- Timed session + window tracking ---
    session_deadline = time.monotonic() + session_duration_s
    session_end_utc = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(seconds=session_duration_s)
    current_window_close_ts: Optional[datetime] = None  # UTC close time of current 15m window
    window_open_utc: Optional[datetime] = None           # UTC time when current window was first detected
    window_start_total = session_start_total             # Portfolio value at start of current window
    consecutive_losing_windows = 0
    window_trades: List[dict] = []                       # Trades placed in the current window
    stop_reason: Optional[str] = None

    print(f"\n  Session started — will run until {session_end_utc.strftime('%H:%M:%S')} UTC ({duration_label})")

    while time.monotonic() < session_deadline:
        scan_i += 1
        scan_t0 = time.monotonic()

        # --- Window transition: detect when the previous 15m window has closed ---
        now_utc = datetime.now(timezone.utc)
        if current_window_close_ts and now_utc > current_window_close_ts:
            print(f"\n{'='*60}")
            print(f"  15-MIN WINDOW ENDED — Running post-market cycle")
            print(f"{'='*60}")

            # Brief pause for market settlement before redemption
            if window_trades and EXEC_MODE == "live":
                print("  Waiting 15s for market settlement...")
                time.sleep(15)

            # 1. Redeem winning positions from the ended window
            if window_trades and EXEC_MODE == "live" and POLY_SIGNATURE_TYPE == 0:
                try:
                    n = redeem_poly_positions(window_trades)
                    print(f"  [window] Redeemed {n} position(s)")
                except Exception as e:
                    print(f"  [window] Redemption failed: {e}")

            # 2. Window contract breakdown by coin and platform
            if window_trades:
                # Gather per-coin, per-exchange fill data from execution legs
                coin_exch: Dict[str, Dict[str, list]] = {}  # coin -> {exchange -> [(qty, price)]}
                for wt in window_trades:
                    c = wt.get("coin", "?")
                    for leg in ("exec_leg1", "exec_leg2"):
                        exch = wt.get(f"{leg}_exchange")
                        qty = wt.get(f"{leg}_filled_qty") or 0
                        px = wt.get(f"{leg}_actual_price") or wt.get(f"{leg.replace('exec_', '')}_planned_price")
                        if exch and qty > 0 and px:
                            coin_exch.setdefault(c, {}).setdefault(exch, []).append((qty, px))

                w = BOX_W
                print(f"\n{'*' * (w + 4)}")
                print(f"*{'WINDOW CONTRACT BREAKDOWN':^{w + 2}}*")
                print(f"{'*' * (w + 4)}")
                print(f"*{'':<{w + 2}}*")
                hdr = f"  {'COIN':<6} {'EXCHANGE':<10} {'CONTRACTS':>10} {'AVG PRICE':>10} {'TOTAL $':>10}"
                print(f"*{hdr:<{w + 2}}*")
                print(f"*{'  ' + '-' * (w - 2):<{w + 2}}*")

                grand_kalshi_qty = 0.0
                grand_kalshi_cost = 0.0
                grand_poly_qty = 0.0
                grand_poly_cost = 0.0

                for coin_name in sorted(coin_exch.keys()):
                    exchanges = coin_exch[coin_name]
                    for exch_name in ("kalshi", "poly"):
                        fills = exchanges.get(exch_name, [])
                        if not fills:
                            continue
                        total_qty = sum(f[0] for f in fills)
                        avg_px = sum(f[0] * f[1] for f in fills) / total_qty if total_qty else 0
                        total_cost = sum(f[0] * f[1] for f in fills)
                        line = f"  {coin_name.upper():<6} {exch_name.upper():<10} {total_qty:>10.1f} {avg_px:>10.4f} {total_cost:>9.2f}"
                        print(f"*{line:<{w + 2}}*")
                        if exch_name == "kalshi":
                            grand_kalshi_qty += total_qty
                            grand_kalshi_cost += total_cost
                        else:
                            grand_poly_qty += total_qty
                            grand_poly_cost += total_cost

                print(f"*{'  ' + '-' * (w - 2):<{w + 2}}*")
                k_avg = grand_kalshi_cost / grand_kalshi_qty if grand_kalshi_qty else 0
                p_avg = grand_poly_cost / grand_poly_qty if grand_poly_qty else 0
                k_line = f"  {'TOTAL':<6} {'KALSHI':<10} {grand_kalshi_qty:>10.1f} {k_avg:>10.4f} {grand_kalshi_cost:>9.2f}"
                p_line = f"  {'':<6} {'POLY':<10} {grand_poly_qty:>10.1f} {p_avg:>10.4f} {grand_poly_cost:>9.2f}"
                print(f"*{k_line:<{w + 2}}*")
                print(f"*{p_line:<{w + 2}}*")
                delta = grand_kalshi_qty - grand_poly_qty
                if abs(delta) > 0.01:
                    warn = f"  UNHEDGED: {abs(delta):.1f} contracts ({'Kalshi' if delta > 0 else 'Poly'} heavy)"
                    print(f"*{warn:<{w + 2}}*")
                print(f"*{'':<{w + 2}}*")
                print(f"{'*' * (w + 4)}")
            else:
                print(f"\n  [window] No trades this window")

            # 3. Balance check + stop contingencies (live mode only)
            if EXEC_MODE == "live":
                try:
                    current_bal = check_balances(logfile)
                    current_total = sum(v for v in current_bal.values() if v >= 0)

                    print(f"  [window] Balances: Kalshi=${current_bal.get('kalshi', -1):.2f}, "
                          f"Poly=${current_bal.get('poly', -1):.2f}, Total=${current_total:.2f}")

                    # Stop contingency 1: $50 minimum balance per account
                    for exch, bal in current_bal.items():
                        if 0 <= bal < MIN_ACCOUNT_BALANCE:
                            stop_reason = (f"BALANCE FLOOR: {exch.upper()} balance ${bal:.2f} "
                                           f"< ${MIN_ACCOUNT_BALANCE:.2f}")
                            break

                    # Stop contingency 2: consecutive losing windows
                    if not stop_reason:
                        window_pnl = current_total - window_start_total
                        if window_pnl < 0:
                            consecutive_losing_windows += 1
                            print(f"  [window] Window P&L: ${window_pnl:+.2f} "
                                  f"(consecutive losses: {consecutive_losing_windows})")
                        else:
                            consecutive_losing_windows = 0
                            print(f"  [window] Window P&L: ${window_pnl:+.2f} (loss streak reset)")

                        if consecutive_losing_windows >= 2:
                            stop_reason = "CONSECUTIVE LOSS STOP: 2 losing windows in a row"

                    # Update baseline for next window
                    window_start_total = current_total
                except Exception as e:
                    print(f"  [window] Post-market cycle failed: {e}")

            window_trades = []
            current_window_close_ts = None  # Will be set from next market data
            window_open_utc = None  # Reset so 2-phase startup triggers for new window
            # Reset per-window trade limits and edge history so coins can trade again
            for c in coin_window_trade_counts:
                coin_window_trade_counts[c] = 0
                coin_window_edges[c] = []

            if stop_reason:
                print(f"\n*** {stop_reason} — stopping session ***")
                break

        # Show remaining session time in header
        session_remaining_s = max(0, session_deadline - time.monotonic())
        session_remaining_m = int(session_remaining_s // 60)
        session_remaining_sec = int(session_remaining_s % 60)
        print_scan_header(scan_i)
        print(f"  Session: {session_remaining_m}m {session_remaining_sec}s remaining | "
              f"Trades: {len(logged)} | Losing windows: {consecutive_losing_windows}/2")

        # Overlap Gamma event fetch with Kalshi fetches so neither exchange's
        # prices go stale while the other is loading.
        # Kalshi doesn't need poly_events, so start both in parallel.
        fetch_t0 = time.monotonic()

        with ThreadPoolExecutor(max_workers=len(selected_coins) + 1) as executor:
            # Kick off Gamma events fetch concurrently with all Kalshi fetches
            gamma_future = executor.submit(
                poly_get_active_15m_crypto_events,
                crypto_tag_id=crypto_tag_id, limit=250,
            )
            kalshi_futures = {
                executor.submit(
                    lambda c=coin: pick_current_kalshi_market(KALSHI_SERIES[c])
                ): coin
                for coin in selected_coins
            }

            # Collect Kalshi results as they arrive
            kalshi_results: Dict[str, Any] = {}
            kalshi_errors: Dict[str, str] = {}
            for future in as_completed(kalshi_futures):
                coin = kalshi_futures[future]
                try:
                    kalshi_results[coin] = future.result(timeout=15)
                except Exception as e:
                    kalshi_errors[coin] = str(e)

            # Wait for Gamma events (may already be done)
            gamma_t0 = time.monotonic()
            try:
                poly_events = gamma_future.result(timeout=20)
            except Exception:
                poly_events = []
            gamma_ms = (time.monotonic() - gamma_t0) * 1000

        # Local filter: keep only events that are actually 15-minute recurrence (if field present)
        pre_filter_count = len(poly_events)
        filtered = []
        for e in poly_events:
            rec = (e.get("recurrence") or "").upper()
            # keep if explicitly 15m, otherwise keep and let title-matching decide (safer than dropping everything)
            if rec in ("15M", "15MIN", "15MINUTES"):
                filtered.append(e)
            elif rec == "":
                filtered.append(e)

        poly_events = filtered

        # Now fetch Poly CLOB quotes for all coins in parallel (Kalshi already done)
        poly_results: Dict[str, Any] = {}
        poly_errors: Dict[str, str] = {}
        with ThreadPoolExecutor(max_workers=len(selected_coins)) as executor:
            poly_futures = {
                executor.submit(
                    extract_poly_quote_for_coin, poly_events, coin
                ): coin
                for coin in selected_coins
            }
            for future in as_completed(poly_futures):
                coin = poly_futures[future]
                try:
                    poly_results[coin] = future.result(timeout=15)
                except Exception as e:
                    poly_errors[coin] = str(e)

        # Assemble coin_data in the same format as before
        coin_data: Dict[str, dict] = {}
        for coin in selected_coins:
            coin_data[coin] = {
                "coin": coin,
                "kalshi": kalshi_results.get(coin),
                "poly": poly_results.get(coin),
                "kalshi_ms": 0.0,
                "poly_ms": 0.0,
                "kalshi_err": kalshi_errors.get(coin),
                "poly_err": poly_errors.get(coin),
            }
        fetch_ms = (time.monotonic() - fetch_t0) * 1000

        # Update current window close time from market data
        prev_window_close = current_window_close_ts
        for coin in selected_coins:
            k = coin_data[coin].get("kalshi")
            if k and hasattr(k, 'close_ts') and k.close_ts:
                if current_window_close_ts is None or k.close_ts > current_window_close_ts:
                    current_window_close_ts = k.close_ts

        # Detect new window: when close_ts advances (or first seen), record window open time
        if current_window_close_ts and (prev_window_close is None or current_window_close_ts != prev_window_close):
            window_open_utc = datetime.now(timezone.utc)
            print(f"  [startup] New window detected (closes {current_window_close_ts.strftime('%H:%M:%S')} UTC) "
                  f"— {WINDOW_STARTUP_DELAY_S:.0f}s startup delay begins")

        # 2-phase startup: skip trading until WINDOW_STARTUP_DELAY_S has elapsed since window open
        window_startup_active = False
        if window_open_utc is not None:
            window_age_s = (datetime.now(timezone.utc) - window_open_utc).total_seconds()
            if window_age_s < WINDOW_STARTUP_DELAY_S:
                window_startup_active = True
                remaining_startup = WINDOW_STARTUP_DELAY_S - window_age_s
                phase = 1 if window_age_s < 90 else 2
                print(f"  [startup] Phase {phase} — {remaining_startup:.0f}s until trading opens "
                      f"(Kalshi+strikes {'loading' if phase == 1 else 'ready'}, "
                      f"Poly {'waiting' if phase == 1 else 'settling'})")

        best_global: Optional[HedgeCandidate] = None
        best_global_poly: Optional[PolyMarketQuote] = None
        best_global_kalshi: Optional[KalshiMarketQuote] = None
        skipped_rows: list = []

        # Fetch spot prices once per scan for strike reference
        spot_prices = fetch_spot_prices()

        _scan_poly_quotes.clear()

        for coin in selected_coins:
            cd = coin_data[coin]
            kalshi, poly = cd["kalshi"], cd["poly"]
            _scan_poly_quotes[coin] = poly

            # --- Determine skip reason (if any) before printing ---
            skip = None  # (reason_key, display_text) or None

            # 2-phase startup delay: block all trading until window has settled
            if window_startup_active:
                skip = ("window_startup", "Window startup delay — orderbooks settling")

            # Per-coin caps
            if skip is None:
                if coin in coin_stopped:
                    skip = ("coin_stopped", f"{coin} stopped: {coin_stopped[coin]}")
                elif coin_trade_counts.get(coin, 0) >= MAX_TRADES_PER_COIN:
                    coin_stopped[coin] = f"reached max {MAX_TRADES_PER_COIN} trades"
                    skip = ("coin_max_trades",
                            f"{coin} reached max {MAX_TRADES_PER_COIN} trades (incl. unwinds) — STOPPING")
                elif coin_window_trade_counts.get(coin, 0) >= MAX_TRADES_PER_COIN_PER_WINDOW:
                    skip = ("coin_window_cap",
                            f"{coin} hit {MAX_TRADES_PER_COIN_PER_WINDOW} trades this window — waiting for next")
                elif coin_consecutive_unwinds.get(coin, 0) >= MAX_CONSECUTIVE_UNWINDS:
                    coin_stopped[coin] = f"{MAX_CONSECUTIVE_UNWINDS} consecutive unwinds"
                    skip = ("coin_max_unwinds",
                            f"{coin} hit {MAX_CONSECUTIVE_UNWINDS} consecutive unwinds — STOPPING")

            if skip is None and (kalshi is None or poly is None):
                skip = ("no_kalshi_market" if kalshi is None else "no_poly_market",
                        f"No {'Kalshi' if kalshi is None else 'Poly'} market found")
                if cd["kalshi_err"]:
                    skip = (skip[0], f"Kalshi fetch failed: {cd['kalshi_err'][:60]}")
                elif cd["poly_err"]:
                    skip = (skip[0], f"Poly fetch failed: {cd['poly_err'][:60]}")

            remaining_s = 0
            edge_str = ""
            best_for_coin = None
            all_combos = []

            if skip is None:
                delta_s = abs((kalshi.close_ts - poly.end_ts).total_seconds())
                now = datetime.now(timezone.utc)
                remaining_s = (kalshi.close_ts - now).total_seconds()

                if delta_s > WINDOW_ALIGN_TOLERANCE_SECONDS:
                    skip = ("alignment", f"Window alignment off by {delta_s:.1f}s (max {WINDOW_ALIGN_TOLERANCE_SECONDS}s)")
                elif remaining_s < MIN_WINDOW_REMAINING_S:
                    remaining_str = f"{int(remaining_s // 60)}m {int(remaining_s % 60)}s"
                    skip = ("window_time", f"Only {remaining_str} left (need {MIN_WINDOW_REMAINING_S:.0f}s)")

            if skip is None:
                kalshi_spread = (kalshi.yes_ask + kalshi.no_ask) - 1.0
                poly_spread = (poly.up_price + poly.down_price) - 1.0
                if kalshi_spread > MAX_SPREAD:
                    skip = ("kalshi_spread", f"Kalshi spread {pct(kalshi_spread)} > {pct(MAX_SPREAD)}")
                elif poly_spread > MAX_SPREAD:
                    skip = ("poly_spread", f"Poly spread {pct(poly_spread)} > {pct(MAX_SPREAD)}")

            if skip is None:
                prices = [poly.up_price, poly.down_price, kalshi.yes_ask, kalshi.no_ask]
                extreme = [p for p in prices if p < PRICE_FLOOR or p > PRICE_CEILING]
                if extreme:
                    skip = ("extreme_price", f"Extreme prices ({', '.join(f'{p:.2f}' for p in extreme)})")

            strike_divergence = None
            if skip is None and kalshi.strike is not None:
                try:
                    strike_val = float(kalshi.strike)
                    spot = spot_prices.get(coin)
                    if spot and spot > 0:
                        strike_divergence = abs(strike_val - spot) / spot
                        if strike_divergence > MAX_STRIKE_SPOT_DIVERGENCE:
                            skip = ("strike_spot_divergence",
                                    f"cross-strike (K${strike_val:,.2f}<>P${spot:,.2f})")
                except (ValueError, TypeError):
                    pass

            if skip is None:
                kalshi_up_prob = 1.0 - kalshi.no_ask
                poly_up_prob = poly.up_price
                prob_div = abs(kalshi_up_prob - poly_up_prob)
                if prob_div > MAX_PROB_DIVERGENCE:
                    skip = ("prob_divergence",
                            f"Prob divergence {pct(prob_div)} > {pct(MAX_PROB_DIVERGENCE)}")

            # Compute edge if no safeguard skip
            if skip is None:
                best_for_coin, all_combos = best_hedge_for_coin(coin, poly, kalshi)
                if best_for_coin is None:
                    # Find best combo to show its edge even when skipping
                    best_combo_edge = max((c.net_edge for c in all_combos), default=0) if all_combos else 0
                    best_combo = max(all_combos, key=lambda c: c.net_edge) if all_combos else None
                    if best_combo:
                        strategy = f"K_{best_combo.direction_on_kalshi}+P_{best_combo.direction_on_poly}"
                        edge_str = f"{best_combo.net_edge * 100:+.1f}% via {strategy}"
                    skip = ("no_viable_edge",
                            f"Edge too low ({edge_str or 'none'} < {pct(MIN_NET_EDGE)} for {coin})")
                else:
                    strategy = f"K_{best_for_coin.direction_on_kalshi}+P_{best_for_coin.direction_on_poly}"
                    safe_tag = ""
                    if strike_divergence is not None:
                        safe_tag = f" *SAFE(cross-strike (K${float(kalshi.strike):,.2f}<P${spot_prices.get(coin, 0):,.2f}))"
                    edge_str = f"{best_for_coin.net_edge * 100:+.1f}% via {strategy}{safe_tag}"

            # Avg-edge gating: after guaranteed trades, require rolling avg >= threshold
            if skip is None and best_for_coin is not None:
                window_count = coin_window_trade_counts.get(coin, 0)
                if window_count >= GUARANTEED_TRADES_PER_COIN:
                    edges = coin_window_edges.get(coin, [])
                    avg_edge = sum(edges) / len(edges) if edges else 0.0
                    if avg_edge < AVG_EDGE_GATE:
                        skip = ("avg_edge_gate",
                                f"{coin} avg edge {avg_edge*100:.1f}% < {AVG_EDGE_GATE*100:.1f}% "
                                f"after {window_count} trades (need ≥{GUARANTEED_TRADES_PER_COIN} guaranteed)")
                        best_for_coin = None

            # Pre-trade depth gate: reject if Poly book is too thin for our size
            if skip is None and best_for_coin is not None and _poly_ws:
                depth_token = (poly.up_token_id if best_for_coin.direction_on_poly == "UP"
                               else poly.down_token_id)
                coin_depth = _poly_ws.get_book_depth(depth_token)
                if coin_depth is None or coin_depth.get("total_size", 0) < MIN_POLY_DEPTH_CONTRACTS:
                    actual = coin_depth["total_size"] if coin_depth else 0
                    skip = ("poly_depth_thin",
                            f"Poly book too thin ({actual} contracts < {MIN_POLY_DEPTH_CONTRACTS} min)")
                    best_for_coin = None

            # --- Print compact coin box ---
            # Build edge display for the box
            box_edge = edge_str
            if not box_edge and skip and skip[0] not in ("no_kalshi_market", "no_poly_market"):
                # Show edge from best combo even on skip
                if all_combos:
                    bc = max(all_combos, key=lambda c: c.net_edge)
                    strat = f"K_{bc.direction_on_kalshi}+P_{bc.direction_on_poly}"
                    box_edge = f"{bc.net_edge * 100:+.1f}% via {strat}"

            if skip:
                log_skip(logfile, skip_counts, scan_i, coin, skip[0],
                         poly=poly, kalshi=kalshi,
                         remaining_s=remaining_s if remaining_s else None)
                skipped_rows.append({
                    "coin": coin, "kalshi": kalshi, "poly": poly,
                    "edge": box_edge, "reason": skip[1],
                })
                continue

            # Non-skipped: show full box
            display_coin_box(coin, kalshi, poly, edge_str=box_edge)

            # Staleness warning for Poly WS data
            if poly and _poly_ws:
                up_stale = _poly_ws.get_staleness_s(poly.up_token_id)
                dn_stale = _poly_ws.get_staleness_s(poly.down_token_id)
                stale_parts = []
                if up_stale is not None and up_stale > 30:
                    stale_parts.append(f"UP {up_stale:.0f}s")
                if dn_stale is not None and dn_stale > 30:
                    stale_parts.append(f"DOWN {dn_stale:.0f}s")
                if stale_parts:
                    print(f"  ⚠ WS stale: {', '.join(stale_parts)}")

            if best_global is None or best_for_coin.net_edge > best_global.net_edge:
                best_global = best_for_coin
                best_global_poly = poly
                best_global_kalshi = kalshi


        # Print compact skip table for all skipped coins in this scan
        if skipped_rows:
            display_skip_table(skipped_rows)

        # Scan timing summary
        scan_ms = (time.monotonic() - scan_t0) * 1000
        process_ms = scan_ms - gamma_ms - fetch_ms
        # Log at most one paper trade per scan (the best across all coins)
        if best_global is not None and best_global_poly is not None and best_global_kalshi is not None:
            # Gather per-coin latency for the winning coin
            winning_cd = coin_data[best_global.coin]

            # Book depth snapshot for the traded Poly leg
            poly_token = best_global_poly.up_token_id if best_global.direction_on_poly == "UP" else best_global_poly.down_token_id
            poly_depth = None
            poly_staleness_s = None
            if _poly_ws:
                poly_depth = _poly_ws.get_book_depth(poly_token)
                poly_staleness_s = _poly_ws.get_staleness_s(poly_token)

            row = {
                "log_type": "trade",
                "ts": utc_ts(),
                "scan_num": scan_i,
                "coin": best_global.coin,
                "poly_side": best_global.direction_on_poly,
                "kalshi_side": best_global.direction_on_kalshi,
                "poly_price": best_global.poly_price,
                "kalshi_price": best_global.kalshi_price,
                "total_cost": best_global.total_cost,
                "gross_edge": best_global.gross_edge,
                "net_edge": best_global.net_edge,
                "poly_fee": best_global.poly_fee,
                "kalshi_fee": best_global.kalshi_fee,
                "extras": best_global.extras,
                "poly_ref": best_global.poly_ref,
                "kalshi_ref": best_global.kalshi_ref,
                "poly_up_token_id": best_global_poly.up_token_id,
                "poly_down_token_id": best_global_poly.down_token_id,
                "window_close_ts": best_global_kalshi.close_ts.isoformat(),
                "spot_price": spot_prices.get(best_global.coin),
                "kalshi_strike": best_global_kalshi.strike,
                "strike_spot_divergence_pct": round(abs(float(best_global_kalshi.strike) - spot_prices.get(best_global.coin, 0)) / spot_prices.get(best_global.coin, 1) * 100, 4) if best_global_kalshi.strike else None,
                # Poly book depth snapshot
                "poly_book_levels": poly_depth["levels"] if poly_depth else None,
                "poly_book_size": poly_depth["total_size"] if poly_depth else None,
                "poly_book_notional_usd": poly_depth["total_notional_usd"] if poly_depth else None,
                "poly_ws_staleness_s": round(poly_staleness_s, 1) if poly_staleness_s is not None else None,
                "depth_capped_contracts": int(min(PAPER_CONTRACTS, int(poly_depth["total_size"] * POLY_DEPTH_CAP_RATIO))) if poly_depth and poly_depth.get("total_size", 0) > 0 else int(PAPER_CONTRACTS),
                # Timing
                "scan_ms": round(scan_ms, 1),
                "gamma_ms": round(gamma_ms, 1),
                "fetch_ms": round(fetch_ms, 1),
                "kalshi_latency_ms": round(winning_cd["kalshi_ms"], 1),
                "poly_latency_ms": round(winning_cd["poly_ms"], 1),
            }

            # Staleness warning in console
            if poly_staleness_s is not None and poly_staleness_s > 30:
                print(f"  [warn] Poly WS data for traded leg is {poly_staleness_s:.0f}s stale!")

            # Book depth in console
            if poly_depth:
                print(f"  [depth] Poly book: {poly_depth['levels']} levels, "
                      f"{poly_depth['total_size']} contracts, ${poly_depth['total_notional_usd']:.2f} notional")

            # Execute both legs (paper=instant fill, live=real orders)
            exec_result = execute_hedge(best_global, best_global_poly, best_global_kalshi, logfile,
                                        poly_depth=poly_depth)

            # Attach execution details to the trade log row
            row["exec_mode"] = EXEC_MODE
            row["exec_both_filled"] = exec_result.both_filled
            row["exec_total_ms"] = round(exec_result.total_latency_ms, 1)
            row["exec_slippage_poly"] = round(exec_result.slippage_poly, 6)
            row["exec_slippage_kalshi"] = round(exec_result.slippage_kalshi, 6)
            row["exec_slippage_total_bps"] = round((exec_result.slippage_poly + exec_result.slippage_kalshi) * 10000, 1)
            row["exec_leg1_exchange"] = exec_result.leg1.exchange
            row["exec_leg1_order_id"] = exec_result.leg1.order_id
            row["exec_leg1_status"] = exec_result.leg1.status
            row["exec_leg1_actual_price"] = exec_result.leg1.actual_price
            row["exec_leg1_planned_qty"] = exec_result.leg1.planned_contracts
            row["exec_leg1_filled_qty"] = exec_result.leg1.filled_contracts
            row["exec_leg1_latency_ms"] = round(exec_result.leg1.latency_ms, 1)
            row["exec_leg2_exchange"] = exec_result.leg2.exchange
            row["exec_leg2_order_id"] = exec_result.leg2.order_id
            row["exec_leg2_status"] = exec_result.leg2.status
            row["exec_leg2_actual_price"] = exec_result.leg2.actual_price
            row["exec_leg2_planned_qty"] = exec_result.leg2.planned_contracts
            row["exec_leg2_filled_qty"] = exec_result.leg2.filled_contracts
            row["exec_leg2_latency_ms"] = round(exec_result.leg2.latency_ms, 1)

            # Print trade complete box
            if exec_result.both_filled:
                print_trade_complete(best_global, exec_result, PAPER_CONTRACTS,
                                     kalshi_quote=best_global_kalshi, poly_quote=best_global_poly)

            # --- Comprehensive diagnostic snapshot ---
            # Captures full context for every trade so Claude can reconstruct
            # the exact state at trade time during post-mortem analysis.
            diag: dict = {
                "log_type": "diagnostic",
                "ts": utc_ts(),
                "scan_num": scan_i,
                "exec_mode": EXEC_MODE,
                # Config snapshot
                "config": {
                    "PAPER_CONTRACTS": PAPER_CONTRACTS,
                    "MAX_TEST_TRADES": MAX_TEST_TRADES,
                    "MIN_NET_EDGE": MIN_NET_EDGE,
                    "MAX_TOTAL_COST": MAX_TOTAL_COST,
                    "MIN_WINDOW_REMAINING_S": MIN_WINDOW_REMAINING_S,
                    "MAX_SPREAD": MAX_SPREAD,
                    "PRICE_FLOOR": PRICE_FLOOR,
                    "PRICE_CEILING": PRICE_CEILING,
                    "MAX_PROB_DIVERGENCE": MAX_PROB_DIVERGENCE,
                    "INCLUDE_POLY_FEES": INCLUDE_POLY_FEES,
                    "INCLUDE_KALSHI_FEES": INCLUDE_KALSHI_FEES,
                    "USE_VWAP_DEPTH": USE_VWAP_DEPTH,
                    "MIN_LEG_NOTIONAL": MIN_LEG_NOTIONAL,
                    "ORDER_TIMEOUT_S": ORDER_TIMEOUT_S,
                },
                # Winning trade details (duplicated from row for self-contained diagnostic)
                "trade": {
                    "coin": best_global.coin,
                    "poly_side": best_global.direction_on_poly,
                    "kalshi_side": best_global.direction_on_kalshi,
                    "poly_price": best_global.poly_price,
                    "kalshi_price": best_global.kalshi_price,
                    "total_cost": best_global.total_cost,
                    "gross_edge": best_global.gross_edge,
                    "net_edge": best_global.net_edge,
                    "poly_fee": best_global.poly_fee,
                    "kalshi_fee": best_global.kalshi_fee,
                    "extras": best_global.extras,
                },
                # Execution result
                "execution": {
                    "both_filled": exec_result.both_filled,
                    "total_latency_ms": round(exec_result.total_latency_ms, 1),
                    "slippage_poly": round(exec_result.slippage_poly, 6),
                    "slippage_kalshi": round(exec_result.slippage_kalshi, 6),
                    "leg1": {
                        "exchange": exec_result.leg1.exchange,
                        "side": exec_result.leg1.side,
                        "planned_price": exec_result.leg1.planned_price,
                        "actual_price": exec_result.leg1.actual_price,
                        "planned_contracts": exec_result.leg1.planned_contracts,
                        "filled_contracts": exec_result.leg1.filled_contracts,
                        "order_id": exec_result.leg1.order_id,
                        "fill_ts": exec_result.leg1.fill_ts,
                        "latency_ms": round(exec_result.leg1.latency_ms, 1),
                        "status": exec_result.leg1.status,
                        "error": exec_result.leg1.error,
                    },
                    "leg2": {
                        "exchange": exec_result.leg2.exchange,
                        "side": exec_result.leg2.side,
                        "planned_price": exec_result.leg2.planned_price,
                        "actual_price": exec_result.leg2.actual_price,
                        "planned_contracts": exec_result.leg2.planned_contracts,
                        "filled_contracts": exec_result.leg2.filled_contracts,
                        "order_id": exec_result.leg2.order_id,
                        "fill_ts": exec_result.leg2.fill_ts,
                        "latency_ms": round(exec_result.leg2.latency_ms, 1),
                        "status": exec_result.leg2.status,
                        "error": exec_result.leg2.error,
                    },
                },
                # All quotes from every coin this scan (not just the winner)
                "all_coin_quotes": {},
                # All hedge combos evaluated (including rejected ones)
                "all_hedge_combos": [],
                # Orderbook snapshots
                "orderbook_snapshots": {},
                # Timing breakdown
                "timing": {
                    "scan_ms": round(scan_ms, 1),
                    "gamma_ms": round(gamma_ms, 1),
                    "fetch_ms": round(fetch_ms, 1),
                    "process_ms": round(process_ms, 1),
                },
                # Spot prices
                "spot_prices": dict(spot_prices),
                # Skip reasons accumulated so far
                "skip_counts": dict(skip_counts),
            }

            # Populate all coin quotes
            for c in selected_coins:
                cd_c = coin_data[c]
                cq: dict = {
                    "kalshi_ms": round(cd_c["kalshi_ms"], 1),
                    "poly_ms": round(cd_c["poly_ms"], 1),
                    "kalshi_err": cd_c["kalshi_err"],
                    "poly_err": cd_c["poly_err"],
                }
                k = cd_c["kalshi"]
                if k:
                    cq["kalshi"] = {
                        "ticker": k.ticker, "title": k.title,
                        "yes_ask": k.yes_ask, "no_ask": k.no_ask,
                        "close_ts": k.close_ts.isoformat(),
                        "strike": k.strike,
                    }
                p = cd_c["poly"]
                if p:
                    cq["poly"] = {
                        "event_slug": p.event_slug, "market_slug": p.market_slug,
                        "title": p.title,
                        "up_price": p.up_price, "down_price": p.down_price,
                        "end_ts": p.end_ts.isoformat(),
                        "up_token_id": p.up_token_id, "down_token_id": p.down_token_id,
                    }
                diag["all_coin_quotes"][c] = cq

            # Populate hedge combos for winning coin (both directions)
            winning_poly = best_global_poly
            winning_kalshi = best_global_kalshi
            _, all_combos = best_hedge_for_coin(best_global.coin, winning_poly, winning_kalshi)
            for combo in all_combos:
                diag["all_hedge_combos"].append({
                    "poly_dir": combo.direction_on_poly,
                    "kalshi_dir": combo.direction_on_kalshi,
                    "poly_price": combo.poly_price,
                    "kalshi_price": combo.kalshi_price,
                    "total_cost": combo.total_cost,
                    "gross_edge": combo.gross_edge,
                    "net_edge": combo.net_edge,
                    "poly_fee": combo.poly_fee,
                    "kalshi_fee": combo.kalshi_fee,
                    "extras": combo.extras,
                    "viable": combo.total_cost < MAX_TOTAL_COST and combo.net_edge >= MIN_NET_EDGE and combo.net_edge <= MAX_NET_EDGE,
                })

            # Poly orderbook depth snapshots for both sides of traded coin
            if _poly_ws and winning_poly:
                for side_label, tid in [("up", winning_poly.up_token_id), ("down", winning_poly.down_token_id)]:
                    depth = _poly_ws.get_book_depth(tid)
                    stale = _poly_ws.get_staleness_s(tid)
                    raw_asks = _poly_ws.get_asks(tid)
                    diag["orderbook_snapshots"][f"poly_{side_label}"] = {
                        "token_id": tid,
                        "depth": depth,
                        "staleness_s": round(stale, 1) if stale is not None else None,
                        "ask_levels": [(p, s) for p, s in (raw_asks or [])][:20],  # top 20 levels
                    }

            # Kalshi orderbook snapshot for traded ticker
            try:
                k_ob = kalshi_get_orderbook(winning_kalshi.ticker)
                k_up_asks, k_down_asks = kalshi_asks_from_orderbook(k_ob)
                diag["orderbook_snapshots"]["kalshi_up"] = {
                    "ticker": winning_kalshi.ticker,
                    "ask_levels": [(p, s) for p, s in k_up_asks[:20]],
                }
                diag["orderbook_snapshots"]["kalshi_down"] = {
                    "ticker": winning_kalshi.ticker,
                    "ask_levels": [(p, s) for p, s in k_down_asks[:20]],
                }
            except Exception as e:
                diag["orderbook_snapshots"]["kalshi_err"] = str(e)

            append_log(logfile, diag)
            # --- End diagnostic snapshot ---

            append_log(logfile, row)
            logged.append(row)
            window_trades.append(row)
            consecutive_skips = 0

            # Per-coin tracking: update trade count, edge history, and unwind counter
            traded_coin = best_global.coin
            coin_trade_counts[traded_coin] = coin_trade_counts.get(traded_coin, 0) + 1
            coin_window_trade_counts[traded_coin] = coin_window_trade_counts.get(traded_coin, 0) + 1
            coin_window_edges[traded_coin].append(best_global.net_edge)
            was_unwound = not exec_result.both_filled
            if was_unwound:
                coin_consecutive_unwinds[traded_coin] = coin_consecutive_unwinds.get(traded_coin, 0) + 1
                print(f"  [unwind-track] UNWIND COUNT {traded_coin}: "
                      f"{coin_consecutive_unwinds[traded_coin]}/{MAX_CONSECUTIVE_UNWINDS} consecutive unwinds")
                # Stop the coin entirely if unwind failed — continuing would compound exposure
                if exec_result.unwind_failed:
                    coin_stopped[traded_coin] = "unwind FAILED — needs manual close"
                    print(f"  [unwind-track] *** {traded_coin} STOPPED: unwind failed, "
                          f"manual close required to avoid compounding exposure ***")
            else:
                coin_consecutive_unwinds[traded_coin] = 0  # reset on success

            mode_tag = "LIVE" if EXEC_MODE == "live" else "paper"
            if exec_result.both_filled:
                fill_tag = f"{GREEN}{BOLD} ✅  FILLED{RESET}"
            else:
                fill_tag = f"{RED}{BOLD} ❌  INCOMPLETE{RESET}"
            active_str = ", ".join(
                f"{c}({coin_trade_counts.get(c, 0)}/{MAX_TRADES_PER_COIN})"
                for c in AVAILABLE_COINS
            )
            print(f"[{mode_tag}] Trade #{len(logged)}{fill_tag} | {active_str}")

            # Check if coin just hit its per-coin cap
            if coin_trade_counts[traded_coin] >= MAX_TRADES_PER_COIN:
                coin_stopped[traded_coin] = f"reached max {MAX_TRADES_PER_COIN} trades"
                print(f"  [limit] {traded_coin} reached max {MAX_TRADES_PER_COIN} trades "
                      f"(incl. unwinds) — STOPPING this crypto")
            elif coin_consecutive_unwinds.get(traded_coin, 0) >= MAX_CONSECUTIVE_UNWINDS:
                coin_stopped[traded_coin] = f"{MAX_CONSECUTIVE_UNWINDS} consecutive unwinds"
                print(f"  [limit] {traded_coin} hit {MAX_CONSECUTIVE_UNWINDS} consecutive unwinds — STOPPING this crypto")

            # Session drawdown + balance floor check (live mode only)
            if EXEC_MODE == "live" and session_start_total > 0:
                try:
                    current_bal = check_balances(logfile)
                    current_total = sum(v for v in current_bal.values() if v >= 0)
                    drawdown = session_start_total - current_total
                    print(f"\n  [drawdown] Session P&L: ${-drawdown:+.2f} "
                          f"(start: ${session_start_total:.2f}, now: ${current_total:.2f})")
                    if MAX_SESSION_DRAWDOWN > 0 and drawdown >= MAX_SESSION_DRAWDOWN:
                        stop_reason = f"DRAWDOWN LIMIT HIT: ${drawdown:.2f} >= ${MAX_SESSION_DRAWDOWN:.2f}"
                        print(f"\n*** {stop_reason} — stopping session ***")
                        break
                    # Balance floor check: stop if either account < $50
                    for exch, bal in current_bal.items():
                        if 0 <= bal < MIN_ACCOUNT_BALANCE:
                            stop_reason = (f"BALANCE FLOOR: {exch.upper()} balance ${bal:.2f} "
                                           f"< ${MIN_ACCOUNT_BALANCE:.2f}")
                            print(f"\n*** {stop_reason} — stopping session ***")
                            break
                    if stop_reason:
                        break
                except Exception as e:
                    print(f"  [drawdown] Balance check failed: {e}")

        else:
            consecutive_skips += 1
            print(f"No viable paper trades found in this scan. ({consecutive_skips} consecutive skips | {len(logged)} trades)")
            # Periodic skip-reason breakdown every 20 scans to help diagnose filter bottlenecks
            if consecutive_skips > 0 and consecutive_skips % 20 == 0 and skip_counts:
                total_skips = sum(skip_counts.values())
                top_reasons = sorted(skip_counts.items(), key=lambda x: -x[1])[:5]
                print(f"\n  [skip summary] Top reasons across {total_skips} total skips:")
                for reason, count in top_reasons:
                    print(f"    {reason:30s} {count:4d} ({count/total_skips*100:5.1f}%)")
                print()
            if consecutive_skips >= MAX_CONSECUTIVE_SKIPS:
                print(f"\n⚠ Circuit breaker: {MAX_CONSECUTIVE_SKIPS} consecutive scans with no viable trades. Stopping.")
                break

        if time.monotonic() < session_deadline:
            time.sleep(SCAN_SLEEP_SECONDS)

    # Log + print why the session ended
    end_reason = stop_reason or ("session_duration_reached" if time.monotonic() >= session_deadline else "unknown")
    log_kill_switch(logfile, end_reason, {
        "trades_executed": len(logged),
        "session_duration_s": session_duration_s,
        "consecutive_losing_windows": consecutive_losing_windows,
    })
    if stop_reason:
        print(f"\nSession stopped: {stop_reason}")
    elif time.monotonic() >= session_deadline:
        print(f"\nSession duration reached ({duration_label}). Session complete.")
    print(f"Trades executed: {len(logged)}")
    print(f"Wrote logs to: {logfile}")

    # Final redemption pass for any remaining window trades
    if window_trades and EXEC_MODE == "live" and POLY_SIGNATURE_TYPE == 0:
        print(f"\n  [session-end] Running final redemption for {len(window_trades)} trade(s) in last window...")
        try:
            time.sleep(15)  # Wait for settlement
            n = redeem_poly_positions(window_trades)
            if n > 0:
                print(f"  [session-end] Redeemed {n} position(s)")
        except Exception as e:
            print(f"  [session-end] Final redemption failed: {e}")

    # Final balance check
    if EXEC_MODE == "live":
        try:
            final_bal = check_balances(logfile)
            final_total = sum(v for v in final_bal.values() if v >= 0)
            session_pnl = final_total - session_start_total
            print(f"\n  [session-end] Final balances: Kalshi=${final_bal.get('kalshi', -1):.2f}, "
                  f"Poly=${final_bal.get('poly', -1):.2f}")
            print(f"  [session-end] Session P&L: ${session_pnl:+.2f} "
                  f"(start: ${session_start_total:.2f}, end: ${final_total:.2f})")
        except Exception as e:
            print(f"  [session-end] Final balance check failed: {e}")

    # Outcome verification: wait for windows to close, then check actual results
    # Skip for single-trade diagnostic runs (long wait for little value);
    # keep for multi-trade sessions where P&L tracking matters.
    if logged and len(logged) > 1:
        ans = input("\nWait for outcome verification? [y/N] ").strip().lower()
        if ans == "y":
            verify_trade_outcomes(logged, logfile)
        else:
            print("Skipping outcome verification.")
    elif logged:
        print("(Skipping outcome verification for single-trade run)")

    # Auto-redeem resolved Polymarket positions (EOA mode only)
    if logged and EXEC_MODE == "live" and POLY_SIGNATURE_TYPE == 0:
        try:
            n = redeem_poly_positions(logged)
            if n > 0:
                print(f"\n  [redeem] Successfully redeemed {n} position(s)")
            elif n == 0:
                print(f"\n  [redeem] No positions to redeem (tokens may not have settled yet)")
        except Exception as e:
            print(f"\n  [redeem] Auto-redemption failed: {e}")

    summarize(logged, selected_coins, skip_counts=skip_counts,
             start_balances=balances, logfile=logfile)


def _run_with_kill_switch() -> None:
    """Entry point that wraps main() in a kill switch for graceful shutdown logging."""
    try:
        main()
    except KeyboardInterrupt:
        # User pressed Ctrl+C — graceful exit
        print("\n\nShutdown: Ctrl+C received.")
        # Try to log the shutdown (logfile may not exist if interrupted before scanning)
        try:
            logfiles = sorted(
                [f for f in os.listdir(LOG_DIR) if f.startswith("arb_logs_market_")],
                reverse=True
            )
            if logfiles:
                logfile = os.path.join(LOG_DIR, logfiles[0])
                log_kill_switch(logfile, "user_interrupt", {"signal": "KeyboardInterrupt"})
        except Exception:
            pass
    except Exception as exc:
        # Unexpected crash — log full context before re-raising
        print(f"\n\nFATAL: {type(exc).__name__}: {exc}")
        try:
            logfiles = sorted(
                [f for f in os.listdir(LOG_DIR) if f.startswith("arb_logs_market_")],
                reverse=True
            )
            if logfiles:
                logfile = os.path.join(LOG_DIR, logfiles[0])
                log_kill_switch(logfile, "unhandled_exception", {
                    "exception_type": type(exc).__name__,
                    "exception_msg": str(exc)[:500],
                })
        except Exception:
            pass
        raise
    finally:
        # Clean up WebSocket
        if _poly_ws is not None:
            _poly_ws.stop()


if __name__ == "__main__":
    _run_with_kill_switch()
