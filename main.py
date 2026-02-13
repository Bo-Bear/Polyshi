import base64
import json
import os
import time
import math
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

import re

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
    from py_clob_client.clob_types import OrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY, SELL
    _HAS_CLOB_CLIENT = True
except ImportError:
    _HAS_CLOB_CLIENT = False


# -----------------------------
# Config
# -----------------------------
load_dotenv()

SCAN_SLEEP_SECONDS = float(os.getenv("SCAN_SLEEP_SECONDS", "5"))
MAX_TEST_TRADES = int(os.getenv("MAX_TEST_TRADES", "1"))
WINDOW_ALIGN_TOLERANCE_SECONDS = int(os.getenv("WINDOW_ALIGN_TOLERANCE_SECONDS", "10"))
MIN_LEG_NOTIONAL = float(os.getenv("MIN_LEG_NOTIONAL", "10"))  # $ minimum liquidity per leg
USE_VWAP_DEPTH = os.getenv("USE_VWAP_DEPTH", "true").lower() == "true"

# -----------------------------
# Execution safeguards
# -----------------------------
# Minimum net edge to accept a trade (protects against slippage/rounding eating thin edges)
MIN_NET_EDGE = float(os.getenv("MIN_NET_EDGE", "0.0004"))  # 0.04%

# Minimum seconds remaining in the window before we'll trade (need time to fill both legs)
MIN_WINDOW_REMAINING_S = float(os.getenv("MIN_WINDOW_REMAINING_S", "30"))  # 30 seconds

# Maximum spread (ask_up + ask_down - 1) we'll accept; wider means unreliable pricing
MAX_SPREAD = float(os.getenv("MAX_SPREAD", "0.10"))  # 10%

# Reject prices in extreme ranges where outcome is nearly decided (hedging is risky)
PRICE_FLOOR = float(os.getenv("PRICE_FLOOR", "0.02"))   # skip legs priced below 2c
PRICE_CEILING = float(os.getenv("PRICE_CEILING", "0.98"))  # skip legs priced above 98c

# Session-level circuit breaker: stop scanning after this many consecutive no-trade scans
# (may indicate stale data or broken feeds)
MAX_CONSECUTIVE_SKIPS = int(os.getenv("MAX_CONSECUTIVE_SKIPS", "200"))

# Maximum gross cost we'll accept (tighter than 1.0 to leave room for execution slippage)
MAX_TOTAL_COST = float(os.getenv("MAX_TOTAL_COST", "0.995"))

# Maximum allowed divergence between implied probabilities across exchanges.
# Large divergence signals mismatched strikes (Kalshi uses fixed $, Poly uses relative).
# If |kalshi_up_prob - poly_up_prob| > this, skip the trade.
MAX_PROB_DIVERGENCE = float(os.getenv("MAX_PROB_DIVERGENCE", "0.155"))  # 15.5 percentage points

# -----------------------------
# Fees (paper-trade model)
# -----------------------------
# Size we assume for fee calculations (both venues). Polymarket fee table is for 100 shares. :contentReference[oaicite:3]{index=3}
PAPER_CONTRACTS = float(os.getenv("PAPER_CONTRACTS", "3"))

# Toggle fee modeling
INCLUDE_POLY_FEES = os.getenv("INCLUDE_POLY_FEES", "true").lower() == "true"
INCLUDE_KALSHI_FEES = os.getenv("INCLUDE_KALSHI_FEES", "true").lower() == "true"

# Optional extras (set to 0 for now; can model later)
EXTRA_WITHDRAW_FEE_USD = float(os.getenv("EXTRA_WITHDRAW_FEE_USD", "0"))  # withdrawal processor fees etc.
EXTRA_GAS_USD = float(os.getenv("EXTRA_GAS_USD", "0"))  # blockchain gas, bridging, etc.

# Amortize one-time extras over N trades (e.g., if you withdraw once after many trades)
AMORTIZE_EXTRAS_OVER_TRADES = int(os.getenv("AMORTIZE_EXTRAS_OVER_TRADES", "1"))


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
}

# Polymarket: We'll discover via tag=crypto + recurrence=15M, then select by title prefix.
AVAILABLE_COINS = ["BTC", "ETH", "SOL"]
POLY_TITLE_PREFIX = {
    "BTC": "Bitcoin Up or Down",
    "ETH": "Ethereum Up or Down",
    "SOL": "Solana Up or Down",
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
ORDER_POLL_INTERVAL_S = float(os.getenv("ORDER_POLL_INTERVAL_S", "1"))  # polling interval


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
    headers = _kalshi_sign("GET", path)
    r = _get_session().get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _kalshi_auth_post(path: str, body: dict, timeout: int = 10) -> dict:
    """Authenticated POST to Kalshi API."""
    url = KALSHI_BASE + path
    headers = _kalshi_sign("POST", path)
    r = _get_session().post(url, json=body, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()


# -----------------------------
# Polymarket CLOB client (live mode)
# -----------------------------
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

COINGECKO_IDS = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana"}

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
                time.sleep(2)

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


def prompt_execution_mode() -> str:
    """Prompt user to choose between live trading and paper testing."""
    print("\nSELECT EXECUTION MODE")
    print("=" * 45)
    print("1) Paper Testing   — simulated trades, no real money")
    print("2) Live Trading    — real orders on Kalshi & Polymarket")

    while True:
        choice = input("\nSelect mode [1]: ").strip()
        if choice == "" or choice == "1":
            return "paper"
        if choice == "2":
            return "live"
        print("Invalid selection. Enter 1 or 2.")


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

    # Debug: show what title-matched events exist and why they pass/fail the 15m filter
    if not title_matches:
        print(f"    [{coin}] debug: 0 events match title prefix '{prefix}'")
    else:
        for e in title_matches[:5]:
            slug = (e.get("slug") or "")
            is_15m = _is_15m_poly_event(e)
            end_raw = e.get("endDate") or e.get("end_date") or ""
            print(f"    [{coin}] debug: slug={slug} | 15m={is_15m} | end={end_raw} | title={e.get('title', '')[:60]}")

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
    net1 = 1.0 - total1 - (poly_fee1 + kalshi_fee1 + extras) / PAPER_CONTRACTS

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
    net2 = 1.0 - total2 - (poly_fee2 + kalshi_fee2 + extras) / PAPER_CONTRACTS

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
    viable = [c for c in cands if c.total_cost < MAX_TOTAL_COST and c.net_edge >= MIN_NET_EDGE]
    if not viable:
        return None, cands

    viable.sort(key=lambda x: x.net_edge, reverse=True)
    return viable[0], cands



# -----------------------------
# Logging + display
# -----------------------------
def print_scan_header(scan_i: int) -> None:
    print(f"\n--- SCAN #{scan_i} | {utc_ts()} ---")


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


def display_market_block(market_type: str, coin: str, kalshi: Optional[KalshiMarketQuote],
                         poly: Optional[PolyMarketQuote], spot_price: Optional[float] = None) -> None:
    label = MARKET_TYPE_LABELS.get(market_type, market_type)
    print(f"\n[MARKET] {coin} ({label})")

    # Spot price reference line
    if spot_price is not None:
        print(f"  Spot:    ${spot_price:,.2f} (CoinGecko)")

    if kalshi is None:
        print("  Kalshi:  (no open market found)")
    else:
        spread = (kalshi.yes_ask + kalshi.no_ask) - 1.0
        if kalshi.strike is not None:
            try:
                strike_val = float(kalshi.strike)
                strike_txt = f" | Strike: ${strike_val:,.2f}"
            except (ValueError, TypeError):
                strike_txt = f" | Strike: {kalshi.strike}"
        else:
            strike_txt = " | Strike: UNKNOWN"
        print(f"  Kalshi:  {kalshi.title}{strike_txt}")
        print(f"          Up/Down (asks): {fmt_price_pair(kalshi.yes_ask, kalshi.no_ask)}")
        print(f"          Close (UTC): {kalshi.close_ts.strftime('%H:%M:%S')}")
        print(f"          Spread (ask sum - 1): {pct(spread)}")

    if poly is None:
        print("  Poly:    (no active 15M event found)")
    else:
        spread = (poly.up_price + poly.down_price) - 1.0
        # Try to extract reference price from description
        desc_price = _parse_price_from_description(poly.description)
        if desc_price is not None:
            ref_txt = f"${desc_price:,.2f} (from description)"
        else:
            ref_txt = "start-of-window (not in API)"
        print(f"  Poly:    {poly.title}  ({poly.event_slug}/{poly.market_slug})")
        print(f"          Up/Down:        {fmt_price_pair(poly.up_price, poly.down_price)}")
        print(f"          End   (UTC): {poly.end_ts.strftime('%H:%M:%S')}")
        print(f"          Spread (sum - 1): {pct(spread)}")
        print(f"          Price to beat: {ref_txt}")


def append_log(path: str, row: dict) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row) + "\n")


def summarize(log_rows: List[dict], coins: List[str], skip_counts: Optional[Dict[str, int]] = None) -> None:
    if not log_rows:
        print("\nNo test trades were logged.")
        if skip_counts:
            print("\nSkip reasons:")
            for reason, count in sorted(skip_counts.items(), key=lambda x: -x[1]):
                print(f"  {reason}: {count}")
        return

    by_coin: Dict[str, List[dict]] = {}
    for r in log_rows:
        by_coin.setdefault(r["coin"], []).append(r)

    print("\n" + "=" * 60)
    print("  TEST TRADE SUMMARY")
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

        print(f"\n--- Paper P&L ({int(PAPER_CONTRACTS)} contracts/trade) ---")
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

    print(f"\n--- Recent Trades ---")
    for r in log_rows[-5:]:
        pnl_str = ""
        if r.get("actual_pnl_total") is not None:
            pnl_str = f" | P&L=${r['actual_pnl_total']:+.4f}"
        depth_str = ""
        if r.get("poly_book_levels") is not None:
            depth_str = f" | depth={r['poly_book_levels']}lvl/${r.get('poly_book_notional_usd', 0):.0f}$"
        print(
            f"  [{r['ts']}] {r['coin']} | Poly {r['poly_side']} {r['poly_price']:.3f} "
            f"+ Kalshi {r['kalshi_side']} {r['kalshi_price']:.3f} "
            f"= total {r['total_cost']:.3f} | net {pct(r['net_edge'])} (gross {pct(r['gross_edge'])}) "
            f"| fees {r['poly_fee']:.4f}+{r['kalshi_fee']:.4f}+{r['extras']:.4f}"
            f"{depth_str}{pnl_str}"
        )


# -----------------------------
# Parallel fetch helper
# -----------------------------
def _fetch_coin_quotes(coin: str, poly_events: List[dict]) -> dict:
    """Fetch Kalshi and Polymarket quotes for a single coin. Returns dict with quotes, timing, and errors."""
    result: dict = {
        "coin": coin, "kalshi": None, "poly": None,
        "kalshi_ms": 0.0, "poly_ms": 0.0,
        "kalshi_err": None, "poly_err": None,
    }

    t0 = time.monotonic()
    try:
        result["kalshi"] = pick_current_kalshi_market(KALSHI_SERIES[coin])
    except Exception as e:
        result["kalshi_err"] = str(e)
    result["kalshi_ms"] = (time.monotonic() - t0) * 1000

    t0 = time.monotonic()
    try:
        result["poly"] = extract_poly_quote_for_coin(poly_events, coin)
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
        bal["kalshi"] = float(data.get("available_balance", 0)) / 100.0
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
                contracts: float, **kwargs) -> LegFill:
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

    try:
        if exchange == "kalshi":
            order_id, actual_price, filled, status, error_msg = _execute_kalshi_leg(
                side, planned_price, contracts, kwargs.get("ticker", "")
            )
        elif exchange == "poly":
            order_id, actual_price, filled, status, error_msg = _execute_poly_leg(
                side, planned_price, contracts, kwargs.get("token_id", "")
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


def _execute_kalshi_leg(side: str, planned_price: float, contracts: float,
                        ticker: str) -> Tuple[Optional[str], Optional[float], float, str, Optional[str]]:
    """Place and poll a Kalshi limit order. Returns (order_id, actual_price, filled, status, error)."""
    kalshi_side = "yes" if side == "UP" else "no"
    price_cents = int(round(planned_price * 100))
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

    # Poll for fill
    deadline = time.monotonic() + ORDER_TIMEOUT_S
    while time.monotonic() < deadline:
        try:
            poll = _kalshi_auth_get(f"/portfolio/orders/{order_id}")
            o = poll.get("order", poll)
            o_status = o.get("status", "")

            if o_status == "executed":
                fill_count = float(o.get("fill_count", o.get("count", contracts)))
                # Kalshi doesn't return average fill price directly;
                # for limit orders, fill price = planned price (exchange guarantees limit-or-better)
                return order_id, planned_price, fill_count, "filled", None

            if o_status == "canceled":
                fill_count = float(o.get("fill_count", 0))
                if fill_count > 0:
                    return order_id, planned_price, fill_count, "partial", "order canceled after partial fill"
                return order_id, None, 0.0, "rejected", "order was canceled"

        except Exception:
            pass
        time.sleep(ORDER_POLL_INTERVAL_S)

    # Timeout — check final state and cancel if still resting
    try:
        poll = _kalshi_auth_get(f"/portfolio/orders/{order_id}")
        o = poll.get("order", poll)
        fill_count = float(o.get("fill_count", 0))
        if o.get("status") == "executed":
            return order_id, planned_price, fill_count, "filled", None
        # Cancel unfilled remainder
        _kalshi_auth_post(f"/portfolio/orders/{order_id}/cancel", {})
        if fill_count > 0:
            return order_id, planned_price, fill_count, "partial", "timeout — canceled remainder"
    except Exception as e:
        return order_id, None, 0.0, "error", f"timeout + cancel failed: {e}"

    return order_id, None, 0.0, "rejected", "order timed out with no fills"


def _execute_poly_leg(side: str, planned_price: float, contracts: float,
                      token_id: str) -> Tuple[Optional[str], Optional[float], float, str, Optional[str]]:
    """Place and poll a Polymarket CLOB limit order. Returns (order_id, actual_price, filled, status, error)."""
    client = _get_poly_clob_client()

    # Round price to 2 decimal places (Poly CLOB tick size is $0.01)
    price = round(planned_price, 2)

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
    deadline = time.monotonic() + ORDER_TIMEOUT_S
    while time.monotonic() < deadline:
        try:
            o = client.get_order(order_id)
            o_status = (o.get("status") or "").lower()

            if o_status == "matched" or o_status == "filled":
                filled_size = float(o.get("size_matched", o.get("original_size", contracts)))
                avg_price = float(o.get("price", planned_price))
                return order_id, avg_price, filled_size, "filled", None

            if o_status == "canceled" or o_status == "cancelled":
                filled_size = float(o.get("size_matched", 0))
                if filled_size > 0:
                    return order_id, planned_price, filled_size, "partial", "order canceled after partial fill"
                return order_id, None, 0.0, "rejected", "order was canceled"

        except Exception:
            pass
        time.sleep(ORDER_POLL_INTERVAL_S)

    # Timeout — cancel unfilled remainder
    try:
        o = client.get_order(order_id)
        filled_size = float(o.get("size_matched", 0))
        if (o.get("status") or "").lower() in ("matched", "filled"):
            return order_id, planned_price, filled_size, "filled", None
        client.cancel(order_id)
        if filled_size > 0:
            return order_id, planned_price, filled_size, "partial", "timeout — canceled remainder"
    except Exception as e:
        return order_id, None, 0.0, "error", f"timeout + cancel failed: {e}"

    return order_id, None, 0.0, "rejected", "order timed out with no fills"


def execute_hedge(candidate: HedgeCandidate,
                  poly_quote: PolyMarketQuote,
                  kalshi_quote: KalshiMarketQuote,
                  logfile: str) -> ExecutionResult:
    """Execute both legs of a hedge and log full execution details."""
    contracts = PAPER_CONTRACTS

    # Determine Poly token ID for the leg we're buying
    if candidate.direction_on_poly == "UP":
        poly_token = poly_quote.up_token_id
    else:
        poly_token = poly_quote.down_token_id

    # Execute leg 1: Polymarket (usually faster, fills via CLOB)
    t_total = time.monotonic()
    leg1 = execute_leg("poly", candidate.direction_on_poly,
                       candidate.poly_price, contracts, token_id=poly_token)
    leg1_done = time.monotonic()

    # Execute leg 2: Kalshi
    leg2 = execute_leg("kalshi", candidate.direction_on_kalshi,
                       candidate.kalshi_price, contracts, ticker=kalshi_quote.ticker)
    total_ms = (time.monotonic() - t_total) * 1000

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

    fill_gap_ms = (leg1_done - t_total) * 1000
    print(f"  [exec] {s1} | {s2} | gap={fill_gap_ms:.0f}ms | total={total_ms:.0f}ms")

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
        # If Poly (leg1) filled but Kalshi (leg2) didn't fully fill,
        # sell back the unmatched Poly contracts to close exposure.
        if leg1.status == "filled" and leg2.status != "filled":
            unmatched = leg1.filled_contracts - leg2.filled_contracts
            if unmatched > 0 and EXEC_MODE == "live":
                print(f"  [unwind] Attempting to sell {unmatched:.0f} Poly contracts to close exposure...")
                unwind_result = _unwind_poly_leg(
                    candidate.direction_on_poly, leg1.actual_price or leg1.planned_price,
                    unmatched, poly_token, logfile,
                )
                if unwind_result:
                    print(f"  [unwind] {unwind_result}")
                exec_row["unwind_attempted"] = True
                exec_row["unwind_contracts"] = unmatched
                exec_row["unwind_result"] = unwind_result
            elif unmatched > 0 and EXEC_MODE == "paper":
                print(f"  [unwind] Paper mode — would sell {unmatched:.0f} Poly contracts")
                exec_row["unwind_attempted"] = False
                exec_row["unwind_contracts"] = unmatched
                exec_row["unwind_result"] = "paper_mode_skip"

        # If Kalshi (leg2) filled but Poly (leg1) didn't fully fill,
        # sell back the unmatched Kalshi contracts.
        elif leg2.status == "filled" and leg1.status != "filled":
            unmatched = leg2.filled_contracts - leg1.filled_contracts
            if unmatched > 0 and EXEC_MODE == "live":
                print(f"  [unwind] Attempting to sell {unmatched:.0f} Kalshi contracts to close exposure...")
                unwind_result = _unwind_kalshi_leg(
                    candidate.direction_on_kalshi, leg2.actual_price or leg2.planned_price,
                    unmatched, kalshi_quote.ticker, logfile,
                )
                if unwind_result:
                    print(f"  [unwind] {unwind_result}")
                exec_row["unwind_attempted"] = True
                exec_row["unwind_contracts"] = unmatched
                exec_row["unwind_result"] = unwind_result
            elif unmatched > 0 and EXEC_MODE == "paper":
                print(f"  [unwind] Paper mode — would sell {unmatched:.0f} Kalshi contracts")
                exec_row["unwind_attempted"] = False
                exec_row["unwind_contracts"] = unmatched
                exec_row["unwind_result"] = "paper_mode_skip"

    return result


def _unwind_poly_leg(side: str, buy_price: float, contracts: float,
                     token_id: str, logfile: str) -> str:
    """Sell back Poly contracts to close unhedged exposure. Returns status message."""
    try:
        client = _get_poly_clob_client()
        # Sell at a discount to ensure fill (buy_price - 0.02, floor at 0.01)
        sell_price = max(0.01, round(buy_price - 0.02, 2))

        order_args = OrderArgs(
            token_id=token_id,
            price=sell_price,
            size=contracts,
            side=SELL,
        )
        signed_order = client.create_order(order_args)
        resp = client.post_order(signed_order, OrderType.GTC)

        if not resp.get("success", False):
            msg = f"unwind rejected: {resp.get('errorMsg') or resp.get('error') or resp}"
            append_log(logfile, {"log_type": "unwind", "ts": utc_ts(), "exchange": "poly",
                                 "side": side, "contracts": contracts, "status": "rejected", "detail": msg})
            return msg

        order_id = resp.get("orderID") or resp.get("id")

        # Poll for fill
        deadline = time.monotonic() + ORDER_TIMEOUT_S
        while time.monotonic() < deadline:
            try:
                o = client.get_order(order_id)
                o_status = (o.get("status") or "").lower()
                if o_status in ("matched", "filled"):
                    msg = f"unwound {contracts:.0f} Poly contracts at ~${sell_price:.2f} (order {order_id})"
                    append_log(logfile, {"log_type": "unwind", "ts": utc_ts(), "exchange": "poly",
                                         "side": side, "contracts": contracts, "sell_price": sell_price,
                                         "order_id": order_id, "status": "filled"})
                    return msg
            except Exception:
                pass
            time.sleep(ORDER_POLL_INTERVAL_S)

        # Timeout — try to cancel
        try:
            client.cancel(order_id)
        except Exception:
            pass
        msg = f"unwind timed out (order {order_id}) — may need manual close"
        append_log(logfile, {"log_type": "unwind", "ts": utc_ts(), "exchange": "poly",
                             "side": side, "contracts": contracts, "order_id": order_id, "status": "timeout"})
        return msg

    except Exception as e:
        msg = f"unwind error: {e}"
        append_log(logfile, {"log_type": "unwind", "ts": utc_ts(), "exchange": "poly",
                             "side": side, "contracts": contracts, "status": "error", "detail": str(e)})
        return msg


def _unwind_kalshi_leg(side: str, buy_price: float, contracts: float,
                       ticker: str, logfile: str) -> str:
    """Sell back Kalshi contracts to close unhedged exposure. Returns status message."""
    try:
        kalshi_side = "yes" if side == "UP" else "no"
        # Sell at a discount to ensure fill
        sell_price_cents = max(1, int(round((buy_price - 0.02) * 100)))

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
            msg = f"unwind rejected: no order_id in response"
            append_log(logfile, {"log_type": "unwind", "ts": utc_ts(), "exchange": "kalshi",
                                 "side": side, "contracts": contracts, "status": "rejected", "detail": msg})
            return msg

        # Poll for fill
        deadline = time.monotonic() + ORDER_TIMEOUT_S
        while time.monotonic() < deadline:
            try:
                poll = _kalshi_auth_get(f"/portfolio/orders/{order_id}")
                o = poll.get("order", poll)
                if o.get("status") == "executed":
                    msg = f"unwound {contracts:.0f} Kalshi contracts at ~${sell_price_cents/100:.2f} (order {order_id})"
                    append_log(logfile, {"log_type": "unwind", "ts": utc_ts(), "exchange": "kalshi",
                                         "side": side, "contracts": contracts,
                                         "sell_price": sell_price_cents / 100.0,
                                         "order_id": order_id, "status": "filled"})
                    return msg
            except Exception:
                pass
            time.sleep(ORDER_POLL_INTERVAL_S)

        # Timeout — cancel
        try:
            _kalshi_auth_post(f"/portfolio/orders/{order_id}/cancel", {})
        except Exception:
            pass
        msg = f"unwind timed out (order {order_id}) — may need manual close"
        append_log(logfile, {"log_type": "unwind", "ts": utc_ts(), "exchange": "kalshi",
                             "side": side, "contracts": contracts, "order_id": order_id, "status": "timeout"})
        return msg

    except Exception as e:
        msg = f"unwind error: {e}"
        append_log(logfile, {"log_type": "unwind", "ts": utc_ts(), "exchange": "kalshi",
                             "side": side, "contracts": contracts, "status": "error", "detail": str(e)})
        return msg


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
    ensure_dir(LOG_DIR)
    logfile = os.path.join(LOG_DIR, f"arb_logs_market_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.jsonl")

    # First choice: execution mode
    EXEC_MODE = prompt_execution_mode()

    market_type = prompt_market_type()
    if market_type != "CRYPTO_15M_UPDOWN":
        raise RuntimeError(f"Market type not implemented: {market_type}")

    selected_coins = prompt_coin_selection(AVAILABLE_COINS)

    print("\nConfirm settings")
    print("=" * 45)
    mode_label = "*** LIVE TRADING ***" if EXEC_MODE == "live" else "Paper Testing"
    print(f"Execution:  {mode_label}")
    print(f"Market Type: {market_type}")
    print(f"Coins: {', '.join(selected_coins)}")
    print(f"Contracts:  {int(PAPER_CONTRACTS)}")
    print(f"\nExecution safeguards:")
    print(f"  Min net edge:       {pct(MIN_NET_EDGE)}")
    print(f"  Max total cost:     {MAX_TOTAL_COST:.3f}")
    print(f"  Min window time:    {MIN_WINDOW_REMAINING_S:.0f}s")
    print(f"  Max spread:         {pct(MAX_SPREAD)}")
    print(f"  Price range:        [{PRICE_FLOOR:.2f}, {PRICE_CEILING:.2f}]")
    print(f"  Max prob diverge:   {pct(MAX_PROB_DIVERGENCE)} (strike mismatch detector)")
    print(f"  Circuit breaker:    {MAX_CONSECUTIVE_SKIPS} consecutive skips")

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
            _get_poly_clob_client()
            print("Poly:    CLOB client initialized OK")
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
    for exch, bal in balances.items():
        if bal < 0:
            print(f"  [balance] {exch}: UNAVAILABLE")
        else:
            print(f"  [balance] {exch}: ${bal:.2f}")

    crypto_tag_id = poly_get_crypto_tag_id()
    if crypto_tag_id is None:
        raise RuntimeError("Could not resolve Polymarket tag id for slug 'crypto' (Gamma /tags/slug/crypto).")

    logged: List[dict] = []
    skip_counts: Dict[str, int] = {}  # reason -> count
    scan_i = 0
    consecutive_skips = 0

    # Store poly quotes per-coin per-scan for book depth lookups on winning trade
    _scan_poly_quotes: Dict[str, Optional[PolyMarketQuote]] = {}

    while len(logged) < MAX_TEST_TRADES:
        scan_i += 1
        scan_t0 = time.monotonic()
        print_scan_header(scan_i)

        # Pull Polymarket 15M crypto events once per scan (covers BTC/ETH/SOL)
        gamma_t0 = time.monotonic()
        poly_events = poly_get_active_15m_crypto_events(crypto_tag_id=crypto_tag_id, limit=250)
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
        print(f"  [timing] Poly Gamma: {gamma_ms:.0f}ms ({pre_filter_count} events, {len(poly_events)} after filter)")


        # Fetch all coin quotes in parallel (Kalshi + Poly CLOB calls concurrently)
        fetch_t0 = time.monotonic()
        coin_data: Dict[str, dict] = {}
        with ThreadPoolExecutor(max_workers=len(selected_coins)) as executor:
            futures = {
                executor.submit(_fetch_coin_quotes, coin, poly_events): coin
                for coin in selected_coins
            }
            for future in as_completed(futures):
                result = future.result()
                coin_data[result["coin"]] = result
        fetch_ms = (time.monotonic() - fetch_t0) * 1000

        # Per-coin timing breakdown
        print(f"  [timing] Parallel coin fetch: {fetch_ms:.0f}ms")
        for coin in selected_coins:
            cd = coin_data[coin]
            parts = [f"Kalshi {cd['kalshi_ms']:.0f}ms", f"Poly CLOB {cd['poly_ms']:.0f}ms"]
            if cd["kalshi_err"]:
                parts.append(f"ERR kalshi: {cd['kalshi_err'][:80]}")
            if cd["poly_err"]:
                parts.append(f"ERR poly: {cd['poly_err'][:80]}")
            print(f"    {coin}: {' | '.join(parts)}")

        best_global: Optional[HedgeCandidate] = None
        best_global_poly: Optional[PolyMarketQuote] = None
        best_global_kalshi: Optional[KalshiMarketQuote] = None

        # Fetch spot prices once per scan for strike reference
        spot_prices = fetch_spot_prices()

        _scan_poly_quotes.clear()

        for coin in selected_coins:
            cd = coin_data[coin]
            kalshi, poly = cd["kalshi"], cd["poly"]
            _scan_poly_quotes[coin] = poly

            display_market_block(market_type, coin, kalshi, poly, spot_price=spot_prices.get(coin))

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
                    print(f"  -> Staleness: ⚠ Poly WS data stale ({', '.join(stale_parts)} since last update)")

            if kalshi is None or poly is None:
                reason = "no_kalshi_market" if kalshi is None else "no_poly_market"
                log_skip(logfile, skip_counts, scan_i, coin, reason, poly=poly, kalshi=kalshi)
                if cd["kalshi_err"]:
                    print(f"  -> Error: Kalshi fetch failed: {cd['kalshi_err']}")
                if cd["poly_err"]:
                    print(f"  -> Error: Poly fetch failed: {cd['poly_err']}")
                continue

            # Window alignment: require Kalshi close_ts ~ Polymarket end_ts
            delta_s = abs((kalshi.close_ts - poly.end_ts).total_seconds())
            now = datetime.now(timezone.utc)
            remaining_s = (kalshi.close_ts - now).total_seconds()
            remaining_str = f"{int(remaining_s // 60)}m {int(remaining_s % 60)}s" if remaining_s > 0 else "CLOSED"

            if delta_s > WINDOW_ALIGN_TOLERANCE_SECONDS:
                print(f"  -> Alignment: ❌ SKIP (Kalshi close vs Poly end differ by {delta_s:.1f}s; tol={WINDOW_ALIGN_TOLERANCE_SECONDS}s)")
                log_skip(logfile, skip_counts, scan_i, coin, "alignment", poly, kalshi, remaining_s)
                continue
            else:
                print(f"  -> Alignment: ✅ OK (Δ {delta_s:.1f}s) | Window closes in: {remaining_str}")

            # Safeguard: minimum time remaining in window
            if remaining_s < MIN_WINDOW_REMAINING_S:
                print(f"  -> Window: ❌ SKIP (only {remaining_str} left; need {MIN_WINDOW_REMAINING_S:.0f}s to fill both legs)")
                log_skip(logfile, skip_counts, scan_i, coin, "window_time", poly, kalshi, remaining_s)
                continue

            # Safeguard: spread sanity — reject if either exchange has abnormally wide spread
            kalshi_spread = (kalshi.yes_ask + kalshi.no_ask) - 1.0
            poly_spread = (poly.up_price + poly.down_price) - 1.0
            if kalshi_spread > MAX_SPREAD:
                print(f"  -> Spread: ❌ SKIP Kalshi spread {pct(kalshi_spread)} exceeds {pct(MAX_SPREAD)} max")
                log_skip(logfile, skip_counts, scan_i, coin, "kalshi_spread", poly, kalshi, remaining_s)
                continue
            if poly_spread > MAX_SPREAD:
                print(f"  -> Spread: ❌ SKIP Poly spread {pct(poly_spread)} exceeds {pct(MAX_SPREAD)} max")
                log_skip(logfile, skip_counts, scan_i, coin, "poly_spread", poly, kalshi, remaining_s)
                continue

            # Safeguard: extreme prices — outcome nearly decided, hedging is unreliable
            prices = [poly.up_price, poly.down_price, kalshi.yes_ask, kalshi.no_ask]
            extreme = [p for p in prices if p < PRICE_FLOOR or p > PRICE_CEILING]
            if extreme:
                print(f"  -> Price: ❌ SKIP extreme prices detected ({', '.join(f'{p:.3f}' for p in extreme)}); "
                      f"outside [{PRICE_FLOOR:.2f}, {PRICE_CEILING:.2f}] range")
                log_skip(logfile, skip_counts, scan_i, coin, "extreme_price", poly, kalshi, remaining_s)
                continue

            # Safeguard: probability divergence — large disagreement signals mismatched strikes.
            kalshi_up_prob = 1.0 - kalshi.no_ask
            poly_up_prob = poly.up_price
            prob_div = abs(kalshi_up_prob - poly_up_prob)
            if prob_div > MAX_PROB_DIVERGENCE:
                print(f"  -> Divergence: ❌ SKIP implied P(up) Kalshi={kalshi_up_prob:.2f} vs Poly={poly_up_prob:.2f} "
                      f"(Δ{pct(prob_div)} > {pct(MAX_PROB_DIVERGENCE)} max — likely different strikes)")
                log_skip(logfile, skip_counts, scan_i, coin, "prob_divergence", poly, kalshi, remaining_s)
                continue

            print(f"          Safeguards: ✅ all passed (Δprob {pct(prob_div)}, min edge {pct(MIN_NET_EDGE)}, min window {MIN_WINDOW_REMAINING_S:.0f}s)")

            best_for_coin, all_combos = best_hedge_for_coin(coin, poly, kalshi)

            # Show both hedge combos for visibility
            print(f"  -> Hedge combos:")
            for c in all_combos:
                tag = "VIABLE" if c.total_cost < MAX_TOTAL_COST and c.net_edge >= MIN_NET_EDGE else "skip"
                print(
                    f"       Poly {c.direction_on_poly} {c.poly_price:.3f} + Kalshi {c.direction_on_kalshi} {c.kalshi_price:.3f} "
                    f"= {c.total_cost:.3f} | gross {pct(c.gross_edge)} | net {pct(c.net_edge)} "
                    f"| fees ${c.poly_fee:.4f}+${c.kalshi_fee:.4f} [{tag}]"
                )

            if best_for_coin is None:
                print(f"  -> Candidate: ❌ SKIP (no combo with net edge >= {pct(MIN_NET_EDGE)} and cost < {MAX_TOTAL_COST:.3f})")
                log_skip(logfile, skip_counts, scan_i, coin, "no_viable_edge", poly, kalshi, remaining_s)
                continue

            print(
                f"  -> Candidate: ✅ BEST | Poly {best_for_coin.direction_on_poly} {best_for_coin.poly_price:.3f} "
                f"+ Kalshi {best_for_coin.direction_on_kalshi} {best_for_coin.kalshi_price:.3f} "
                f"= {best_for_coin.total_cost:.3f} | gross {pct(best_for_coin.gross_edge)} | "
                f"fees poly {fmt_money(best_for_coin.poly_fee)} + kalshi {fmt_money(best_for_coin.kalshi_fee)} "
                f"+ extras {fmt_money(best_for_coin.extras)} = net {pct(best_for_coin.net_edge)}"
            )

            if best_global is None or best_for_coin.net_edge > best_global.net_edge:
                best_global = best_for_coin
                best_global_poly = poly
                best_global_kalshi = kalshi


        # Scan timing summary
        scan_ms = (time.monotonic() - scan_t0) * 1000
        process_ms = scan_ms - gamma_ms - fetch_ms
        timing_parts = f"Gamma {gamma_ms:.0f}ms + fetch {fetch_ms:.0f}ms + process {process_ms:.0f}ms"
        if _poly_ws is not None:
            ws_hits, ws_misses = _poly_ws.get_and_reset_stats()
            timing_parts += f" | WS cache: {ws_hits} hits, {ws_misses} misses"
        print(f"\n  [timing] Scan #{scan_i} total: {scan_ms:.0f}ms ({timing_parts})")
        # Log at most one paper trade per scan (the best across BTC/ETH/SOL)
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
                "window_close_ts": best_global_kalshi.close_ts.isoformat(),
                "spot_price": spot_prices.get(best_global.coin),
                "kalshi_strike": best_global_kalshi.strike,
                # Poly book depth snapshot
                "poly_book_levels": poly_depth["levels"] if poly_depth else None,
                "poly_book_size": poly_depth["total_size"] if poly_depth else None,
                "poly_book_notional_usd": poly_depth["total_notional_usd"] if poly_depth else None,
                "poly_ws_staleness_s": round(poly_staleness_s, 1) if poly_staleness_s is not None else None,
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
            exec_result = execute_hedge(best_global, best_global_poly, best_global_kalshi, logfile)

            # Attach execution details to the trade log row
            row["exec_mode"] = EXEC_MODE
            row["exec_both_filled"] = exec_result.both_filled
            row["exec_total_ms"] = round(exec_result.total_latency_ms, 1)
            row["exec_slippage_poly"] = round(exec_result.slippage_poly, 6)
            row["exec_slippage_kalshi"] = round(exec_result.slippage_kalshi, 6)
            row["exec_slippage_total_bps"] = round((exec_result.slippage_poly + exec_result.slippage_kalshi) * 10000, 1)
            row["exec_leg1_order_id"] = exec_result.leg1.order_id
            row["exec_leg1_status"] = exec_result.leg1.status
            row["exec_leg1_actual_price"] = exec_result.leg1.actual_price
            row["exec_leg1_latency_ms"] = round(exec_result.leg1.latency_ms, 1)
            row["exec_leg2_order_id"] = exec_result.leg2.order_id
            row["exec_leg2_status"] = exec_result.leg2.status
            row["exec_leg2_actual_price"] = exec_result.leg2.actual_price
            row["exec_leg2_latency_ms"] = round(exec_result.leg2.latency_ms, 1)

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
                    "viable": combo.total_cost < MAX_TOTAL_COST and combo.net_edge >= MIN_NET_EDGE,
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
            consecutive_skips = 0
            mode_tag = "LIVE" if EXEC_MODE == "live" else "paper"
            fill_tag = "FILLED" if exec_result.both_filled else "INCOMPLETE"
            print(f"[{mode_tag}] Logged trade #{len(logged)} -> {best_global.coin} | net {pct(best_global.net_edge)} "
                  f"(gross {pct(best_global.gross_edge)}) | exec: {fill_tag}")

            # Print full diagnostic to console for easy copy-paste
            print("\n" + "=" * 60)
            print("  DIAGNOSTIC DUMP (copy everything below for analysis)")
            print("=" * 60)
            print(json.dumps(diag, indent=2, default=str))
        else:
            consecutive_skips += 1
            print(f"No viable paper trades found in this scan. ({consecutive_skips} consecutive skips)")
            if consecutive_skips >= MAX_CONSECUTIVE_SKIPS:
                print(f"\n⚠ Circuit breaker: {MAX_CONSECUTIVE_SKIPS} consecutive scans with no viable trades. Stopping.")
                break

        if len(logged) < MAX_TEST_TRADES:
            time.sleep(SCAN_SLEEP_SECONDS)

    print(f"\nDone scanning. Wrote logs to: {logfile}")

    # Outcome verification: wait for windows to close, then check actual results
    # Skip for single-trade diagnostic runs (long wait for little value);
    # keep for multi-trade sessions where P&L tracking matters.
    if logged and MAX_TEST_TRADES > 1:
        ans = input("\nWait for outcome verification? [y/N] ").strip().lower()
        if ans == "y":
            verify_trade_outcomes(logged, logfile)
        else:
            print("Skipping outcome verification.")
    elif logged:
        print("(Skipping outcome verification for single-trade run)")

    summarize(logged, selected_coins, skip_counts=skip_counts)


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
