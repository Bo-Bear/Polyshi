import json
import os
import time
import math
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

try:
    from websocket import WebSocketApp as _WebSocketApp
    _HAS_WS_CLIENT = True
except ImportError:
    _HAS_WS_CLIENT = False


# -----------------------------
# Config
# -----------------------------
load_dotenv()

SCAN_SLEEP_SECONDS = float(os.getenv("SCAN_SLEEP_SECONDS", "5"))
MAX_TEST_TRADES = int(os.getenv("MAX_TEST_TRADES", "5"))
WINDOW_ALIGN_TOLERANCE_SECONDS = int(os.getenv("WINDOW_ALIGN_TOLERANCE_SECONDS", "10"))
MIN_LEG_NOTIONAL = float(os.getenv("MIN_LEG_NOTIONAL", "10"))  # $ minimum liquidity per leg
USE_VWAP_DEPTH = os.getenv("USE_VWAP_DEPTH", "true").lower() == "true"

# -----------------------------
# Fees (paper-trade model)
# -----------------------------
# Size we assume for fee calculations (both venues). Polymarket fee table is for 100 shares. :contentReference[oaicite:3]{index=3}
PAPER_CONTRACTS = float(os.getenv("PAPER_CONTRACTS", "100"))

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

    # Best-effort strike extraction (often present in market fields like functional_strike)
    strike = full.get("functional_strike") or None

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


def extract_poly_quote_for_coin(events: List[dict], coin: str) -> Optional[PolyMarketQuote]:
    prefix = POLY_TITLE_PREFIX[coin]
    now = datetime.now(timezone.utc)

    # pick earliest-ending active event matching title prefix
    all_matches = [e for e in events if isinstance(e.get("title"), str) and e["title"].startswith(prefix)]

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

        return PolyMarketQuote(
            event_slug=e.get("slug") or "",
            market_slug=m0.get("slug") or "",
            title=e.get("title") or "",
            up_price=up_ask,
            down_price=down_ask,
            end_ts=end_ts,
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

    # Fee-aware viability:
    # still require total_cost < 1 (otherwise you lose before fees),
    # and require net_edge > 0 (profit after fees/extras).
    viable = [c for c in cands if c.total_cost < 1.0 and c.net_edge > 0]
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


def display_market_block(market_type: str, coin: str, kalshi: Optional[KalshiMarketQuote], poly: Optional[PolyMarketQuote]) -> None:
    label = MARKET_TYPE_LABELS.get(market_type, market_type)
    print(f"\n[MARKET] {coin} ({label})")


    if kalshi is None:
        print("  Kalshi:  (no open market found)")
    else:
        spread = (kalshi.yes_ask + kalshi.no_ask) - 1.0
        strike_txt = f" | Strike: {kalshi.strike}" if kalshi.strike else ""
        print(f"  Kalshi:  {kalshi.title}{strike_txt}")
        print(f"          Up/Down (asks): {fmt_price_pair(kalshi.yes_ask, kalshi.no_ask)}")
        print(f"          Close (UTC): {kalshi.close_ts.strftime('%H:%M:%S')}")
        print(f"          Spread (ask sum - 1): {pct(spread)}")

    if poly is None:
        print("  Poly:    (no active 15M event found)")
    else:
        spread = (poly.up_price + poly.down_price) - 1.0
        print(f"  Poly:    {poly.title}  ({poly.event_slug}/{poly.market_slug})")
        print(f"          Up/Down:        {fmt_price_pair(poly.up_price, poly.down_price)}")
        print(f"          End   (UTC): {poly.end_ts.strftime('%H:%M:%S')}")
        print(f"          Spread (sum - 1): {pct(spread)}")


def append_log(path: str, row: dict) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row) + "\n")


def summarize(log_rows: List[dict], coins: List[str]) -> None:
    if not log_rows:
        print("\nNo test trades were logged.")
        return

    # Aggregate by coin + direction
    by_coin: Dict[str, List[dict]] = {}
    for r in log_rows:
        by_coin.setdefault(r["coin"], []).append(r)

    print("\n=========================")
    print("  TEST TRADE SUMMARY")
    print("=========================")

    total_edge = sum(r["net_edge"] for r in log_rows)
    avg_edge = total_edge / len(log_rows)

    print(f"\nLogged trades: {len(log_rows)}")
    print(f"Avg net:       {pct(avg_edge)}")
    print(f"Best net:      {pct(max(r['net_edge'] for r in log_rows))}")
    print(f"Worst net:     {pct(min(r['net_edge'] for r in log_rows))}")


    print("\nBy coin:")
    for coin in coins:
        rows = by_coin.get(coin, [])
        if not rows:
            continue
        avg = sum(r["net_edge"] for r in rows) / len(rows)
        best = max(r["net_edge"] for r in rows)
        print(f"  {coin}: n={len(rows)} | avg_net={pct(avg)} | best_net={pct(best)}")


    print("\nMost recent trades:")
    for r in log_rows[-5:]:
        print(
            f"  [{r['ts']}] {r['coin']} | Poly {r['poly_side']} {r['poly_price']:.3f} "
            f"+ Kalshi {r['kalshi_side']} {r['kalshi_price']:.3f} "
            f"= total {r['total_cost']:.3f} | net {pct(r['net_edge'])} (gross {pct(r['gross_edge'])}) "
            f"| fees {r['poly_fee']:.4f}+{r['kalshi_fee']:.4f}+{r['extras']:.4f}"
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
# Main loop
# -----------------------------
def main() -> None:
    ensure_dir(LOG_DIR)
    logfile = os.path.join(LOG_DIR, f"arb_logs_market_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.jsonl")

    market_type = prompt_market_type()
    if market_type != "CRYPTO_15M_UPDOWN":
        raise RuntimeError(f"Market type not implemented: {market_type}")

    selected_coins = prompt_coin_selection(AVAILABLE_COINS)

    print("\nConfirm settings")
    print("=" * 45)
    print(f"Market Type: {market_type}")
    print(f"Coins: {', '.join(selected_coins)}")

    # Start Polymarket CLOB WebSocket for real-time orderbook data
    global _poly_ws
    if _HAS_WS_CLIENT:
        _poly_ws = _PolyOrderbookWS()
        _poly_ws.start()
        print("WebSocket: Polymarket CLOB connected (cache warms after first scan)")
    else:
        print("WebSocket: websocket-client not installed; using HTTP-only (pip install websocket-client)")

    print("\nPress ENTER to start scanning (Ctrl+C to stop)...")
    input()

    crypto_tag_id = poly_get_crypto_tag_id()
    if crypto_tag_id is None:
        raise RuntimeError("Could not resolve Polymarket tag id for slug 'crypto' (Gamma /tags/slug/crypto).")

    logged: List[dict] = []
    scan_i = 0

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

        for coin in selected_coins:
            cd = coin_data[coin]
            kalshi, poly = cd["kalshi"], cd["poly"]

            display_market_block(market_type, coin, kalshi, poly)

            if kalshi is None or poly is None:
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
                continue
            else:
                print(f"  -> Alignment: ✅ OK (Δ {delta_s:.1f}s) | Window closes in: {remaining_str}")
                print(f"          Liquidity threshold: ${MIN_LEG_NOTIONAL:.0f} per outcome (enforced)")

            best_for_coin, all_combos = best_hedge_for_coin(coin, poly, kalshi)

            # Show both hedge combos for visibility
            print(f"  -> Hedge combos:")
            for c in all_combos:
                tag = "VIABLE" if c.total_cost < 1.0 and c.net_edge > 0 else "skip"
                print(
                    f"       Poly {c.direction_on_poly} {c.poly_price:.3f} + Kalshi {c.direction_on_kalshi} {c.kalshi_price:.3f} "
                    f"= {c.total_cost:.3f} | gross {pct(c.gross_edge)} | net {pct(c.net_edge)} "
                    f"| fees ${c.poly_fee:.4f}+${c.kalshi_fee:.4f} [{tag}]"
                )

            if best_for_coin is None:
                print("  -> Candidate: ❌ SKIP (no combo with positive net edge)")
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


        # Scan timing summary
        scan_ms = (time.monotonic() - scan_t0) * 1000
        process_ms = scan_ms - gamma_ms - fetch_ms
        timing_parts = f"Gamma {gamma_ms:.0f}ms + fetch {fetch_ms:.0f}ms + process {process_ms:.0f}ms"
        if _poly_ws is not None:
            ws_hits, ws_misses = _poly_ws.get_and_reset_stats()
            timing_parts += f" | WS cache: {ws_hits} hits, {ws_misses} misses"
        print(f"\n  [timing] Scan #{scan_i} total: {scan_ms:.0f}ms ({timing_parts})")
        # Log at most one paper trade per scan (the best across BTC/ETH/SOL)
        if best_global is not None:
            # Gather per-coin latency for the winning coin
            winning_cd = coin_data[best_global.coin]
            row = {
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
                "scan_ms": round(scan_ms, 1),
                "gamma_ms": round(gamma_ms, 1),
                "fetch_ms": round(fetch_ms, 1),
                "kalshi_latency_ms": round(winning_cd["kalshi_ms"], 1),
                "poly_latency_ms": round(winning_cd["poly_ms"], 1),
            }
            append_log(logfile, row)
            logged.append(row)
            print(f"[paper] Logged test trade #{len(logged)} -> {best_global.coin} | net {pct(best_global.net_edge)} (gross {pct(best_global.gross_edge)})")
        else:
            print("No viable paper trades found in this scan.")

        if len(logged) < MAX_TEST_TRADES:
            time.sleep(SCAN_SLEEP_SECONDS)

    print(f"\nDone. Wrote logs to: {logfile}")

    summarize(logged, selected_coins)


if __name__ == "__main__":
    main()
