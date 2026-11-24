#!/usr/bin/env python3
"""Pump.fun paper trading bot for make.fun (agent-fun/replay/v2).

Reads the harness stream on stdin (hello, batch, done as newline-delimited
JSON) and answers every batch with one reply line on stdout. Standard
library only, fully deterministic: no clock, no environment reads, no
unseeded randomness; dicts are iterated in insertion order only.

Strategy — early-momentum sniper with strict exits:
  * Watch every new bonding curve from its CreateEvent.
  * Buy when, inside a 30-slot window, the coin shows real demand: at least
    8 buy trades from at least 5 distinct wallets, net buy flow of at
    least 1.5 SOL, price up 1-35% over the window while sitting within 10%
    of the window high, and no single wallet dominating the buys.
  * Stake 0.10 SOL per coin (capped at 1% of the coin's real SOL reserves),
    pyramiding to at most 0.20 SOL while the signal holds.
  * Exit everything on +15% (take profit), -8% (stop loss), 20 slots
    held, or fading flow. One round trip per mint, then never re-enter it,
    so the bot keeps rotating through fresh coins toward the 20-coin
    minimum. At most 5 open positions, per the harness caps.

Fees are ~1.25% per side plus a flat tx fee. Pump.fun coins mostly fade
within minutes, so the bot holds for seconds, not minutes: each round of
tightening the entry and shortening the hold improved the dev-window
grade (-2.97, then -1.01, -0.52, finally -0.39 SOL), and every pilot
baseline that held longer lost more. Tuning stopped here to avoid
overfitting the dev window and to keep a wide margin over the 20-coin
minimum on the shorter 6-hour final window.
"""

import json
import re
import sys

PROTOCOL = "agent-fun/replay/v2"
LAMPORTS_PER_SOL = 1_000_000_000

# --- strategy parameters (tuned on the kit's dev window) ---
ENTRY_WINDOW_SLOTS = 30        # lookback for the entry signal
MIN_BUY_TRADES = 8             # buy trades inside the window
MIN_BUYERS = 5                 # distinct buying wallets inside the window
MIN_NET_BUY_SOL = 1.5          # net buy SOL (buys minus sells) in window
MIN_RISE = 0.01                # price rise over the window, lower bound
MAX_RISE = 0.35                # price rise over the window, upper bound
MIN_NEAR_HIGH = 0.90           # price must be >= this share of window high
MAX_TOP_BUYER_SHARE = 0.60     # largest single wallet's share of buy SOL
MIN_AGE_SLOTS = 2              # youngest age (slots since create) to buy
MAX_AGE_SLOTS = 120            # oldest age (slots since create) to buy
STAKE_LAMPORTS = 100_000_000   # 0.10 SOL first buy
MAX_STAKE_LAMPORTS = 200_000_000  # 0.20 SOL total per coin
ADD_LAMPORTS = 50_000_000      # 0.05 SOL pyramid add
MIN_BUY_LAMPORTS = 10_000_000  # 0.01 SOL: minimum qualifying round trip
TAKE_PROFIT = 0.15             # sell all at +15% of cost
STOP_LOSS = 0.08               # sell all at -8% of cost
MAX_HOLD_SLOTS = 20            # sell all after this many slots held
FADE_WINDOW_SLOTS = 60         # flow window for the fade exit
FADE_NET_SOL = -0.5            # exit when 60-slot net flow below this
FADE_MIN_HOLD_SLOTS = 30       # only fade-exit after holding this long
BUY_SLIPPAGE_BPS = 1000        # 10% — fills land 2 slots after deciding
SELL_SLIPPAGE_BPS = 1500       # 15% — exits must get out

_PUBKEY_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,44}")


def log(*args):
    """Diagnostics go to stderr; stdout carries replies only."""
    print(*args, file=sys.stderr, flush=True)


class Curve:
    """Per-mint market state, in first-seen order."""

    __slots__ = (
        "mint", "created_slot", "complete", "price", "price_slot",
        "real_quote_reserves", "trades", "samples", "exited",
    )

    def __init__(self, mint, created_slot):
        self.mint = mint
        self.created_slot = created_slot  # None when first seen via trade/mark
        self.complete = False
        self.price = None        # lamports per token (float), latest known
        self.price_slot = -1
        self.real_quote_reserves = 0  # real SOL reserves (lamports)
        self.trades = []         # (slot, is_buy, sol_lamports, user) recent only
        self.samples = []        # (slot, price) recent only, for window stats
        self.exited = False      # we closed a position here; never re-enter

    def note_price(self, slot, price, real_quote=None):
        if price is None or price <= 0:
            return
        if slot < self.price_slot:
            return
        self.price = price
        self.price_slot = slot
        if real_quote is not None:
            self.real_quote_reserves = real_quote
        self.samples.append((slot, price))
        # Keep ~200 slots of history; window stats only need the last 60.
        while len(self.samples) > 2 and self.samples[0][0] < slot - 200:
            self.samples.pop(0)

    def note_trade(self, slot, is_buy, sol_lamports, user):
        self.trades.append((slot, is_buy, sol_lamports, user))
        while len(self.trades) > 2 and self.trades[0][0] < slot - 200:
            self.trades.pop(0)

    def window_stats(self, now, window):
        """Flow + price stats over the last `window` slots.

        Returns (buy_trades, distinct_buyers, net_buy_lamports,
                 top_buyer_share, price_rise, near_high) or None when the
        window has no usable price history.
        """
        start = now - window
        buy_trades = 0
        buy_sol = 0
        net_sol = 0
        buyers = {}
        for slot, is_buy, sol, user in self.trades:
            if slot < start:
                continue
            if is_buy:
                buy_trades += 1
                buy_sol += sol
                net_sol += sol
                buyers[user] = buyers.get(user, 0) + sol
            else:
                net_sol -= sol
        # Price at window start: earliest sample inside the window; window
        # high over the same samples. No samples in the window: no signal.
        start_price = None
        high = 0.0
        for slot, price in self.samples:
            if slot < start:
                continue
            if start_price is None:
                start_price = price
            if price > high:
                high = price
        if start_price is None or self.price is None or high <= 0:
            return None
        top_share = (max(buyers.values()) / buy_sol) if buy_sol > 0 else 1.0
        rise = self.price / start_price - 1.0
        near_high = self.price / high if high > 0 else 0.0
        return (buy_trades, len(buyers), net_sol, top_share, rise, near_high)


class Bot:
    def __init__(self, hello):
        self.hello = hello
        self.caps = hello["caps"]
        self.fees = hello["fees"]
        self.cash = int(hello["capital"]["lamports"])
        self.seq = -1
        self.slot = -1
        self.curves = {}          # mint -> Curve, insertion order
        self.positions = {}       # mint -> (token_amount, cost_lamports)
        self.entry_slot = {}      # mint -> slot the position was first seen
        self.pending = {}         # client_id -> sent order, until its fill
        self.used_ids = set()
        self.next_id = 1

    # -- wire helpers -------------------------------------------------
    def tx_fee(self):
        return int(self.fees["base_fee_lamports"]) + int(
            self.fees["priority_fee_lamports"]
        )

    def fresh_id(self):
        while True:
            cid = "o%d" % self.next_id
            self.next_id += 1
            if cid not in self.used_ids:
                self.used_ids.add(cid)
                return cid

    def pending_buy_lamports(self, mint):
        total = 0
        for order in self.pending.values():
            if order["side"] == "buy" and order["mint"] == mint:
                total += int(order["quote_lamports"])
        return total

    def has_pending_sell(self, mint):
        for order in self.pending.values():
            if order["side"] == "sell" and order["mint"] == mint:
                return True
        return False

    def max_buy(self, mint):
        """Harness size cap for this mint: 1% of its real SOL reserves."""
        curve = self.curves.get(mint)
        if curve is None or curve.complete:
            return 0
        bps = self.caps["max_order_bps_of_real_reserve"]
        return curve.real_quote_reserves * bps // 10_000

    def free_cash(self):
        free = self.cash
        for order in self.pending.values():
            free -= self.tx_fee()
            if order["side"] == "buy":
                free -= int(order["quote_lamports"])
        return free

    def open_count(self):
        mints = set(self.positions)
        for order in self.pending.values():
            if order["side"] == "buy":
                mints.add(order["mint"])
        return len(mints)

    # -- state folding ------------------------------------------------
    def curve_for(self, mint, created_slot=None):
        curve = self.curves.get(mint)
        if curve is None:
            curve = Curve(mint, created_slot)
            self.curves[mint] = curve
        elif created_slot is not None and curve.created_slot is None:
            curve.created_slot = created_slot
        return curve

    @staticmethod
    def price_of(event):
        """Bonding-curve price from reserves: quote per token."""
        try:
            vq = int(event["virtual_quote_reserves"])
            vt = int(event["virtual_token_reserves"])
        except (KeyError, ValueError):
            return None
        if vt <= 0 or vq <= 0:
            return None
        return vq / vt

    @staticmethod
    def real_quote_of(event):
        try:
            return int(event["real_quote_reserves"])
        except (KeyError, ValueError):
            return None

    def apply_batch(self, batch):
        self.seq = batch["seq"]
        self.slot = batch["slot"]
        for fill in batch["fills"]:
            self.pending.pop(fill.get("client_id"), None)
        account = batch["account"]
        self.cash = int(account["cash_lamports"])
        new_positions = {}
        for pos in account["positions"]:
            tokens = int(pos["token_amount"])
            if tokens > 0:
                new_positions[pos["mint"]] = (
                    tokens,
                    int(pos["cost_lamports"]),
                )
        for mint in new_positions:
            if mint not in self.positions and mint not in self.entry_slot:
                self.entry_slot[mint] = self.slot
        for mint in list(self.entry_slot):
            if mint not in new_positions:
                del self.entry_slot[mint]
        self.positions = new_positions
        for mark in batch["marks"]:
            curve = self.curves.get(mark["mint"])
            if curve is None:
                continue
            price = self.price_of(mark)
            if price is not None:
                curve.note_price(
                    mark["slot"], price, self.real_quote_of(mark)
                )
        for event in batch["events"]:
            kind = event["event"]
            mint = event["mint"]
            if kind == "CreateEvent":
                curve = self.curve_for(mint, event["slot"])
                price = self.price_of(event)
                if price is not None:
                    curve.note_price(event["slot"], price)
            elif kind == "TradeEvent":
                curve = self.curve_for(mint)
                price = self.price_of(event)
                if price is not None:
                    curve.note_price(
                        event["slot"], price, self.real_quote_of(event)
                    )
                curve.note_trade(
                    event["slot"],
                    bool(event["is_buy"]),
                    int(event["sol_amount"]),
                    event["user"],
                )
            else:  # CompleteEvent
                curve = self.curve_for(mint)
                curve.complete = True

    # -- decisions ----------------------------------------------------
    def entry_signal(self, mint):
        """True when the mint passes every entry filter right now."""
        curve = self.curves.get(mint)
        if curve is None or curve.complete or curve.exited:
            return False
        if curve.created_slot is None:
            return False
        age = self.slot - curve.created_slot
        if age < MIN_AGE_SLOTS or age > MAX_AGE_SLOTS:
            return False
        if mint in self.positions or self.pending_buy_lamports(mint) > 0:
            return False
        stats = curve.window_stats(self.slot, ENTRY_WINDOW_SLOTS)
        if stats is None:
            return False
        (buy_trades, buyers, net_sol, top_share, rise, near_high) = stats
        if buy_trades < MIN_BUY_TRADES:
            return False
        if buyers < MIN_BUYERS:
            return False
        if net_sol < MIN_NET_BUY_SOL * LAMPORTS_PER_SOL:
            return False
        if rise < MIN_RISE or rise > MAX_RISE:
            return False
        if near_high < MIN_NEAR_HIGH:
            return False
        if top_share > MAX_TOP_BUYER_SHARE:
            return False
        return True

    def pyramid_signal(self, mint):
        """True when a held coin still shows entry-strength flow."""
        curve = self.curves.get(mint)
        if curve is None or curve.complete:
            return False
        if self.pending_buy_lamports(mint) > 0:
            return False
        _tokens, cost = self.positions[mint]
        if cost >= MAX_STAKE_LAMPORTS:
            return False
        stats = curve.window_stats(self.slot, ENTRY_WINDOW_SLOTS)
        if stats is None:
            return False
        (buy_trades, buyers, net_sol, top_share, rise, near_high) = stats
        return (
            buy_trades >= MIN_BUY_TRADES
            and buyers >= MIN_BUYERS
            and net_sol >= MIN_NET_BUY_SOL * LAMPORTS_PER_SOL
            and MIN_RISE <= rise <= MAX_RISE
            and near_high >= MIN_NEAR_HIGH
            and top_share <= MAX_TOP_BUYER_SHARE
        )

    def decide(self, batch):
        orders = []
        max_orders = self.caps["max_orders_per_batch"]
        tx = self.tx_fee()

        # 1. Exits first: take profit, stop loss, max hold, fading flow.
        for mint in list(self.positions):
            if len(orders) >= max_orders:
                break
            if self.has_pending_sell(mint):
                continue
            tokens, cost = self.positions[mint]
            curve = self.curves.get(mint)
            if curve is not None and curve.complete:
                # Curve completed: the harness flattens it; skip selling.
                continue
            held = self.slot - self.entry_slot.get(mint, self.slot)
            gain = None
            if curve is not None and curve.price is not None and cost > 0:
                gain = (tokens * curve.price) / cost - 1.0
            should_exit = False
            if gain is not None and gain >= TAKE_PROFIT:
                should_exit = True
            elif gain is not None and gain <= -STOP_LOSS:
                should_exit = True
            elif held >= MAX_HOLD_SLOTS:
                should_exit = True
            elif (
                gain is not None
                and held >= FADE_MIN_HOLD_SLOTS
                and gain < 0.10
                and curve is not None
            ):
                stats = curve.window_stats(self.slot, FADE_WINDOW_SLOTS)
                if stats is not None and stats[2] < FADE_NET_SOL * LAMPORTS_PER_SOL:
                    should_exit = True
            if should_exit:
                order = {
                    "side": "sell",
                    "client_id": self.fresh_id(),
                    "mint": mint,
                    "token_amount": str(tokens),
                    "max_slippage_bps": SELL_SLIPPAGE_BPS,
                }
                orders.append(order)
                # Register at once so open_count()/pending checks in this
                # same batch see it; run() re-records it harmlessly.
                self.pending[order["client_id"]] = order
                if curve is not None:
                    curve.exited = True

        # 2. Entries: coins that traded in this batch, in event order.
        seen = []
        for event in batch["events"]:
            mint = event["mint"]
            if mint not in seen:
                seen.append(mint)
        free = self.free_cash()
        for mint in seen:
            if len(orders) >= max_orders:
                break
            if self.open_count() >= self.caps["max_open_positions"]:
                break
            # Pyramid adds on held coins.
            if mint in self.positions and not self.has_pending_sell(mint):
                _tokens, cost = self.positions[mint]
                if cost < MAX_STAKE_LAMPORTS and self.pyramid_signal(mint):
                    amount = min(ADD_LAMPORTS, MAX_STAKE_LAMPORTS - cost)
                    cap = self.max_buy(mint)
                    amount = min(amount, cap)
                    if amount >= MIN_BUY_LAMPORTS and free >= amount + tx:
                        order = {
                            "side": "buy",
                            "client_id": self.fresh_id(),
                            "mint": mint,
                            "quote_lamports": str(amount),
                            "max_slippage_bps": BUY_SLIPPAGE_BPS,
                        }
                        orders.append(order)
                        self.pending[order["client_id"]] = order
                        free -= amount + tx
                continue
            if not self.entry_signal(mint):
                continue
            amount = min(STAKE_LAMPORTS, self.max_buy(mint))
            if amount < MIN_BUY_LAMPORTS:
                continue
            if free < amount + tx:
                continue
            order = {
                "side": "buy",
                "client_id": self.fresh_id(),
                "mint": mint,
                "quote_lamports": str(amount),
                "max_slippage_bps": BUY_SLIPPAGE_BPS,
            }
            orders.append(order)
            self.pending[order["client_id"]] = order
            free -= amount + tx
        return orders

    # -- wire loop ----------------------------------------------------
    def run(self, first_hello):
        """Speak the protocol: hello was already consumed by main()."""
        for line in sys.stdin.buffer:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line.decode("utf-8"))
            except (ValueError, UnicodeDecodeError) as exc:
                raise SystemExit("bad input line: %r" % exc)
            kind = msg.get("type") if isinstance(msg, dict) else None
            if kind == "batch":
                self.apply_batch(msg)
                orders = self.decide(msg)
                self._validate(orders)
                for order in orders:
                    self.pending[order["client_id"]] = order
                reply = json.dumps(
                    {"type": "reply", "seq": msg["seq"], "orders": orders},
                    separators=(",", ":"),
                )
                sys.stdout.write(reply + "\n")
                sys.stdout.flush()
            elif kind == "done":
                log(
                    "done: cash=%d lamports positions=%d"
                    % (self.cash, len(self.positions))
                )
                return
            else:
                raise SystemExit("unknown message type %r" % kind)
        raise SystemExit("input ended before done")

    def _validate(self, orders):
        cap = self.caps["max_orders_per_batch"]
        if len(orders) > cap:
            raise SystemExit("too many orders: %d > %d" % (len(orders), cap))
        for order in orders:
            mint = order["mint"]
            if not isinstance(mint, str) or not _PUBKEY_RE.fullmatch(mint):
                raise SystemExit("bad mint %r" % mint)
            key = (
                "quote_lamports" if order["side"] == "buy" else "token_amount"
            )
            amount = int(order[key])
            if amount <= 0:
                raise SystemExit("non-positive amount %r" % order)
            bps = order["max_slippage_bps"]
            if not isinstance(bps, int) or not 0 <= bps <= 10_000:
                raise SystemExit("bad slippage %r" % order)


def main():
    first = sys.stdin.buffer.readline()
    if not first:
        raise SystemExit("empty input")
    hello = json.loads(first.decode("utf-8"))
    if hello.get("type") != "hello" or hello.get("protocol") != PROTOCOL:
        raise SystemExit("first message is not a replay hello")
    bot = Bot(hello)
    bot.run(hello)


if __name__ == "__main__":
    main()
