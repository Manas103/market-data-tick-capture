"""A full-depth (L3) order book in pure Python.

Deliberately a second, independent implementation of the same semantics as
include/order_book_l3.hpp. Its job is to disagree if the C++ book is wrong --
so it is written from the message semantics rather than transliterated from
the C++, and it uses different data structures to get there.

Where the C++ uses std::map plus std::list, this uses a dict of dicts.
Python's dict has preserved insertion order since 3.7, which is exactly the
FIFO time priority a price level needs, and it deletes by key in O(1) without
the C++ version's separate iterator handle. Price ordering, which the C++ gets
from std::map for free, is recovered here by sorting keys only when top of
book is actually asked for -- appropriate because this implementation is for
cross-checking a final state, not for keeping up with a live feed.
"""

from __future__ import annotations

from typing import Dict, Optional, Tuple

BUY = ord("B")
SELL = ord("S")

ADD, EXECUTE, CANCEL, DELETE, REPLACE = (ord(c) for c in "AEXDU")


class InstrumentBook:
    """One instrument: price -> {order_id: [seq_added, qty]} per side."""

    def __init__(self):
        self.bids: Dict[int, Dict[int, list]] = {}
        self.asks: Dict[int, Dict[int, list]] = {}

    def side(self, s: int) -> Dict[int, Dict[int, list]]:
        return self.bids if s == BUY else self.asks

    def add(self, s: int, price: int, order_id: int, seq: int, qty: int) -> None:
        self.side(s).setdefault(price, {})[order_id] = [seq, qty]

    def reduce(self, s: int, price: int, order_id: int, qty: int) -> int:
        levels = self.side(s)
        level = levels.get(price)
        if level is None or order_id not in level:
            return 0
        entry = level[order_id]
        removed = min(qty, entry[1])
        entry[1] -= removed
        if entry[1] == 0:
            del level[order_id]
            if not level:
                del levels[price]
        return removed

    def remove(self, s: int, price: int, order_id: int) -> int:
        levels = self.side(s)
        level = levels.get(price)
        if level is None or order_id not in level:
            return 0
        gone = level.pop(order_id)[1]
        if not level:
            del levels[price]
        return gone

    def resting(self) -> int:
        return sum(len(l) for l in self.bids.values()) + sum(
            len(l) for l in self.asks.values()
        )

    def top(self) -> Tuple[Optional[int], int, int, Optional[int], int, int]:
        """(bid_px, bid_qty, bid_orders, ask_px, ask_qty, ask_orders)."""
        bid_px = max(self.bids) if self.bids else None
        ask_px = min(self.asks) if self.asks else None
        bq = sum(e[1] for e in self.bids[bid_px].values()) if bid_px is not None else 0
        bn = len(self.bids[bid_px]) if bid_px is not None else 0
        aq = sum(e[1] for e in self.asks[ask_px].values()) if ask_px is not None else 0
        an = len(self.asks[ask_px]) if ask_px is not None else 0
        return bid_px, bq, bn, ask_px, aq, an

    def qty_ahead(self, s: int, price: int, order_id: int) -> Optional[int]:
        """Resting quantity ahead of an order in its own queue.

        The L3-only query. Relies on dict insertion order being FIFO.
        """
        level = self.side(s).get(price)
        if level is None or order_id not in level:
            return None
        ahead = 0
        for oid, entry in level.items():
            if oid == order_id:
                return ahead
            ahead += entry[1]
        return ahead


class BookUniverse:
    def __init__(self, num_instruments: int = 128):
        self.books = [InstrumentBook() for _ in range(num_instruments)]
        self.index: Dict[int, Tuple[int, int, int]] = {}  # order_id -> (instr, side, price)
        self.unknown_order_refs = 0
        self.duplicate_adds = 0
        self.applied = 0

    def apply(
        self,
        msg_type: int,
        instrument_id: int,
        order_id: int,
        new_order_id: int,
        side: int,
        price: int,
        qty: int,
        seq: int,
    ) -> None:
        self.applied += 1

        if msg_type == ADD:
            if order_id in self.index:
                self.duplicate_adds += 1
                return
            if instrument_id >= len(self.books):
                return
            self.books[instrument_id].add(side, price, order_id, seq, qty)
            self.index[order_id] = (instrument_id, side, price)
            return

        if msg_type in (EXECUTE, CANCEL):
            loc = self.index.get(order_id)
            if loc is None:
                self.unknown_order_refs += 1
                return
            instr, s, px = loc
            book = self.books[instr]
            level = book.side(s).get(px)
            before = level[order_id][1] if level and order_id in level else 0
            gone = book.reduce(s, px, order_id, qty)
            if gone >= before:
                self.index.pop(order_id, None)
            return

        if msg_type == DELETE:
            loc = self.index.get(order_id)
            if loc is None:
                self.unknown_order_refs += 1
                return
            instr, s, px = loc
            self.books[instr].remove(s, px, order_id)
            del self.index[order_id]
            return

        if msg_type == REPLACE:
            loc = self.index.get(order_id)
            if loc is None:
                self.unknown_order_refs += 1
                return
            instr, s, px = loc
            self.books[instr].remove(s, px, order_id)
            del self.index[order_id]
            # A replace loses time priority: it joins the back of the queue
            # at its new price, keyed by the replacing message's sequence.
            self.books[instr].add(s, price, new_order_id, seq, qty)
            self.index[new_order_id] = (instr, s, price)
            return

        raise ValueError(f"unknown message type {msg_type!r}")

    def total_resting(self) -> int:
        return sum(b.resting() for b in self.books)
