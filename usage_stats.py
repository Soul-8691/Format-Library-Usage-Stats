#!/usr/bin/env python3
import json, sys, time, argparse
from collections import defaultdict, Counter
from urllib.parse import urljoin
from typing import Dict, Any, List

BASE = "https://formatlibrary.com"
GALLERY_ENDPOINT = "/api/events/gallery/goat"
EVENT_ENDPOINT = "/api/events/{slug}"
DECK_ENDPOINT = "/api/decks/{deck_id}"

CUT_TIERS = [
    ("Winner", 1),
    ("Finalist", 2),
    ("Top 4", 4),
    ("Top 8", 8),
    ("Top 16", 16),
    ("Top 24", 24),
    ("Top 32", 32),
    ("Top 48", 48),
    ("Top 64", 64),
]

def tiers_applicable(placement: int, cut_size: int) -> List[str]:
    applicable = []
    for label, threshold in CUT_TIERS:
        if threshold <= cut_size and placement <= threshold:
            applicable.append(label)
    return applicable

def fetch_json(path: str, params: Dict[str, Any]=None, max_retries: int=3, sleep: float=0.8) -> Any:
    import requests
    url = urljoin(BASE, path)
    for attempt in range(1, max_retries+1):
        r = requests.get(url, params=params, timeout=20)
        if r.status_code == 200:
            try:
                return r.json()
            except json.JSONDecodeError:
                return r.text
        time.sleep(sleep * attempt)
    r.raise_for_status()

def normalize_card_name(name: str) -> str:
    return name.strip()

def parse_deck_payload(payload) -> Dict[str, int]:
    counts: Dict[str, int] = defaultdict(int)

    def add_from_section(items):
        for obj in items or []:
            nm = obj.get("name") or obj.get("cleanName") or obj.get("cardName")
            if nm:
                counts[normalize_card_name(nm)] += 1

    if isinstance(payload, dict):
        if any(k in payload for k in ("main", "extra", "side")):
            add_from_section(payload.get("main", []))
            add_from_section(payload.get("extra", []))
            add_from_section(payload.get("side", []))
            return counts

        ydk = payload.get("ydk")
        if isinstance(ydk, str):
            lines = [ln.strip() for ln in ydk.splitlines()]
            for ln in lines:
                if not ln or ln.startswith("#") or ln.startswith("!"):
                    continue
                if ln.isdigit():
                    counts[f"CARD_ID:{ln}"] += 1
                else:
                    counts[normalize_card_name(ln)] += 1
            return counts

    if isinstance(payload, str):
        try:
            maybe = json.loads(payload)
            return parse_deck_payload(maybe)
        except Exception:
            lines = [ln.strip() for ln in payload.splitlines()]
            for ln in lines:
                if not ln or ln.startswith("#") or ln.startswith("!"):
                    continue
                if ln.isdigit():
                    counts[f"CARD_ID:{ln}"] += 1
                else:
                    counts[normalize_card_name(ln)] += 1
            return counts

    return counts

def parse_deck_sections(payload):
    """Return three dicts of counts by section: (main, side, extra)."""
    main_counts = defaultdict(int)
    side_counts = defaultdict(int)
    extra_counts = defaultdict(int)

    if isinstance(payload, dict):
        for obj in payload.get("main", []) or []:
            nm = obj.get("name") or obj.get("cleanName") or obj.get("cardName")
            if nm:
                main_counts[normalize_card_name(nm)] += 1
        for obj in payload.get("side", []) or []:
            nm = obj.get("name") or obj.get("cleanName") or obj.get("cardName")
            if nm:
                side_counts[normalize_card_name(nm)] += 1
        for obj in payload.get("extra", []) or []:
            nm = obj.get("name") or obj.get("cleanName") or obj.get("cardName")
            if nm:
                extra_counts[normalize_card_name(nm)] += 1
    return dict(main_counts), dict(side_counts), dict(extra_counts)

def infer_cut_size_from_top(top_len: int) -> int:
    for _, thr in CUT_TIERS:
        if top_len <= thr:
            return thr
    return top_len

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit-events", type=int, default=None)
    ap.add_argument("--sleep", type=float, default=0.4)
    ap.add_argument("--out", type=str, default="usage_aggregates.json")
    args = ap.parse_args()

    gallery = fetch_json(GALLERY_ENDPOINT)
    if isinstance(gallery, dict) and "events" in gallery:
        events = gallery.get("events") or []
    elif isinstance(gallery, list):
        events = gallery
    else:
        print(f"Unexpected gallery payload shape: {type(gallery).__name__}. Aborting.", file=sys.stderr)
        sys.exit(1)

    if args.limit_events:
        events = events[: args.limit_events]

    per_card_total = Counter()
    per_card_by_cut: Dict[str, Counter] = defaultdict(Counter)
    per_card_qty_by_cut: Dict[str, Counter] = defaultdict(Counter)

    # Section-specific totals
    per_card_total_main = Counter()
    per_card_total_side = Counter()
    per_card_total_extra = Counter()
    per_card_total_main_side = Counter()

    # Section-specific by-cut presence
    per_card_by_cut_main: Dict[str, Counter] = defaultdict(Counter)
    per_card_by_cut_side: Dict[str, Counter] = defaultdict(Counter)
    per_card_by_cut_extra: Dict[str, Counter] = defaultdict(Counter)
    per_card_by_cut_main_side: Dict[str, Counter] = defaultdict(Counter)

    # Section-specific by-cut quantity
    per_card_qty_by_cut_main: Dict[str, Counter] = defaultdict(Counter)
    per_card_qty_by_cut_side: Dict[str, Counter] = defaultdict(Counter)
    per_card_qty_by_cut_extra: Dict[str, Counter] = defaultdict(Counter)
    per_card_qty_by_cut_main_side: Dict[str, Counter] = defaultdict(Counter)

    per_archetype_total = Counter()
    per_archetype_by_cut: Dict[str, Counter] = defaultdict(Counter)
    per_deck_cut_total = Counter()

    per_card_by_archetype: Dict[str, Counter] = defaultdict(Counter)       # card -> archetype -> deck count (presence)
    per_card_qty_by_archetype: Dict[str, Counter] = defaultdict(Counter)   # card -> archetype -> total qty

    per_archetype_card_presence: Dict[str, Counter] = defaultdict(Counter) # archetype -> card -> deck count (presence)
    per_archetype_card_qty: Dict[str, Counter] = defaultdict(Counter)      # archetype -> card -> total qty

    processed = 0
    for evt in events:
        slug = (evt.get("slug") or evt.get("event", {}).get("slug") or evt.get("abbreviation"))
        if not slug:
            name = (evt.get("name") or "").strip()
            if name:
                import re as _re
                slug = _re.sub(r"[^A-Za-z0-9]+", "", name)
        if not slug:
            continue

        time.sleep(args.sleep)
        ev = fetch_json(EVENT_ENDPOINT.format(slug=slug))
        top = ev.get("topDecks") or []
        if not top:
            continue

        cut_size = infer_cut_size_from_top(len(top))

        for entry in top:
            placing = entry.get("placing") or entry.get("place") or entry.get("rank")
            try:
                placing = int(placing)
            except Exception:
                placing = 9999

            deck_id = entry.get("deckId") or entry.get("id") or entry.get("_id") or entry.get("deckSlug")
            archetype = entry.get("deckType") or entry.get("archetype") or entry.get("name") or "Unknown"

            if deck_id:
                time.sleep(args.sleep)
                payload = fetch_json(DECK_ENDPOINT.format(deck_id=deck_id))

                if isinstance(payload, dict):
                    if "placement" in payload:
                        try:
                            placing = int(payload["placement"])
                        except Exception:
                            pass
                    if payload.get("deckTypeName"):
                        archetype = payload["deckTypeName"]

                counts = parse_deck_payload(payload)
                main_counts, side_counts, extra_counts = parse_deck_sections(payload)

                # Combined totals
                for card, qty in counts.items():
                    per_card_total[card] += qty

                # Section totals
                for card, qty in main_counts.items():
                    per_card_total_main[card] += qty
                for card, qty in side_counts.items():
                    per_card_total_side[card] += qty
                for card, qty in extra_counts.items():
                    per_card_total_extra[card] += qty
                # main + side total
                all_ms = set(main_counts.keys()) | set(side_counts.keys())
                for card in all_ms:
                    per_card_total_main_side[card] += main_counts.get(card, 0) + side_counts.get(card, 0)

                # Combined presence/quantity by cut
                for card in set(counts.keys()):
                    for tier in tiers_applicable(placing, cut_size):
                        per_card_by_cut[card][tier] += 1
                for card, qty in counts.items():
                    for tier in tiers_applicable(placing, cut_size):
                        per_card_qty_by_cut[card][tier] += qty

                # Section presence by cut
                for card in main_counts.keys():
                    for tier in tiers_applicable(placing, cut_size):
                        per_card_by_cut_main[card][tier] += 1
                for card in side_counts.keys():
                    for tier in tiers_applicable(placing, cut_size):
                        per_card_by_cut_side[card][tier] += 1
                for card in extra_counts.keys():
                    for tier in tiers_applicable(placing, cut_size):
                        per_card_by_cut_extra[card][tier] += 1
                # main + side presence
                for card in all_ms:
                    for tier in tiers_applicable(placing, cut_size):
                        per_card_by_cut_main_side[card][tier] += 1

                # Section quantities by cut
                for card, qty in main_counts.items():
                    for tier in tiers_applicable(placing, cut_size):
                        per_card_qty_by_cut_main[card][tier] += qty
                for card, qty in side_counts.items():
                    for tier in tiers_applicable(placing, cut_size):
                        per_card_qty_by_cut_side[card][tier] += qty
                for card, qty in extra_counts.items():
                    for tier in tiers_applicable(placing, cut_size):
                        per_card_qty_by_cut_extra[card][tier] += qty
                # main + side quantities
                for card in all_ms:
                    ms_qty = main_counts.get(card, 0) + side_counts.get(card, 0)
                    for tier in tiers_applicable(placing, cut_size):
                        per_card_qty_by_cut_main_side[card][tier] += ms_qty

                # Archetype & deck-cut tallies
                per_archetype_total[archetype] += 1
                for tier in tiers_applicable(placing, cut_size):
                    per_deck_cut_total[tier] += 1
                    per_archetype_by_cut[archetype][tier] += 1

                # ------- NEW: per card x archetype tallies -------
                # presence = card appears at least once in this deck
                for card in set(counts.keys()):
                    per_card_by_archetype[card][archetype] += 1
                    per_archetype_card_presence[archetype][card] += 1

                # quantity = sum of copies in this deck
                for card, qty in counts.items():
                    per_card_qty_by_archetype[card][archetype] += qty
                    per_archetype_card_qty[archetype][card] += qty
                # -----------------------------------------------

        processed += 1
        print(f"Processed {processed} events...", file=sys.stderr)

    result = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source": {
            "gallery_endpoint": urljoin(BASE, GALLERY_ENDPOINT),
            "event_endpoint_pattern": urljoin(BASE, "/api/events/{slug}"),
            "deck_download_pattern": urljoin(BASE, "/api/decks/{deck_id}"),
        },
        "notes": [
            "per_card_total: total quantity of each card across all parsed decks (summing copies, all sections)",
            "per_card_by_cut: number of decks that included the card at each cut tier (presence-based, all sections)",
            "per_card_qty_by_cut: total copies of the card at each cut tier (all sections)",
            "per_card_total_main/side/extra: totals by section",
            "per_card_total_main_side: totals for main+side (excluding extra)",
            "per_card_by_cut_* and per_card_qty_by_cut_*: section-specific presence/quantity by cut, including main_side",
            "per_archetype_total: number of decks per archetype (each deck counted once)",
            "per_archetype_by_cut: number of decks of that archetype that made each cut tier",
            "per_deck_cut_total: total decks that reached each cut tier (all archetypes)",
            "per_card_by_archetype: for each card, how many decks of each archetype included it (presence).",
            "per_card_qty_by_archetype: for each card, total copies across decks of each archetype.",
            "per_archetype_card_presence: for each archetype, how many decks used each card (presence).",
            "per_archetype_card_qty: for each archetype, total copies of each card.",
        ],
        "per_card_total": dict(per_card_total),
        "per_card_by_cut": {k: dict(v) for k, v in per_card_by_cut.items()},
        "per_card_qty_by_cut": {k: dict(v) for k, v in per_card_qty_by_cut.items()},

        "per_card_total_main": dict(per_card_total_main),
        "per_card_total_side": dict(per_card_total_side),
        "per_card_total_extra": dict(per_card_total_extra),
        "per_card_total_main_side": dict(per_card_total_main_side),

        "per_card_by_cut_main": {k: dict(v) for k, v in per_card_by_cut_main.items()},
        "per_card_by_cut_side": {k: dict(v) for k, v in per_card_by_cut_side.items()},
        "per_card_by_cut_extra": {k: dict(v) for k, v in per_card_by_cut_extra.items()},
        "per_card_by_cut_main_side": {k: dict(v) for k, v in per_card_by_cut_main_side.items()},

        "per_card_qty_by_cut_main": {k: dict(v) for k, v in per_card_qty_by_cut_main.items()},
        "per_card_qty_by_cut_side": {k: dict(v) for k, v in per_card_qty_by_cut_side.items()},
        "per_card_qty_by_cut_extra": {k: dict(v) for k, v in per_card_qty_by_cut_extra.items()},
        "per_card_qty_by_cut_main_side": {k: dict(v) for k, v in per_card_qty_by_cut_main_side.items()},

        "per_archetype_total": dict(per_archetype_total),
        "per_archetype_by_cut": {k: dict(v) for k, v in per_archetype_by_cut.items()},
        "per_deck_cut_total": dict(per_deck_cut_total),

        "per_card_by_archetype": {card: dict(cnt) for card, cnt in per_card_by_archetype.items()},
        "per_card_qty_by_archetype": {card: dict(cnt) for card, cnt in per_card_qty_by_archetype.items()},
        "per_archetype_card_presence": {arch: dict(cnt) for arch, cnt in per_archetype_card_presence.items()},
        "per_archetype_card_qty": {arch: dict(cnt) for arch, cnt in per_archetype_card_qty.items()},
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
