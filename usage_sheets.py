#!/usr/bin/env python3
"""
Builds a Google Sheet from usage_aggregates_.json with:
- card (card list)
- lists (cut labels)
- per_card_total, per_card_by_cut, per_card_qty_by_cut, per_card_total_main, per_card_total_side,
  per_card_total_extra, per_card_total_main_side, per_card_by_cut_main, per_card_by_cut_side,
  per_card_by_cut_extra, per_card_by_cut_main_side, per_card_qty_by_cut_main, per_card_qty_by_cut_side,
  per_card_qty_by_cut_extra, per_card_qty_by_cut_main_side
- Goat (overview): Card + Label dropdowns + live values for each stat.
"""

import argparse, json, time, math, functools, random
from pathlib import Path
from typing import Dict, Any, List, Set

import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials

CUT_LABELS_DEFAULT = ["Winner","Finalist","Top 4","Top 8","Top 16","Top 24","Top 32","Top 48","Top 64"]

# -------------------------- Throttle + Retry --------------------------
SLEEP_BETWEEN_WRITES = 0.8   # seconds between write calls to keep under rpm
MAX_RETRIES = 7

def is_429(e: APIError) -> bool:
    try:
        return "429" in str(e)
    except Exception:
        return False

def backoff_sleep(attempt: int):
    # Exponential + jitter
    base = 1.2
    t = (base ** attempt) + random.uniform(0, 0.5)
    time.sleep(t)

def throttle():
    time.sleep(SLEEP_BETWEEN_WRITES)

def retry_429(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        for attempt in range(1, MAX_RETRIES+1):
            try:
                throttle()
                return fn(*args, **kwargs)
            except APIError as e:
                if is_429(e) and attempt < MAX_RETRIES:
                    backoff_sleep(attempt)
                    continue
                raise
    return wrapper

# -------------------------- gspread helpers --------------------------
def gclient(creds_path: Path) -> gspread.Client:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(str(creds_path), scopes=scopes)
    return gspread.authorize(creds)

def get_or_create_spreadsheet(gc: gspread.Client, title: str, spreadsheet_id: str|None):
    if spreadsheet_id:
        return gc.open_by_key(spreadsheet_id)
    return gc.create(title)

@retry_429
def ws_update(ws: gspread.Worksheet, *, range_name: str, values: List[List[Any]], value_input_option="RAW"):
    return ws.update(values=values, range_name=range_name, value_input_option=value_input_option)

@retry_429
def ss_batch_update(ss: gspread.Spreadsheet, body: Dict[str, Any]):
    return ss.batch_update(body)

def write_matrix(ws: gspread.Worksheet, start_row: int, start_col: int, matrix: List[List[Any]], value_input_option="RAW"):
    if not matrix: return
    end_row = start_row + len(matrix) - 1
    end_col = start_col + len(matrix[0]) - 1
    a1 = gspread.utils.rowcol_to_a1(start_row, start_col)
    a1_end = gspread.utils.rowcol_to_a1(end_row, end_col)
    ws_update(ws, range_name=f"{a1}:{a1_end}", values=matrix, value_input_option=value_input_option)

def set_cells(ws: gspread.Worksheet, a1: str, values_2d: List[List[Any]], value_input_option="USER_ENTERED"):
    ws_update(ws, range_name=a1, values=values_2d, value_input_option=value_input_option)

@retry_429
def ws_clear(ws: gspread.Worksheet):
    ws.clear()

@retry_429
def ss_add_worksheet(ss: gspread.Spreadsheet, title: str, rows: int, cols: int):
    return ss.add_worksheet(title=title, rows=str(rows), cols=str(cols))

def upsert_worksheet(ss: gspread.Spreadsheet, title: str, rows=1000, cols=26) -> gspread.Worksheet:
    try:
        ws = ss.worksheet(title)
        ws_clear(ws)
    except gspread.WorksheetNotFound:
        ws = ss_add_worksheet(ss, title, rows, cols)
    return ws

@retry_429
def freeze(ws: gspread.Worksheet, rows=1, cols=0):
    ws.freeze(rows=rows, cols=cols)

def add_validation_dropdown_range(ss: gspread.Spreadsheet, ws: gspread.Worksheet, a1: str, list_a1: str):
    # Use a single batch request
    rng_start, rng_end = a1.split(":")
    sr, sc = gspread.utils.a1_to_rowcol(rng_start)
    er, ec = gspread.utils.a1_to_rowcol(rng_end)
    body = {
        "requests": [{
            "setDataValidation": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": sr-1,
                    "startColumnIndex": sc-1,
                    "endRowIndex": er,
                    "endColumnIndex": ec
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_RANGE",
                        "values": [{"userEnteredValue": f"='{list_a1.split('!')[0]}'!{list_a1.split('!')[1]}"}]
                    },
                    "showCustomUi": True,
                    "strict": True
                }
            }
        }]}
    ss_batch_update(ss, body)

def add_validation_dropdown_list(ss: gspread.Spreadsheet, ws: gspread.Worksheet, a1: str, items: List[str]):
    rng_start, rng_end = a1.split(":")
    sr, sc = gspread.utils.a1_to_rowcol(rng_start)
    er, ec = gspread.utils.a1_to_rowcol(rng_end)
    body = {
        "requests": [{
            "setDataValidation": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": sr-1,
                    "startColumnIndex": sc-1,
                    "endRowIndex": er,
                    "endColumnIndex": ec
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [{"userEnteredValue": it} for it in items]
                    },
                    "showCustomUi": True,
                    "strict": True
                }
            }
        }]}
    ss_batch_update(ss, body)

@retry_429
def move_sheet_first(ss: gspread.Spreadsheet, ws: gspread.Worksheet):
    body = {"requests": [{
        "updateSheetProperties": {
            "properties": {"sheetId": ws.id, "index": 0},
            "fields": "index"
        }
    }]}
    ss.batch_update(body)

# -------------------------- Builders --------------------------
def union_cut_labels(*dicts: Dict[str, Dict[str,int]]) -> List[str]:
    found: Set[str] = set()
    for m in dicts:
        if not isinstance(m, dict): continue
        for _, tiers in m.items():
            if isinstance(tiers, dict):
                found.update(tiers.keys())
    ordered = [t for t in CUT_LABELS_DEFAULT if t in found]
    for t in sorted(found):
        if t not in ordered:
            ordered.append(t)
    return ordered or CUT_LABELS_DEFAULT

def build_card_sheet(ss: gspread.Spreadsheet, cards: List[str]):
    ws = upsert_worksheet(ss, "card", rows=max(1000, len(cards)+10), cols=2)
    matrix = [["Card"]] + [[c] for c in cards]
    write_matrix(ws, 1, 1, matrix)
    freeze(ws, rows=1)
    return ws

def build_lists_sheet(ss: gspread.Spreadsheet, cut_labels: List[str]):
    ws = upsert_worksheet(ss, "lists", rows=max(100, len(cut_labels)+10), cols=2)
    matrix = [["Cut Labels"]] + [[t] for t in cut_labels]
    write_matrix(ws, 1, 1, matrix)
    freeze(ws, rows=1)
    return ws

def build_total_sheet(ss: gspread.Spreadsheet, title: str, mapping: Dict[str,int], cards: List[str]):
    ws = upsert_worksheet(ss, title, rows=max(2000, len(mapping)+20), cols=4)
    # Header + controls in one write
    hdr = [
        ["Card", "Value", "", ""],
        ["", "=IFERROR(VLOOKUP(A2, A4:B, 2, FALSE), 0)", "", ""],
        ["", "", "", ""],
    ]
    write_matrix(ws, 1, 1, hdr, value_input_option="USER_ENTERED")
    add_validation_dropdown_range(ss, ws, "A2:A2", "card!A2:A")

    # Table (sorted by card name to align with dropdown VLOOKUP)
    rows = [["Card","Value"]]
    for c in cards:
        rows.append([c, int(mapping.get(c, 0))])
    write_matrix(ws, 4, 1, rows)
    freeze(ws, rows=4)
    return ws

def col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

def build_by_cut_sheet(ss: gspread.Spreadsheet, title: str, mapping: Dict[str, Dict[str,int]], cut_labels: List[str], cards: List[str]):
    ncols = 1 + len(cut_labels)
    last_col = col_letter(ncols)
    ws = upsert_worksheet(ss, title, rows=max(2000, len(mapping)+20), cols=max(5, ncols+2))

    # Header + controls area (one write)
    # Table starts row 4; header row is row 4; values begin row 5
    formula = f"=IFERROR(INDEX(A5:{last_col}100000, MATCH(A2, A5:A100000, 0), MATCH(B2, A4:{last_col}4, 0)), 0)"
    top = [
        ["Card", "Label", "Value"],
        ["", cut_labels[0] if cut_labels else "", formula],
        [""]
    ]
    write_matrix(ws, 1, 1, top, value_input_option="USER_ENTERED")
    add_validation_dropdown_range(ss, ws, "A2:A2", "card!A2:A")
    if cut_labels:
        add_validation_dropdown_list(ss, ws, "B2:B2", cut_labels)

    # Table: header + rows
    table = [["Card"] + cut_labels]
    for c in cards:
        tier_map = mapping.get(c, {}) or {}
        row = [c] + [int(tier_map.get(lbl, 0)) for lbl in cut_labels]
        table.append(row)
    write_matrix(ws, 4, 1, table)
    freeze(ws, rows=4)
    return ws

def col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

def build_goat_sheet(ss: gspread.Spreadsheet, cards: list[str], cut_labels: list[str]):
    """
    Build a sortable Goat sheet:
      A: Card
      B: Label (dropdown)
      C: per_card_total
      D: per_card_total_main
      E: per_card_total_side
      F: per_card_total_extra
      G: per_card_total_main_side
      H: per_card_by_cut
      I: per_card_qty_by_cut
      J: per_card_by_cut_main
      K: per_card_by_cut_side
      L: per_card_by_cut_extra
      M: per_card_by_cut_main_side
      N: per_card_qty_by_cut_main
      O: per_card_qty_by_cut_side
      P: per_card_qty_by_cut_extra
      Q: per_card_qty_by_cut_main_side
    """
    ws = upsert_worksheet(ss, "Goat", rows=max(2000, len(cards)+10), cols=17)

    # Header row + label control row
    header = [
        ["Card", "Label",
         "per_card_total", "per_card_total_main", "per_card_total_side", "per_card_total_extra", "per_card_total_main_side",
         "per_card_by_cut", "per_card_qty_by_cut",
         "per_card_by_cut_main", "per_card_by_cut_side", "per_card_by_cut_extra", "per_card_by_cut_main_side",
         "per_card_qty_by_cut_main", "per_card_qty_by_cut_side", "per_card_qty_by_cut_extra", "per_card_qty_by_cut_main_side"]
    ]
    control = [[ "", cut_labels[0] if cut_labels else "" ]]
    write_matrix(ws, 1, 1, header)
    write_matrix(ws, 2, 1, control)
    add_validation_dropdown_list(ss, ws, "B2:B2", cut_labels)

    # Write all card names (col A)
    card_rows = [[c] for c in cards]
    write_matrix(ws, 3, 1, card_rows)

    # Build formula rows for each card (row i corresponds to cards[i-3])
    def vlookup_total(tab: str, row: int) -> str:
        # A{row} is card; table on that tab is A4:B
        return f"=IFERROR(VLOOKUP($A{row},'{tab}'!A4:B,2,FALSE),0)"

    def bycut(tab: str, row: int) -> str:
        # index across the table on that tab; label in $B$2
        # Use ZZ col as a wide guard; header on row 4; data starts row 5
        return f"=IFERROR(INDEX('{tab}'!A5:ZZ100000, MATCH($A{row},'{tab}'!A5:A100000,0), MATCH($B$2,'{tab}'!A4:ZZ4,0)),0)"

    rows = []
    start_row = 3
    for i, c in enumerate(cards, start=start_row):
        row = [
            "",  # column B only has the label control in row 2
            "",  # (we keep B empty for data rows)
            vlookup_total("per_card_total", i),
            vlookup_total("per_card_total_main", i),
            vlookup_total("per_card_total_side", i),
            vlookup_total("per_card_total_extra", i),
            vlookup_total("per_card_total_main_side", i),

            bycut("per_card_by_cut", i),
            bycut("per_card_qty_by_cut", i),

            bycut("per_card_by_cut_main", i),
            bycut("per_card_by_cut_side", i),
            bycut("per_card_by_cut_extra", i),
            bycut("per_card_by_cut_main_side", i),

            bycut("per_card_qty_by_cut_main", i),
            bycut("per_card_qty_by_cut_side", i),
            bycut("per_card_qty_by_cut_extra", i),
            bycut("per_card_qty_by_cut_main_side", i),
        ]
        rows.append(row)

    # Write formulas for C:Q in one batch (columns 3..17)
    if rows:
        write_matrix(ws, start_row, 3, rows, value_input_option="USER_ENTERED")

    freeze(ws, rows=2)
    move_sheet_first(ss, ws)
    return ws

# -------------------------- Main --------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", default="usage_aggregates_.json")
    ap.add_argument("--creds", default="sa.json")
    ap.add_argument("--title", default="GOAT Usage Stats (auto)")
    ap.add_argument("--spreadsheet-id", default=None,
                    help="Existing sheet ID (recommended when Drive quota prevents creation)")
    args = ap.parse_args()

    data = json.loads(Path(args.json).read_text(encoding="utf-8"))

    # cards universe from per_card_total
    per_card_total = data.get("per_card_total", {})
    cards = sorted(per_card_total.keys())

    # collect label universe
    dicts_for_labels = []
    for k in [
        "per_card_by_cut",
        "per_card_qty_by_cut",
        "per_card_by_cut_main",
        "per_card_by_cut_side",
        "per_card_by_cut_extra",
        "per_card_by_cut_main_side",
        "per_card_qty_by_cut_main",
        "per_card_qty_by_cut_side",
        "per_card_qty_by_cut_extra",
        "per_card_qty_by_cut_main_side",
    ]:
        if isinstance(data.get(k), dict):
            dicts_for_labels.append(data[k])
    cut_labels = union_cut_labels(*dicts_for_labels)

    gc = gclient(Path(args.creds))
    ss = get_or_create_spreadsheet(gc, args.title, args.spreadsheet_id)
    print("Using spreadsheet:", ss.url)

    # sources for dropdowns
    build_card_sheet(ss, cards)
    build_lists_sheet(ss, cut_labels)

    # totals (single-valued)
    for key in [
        "per_card_total",
        "per_card_total_main",
        "per_card_total_side",
        "per_card_total_extra",
        "per_card_total_main_side",
    ]:
        m = data.get(key, {})
        if isinstance(m, dict):
            build_total_sheet(ss, key, m, cards)

    # by-cut maps
    for key in [
        "per_card_by_cut",
        "per_card_qty_by_cut",
        "per_card_by_cut_main",
        "per_card_by_cut_side",
        "per_card_by_cut_extra",
        "per_card_by_cut_main_side",
        "per_card_qty_by_cut_main",
        "per_card_qty_by_cut_side",
        "per_card_qty_by_cut_extra",
        "per_card_qty_by_cut_main_side",
    ]:
        m = data.get(key)
        if isinstance(m, dict):
            # normalize zeroes for missing labels per card
            norm = {}
            for c in cards:
                tiers = dict(m.get(c, {})) if isinstance(m.get(c), dict) else {}
                for lbl in cut_labels:
                    tiers.setdefault(lbl, 0)
                norm[c] = tiers
            build_by_cut_sheet(ss, key, norm, cut_labels, cards)

    # overview & make it first
    build_goat_sheet(ss, cards, cut_labels)

    print("Done. Open:", ss.url)

if __name__ == "__main__":
    main()
