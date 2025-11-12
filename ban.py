#!/usr/bin/env python3
import argparse
import json
import time
from pathlib import Path
from typing import Dict, Any, List, Tuple
from urllib.parse import quote

import requests
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

API_BASE = "https://formatlibrary.com/api/banlists/{banlist}?category={category}"

def gclient(creds_path: Path) -> gspread.Client:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(str(creds_path), scopes=scopes)
    return gspread.authorize(creds)

def get_or_create_spreadsheet(gc: gspread.Client, title: str|None, spreadsheet_id: str|None) -> gspread.Spreadsheet:
    if spreadsheet_id:
        return gc.open_by_key(spreadsheet_id)
    if not title:
        title = "GOAT Usage Stats (auto)"
    return gc.create(title)

def upsert_worksheet(ss: gspread.Spreadsheet, title: str, rows=1000, cols=4) -> gspread.Worksheet:
    try:
        ws = ss.worksheet(title)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=title, rows=str(rows), cols=str(cols))
    return ws

def ws_update(ws: gspread.Worksheet, *, range_name: str, values: List[List[Any]], value_input_option="RAW"):
    for attempt in range(1, 6):
        try:
            return ws.update(values=values, range_name=range_name, value_input_option=value_input_option)
        except APIError as e:
            if attempt >= 5:
                raise
            time.sleep(0.8 * attempt)

def write_matrix(ws: gspread.Worksheet, start_row: int, start_col: int, matrix: List[List[Any]], value_input_option="RAW"):
    if not matrix: return
    end_row = start_row + len(matrix) - 1
    end_col = start_col + len(matrix[0]) - 1
    a1 = gspread.utils.rowcol_to_a1(start_row, start_col)
    a1_end = gspread.utils.rowcol_to_a1(end_row, end_col)
    ws_update(ws, range_name=f"{a1}:{a1_end}", values=matrix, value_input_option=value_input_option)

def freeze(ws: gspread.Worksheet, rows=1, cols=0):
    for attempt in range(1, 6):
        try:
            ws.freeze(rows=rows, cols=cols)
            return
        except APIError:
            if attempt >= 5:
                raise
            time.sleep(0.8 * attempt)

def fetch_banlist(banlist: str, category: str) -> Dict[str, Any]:
    url = API_BASE.format(banlist=quote(banlist, safe=""), category=quote(category, safe=""))
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

def extract_cards(payload: Dict[str, Any]) -> List[Tuple[str, int]]:
    out: Dict[str, int] = {}

    def add_from_bucket(bucket, ban_value: int):
        if not bucket:
            return
        if isinstance(bucket, dict):
            items = []
            if "items" in bucket and isinstance(bucket["items"], list):
                items = bucket["items"]
            else:
                items = list(bucket.values())
        elif isinstance(bucket, list):
            items = bucket
        else:
            items = []

        for obj in items:
            if not isinstance(obj, dict):
                continue
            name = obj.get("cardName") or obj.get("name")
            if not name:
                continue
            prev = out.get(name)
            if prev is None or ban_value < prev:
                out[name] = ban_value

    add_from_bucket(payload.get("limited"), 1)
    add_from_bucket(payload.get("semiLimited"), 2)

    return sorted(out.items(), key=lambda x: x[0].lower())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--creds", required=True, help="Path to service account JSON (sa.json)")
    ap.add_argument("--spreadsheet-id", default=None, help="Existing Google Sheet ID to write into")
    ap.add_argument("--title", default=None, help="If no spreadsheet-id, the title for a new Sheet")
    ap.add_argument("--sheet-name", default="ban", help="Worksheet name to write (default: ban)")
    ap.add_argument("--banlist", default="April 2005", help="Banlist name (default: 'April 2005')")
    ap.add_argument("--category", default="TCG", help="Category (default: 'TCG')")
    args = ap.parse_args()

    payload = fetch_banlist(args.banlist, args.category)
    rows = extract_cards(payload)

    gc = gclient(Path(args.creds))
    ss = get_or_create_spreadsheet(gc, args.title, args.spreadsheet_id)
    print("Using spreadsheet:", ss.url)

    ws = upsert_worksheet(ss, args.sheet_name, rows=max(100, len(rows)+10), cols=2)

    header = [["card", "ban"]]
    write_matrix(ws, 1, 1, header)

    data = [[name, ban] for name, ban in rows]
    write_matrix(ws, 2, 1, data)

    freeze(ws, rows=1)

    print(f"Wrote {len(rows)} rows to '{args.sheet_name}' in {ss.url}")

if __name__ == "__main__":
    main()
