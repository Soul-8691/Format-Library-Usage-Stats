import json
import argparse
import gspread
from gspread.exceptions import WorksheetNotFound


def ensure_worksheet(ss, title, rows=1, cols=1):
    """Get worksheet by title, clearing it if it exists; otherwise create it."""
    try:
        ws = ss.worksheet(title)
        ws.clear()
    except WorksheetNotFound:
        ws = ss.add_worksheet(title=title, rows=rows, cols=cols)
    return ws


def build_per_card_matrix(ss, data, key, sheet_name):
    """
    Build a sheet for card->archetype dicts:
    data[key] is {card: {archetype: value}}
    Sheet layout:
      Row 1: "Card", then archetype names as headers
      Rows 2+: card name, then value per archetype (0 if missing)
    """
    mapping = data.get(key, {})
    if not mapping:
        print(f"No data for {key}, skipping {sheet_name}")
        return

    cards = sorted(mapping.keys())
    archetypes = sorted({a for inner in mapping.values() for a in inner.keys()})

    header = ["Card"] + archetypes
    matrix = [header]

    for card in cards:
        row_dict = mapping.get(card, {})
        row = [card] + [row_dict.get(a, 0) for a in archetypes]
        matrix.append(row)

    rows = len(matrix)
    cols = len(header)
    ws = ensure_worksheet(ss, sheet_name, rows=rows, cols=cols)
    ws.update(range_name="A1", values=matrix, value_input_option="RAW")
    print(f"Wrote {rows - 1} rows to {sheet_name}")


def build_per_archetype_matrix(ss, data, key, sheet_name):
    """
    Build a sheet for archetype->card dicts:
    data[key] is {archetype: {card: value}}
    Sheet layout:
      Row 1: "Archetype", then card names as headers
      Rows 2+: archetype name, then value per card (0 if missing)
    """
    mapping = data.get(key, {})
    if not mapping:
        print(f"No data for {key}, skipping {sheet_name}")
        return

    archetypes = sorted(mapping.keys())
    cards = sorted({c for inner in mapping.values() for c in inner.keys()})

    header = ["Archetype"] + cards
    matrix = [header]

    for arch in archetypes:
        row_dict = mapping.get(arch, {})
        row = [arch] + [row_dict.get(c, 0) for c in cards]
        matrix.append(row)

    rows = len(matrix)
    cols = len(header)
    ws = ensure_worksheet(ss, sheet_name, rows=rows, cols=cols)
    ws.update(range_name="A1", values=matrix, value_input_option="RAW")
    print(f"Wrote {rows - 1} rows to {sheet_name}")


def main():
    parser = argparse.ArgumentParser(description="Build archetype usage sheets from usage_aggregates.json")
    parser.add_argument(
        "--json",
        default="usage_aggregates.json",
        help="Path to usage_aggregates.json (default: usage_aggregates.json)",
    )
    parser.add_argument(
        "--spreadsheet",
        required=True,
        help="Title of the existing Google Sheet to update (e.g. 'Goat Usage Stats')",
    )
    parser.add_argument(
        "--service-account",
        default="credentials.json",
        help="Service account credentials JSON for gspread (default: service_account.json)",
    )
    args = parser.parse_args()

    # Load data
    with open(args.json, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Auth + spreadsheet
    gc = gspread.service_account(filename=args.service_account)
    ss = gc.open(args.spreadsheet)

    # 1) card -> archetype
    build_per_card_matrix(
        ss,
        data,
        key="per_card_by_archetype",
        sheet_name="per_card_by_archetype",
    )
    build_per_card_matrix(
        ss,
        data,
        key="per_card_qty_by_archetype",
        sheet_name="per_card_qty_by_archetype",
    )

    # 2) archetype -> card
    build_per_archetype_matrix(
        ss,
        data,
        key="per_archetype_card_presence",
        sheet_name="per_archetype_card_presence",
    )
    build_per_archetype_matrix(
        ss,
        data,
        key="per_archetype_card_qty",
        sheet_name="per_archetype_card_qty",
    )

    print("Done.")


if __name__ == "__main__":
    main()
