import csv
import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, Optional, Set

BASE_DIR = Path(__file__).resolve().parent

CHARITY_FILE = BASE_DIR / "input/publicextract.charity.json"
ANNUAL_RETURN_FILE = BASE_DIR / "input/publicextract.charity_annual_return_parta.json"
CLASSIFICATION_FILE = BASE_DIR / "input/publicextract.charity_classification.json"
ANNUAL_RETURN_HISTORY_FILE = BASE_DIR / "input/publicextract.charity_annual_return_history.json"
AREA_OF_OPERATION_FILE = BASE_DIR / "input/publicextract.charity_area_of_operation.json"
ANNUAL_RETURN_PARTB_FILE = BASE_DIR / "input/publicextract.charity_annual_return_partb.json"

ANNUAL_RETURN_OUTPUT = BASE_DIR / "output/grant_making_main_activity_active.json"
CLASSIFICATION_OUTPUT = BASE_DIR / "output/grant_making_classification_302_active.json"
COMPARISON_OUTPUT = BASE_DIR / "output/grant_making_cross_file_comparison.json"
MERGED_OUTPUT = BASE_DIR / "output/grant_making_merged_detailed.csv"
CHARITY_DETAILS_URL_TEMPLATE = (
    "https://register-of-charities.charitycommission.gov.uk/en/charity-search/-/charity-details/"
    "{charity_number}?_uk_gov_ccew_onereg_charitydetails_web_portlet_CharityDetailsPortlet_organisationNumber="
    "{charity_number}"
)
RECENT_SUBMISSION_WINDOW_DAYS = 365
MIN_HISTORY_TOTAL_GROSS_INCOME = 250_000
GEOGRAPHICAL_AREAS_WANTED = {
    "Kent",
    "Medway",
    # "Thurrock",
    # "Bromley",
    # "Greenwich",
    "Throughout London",
    "Throughout England",
    "Throughout England And Wales",
}


@dataclass
class FilterResult:
    source_rows: int
    predicate_rows: int
    active_rows: int
    written_rows: int
    duplicate_rows_skipped: int
    missing_charity_id_rows: int
    charity_ids: Set[int]


def coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(text)
        except ValueError:
            return None
    return None


def coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def is_trueish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value == 1
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "t", "yes", "y"}
    return False


def is_null_like(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().upper() in {"", "NULL"}
    return False


def get_charity_id(row: Dict[str, Any]) -> Optional[int]:
    registered = coerce_int(row.get("registered_charity_number"))
    if registered is not None:
        return registered
    return coerce_int(row.get("organisation_number"))


def parse_iso_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return date.fromisoformat(text.split("T", 1)[0])
        except ValueError:
            return None
    return None


def iter_json_array(path: Path, chunk_size: int = 1_048_576) -> Iterator[Dict[str, Any]]:
    decoder = json.JSONDecoder()
    # Some extracts are UTF-8 with BOM, so use utf-8-sig.
    with path.open("r", encoding="utf-8-sig") as handle:
        buffer = ""
        in_array = False
        reached_eof = False

        while True:
            if not reached_eof and len(buffer) < chunk_size:
                chunk = handle.read(chunk_size)
                if chunk:
                    buffer += chunk
                else:
                    reached_eof = True

            idx = 0
            buffer_len = len(buffer)

            while True:
                while idx < buffer_len and buffer[idx].isspace():
                    idx += 1

                if not in_array:
                    if idx >= buffer_len:
                        break
                    if buffer[idx] != "[":
                        raise ValueError(f"{path} is not a JSON array.")
                    in_array = True
                    idx += 1
                    continue

                while idx < buffer_len and buffer[idx].isspace():
                    idx += 1

                if idx < buffer_len and buffer[idx] == ",":
                    idx += 1
                    continue

                while idx < buffer_len and buffer[idx].isspace():
                    idx += 1

                if idx < buffer_len and buffer[idx] == "]":
                    return

                if idx >= buffer_len:
                    break

                try:
                    obj, next_idx = decoder.raw_decode(buffer, idx)
                except json.JSONDecodeError:
                    break

                if not isinstance(obj, dict):
                    raise ValueError(f"Expected objects in {path}, got {type(obj).__name__}.")

                yield obj
                idx = next_idx

            buffer = buffer[idx:]

            if reached_eof:
                trailing = buffer.strip()
                if trailing and trailing != "]":
                    raise ValueError(f"Unexpected trailing content in {path}: {trailing[:80]!r}")
                return


def write_json_array(output_path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write("[\n")
        first = True
        for row in rows:
            if first:
                first = False
            else:
                handle.write(",\n")
            json.dump(row, handle, ensure_ascii=False, separators=(",", ":"))
            count += 1
        handle.write("\n]\n")
    return count


def write_json_object(output_path: Path, payload: Dict[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
        handle.write("\n")


def write_merged_csv(output_path: Path, charities: list[Dict[str, Any]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "registered_charity_number",
        "url",
        "segment",
        "annual_return_filtered_count",
        "classification_filtered_count",
        "charity_count",
        "area_of_operation_count",
        "classification_all_rows_count",
        "annual_return_history_count",
        "annual_return_parta_count",
        "annual_return_partb_count",
        "annual_return_filtered_rows_json",
        "classification_filtered_rows_json",
        "charity_rows_json",
        "area_of_operation_rows_json",
        "classification_all_rows_json",
        "annual_return_history_rows_json",
        "annual_return_parta_rows_json",
        "annual_return_partb_rows_json",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for item in charities:
            charity_number = item["registered_charity_number"]
            annual_filtered = item["matched_rows"]["annual_return_filtered"]
            classification_filtered = item["matched_rows"]["classification_filtered"]
            charity_rows = item["charity"]
            area_rows = item["area_of_operation"]
            classification_rows = item["classification_all_rows"]
            history_rows = item["annual_return_history"]
            parta_rows = item["annual_return_parta"]
            partb_rows = item["annual_return_partb"]
            writer.writerow(
                {
                    "registered_charity_number": charity_number,
                    "url": CHARITY_DETAILS_URL_TEMPLATE.format(
                        charity_number=charity_number
                    ),
                    "segment": item["segment"],
                    "annual_return_filtered_count": len(annual_filtered),
                    "classification_filtered_count": len(classification_filtered),
                    "charity_count": len(charity_rows),
                    "area_of_operation_count": len(area_rows),
                    "classification_all_rows_count": len(classification_rows),
                    "annual_return_history_count": len(history_rows),
                    "annual_return_parta_count": len(parta_rows),
                    "annual_return_partb_count": len(partb_rows),
                    "annual_return_filtered_rows_json": json.dumps(
                        annual_filtered, ensure_ascii=False, separators=(",", ":")
                    ),
                    "classification_filtered_rows_json": json.dumps(
                        classification_filtered, ensure_ascii=False, separators=(",", ":")
                    ),
                    "charity_rows_json": json.dumps(
                        charity_rows, ensure_ascii=False, separators=(",", ":")
                    ),
                    "area_of_operation_rows_json": json.dumps(
                        area_rows, ensure_ascii=False, separators=(",", ":")
                    ),
                    "classification_all_rows_json": json.dumps(
                        classification_rows, ensure_ascii=False, separators=(",", ":")
                    ),
                    "annual_return_history_rows_json": json.dumps(
                        history_rows, ensure_ascii=False, separators=(",", ":")
                    ),
                    "annual_return_parta_rows_json": json.dumps(
                        parta_rows, ensure_ascii=False, separators=(",", ":")
                    ),
                    "annual_return_partb_rows_json": json.dumps(
                        partb_rows, ensure_ascii=False, separators=(",", ":")
                    ),
                }
            )


def build_active_charity_set(charity_file: Path) -> Set[int]:
    active_charities: Set[int] = set()
    for row in iter_json_array(charity_file):
        charity_id = get_charity_id(row)
        if charity_id is None:
            continue
        if is_null_like(row.get("date_of_removal")):
            active_charities.add(charity_id)
    return active_charities


def build_recent_submission_charity_set(
    history_file: Path, window_days: int
) -> tuple[Set[int], Optional[date], int]:
    in_date_charities: Set[int] = set()
    extract_date: Optional[date] = None
    cutoff_date: Optional[date] = None
    source_rows = 0

    for row in iter_json_array(history_file):
        source_rows += 1
        if extract_date is None:
            extract_date = parse_iso_date(row.get("date_of_extract"))
            if extract_date is not None:
                cutoff_date = extract_date - timedelta(days=window_days)

        if cutoff_date is None:
            continue

        charity_id = get_charity_id(row)
        if charity_id is None:
            continue

        annual_return_date = parse_iso_date(row.get("date_annual_return_received"))
        accounts_date = parse_iso_date(row.get("date_accounts_received"))
        latest_submission_date = max(
            (d for d in (annual_return_date, accounts_date) if d is not None),
            default=None,
        )

        if latest_submission_date is not None and latest_submission_date >= cutoff_date:
            in_date_charities.add(charity_id)

    return in_date_charities, extract_date, source_rows


def build_history_income_charity_set(
    history_file: Path, minimum_income: float
) -> tuple[Set[int], int]:
    income_charities: Set[int] = set()
    source_rows = 0

    for row in iter_json_array(history_file):
        source_rows += 1
        charity_id = get_charity_id(row)
        if charity_id is None:
            continue

        total_gross_income = coerce_float(row.get("total_gross_income"))
        if total_gross_income is None:
            continue

        if total_gross_income >= minimum_income:
            income_charities.add(charity_id)

    return income_charities, source_rows


def build_area_filtered_charity_set(
    area_file: Path, wanted_areas: Set[str]
) -> tuple[Set[int], int]:
    area_charities: Set[int] = set()
    source_rows = 0
    wanted_lookup = {name.casefold() for name in wanted_areas}

    for row in iter_json_array(area_file):
        source_rows += 1
        description = row.get("geographic_area_description")
        if not isinstance(description, str):
            continue
        if description.strip().casefold() not in wanted_lookup:
            continue

        charity_id = get_charity_id(row)
        if charity_id is None:
            continue
        area_charities.add(charity_id)

    return area_charities, source_rows


def build_partb_filtered_charity_set(partb_file: Path) -> tuple[Set[int], int]:
    grants_positive_charities: Set[int] = set()
    source_rows = 0

    for row in iter_json_array(partb_file):
        source_rows += 1
        charity_id = get_charity_id(row)
        if charity_id is None:
            continue

        grants_institution = coerce_float(row.get("expenditure_grants_institution"))
        if grants_institution is None:
            continue

        if grants_institution > 0:
            grants_positive_charities.add(charity_id)

    return grants_positive_charities, source_rows


def collect_rows_for_charities(
    source_file: Path, charity_ids: Set[int]
) -> Dict[int, list[Dict[str, Any]]]:
    rows_by_charity: Dict[int, list[Dict[str, Any]]] = {}
    if not charity_ids:
        return rows_by_charity

    for row in iter_json_array(source_file):
        charity_id = get_charity_id(row)
        if charity_id is None or charity_id not in charity_ids:
            continue
        rows_by_charity.setdefault(charity_id, []).append(row)

    return rows_by_charity


def filter_source_to_output(
    source_file: Path,
    output_file: Path,
    active_charities: Set[int],
    predicate: Callable[[Dict[str, Any]], bool],
) -> FilterResult:
    source_rows = 0
    predicate_rows = 0
    active_rows = 0
    duplicate_rows_skipped = 0
    missing_charity_id_rows = 0
    written_charity_ids: Set[int] = set()

    def iter_filtered_rows() -> Iterator[Dict[str, Any]]:
        nonlocal source_rows
        nonlocal predicate_rows
        nonlocal active_rows
        nonlocal duplicate_rows_skipped
        nonlocal missing_charity_id_rows

        for row in iter_json_array(source_file):
            source_rows += 1

            if not predicate(row):
                continue
            predicate_rows += 1

            charity_id = get_charity_id(row)
            if charity_id is None:
                missing_charity_id_rows += 1
                continue

            if charity_id not in active_charities:
                continue
            active_rows += 1

            if charity_id in written_charity_ids:
                duplicate_rows_skipped += 1
                continue

            written_charity_ids.add(charity_id)
            yield row

    written_rows = write_json_array(output_file, iter_filtered_rows())

    return FilterResult(
        source_rows=source_rows,
        predicate_rows=predicate_rows,
        active_rows=active_rows,
        written_rows=written_rows,
        duplicate_rows_skipped=duplicate_rows_skipped,
        missing_charity_id_rows=missing_charity_id_rows,
        charity_ids=written_charity_ids,
    )


def verify_unique_charity_ids(output_file: Path) -> int:
    seen: Set[int] = set()
    duplicates = 0
    for row in iter_json_array(output_file):
        charity_id = get_charity_id(row)
        if charity_id is None:
            continue
        if charity_id in seen:
            duplicates += 1
        else:
            seen.add(charity_id)
    return duplicates


def main() -> None:
    active_charities = build_active_charity_set(CHARITY_FILE)
    in_date_charities, extract_date, history_rows = build_recent_submission_charity_set(
        ANNUAL_RETURN_HISTORY_FILE, RECENT_SUBMISSION_WINDOW_DAYS
    )
    history_income_charities, history_income_rows = build_history_income_charity_set(
        ANNUAL_RETURN_HISTORY_FILE, MIN_HISTORY_TOTAL_GROSS_INCOME
    )
    area_charities, area_rows = build_area_filtered_charity_set(
        AREA_OF_OPERATION_FILE, GEOGRAPHICAL_AREAS_WANTED
    )
    partb_grants_positive_charities, partb_rows = build_partb_filtered_charity_set(
        ANNUAL_RETURN_PARTB_FILE
    )

    eligible_after_recent = active_charities & in_date_charities
    eligible_after_history_income = eligible_after_recent & history_income_charities
    eligible_after_area = eligible_after_history_income & area_charities
    eligible_charities = eligible_after_area & partb_grants_positive_charities

    if extract_date is None:
        raise ValueError(
            "Could not determine date_of_extract from annual return history file."
        )
    cutoff_date = extract_date - timedelta(days=RECENT_SUBMISSION_WINDOW_DAYS)

    print(f"Active charities found (date_of_removal is NULL): {len(active_charities):,}")
    print(
        "Charities with at least one submission in the last "
        f"{RECENT_SUBMISSION_WINDOW_DAYS} days "
        f"(from {cutoff_date.isoformat()} to {extract_date.isoformat()}): "
        f"{len(in_date_charities):,}"
    )
    print(
        "After AND filter (active + recent submission): "
        f"{len(eligible_after_recent):,}"
    )
    print(
        f"Charities with total_gross_income >= {MIN_HISTORY_TOTAL_GROSS_INCOME:,.0f} "
        f"(history): {len(history_income_charities):,}"
    )
    print(
        "After AND filter (+ history total_gross_income >= 250,000): "
        f"{len(eligible_after_history_income):,}"
    )
    print(
        "Charities in selected geographic areas: "
        f"{len(area_charities):,}"
    )
    print(
        "After AND filter (+ selected area): "
        f"{len(eligible_after_area):,}"
    )
    print(
        "Charities with expenditure_grants_institution > 0 (partb): "
        f"{len(partb_grants_positive_charities):,}"
    )
    print(
        "After AND filter (+ partb grants condition): "
        f"{len(eligible_charities):,}"
    )
    print(f"Annual return history rows scanned: {history_rows:,}")
    print(f"Annual return history rows scanned (income check): {history_income_rows:,}")
    print(f"Area of operation rows scanned: {area_rows:,}")
    print(f"Annual return partb rows scanned: {partb_rows:,}")

    annual_result = filter_source_to_output(
        source_file=ANNUAL_RETURN_FILE,
        output_file=ANNUAL_RETURN_OUTPUT,
        active_charities=eligible_charities,
        predicate=lambda row: is_trueish(row.get("grant_making_is_main_activity")),
    )

    classification_result = filter_source_to_output(
        source_file=CLASSIFICATION_FILE,
        output_file=CLASSIFICATION_OUTPUT,
        active_charities=eligible_charities,
        predicate=lambda row: coerce_int(row.get("classification_code")) == 302,
    )

    annual_duplicates_in_output = verify_unique_charity_ids(ANNUAL_RETURN_OUTPUT)
    classification_duplicates_in_output = verify_unique_charity_ids(CLASSIFICATION_OUTPUT)

    overlap = annual_result.charity_ids & classification_result.charity_ids
    only_annual = annual_result.charity_ids - classification_result.charity_ids
    only_classification = classification_result.charity_ids - annual_result.charity_ids
    union_ids = annual_result.charity_ids | classification_result.charity_ids
    only_one_file_total = len(only_annual) + len(only_classification)
    either_file_total = len(overlap) + only_one_file_total

    def pct(numerator: int, denominator: int) -> float:
        if denominator == 0:
            return 0.0
        return (numerator / denominator) * 100

    print()
    print("Saved files:")
    print(f"- {ANNUAL_RETURN_OUTPUT.name}")
    print(f"- {CLASSIFICATION_OUTPUT.name}")
    print()
    print("Counts after filtering to all AND conditions:")
    print(f"- Annual return (grant_making_is_main_activity = True): {annual_result.written_rows:,}")
    print(
        "- Classification (classification_code = 302): "
        f"{classification_result.written_rows:,}"
    )
    print(
        "- Count difference (classification - annual): "
        f"{classification_result.written_rows - annual_result.written_rows:,}"
    )
    print()
    print("Cross-file comparison by charity id:")
    print(f"- In both files: {len(overlap):,}")
    print(f"- Only in annual return file: {len(only_annual):,}")
    print(f"- Only in classification file: {len(only_classification):,}")
    print()
    print("Percentages:")
    print(
        "- Annual-only as % of all charities found in either file: "
        f"{pct(len(only_annual), either_file_total):.2f}%"
    )
    print(
        "- Classification-only as % of all charities found in either file: "
        f"{pct(len(only_classification), either_file_total):.2f}%"
    )
    print(
        "- In both as % of all charities found in either file: "
        f"{pct(len(overlap), either_file_total):.2f}%"
    )
    print(
        "- Annual-only as % of charities that appear in only one file: "
        f"{pct(len(only_annual), only_one_file_total):.2f}%"
    )
    print(
        "- Classification-only as % of charities that appear in only one file: "
        f"{pct(len(only_classification), only_one_file_total):.2f}%"
    )
    print(
        "- Annual-only as % of annual file: "
        f"{pct(len(only_annual), annual_result.written_rows):.2f}%"
    )
    print(
        "- Classification-only as % of classification file: "
        f"{pct(len(only_classification), classification_result.written_rows):.2f}%"
    )
    print()
    print("Uniqueness checks (charity ids):")
    print(
        "- Annual return output unique: "
        f"{'YES' if annual_duplicates_in_output == 0 else f'NO ({annual_duplicates_in_output} duplicates)'}"
    )
    print(
        "- Classification output unique: "
        f"{'YES' if classification_duplicates_in_output == 0 else f'NO ({classification_duplicates_in_output} duplicates)'}"
    )

    comparison_payload = {
        "counts": {
            "in_both_files": len(overlap),
            "only_in_annual_return_file": len(only_annual),
            "only_in_classification_file": len(only_classification),
            "in_either_file_total": len(union_ids),
        },
        "charity_ids": {
            "in_both_files": sorted(overlap),
            "only_in_annual_return_file": sorted(only_annual),
            "only_in_classification_file": sorted(only_classification),
        },
    }
    write_json_object(COMPARISON_OUTPUT, comparison_payload)

    charity_rows = collect_rows_for_charities(CHARITY_FILE, union_ids)
    area_rows = collect_rows_for_charities(AREA_OF_OPERATION_FILE, union_ids)
    classification_rows = collect_rows_for_charities(CLASSIFICATION_FILE, union_ids)
    history_rows_by_charity = collect_rows_for_charities(ANNUAL_RETURN_HISTORY_FILE, union_ids)
    parta_rows = collect_rows_for_charities(ANNUAL_RETURN_FILE, union_ids)
    partb_rows = collect_rows_for_charities(ANNUAL_RETURN_PARTB_FILE, union_ids)
    annual_filtered_rows = collect_rows_for_charities(ANNUAL_RETURN_OUTPUT, union_ids)
    classification_filtered_rows = collect_rows_for_charities(CLASSIFICATION_OUTPUT, union_ids)

    merged_charities: list[Dict[str, Any]] = []
    for charity_id in sorted(union_ids):
        if charity_id in overlap:
            segment = "in_both_files"
        elif charity_id in only_annual:
            segment = "only_in_annual_return_file"
        else:
            segment = "only_in_classification_file"

        merged_charities.append(
            {
                "registered_charity_number": charity_id,
                "segment": segment,
                "matched_rows": {
                    "annual_return_filtered": annual_filtered_rows.get(charity_id, []),
                    "classification_filtered": classification_filtered_rows.get(charity_id, []),
                },
                "charity": charity_rows.get(charity_id, []),
                "area_of_operation": area_rows.get(charity_id, []),
                "classification_all_rows": classification_rows.get(charity_id, []),
                "annual_return_history": history_rows_by_charity.get(charity_id, []),
                "annual_return_parta": parta_rows.get(charity_id, []),
                "annual_return_partb": partb_rows.get(charity_id, []),
            }
        )

    write_merged_csv(MERGED_OUTPUT, merged_charities)
    print()
    print(f"Saved cross-file comparison JSON: {COMPARISON_OUTPUT.name}")
    print(f"Saved merged detailed CSV: {MERGED_OUTPUT.name}")


if __name__ == "__main__":
    main()
