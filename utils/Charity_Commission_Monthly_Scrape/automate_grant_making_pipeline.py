from __future__ import annotations

import argparse
import csv
import html.parser
import json
import re
import shutil
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional


BASE_DIR = Path(__file__).resolve().parent
DOWNLOAD_PAGE_URL = (
    "https://register-of-charities.charitycommission.gov.uk/en/register/full-register-download"
)
LOCAL_HTML_FALLBACK = BASE_DIR / "ChatiryCommission.html"
ZIP_DOWNLOAD_DIR = BASE_DIR / "data" / "charity_commission" / "json_zips"
DOWNLOAD_MANIFEST_PATH = ZIP_DOWNLOAD_DIR / "download_manifest.json"
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
RUNS_DIR_DEFAULT = OUTPUT_DIR / "monthly_runs"
LATEST_DIR_DEFAULT = OUTPUT_DIR / "latest"
TO_SCRAPE_DIR_DEFAULT = OUTPUT_DIR / "to_scrape"
GRANT_SCRIPT = BASE_DIR / "grant_prospector.py"

ANNUAL_RETURN_OUTPUT_NAME = "grant_making_main_activity_active.json"
CLASSIFICATION_OUTPUT_NAME = "grant_making_classification_302_active.json"
COMPARISON_OUTPUT_NAME = "grant_making_cross_file_comparison.json"
MERGED_OUTPUT_NAME = "grant_making_merged_detailed.csv"

GRANT_OUTPUT_FILENAMES = (
    ANNUAL_RETURN_OUTPUT_NAME,
    CLASSIFICATION_OUTPUT_NAME,
    COMPARISON_OUTPUT_NAME,
    MERGED_OUTPUT_NAME,
)

REQUIRED_JSON_FILENAMES = {
    "publicextract.charity.json",
    "publicextract.charity_annual_return_history.json",
    "publicextract.charity_annual_return_parta.json",
    "publicextract.charity_annual_return_partb.json",
    "publicextract.charity_area_of_operation.json",
    "publicextract.charity_classification.json",
}

FALLBACK_JSON_ZIP_NAMES = [
    "publicextract.charity.zip",
    "publicextract.charity_annual_return_history.zip",
    "publicextract.charity_annual_return_parta.zip",
    "publicextract.charity_annual_return_partb.zip",
    "publicextract.charity_area_of_operation.zip",
    "publicextract.charity_classification.zip",
    "publicextract.charity_event_history.zip",
    "publicextract.charity_governing_document.zip",
    "publicextract.charity_other_names.zip",
    "publicextract.charity_other_regulators.zip",
    "publicextract.charity_policy.zip",
    "publicextract.charity_published_report.zip",
    "publicextract.charity_trustee.zip",
]

PIPELINE_FIELDNAMES = [
    "run_id",
    "run_started_utc",
    "source_extract_date",
    "previous_run_id",
    "change_type",
    "change_reasons",
    "registered_charity_number",
    "charity_name",
    "url",
    "current_segment",
    "previous_segment",
    "current_latest_income",
    "previous_latest_income",
    "current_latest_expenditure",
    "previous_latest_expenditure",
    "current_latest_total_gross_income",
    "previous_latest_total_gross_income",
    "current_latest_total_gross_expenditure",
    "previous_latest_total_gross_expenditure",
    "current_latest_grants_to_institutions",
    "previous_latest_grants_to_institutions",
    "current_latest_ar_cycle_reference",
    "previous_latest_ar_cycle_reference",
    "current_latest_ar_received_date",
    "previous_latest_ar_received_date",
    "current_latest_accounts_received_date",
    "previous_latest_accounts_received_date",
]


class LinkCollector(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        href = dict(attrs).get("href")
        if href:
            self.links.append(href)


def utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def parse_optional_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(text)
        except ValueError:
            return None
    return None


def parse_optional_float(value: Any) -> Optional[float]:
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


def normalize_date_text(value: Any) -> str:
    parsed = parse_iso_date(value)
    return parsed.isoformat() if parsed is not None else ""


def read_local_html(path: Path) -> str:
    if not path.exists() or path.stat().st_size == 0:
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def fetch_remote_html(url: str, timeout: int) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


def extract_json_zip_links(html_text: str, base_url: str) -> list[str]:
    parser = LinkCollector()
    parser.feed(html_text)
    candidates: list[str] = []
    for href in parser.links:
        absolute = urllib.parse.urljoin(base_url, href)
        lower = absolute.lower()
        if "/data/json/" in lower and re.search(
            r"/publicextract\.[a-z0-9_]+\.zip(?:$|\?)", lower
        ):
            candidates.append(absolute)
    return list(dict.fromkeys(candidates))


def discover_json_zip_urls(timeout: int, local_html_path: Path) -> list[str]:
    local_html = read_local_html(local_html_path)
    urls = extract_json_zip_links(local_html, DOWNLOAD_PAGE_URL) if local_html else []
    if urls:
        print(
            f"Found {len(urls)} JSON ZIP links in local HTML fallback: {local_html_path.name}"
        )
        return urls

    try:
        remote_html = fetch_remote_html(DOWNLOAD_PAGE_URL, timeout=timeout)
    except urllib.error.URLError as exc:
        print(f"Warning: failed to fetch download page ({exc}). Using fallback URL list.")
        return [
            f"https://ccewuksprdoneregsadata1.blob.core.windows.net/data/json/{name}"
            for name in FALLBACK_JSON_ZIP_NAMES
        ]

    urls = extract_json_zip_links(remote_html, DOWNLOAD_PAGE_URL)
    if urls:
        print(f"Found {len(urls)} JSON ZIP links on live download page.")
        return urls

    print("Warning: no JSON ZIP links found on page. Using fallback URL list.")
    return [
        f"https://ccewuksprdoneregsadata1.blob.core.windows.net/data/json/{name}"
        for name in FALLBACK_JSON_ZIP_NAMES
    ]


def is_required_zip_name(zip_name: str) -> bool:
    return zip_name.removesuffix(".zip") + ".json" in REQUIRED_JSON_FILENAMES


def select_urls(urls: Iterable[str], required_only: bool) -> list[str]:
    if not required_only:
        return list(urls)
    selected: list[str] = []
    for url in urls:
        zip_name = Path(urllib.parse.urlparse(url).path).name
        if is_required_zip_name(zip_name):
            selected.append(url)
    return selected


def load_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


def write_json_payload(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False, sort_keys=True)
        handle.write("\n")


def load_download_manifest(path: Path) -> dict[str, dict[str, Any]]:
    payload = load_json_object(path)
    result: dict[str, dict[str, Any]] = {}
    for name, value in payload.items():
        if isinstance(name, str) and isinstance(value, dict):
            result[name] = value
    return result


def save_download_manifest(path: Path, manifest: dict[str, dict[str, Any]]) -> None:
    write_json_payload(path, manifest)


def metadata_from_headers(headers: Any) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    etag = headers.get("ETag")
    last_modified = headers.get("Last-Modified")
    content_length = parse_optional_int(headers.get("Content-Length"))

    if isinstance(etag, str) and etag.strip():
        metadata["etag"] = etag.strip()
    if isinstance(last_modified, str) and last_modified.strip():
        metadata["last_modified"] = last_modified.strip()
    if content_length is not None:
        metadata["content_length"] = content_length
    return metadata


def probe_remote_zip_metadata(url: str, timeout: int) -> dict[str, Any]:
    req = urllib.request.Request(
        url, method="HEAD", headers={"User-Agent": "Mozilla/5.0"}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return metadata_from_headers(response.headers)
    except urllib.error.HTTPError as exc:
        if exc.code in {405, 501}:
            return {}
        raise


def metadata_indicates_unchanged(
    destination: Path,
    previous_metadata: dict[str, Any],
    remote_metadata: dict[str, Any],
) -> bool:
    if not destination.exists() or destination.stat().st_size <= 0:
        return False

    local_size = destination.stat().st_size
    remote_size = parse_optional_int(remote_metadata.get("content_length"))
    if remote_size is not None and remote_size != local_size:
        return False

    previous_etag = str(previous_metadata.get("etag") or "").strip()
    remote_etag = str(remote_metadata.get("etag") or "").strip()
    if previous_etag and remote_etag:
        if previous_etag != remote_etag:
            return False
        return True

    previous_last_modified = str(previous_metadata.get("last_modified") or "").strip()
    remote_last_modified = str(remote_metadata.get("last_modified") or "").strip()
    if previous_last_modified and remote_last_modified:
        if previous_last_modified != remote_last_modified:
            return False
        return True

    if remote_size is not None:
        return remote_size == local_size

    return False


def download_file(
    url: str,
    destination: Path,
    timeout: int,
    force: bool,
    previous_metadata: dict[str, Any],
) -> tuple[bool, dict[str, Any]]:
    destination.parent.mkdir(parents=True, exist_ok=True)

    remote_metadata: dict[str, Any] = {}
    if destination.exists() and destination.stat().st_size > 0 and not force:
        try:
            remote_metadata = probe_remote_zip_metadata(url, timeout=timeout)
        except urllib.error.URLError as exc:
            print(
                f"Warning: metadata probe failed for {destination.name} ({exc}). "
                "Downloading to ensure freshness."
            )

        if remote_metadata and metadata_indicates_unchanged(
            destination=destination,
            previous_metadata=previous_metadata,
            remote_metadata=remote_metadata,
        ):
            print(f"Skipping unchanged ZIP: {destination.name}")
            return False, remote_metadata

    tmp = destination.with_suffix(destination.suffix + ".part")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    print(f"Downloading: {destination.name}")
    with urllib.request.urlopen(req, timeout=timeout) as response, tmp.open("wb") as fh:
        downloaded_metadata = metadata_from_headers(response.headers)
        shutil.copyfileobj(response, fh, length=1024 * 1024)
    tmp.replace(destination)

    merged_metadata = dict(remote_metadata)
    merged_metadata.update(downloaded_metadata)

    print(f"Saved ZIP: {destination} ({destination.stat().st_size:,} bytes)")
    return True, merged_metadata


def build_download_manifest_entry(
    url: str,
    destination: Path,
    previous_entry: dict[str, Any],
    remote_metadata: dict[str, Any],
    downloaded: bool,
) -> dict[str, Any]:
    entry = dict(previous_entry)
    entry["url"] = url
    entry["local_path"] = str(destination)
    entry["local_size"] = destination.stat().st_size if destination.exists() else None
    entry["last_checked_utc"] = utc_iso(datetime.now(timezone.utc))

    if "etag" in remote_metadata:
        entry["etag"] = remote_metadata["etag"]
    if "last_modified" in remote_metadata:
        entry["last_modified"] = remote_metadata["last_modified"]
    if "content_length" in remote_metadata:
        entry["content_length"] = remote_metadata["content_length"]

    if downloaded:
        entry["downloaded_at_utc"] = utc_iso(datetime.now(timezone.utc))
    return entry


def extract_json_files(zip_path: Path, output_dir: Path, overwrite: bool) -> list[Path]:
    extracted: list[Path] = []
    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.infolist():
            if member.is_dir():
                continue
            source_name = Path(member.filename).name
            if not source_name.lower().endswith(".json"):
                continue
            output_path = output_dir / source_name
            if output_path.exists() and not overwrite:
                extracted.append(output_path)
                continue
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(member, "r") as src, output_path.open("wb") as dst:
                shutil.copyfileobj(src, dst, length=1024 * 1024)
            extracted.append(output_path)
            print(f"Extracted JSON: {output_path}")
    return extracted


def ensure_required_inputs_present(input_dir: Path) -> None:
    missing = sorted(
        name for name in REQUIRED_JSON_FILENAMES if not (input_dir / name).exists()
    )
    if missing:
        raise RuntimeError(
            "Required input files are missing after download/extract:\n"
            + "\n".join(f"- {name}" for name in missing)
        )


def run_grant_script(script_path: Path) -> None:
    if not script_path.exists():
        raise FileNotFoundError(f"Could not find script: {script_path}")

    print(f"\nRunning analysis script: {script_path.name}\n")
    result = subprocess.run([sys.executable, str(script_path)], cwd=str(BASE_DIR))
    if result.returncode != 0:
        raise RuntimeError(
            f"{script_path.name} failed with exit code {result.returncode}."
        )
    print(f"\nCompleted: {script_path.name}")


def configure_csv_field_limit() -> None:
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            return
        except OverflowError:
            limit //= 10
            if limit <= 0:
                raise


def parse_json_array_cell(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, str):
        return []
    text = value.strip()
    if not text:
        return []
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, dict)]


def select_latest_row(
    rows: list[dict[str, Any]], date_fields: tuple[str, ...]
) -> Optional[dict[str, Any]]:
    if not rows:
        return None

    best_row: Optional[dict[str, Any]] = None
    best_key: Optional[tuple[date, int]] = None
    for idx, row in enumerate(rows):
        candidate_dates = [
            parsed_date
            for parsed_date in (parse_iso_date(row.get(field)) for field in date_fields)
            if parsed_date is not None
        ]
        row_date = max(candidate_dates) if candidate_dates else None
        key = (row_date or date.min, idx)
        if best_key is None or key > best_key:
            best_key = key
            best_row = row
    return best_row


def first_non_empty_text(*values: Any) -> str:
    for value in values:
        if isinstance(value, str):
            text = value.strip()
            if text:
                return text
    return ""


def build_funds_snapshot(
    merged_csv_path: Path, run_id: str, generated_at_utc: str
) -> dict[str, Any]:
    configure_csv_field_limit()
    funds: list[dict[str, Any]] = []
    extract_dates: set[str] = set()

    with merged_csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            charity_number = parse_optional_int(row.get("registered_charity_number"))
            if charity_number is None:
                continue

            charity_rows = parse_json_array_cell(row.get("charity_rows_json"))
            history_rows = parse_json_array_cell(row.get("annual_return_history_rows_json"))
            partb_rows = parse_json_array_cell(row.get("annual_return_partb_rows_json"))

            latest_charity_row = select_latest_row(
                charity_rows, ("latest_acc_fin_period_end_date", "date_of_extract")
            )
            latest_history_row = select_latest_row(
                history_rows,
                (
                    "fin_period_end_date",
                    "date_annual_return_received",
                    "date_accounts_received",
                    "date_of_extract",
                ),
            )
            latest_partb_row = select_latest_row(
                partb_rows, ("fin_period_end_date", "ar_received_date", "date_of_extract")
            )

            source_extract_date = first_non_empty_text(
                normalize_date_text(
                    (latest_history_row or {}).get("date_of_extract")
                    if latest_history_row
                    else None
                ),
                normalize_date_text(
                    (latest_charity_row or {}).get("date_of_extract")
                    if latest_charity_row
                    else None
                ),
                normalize_date_text(
                    (latest_partb_row or {}).get("date_of_extract")
                    if latest_partb_row
                    else None
                ),
            )
            if source_extract_date:
                extract_dates.add(source_extract_date)

            fund_record = {
                "registered_charity_number": charity_number,
                "charity_name": first_non_empty_text(
                    (latest_charity_row or {}).get("charity_name")
                    if latest_charity_row
                    else "",
                    row.get("charity_name"),
                ),
                "url": first_non_empty_text(row.get("url")),
                "segment": first_non_empty_text(row.get("segment")),
                "annual_return_filtered_count": parse_optional_int(
                    row.get("annual_return_filtered_count")
                )
                or 0,
                "classification_filtered_count": parse_optional_int(
                    row.get("classification_filtered_count")
                )
                or 0,
                "latest_income": parse_optional_float(
                    (latest_charity_row or {}).get("latest_income")
                    if latest_charity_row
                    else None
                ),
                "latest_expenditure": parse_optional_float(
                    (latest_charity_row or {}).get("latest_expenditure")
                    if latest_charity_row
                    else None
                ),
                "latest_total_gross_income": parse_optional_float(
                    (latest_history_row or {}).get("total_gross_income")
                    if latest_history_row
                    else None
                ),
                "latest_total_gross_expenditure": parse_optional_float(
                    (latest_history_row or {}).get("total_gross_expenditure")
                    if latest_history_row
                    else None
                ),
                "latest_grants_to_institutions": parse_optional_float(
                    (latest_partb_row or {}).get("expenditure_grants_institution")
                    if latest_partb_row
                    else None
                ),
                "latest_ar_cycle_reference": first_non_empty_text(
                    (latest_history_row or {}).get("ar_cycle_reference")
                    if latest_history_row
                    else "",
                    (latest_partb_row or {}).get("ar_cycle_reference")
                    if latest_partb_row
                    else "",
                ),
                "latest_ar_received_date": first_non_empty_text(
                    normalize_date_text(
                        (latest_history_row or {}).get("date_annual_return_received")
                        if latest_history_row
                        else None
                    ),
                    normalize_date_text(
                        (latest_partb_row or {}).get("ar_received_date")
                        if latest_partb_row
                        else None
                    ),
                ),
                "latest_accounts_received_date": normalize_date_text(
                    (latest_history_row or {}).get("date_accounts_received")
                    if latest_history_row
                    else None
                ),
                "source_extract_date": source_extract_date,
            }
            funds.append(fund_record)

    funds.sort(key=lambda item: item["registered_charity_number"])
    source_extract_date = sorted(extract_dates)[-1] if extract_dates else ""
    source_extract_month = source_extract_date[:7] if source_extract_date else ""

    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": generated_at_utc,
        "source_extract_date": source_extract_date,
        "source_extract_month": source_extract_month,
        "charity_count": len(funds),
        "funds": funds,
    }


def snapshot_to_index(snapshot_payload: dict[str, Any]) -> dict[int, dict[str, Any]]:
    index: dict[int, dict[str, Any]] = {}
    for row in snapshot_payload.get("funds", []):
        if not isinstance(row, dict):
            continue
        charity_number = parse_optional_int(row.get("registered_charity_number"))
        if charity_number is None:
            continue
        index[charity_number] = row
    return index


def significant_numeric_change(
    previous_value: Optional[float],
    current_value: Optional[float],
    absolute_threshold: float,
    percent_threshold: float,
) -> tuple[bool, Optional[float], Optional[float]]:
    if previous_value is None or current_value is None:
        return previous_value != current_value, None, None

    delta = abs(current_value - previous_value)
    if delta < absolute_threshold:
        if previous_value == 0:
            return False, delta, None
        return False, delta, delta / abs(previous_value)

    if previous_value == 0:
        return True, delta, None

    pct_change = delta / abs(previous_value)
    if pct_change < percent_threshold:
        return False, delta, pct_change
    return True, delta, pct_change


def describe_numeric_change(
    label: str,
    previous_value: Optional[float],
    current_value: Optional[float],
    absolute_threshold: float,
    percent_threshold: float,
) -> Optional[str]:
    if previous_value is None and current_value is None:
        return None
    if previous_value is None and current_value is not None:
        return f"{label} changed from empty to {current_value:,.2f}"
    if previous_value is not None and current_value is None:
        return f"{label} changed from {previous_value:,.2f} to empty"

    changed, delta, pct = significant_numeric_change(
        previous_value=previous_value,
        current_value=current_value,
        absolute_threshold=absolute_threshold,
        percent_threshold=percent_threshold,
    )
    if not changed:
        return None

    if pct is None:
        return (
            f"{label} changed from {previous_value:,.2f} to {current_value:,.2f} "
            f"(delta {delta:,.2f})"
        )
    return (
        f"{label} changed from {previous_value:,.2f} to {current_value:,.2f} "
        f"(delta {delta:,.2f}, {pct * 100:.1f}%)"
    )


def compare_fund_records(
    previous_row: dict[str, Any],
    current_row: dict[str, Any],
    absolute_threshold: float,
    percent_threshold: float,
) -> list[str]:
    reasons: list[str] = []

    for field_name, label in (
        ("segment", "segment"),
        ("latest_ar_cycle_reference", "latest_ar_cycle_reference"),
        ("latest_ar_received_date", "latest_ar_received_date"),
        ("latest_accounts_received_date", "latest_accounts_received_date"),
    ):
        previous_value = first_non_empty_text(previous_row.get(field_name))
        current_value = first_non_empty_text(current_row.get(field_name))
        if previous_value != current_value:
            reasons.append(f"{label} changed from '{previous_value}' to '{current_value}'")

    for field_name, label in (
        ("latest_income", "latest_income"),
        ("latest_expenditure", "latest_expenditure"),
        ("latest_total_gross_income", "latest_total_gross_income"),
        ("latest_total_gross_expenditure", "latest_total_gross_expenditure"),
        ("latest_grants_to_institutions", "latest_grants_to_institutions"),
    ):
        previous_value = parse_optional_float(previous_row.get(field_name))
        current_value = parse_optional_float(current_row.get(field_name))
        description = describe_numeric_change(
            label=label,
            previous_value=previous_value,
            current_value=current_value,
            absolute_threshold=absolute_threshold,
            percent_threshold=percent_threshold,
        )
        if description:
            reasons.append(description)

    return reasons


def build_to_scrape_row(
    run_id: str,
    run_started_utc: str,
    source_extract_date: str,
    previous_run_id: str,
    change_type: str,
    change_reasons: list[str],
    current_row: dict[str, Any],
    previous_row: Optional[dict[str, Any]],
) -> dict[str, Any]:
    previous_row = previous_row or {}
    return {
        "run_id": run_id,
        "run_started_utc": run_started_utc,
        "source_extract_date": source_extract_date,
        "previous_run_id": previous_run_id,
        "change_type": change_type,
        "change_reasons": "; ".join(change_reasons),
        "registered_charity_number": current_row.get("registered_charity_number"),
        "charity_name": first_non_empty_text(current_row.get("charity_name")),
        "url": first_non_empty_text(current_row.get("url")),
        "current_segment": first_non_empty_text(current_row.get("segment")),
        "previous_segment": first_non_empty_text(previous_row.get("segment")),
        "current_latest_income": current_row.get("latest_income"),
        "previous_latest_income": previous_row.get("latest_income"),
        "current_latest_expenditure": current_row.get("latest_expenditure"),
        "previous_latest_expenditure": previous_row.get("latest_expenditure"),
        "current_latest_total_gross_income": current_row.get("latest_total_gross_income"),
        "previous_latest_total_gross_income": previous_row.get(
            "latest_total_gross_income"
        ),
        "current_latest_total_gross_expenditure": current_row.get(
            "latest_total_gross_expenditure"
        ),
        "previous_latest_total_gross_expenditure": previous_row.get(
            "latest_total_gross_expenditure"
        ),
        "current_latest_grants_to_institutions": current_row.get(
            "latest_grants_to_institutions"
        ),
        "previous_latest_grants_to_institutions": previous_row.get(
            "latest_grants_to_institutions"
        ),
        "current_latest_ar_cycle_reference": first_non_empty_text(
            current_row.get("latest_ar_cycle_reference")
        ),
        "previous_latest_ar_cycle_reference": first_non_empty_text(
            previous_row.get("latest_ar_cycle_reference")
        ),
        "current_latest_ar_received_date": first_non_empty_text(
            current_row.get("latest_ar_received_date")
        ),
        "previous_latest_ar_received_date": first_non_empty_text(
            previous_row.get("latest_ar_received_date")
        ),
        "current_latest_accounts_received_date": first_non_empty_text(
            current_row.get("latest_accounts_received_date")
        ),
        "previous_latest_accounts_received_date": first_non_empty_text(
            previous_row.get("latest_accounts_received_date")
        ),
    }


def calculate_monthly_delta(
    current_snapshot: dict[str, Any],
    previous_snapshot: dict[str, Any],
    run_id: str,
    run_started_utc: str,
    previous_run_id: str,
    absolute_threshold: float,
    percent_threshold: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    current_index = snapshot_to_index(current_snapshot)
    previous_index = snapshot_to_index(previous_snapshot)

    current_ids = set(current_index)
    previous_ids = set(previous_index)

    new_ids = sorted(current_ids - previous_ids)
    removed_ids = sorted(previous_ids - current_ids)

    changed_details: list[dict[str, Any]] = []
    changed_ids: list[int] = []
    to_scrape_rows: list[dict[str, Any]] = []

    source_extract_date = first_non_empty_text(current_snapshot.get("source_extract_date"))

    for charity_id in new_ids:
        current_row = current_index[charity_id]
        reasons = ["not present in previous snapshot"]
        to_scrape_rows.append(
            build_to_scrape_row(
                run_id=run_id,
                run_started_utc=run_started_utc,
                source_extract_date=source_extract_date,
                previous_run_id=previous_run_id,
                change_type="new",
                change_reasons=reasons,
                current_row=current_row,
                previous_row=None,
            )
        )

    for charity_id in sorted(current_ids & previous_ids):
        current_row = current_index[charity_id]
        previous_row = previous_index[charity_id]
        reasons = compare_fund_records(
            previous_row=previous_row,
            current_row=current_row,
            absolute_threshold=absolute_threshold,
            percent_threshold=percent_threshold,
        )
        if not reasons:
            continue

        changed_ids.append(charity_id)
        changed_details.append(
            {
                "registered_charity_number": charity_id,
                "change_reasons": reasons,
            }
        )
        to_scrape_rows.append(
            build_to_scrape_row(
                run_id=run_id,
                run_started_utc=run_started_utc,
                source_extract_date=source_extract_date,
                previous_run_id=previous_run_id,
                change_type="significant_change",
                change_reasons=reasons,
                current_row=current_row,
                previous_row=previous_row,
            )
        )

    to_scrape_rows.sort(
        key=lambda row: (
            0 if row.get("change_type") == "new" else 1,
            parse_optional_int(row.get("registered_charity_number")) or 0,
        )
    )

    delta_payload = {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": utc_iso(datetime.now(timezone.utc)),
        "previous_run_id": previous_run_id,
        "source_extract_date": source_extract_date,
        "thresholds": {
            "absolute_threshold": absolute_threshold,
            "percent_threshold": percent_threshold,
        },
        "counts": {
            "current_charities": len(current_ids),
            "previous_charities": len(previous_ids),
            "new_charities": len(new_ids),
            "significant_change_charities": len(changed_ids),
            "removed_charities": len(removed_ids),
            "to_scrape_charities": len(to_scrape_rows),
        },
        "charity_ids": {
            "new": new_ids,
            "significant_change": changed_ids,
            "removed": removed_ids,
        },
        "significant_change_details": changed_details,
    }
    return delta_payload, to_scrape_rows


def csv_value(value: Any) -> Any:
    if value is None:
        return ""
    return value


def write_to_scrape_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=PIPELINE_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: csv_value(row.get(name)) for name in PIPELINE_FIELDNAMES})


def build_input_manifest(input_dir: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in sorted(input_dir.glob("publicextract.*.json")):
        if not path.is_file():
            continue
        stat = path.stat()
        records.append(
            {
                "file_name": path.name,
                "path": str(path),
                "size_bytes": stat.st_size,
                "modified_utc": utc_iso(datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)),
            }
        )
    return records


def archive_grant_outputs(output_dir: Path, run_dir: Path) -> dict[str, Path]:
    archived: dict[str, Path] = {}
    for file_name in GRANT_OUTPUT_FILENAMES:
        source_path = output_dir / file_name
        if not source_path.exists():
            raise FileNotFoundError(
                f"Expected output file is missing after grant script run: {source_path}"
            )
        destination_path = run_dir / file_name
        shutil.copy2(source_path, destination_path)
        archived[file_name] = destination_path
    return archived


def find_latest_snapshot(runs_dir: Path) -> tuple[Optional[Path], str]:
    if not runs_dir.exists():
        return None, ""
    candidates: list[tuple[str, Path]] = []
    for child in runs_dir.iterdir():
        if not child.is_dir():
            continue
        snapshot_path = child / "funds_snapshot.json"
        if snapshot_path.exists():
            candidates.append((child.name, snapshot_path))
    if not candidates:
        return None, ""
    candidates.sort(key=lambda item: item[0])
    run_id, path = candidates[-1]
    return path, run_id


def load_snapshot_payload(path: Path) -> dict[str, Any]:
    payload = load_json_object(path)
    funds = payload.get("funds")
    if not isinstance(funds, list):
        raise ValueError(f"Snapshot file has no 'funds' array: {path}")
    return payload


def allocate_run_id(runs_dir: Path, preferred_run_id: str) -> str:
    if not (runs_dir / preferred_run_id).exists():
        return preferred_run_id
    suffix = 2
    while True:
        candidate = f"{preferred_run_id}_{suffix}"
        if not (runs_dir / candidate).exists():
            return candidate
        suffix += 1


def copy_latest_aliases(latest_dir: Path, alias_map: dict[str, Path]) -> None:
    latest_dir.mkdir(parents=True, exist_ok=True)
    for alias_name, source_path in alias_map.items():
        destination = latest_dir / alias_name
        shutil.copy2(source_path, destination)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Monthly grant pipeline orchestrator: download Charity Commission extracts, "
            "run grant_prospector.py, archive dated outputs, compare with previous run, "
            "and generate a to_scrape pipeline CSV."
        )
    )
    parser.add_argument(
        "--required-only",
        action="store_true",
        help=(
            "Only download ZIPs required by grant_prospector.py "
            "(charity, annual return history/parta/partb, area, classification)."
        ),
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download/extract and only run grant_prospector.py.",
    )
    parser.add_argument(
        "--skip-run",
        action="store_true",
        help="Download/extract only, and do not run grant_prospector.py.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download ZIPs and overwrite extracted JSON files.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="HTTP timeout in seconds for each request (default: 300).",
    )
    parser.add_argument(
        "--local-html",
        type=Path,
        default=LOCAL_HTML_FALLBACK,
        help=(
            "Optional local HTML file to parse first for download links "
            "(default: ChatiryCommission.html)."
        ),
    )
    parser.add_argument(
        "--download-manifest",
        type=Path,
        default=DOWNLOAD_MANIFEST_PATH,
        help=(
            "Path to ZIP metadata manifest used to detect unchanged downloads "
            "(default: data/charity_commission/json_zips/download_manifest.json)."
        ),
    )
    parser.add_argument(
        "--runs-dir",
        type=Path,
        default=RUNS_DIR_DEFAULT,
        help="Directory for dated monthly run folders (default: output/monthly_runs).",
    )
    parser.add_argument(
        "--latest-dir",
        type=Path,
        default=LATEST_DIR_DEFAULT,
        help="Directory for latest-file aliases (default: output/latest).",
    )
    parser.add_argument(
        "--to-scrape-dir",
        type=Path,
        default=TO_SCRAPE_DIR_DEFAULT,
        help=(
            "Directory for dated and latest to_scrape CSV files "
            "(default: output/to_scrape)."
        ),
    )
    parser.add_argument(
        "--significant-change-abs",
        type=float,
        default=50_000.0,
        help=(
            "Absolute numeric delta threshold for significant change detection "
            "(default: 50000)."
        ),
    )
    parser.add_argument(
        "--significant-change-pct",
        type=float,
        default=0.20,
        help=(
            "Relative numeric delta threshold for significant change detection "
            "(default: 0.20 = 20%%)."
        ),
    )
    parser.add_argument(
        "--run-id",
        default="",
        help=(
            "Optional run id for the dated output folder. If omitted, UTC timestamp is used "
            "(for example: 20260225T120000Z)."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.timeout <= 0:
        raise ValueError("--timeout must be greater than 0.")
    if args.significant_change_abs < 0:
        raise ValueError("--significant-change-abs must be >= 0.")
    if args.significant_change_pct < 0:
        raise ValueError("--significant-change-pct must be >= 0.")

    run_started = datetime.now(timezone.utc)
    run_started_utc = utc_iso(run_started)

    download_manifest = load_download_manifest(args.download_manifest)
    download_events: list[dict[str, Any]] = []

    if not args.skip_download:
        urls = discover_json_zip_urls(timeout=args.timeout, local_html_path=args.local_html)
        urls = select_urls(urls, required_only=args.required_only)
        if not urls:
            raise RuntimeError("No JSON ZIP URLs selected for download.")

        print(f"Preparing to process {len(urls)} JSON ZIP file(s).")
        all_extracted: list[Path] = []
        for url in urls:
            zip_name = Path(urllib.parse.urlparse(url).path).name
            if not zip_name.lower().endswith(".zip"):
                continue

            zip_path = ZIP_DOWNLOAD_DIR / zip_name
            previous_entry = download_manifest.get(zip_name, {})
            downloaded, remote_metadata = download_file(
                url=url,
                destination=zip_path,
                timeout=args.timeout,
                force=args.force,
                previous_metadata=previous_entry,
            )
            updated_entry = build_download_manifest_entry(
                url=url,
                destination=zip_path,
                previous_entry=previous_entry,
                remote_metadata=remote_metadata,
                downloaded=downloaded,
            )
            download_manifest[zip_name] = updated_entry

            extracted = extract_json_files(
                zip_path=zip_path,
                output_dir=INPUT_DIR,
                overwrite=args.force or downloaded,
            )
            all_extracted.extend(extracted)

            download_events.append(
                {
                    "zip_name": zip_name,
                    "url": url,
                    "downloaded": downloaded,
                    "local_size": zip_path.stat().st_size if zip_path.exists() else None,
                    "etag": updated_entry.get("etag"),
                    "last_modified": updated_entry.get("last_modified"),
                }
            )

        save_download_manifest(args.download_manifest, download_manifest)

        print(f"JSON files available in input/: {len(set(all_extracted))}")
        ensure_required_inputs_present(INPUT_DIR)
        print("Required JSON inputs are present.")
    else:
        ensure_required_inputs_present(INPUT_DIR)
        print("Skipped download. Required JSON inputs already present.")

    if args.skip_run:
        print("Skipped running grant_prospector.py as requested.")
        return

    run_grant_script(GRANT_SCRIPT)

    args.runs_dir.mkdir(parents=True, exist_ok=True)
    previous_snapshot_path, previous_run_id = find_latest_snapshot(args.runs_dir)

    preferred_run_id = (
        args.run_id.strip() if isinstance(args.run_id, str) and args.run_id.strip() else ""
    )
    if not preferred_run_id:
        preferred_run_id = run_started.strftime("%Y%m%dT%H%M%SZ")
    run_id = allocate_run_id(args.runs_dir, preferred_run_id)

    run_dir = args.runs_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=False)

    archived_outputs = archive_grant_outputs(OUTPUT_DIR, run_dir)
    merged_csv_path = archived_outputs[MERGED_OUTPUT_NAME]

    snapshot_payload = build_funds_snapshot(
        merged_csv_path=merged_csv_path,
        run_id=run_id,
        generated_at_utc=utc_iso(datetime.now(timezone.utc)),
    )
    snapshot_path = run_dir / "funds_snapshot.json"
    write_json_payload(snapshot_path, snapshot_payload)

    previous_snapshot_payload: dict[str, Any] = {}
    if previous_snapshot_path is not None:
        try:
            previous_snapshot_payload = load_snapshot_payload(previous_snapshot_path)
        except Exception as exc:
            print(
                "Warning: failed to load previous snapshot "
                f"({previous_snapshot_path}): {exc}. Treating this run as baseline."
            )
            previous_snapshot_payload = {}
            previous_run_id = ""

    delta_payload, to_scrape_rows = calculate_monthly_delta(
        current_snapshot=snapshot_payload,
        previous_snapshot=previous_snapshot_payload,
        run_id=run_id,
        run_started_utc=run_started_utc,
        previous_run_id=previous_run_id,
        absolute_threshold=args.significant_change_abs,
        percent_threshold=args.significant_change_pct,
    )
    delta_path = run_dir / "monthly_delta.json"
    write_json_payload(delta_path, delta_payload)

    to_scrape_run_path = run_dir / "to_scrape_pipeline.csv"
    write_to_scrape_csv(to_scrape_run_path, to_scrape_rows)

    source_extract_month = first_non_empty_text(snapshot_payload.get("source_extract_month"))
    if not source_extract_month:
        source_extract_month = run_started.strftime("%Y-%m")

    to_scrape_monthly_path = (
        args.to_scrape_dir / f"to_scrape_pipeline_{source_extract_month}.csv"
    )
    write_to_scrape_csv(to_scrape_monthly_path, to_scrape_rows)

    to_scrape_latest_path = args.to_scrape_dir / "to_scrape_pipeline_latest.csv"
    write_to_scrape_csv(to_scrape_latest_path, to_scrape_rows)

    run_metadata = {
        "schema_version": 1,
        "run_id": run_id,
        "run_started_utc": run_started_utc,
        "run_completed_utc": utc_iso(datetime.now(timezone.utc)),
        "source_extract_date": snapshot_payload.get("source_extract_date"),
        "source_extract_month": source_extract_month,
        "previous_run_id": previous_run_id,
        "previous_snapshot_path": str(previous_snapshot_path) if previous_snapshot_path else "",
        "configuration": {
            "required_only": bool(args.required_only),
            "skip_download": bool(args.skip_download),
            "force": bool(args.force),
            "timeout": int(args.timeout),
            "download_manifest": str(args.download_manifest),
            "runs_dir": str(args.runs_dir),
            "latest_dir": str(args.latest_dir),
            "to_scrape_dir": str(args.to_scrape_dir),
            "significant_change_abs": args.significant_change_abs,
            "significant_change_pct": args.significant_change_pct,
        },
        "download_events": download_events,
        "input_files": build_input_manifest(INPUT_DIR),
        "output_files": {
            "run_dir": str(run_dir),
            "annual_return_output": str(run_dir / ANNUAL_RETURN_OUTPUT_NAME),
            "classification_output": str(run_dir / CLASSIFICATION_OUTPUT_NAME),
            "comparison_output": str(run_dir / COMPARISON_OUTPUT_NAME),
            "merged_output": str(run_dir / MERGED_OUTPUT_NAME),
            "snapshot": str(snapshot_path),
            "delta": str(delta_path),
            "to_scrape_run": str(to_scrape_run_path),
            "to_scrape_monthly": str(to_scrape_monthly_path),
            "to_scrape_latest": str(to_scrape_latest_path),
        },
        "delta_counts": delta_payload.get("counts", {}),
    }
    metadata_path = run_dir / "run_metadata.json"
    write_json_payload(metadata_path, run_metadata)

    latest_aliases = dict(archived_outputs)
    latest_aliases.update(
        {
            "funds_snapshot.json": snapshot_path,
            "monthly_delta.json": delta_path,
            "to_scrape_pipeline.csv": to_scrape_run_path,
            "run_metadata.json": metadata_path,
        }
    )
    copy_latest_aliases(args.latest_dir, latest_aliases)

    print()
    print(f"Monthly run id: {run_id}")
    print(f"Run folder: {run_dir}")
    print(f"Current charities in snapshot: {snapshot_payload.get('charity_count', 0):,}")
    print(
        "To-scrape charities (new + significant changes): "
        f"{delta_payload.get('counts', {}).get('to_scrape_charities', 0):,}"
    )
    print(f"Saved dated to-scrape CSV: {to_scrape_monthly_path}")
    print(f"Saved latest to-scrape CSV: {to_scrape_latest_path}")
    print(f"Saved delta summary JSON: {delta_path}")


if __name__ == "__main__":
    main()
