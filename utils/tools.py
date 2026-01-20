import json
import logging
import os
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import gspread
import pandas as pd
import requests
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
from openai import OpenAI

from Web_scraper_2 import OUTPUT_CSV
from utils.constants import (
    CSV_COLUMNS,
    DISCOVERY_DEPTH,
    ELIGIBILITY_ORDER,
    HEADERS,
    KEYWORDS,
    LLM_PROMPT,
    MAX_DISCOVERY_PAGES,
    MAX_PAGES,
    PAUSE_BETWEEN_REQUESTS,
    SAVE_DIR,
)


logger = logging.getLogger(__name__)


@dataclass
class ToolSettings:
    openai_api_key: Optional[str] = None
    google_service_account: Optional[dict] = None
    google_sheet_id: Optional[str] = None
    log_callback: Optional[Callable[[str, str], None]] = None


@dataclass
class ScrapeProgress:
    done: bool = False
    progress_percent: int = 0
    results: List[dict] = field(default_factory=list)
    errors: List[Tuple[str, str]] = field(default_factory=list)
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    current_url: Optional[str] = None
    current_started_at: Optional[float] = None
    url_timings: List[Dict[str, Any]] = field(default_factory=list)


_SETTINGS = ToolSettings()


def configure_tools(
    *,
    openai_api_key: Optional[str] = None,
    google_service_account: Optional[dict] = None,
    google_sheet_id: Optional[str] = None,
    log_callback: Optional[Callable[[str, str], None]] = None,
):
    """Update runtime settings used by the scraping utilities."""

    if openai_api_key is not None:
        _SETTINGS.openai_api_key = openai_api_key
    if google_service_account is not None:
        _SETTINGS.google_service_account = google_service_account
    if google_sheet_id is not None:
        _SETTINGS.google_sheet_id = google_sheet_id
    if log_callback is not None:
        _SETTINGS.log_callback = log_callback


# ========== API KEY VAULT =========
def get_client() -> Optional[OpenAI]:
    api_key = (_SETTINGS.openai_api_key or "").strip()
    if not api_key:
        return None
    try:
        return OpenAI(api_key=api_key)
    except Exception:
        return None


# ==================================
# ========== UTIL HELPERS ==========
# ==================================


def safe_filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    name = parsed.netloc + parsed.path
    name = name.strip("/").replace("/", "_")
    name = re.sub(r"[^a-zA-Z0-9._-]", "_", name)
    return name[:150]


def normalize_url(url: str) -> str:
    """Simple normalization for deduplication within one domain."""
    parsed = urlparse(url)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc
    path = parsed.path.rstrip("/")
    qs = ("?" + parsed.query) if parsed.query else ""
    return f"{scheme}://{netloc}{path}{qs}"


def initial_normalize_url(url: str) -> str:
    """
    For initial seed URLs (user-provided), produce a base link to restrict crawling.
    e.g. turns
    https://register-of-charities.charitycommission.gov.uk/en/charity-search/-/charity-details/1010625/charity-overview?...
    into
    https://register-of-charities.charitycommission.gov.uk/en/charity-search/-/charity-details/1010625
    """
    url = url.strip()
    if not url:
        return url

    parsed = urlparse(url)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc.lower().replace("www.", "")
    path = parsed.path or "/"
    path = re.sub(r"/+$", "", path)

    # special case: Charity Commission pattern
    if "charitycommission.gov.uk" in netloc and "/charity-details/" in path:
        # keep only the ID part
        match = re.search(r"(/charity-details/\d+)", path)
        if match:
            path = "/en/charity-search/-" + match.group(1)

    normalized = f"{scheme}://{netloc}{path}"
    return normalized


def _log(message: str, level: str = "info") -> None:
    """Write to the configured callback and fall back to logging."""

    callback = _SETTINGS.log_callback
    if callback:
        try:
            callback(level, message)
            return
        except Exception:  # pragma: no cover - defensive
            logger.exception("Log callback failed")

    log_fn = getattr(logger, level, None)
    if callable(log_fn):
        log_fn(message)
    else:
        logger.info("%s", message)


def fetch_page(url: str, retries: int = 4, backoff_factor: int = 2) -> Optional[str]:
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", (backoff_factor**attempt) * 5))
                _log(f"Rate limited ({resp.status_code}) – pausing {wait}s", "warning")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.text
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                pause = (backoff_factor**attempt) * 2
                _log(f"Fetch failed: {url} ({e}); retrying in {pause}s", "warning")
                time.sleep(pause)
            else:
                _log(f"Fetch failed for {url}: {e}", "error")
    return None


def extract_visible_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "svg", "footer", "nav", "form", "header"]):
        tag.decompose()
    text = " ".join(t.get_text(" ", strip=True) for t in soup.find_all(["h1", "h2", "h3", "p", "li", "td", "th"]))
    return re.sub(r"\s+", " ", text).strip()


def discover_links(seed_url: str, discovery_depth: int = DISCOVERY_DEPTH, max_pages: int = MAX_DISCOVERY_PAGES) -> Dict:
    """
    Crawl only pages related to the same base entity (same charity ID or program).
    For Charity Commission, restrict to links that start with the seed base path.
    """
    seed_base = initial_normalize_url(seed_url)
    seed_norm = normalize_url(seed_url)
    base_domain = urlparse(seed_norm).netloc.replace("www.", "")
    queue = [(seed_norm, 0)]
    visited, candidates = set(), {}
    pages_visited = 0

    while queue:
        url, depth = queue.pop(0)
        if url in visited or depth > discovery_depth or pages_visited >= max_pages:
            continue
        visited.add(url)
        pages_visited += 1

        html = fetch_page(url)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        title = soup.title.string.strip() if soup.title and soup.title.string else ""
        snippet = ""
        if p := soup.find("p"):
            snippet = p.get_text(" ", strip=True)[:300]

        for a in soup.find_all("a", href=True):
            href = urljoin(url, a["href"].split("#")[0])
            if not href.startswith("http"):
                continue
            parsed_href = urlparse(href)
            if base_domain not in parsed_href.netloc:
                continue
            if any(href.lower().endswith(ext) for ext in [".pdf", ".jpg", ".jpeg", ".png", ".zip", ".mp4", ".doc", ".docx"]):
                continue

            # Restrict to links under the same base path for Charity Commission
            if "charitycommission.gov.uk" in base_domain:
                if not href.startswith(seed_base):
                    continue

            hnorm = normalize_url(href)
            anchor = (a.get_text(" ", strip=True) or "").strip()
            meta = candidates.setdefault(hnorm, {"anchor_texts": set(), "source_titles": set(), "source_snippets": set()})
            if anchor:
                meta["anchor_texts"].add(anchor)
            if title:
                meta["source_titles"].add(title)
            if snippet:
                meta["source_snippets"].add(snippet)
            if hnorm not in visited and depth + 1 <= discovery_depth:
                queue.append((hnorm, depth + 1))

        time.sleep(PAUSE_BETWEEN_REQUESTS)

    _log(f"➕ Found {len(candidates)} internal links (visited {pages_visited} pages) from {seed_base}")
    return candidates


def score_candidate(url: str, meta: Dict) -> int:
    score = 0
    u = url.lower()
    for kw in KEYWORDS:
        if kw in u:
            score += 50
    for a in meta.get("anchor_texts", []):
        if any(kw in a.lower() for kw in KEYWORDS):
            score += 25
    for t in meta.get("source_titles", []):
        if any(kw in t.lower() for kw in KEYWORDS):
            score += 10
    for s in meta.get("source_snippets", []):
        if any(kw in s.lower() for kw in KEYWORDS):
            score += 7
    depth_penalty = len(urlparse(url).path.strip("/").split("/")) - 3
    score -= max(0, depth_penalty) * 3
    score += max(0, 10 - len(url) / 50)
    return score


def prioritized_crawl(seed_url: str) -> Tuple[str, str, int, List[str]]:
    """Crawl and prioritize only the related internal pages."""
    seed_base = initial_normalize_url(seed_url)
    seed_norm = normalize_url(seed_url)
    domain_folder = os.path.join(SAVE_DIR, safe_filename_from_url(seed_base))
    os.makedirs(domain_folder, exist_ok=True)

    candidates = discover_links(seed_norm)
    candidates.setdefault(seed_norm, {"anchor_texts": set(), "source_titles": set(), "source_snippets": set()})
    scored = [(score_candidate(url, meta), url) for url, meta in candidates.items()]
    scored.sort(reverse=True)

    top_links = [url for _, url in scored[:MAX_PAGES]]
    _log(f"🌐 Fetching top {len(top_links)} links from {seed_base}")

    all_text = []
    for i, url in enumerate(top_links, 1):
        _log(f"&nbsp;&nbsp;↳ ({i}/{len(top_links)}) {url}")
        html = fetch_page(url)
        if not html:
            continue
        text = extract_visible_text(html)
        all_text.append(text)
        fname = safe_filename_from_url(url) + ".txt"
        with open(os.path.join(domain_folder, fname), "w", encoding="utf-8") as f:
            f.write(text)
        time.sleep(PAUSE_BETWEEN_REQUESTS)

    combined_text = " ".join(all_text)
    return combined_text, domain_folder, len(all_text), top_links


def call_llm_extract(text: str) -> Dict:
    client = get_client()
    if client is None:
        return {
            "applicant_types": "",
            "geographic_scope": "",
            "beneficiary_focus": "",
            "funding_range": "",
            "restrictions": "",
            "application_status": "unclear",
            "deadline": "",
            "notes": "LLM not configured (no API key).",
            "eligibility": "Low Match",
            "evidence": "LLM extraction skipped (no API key).",
        }

    max_chars = 50000
    if len(text) > max_chars:
        _log(f"Text truncated from {len(text)} to {max_chars} chars", "warning")
        text = text[: max_chars // 2] + "\n...[content truncated]...\n" + text[-max_chars // 2 :]

    prompt = LLM_PROMPT.replace("{text}", text)

    try:
        resp = client.chat.completions.create(
            model="gpt-4.1",
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert at evaluating charity funding eligibility. You extract structured data and provide accurate eligibility assessments based on specific criteria.",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0,
            response_format={"type": "json_object"},
        )
        output = resp.choices[0].message.content.strip()
        output = re.sub(r"^```json\s*|\s*```$", "", output, flags=re.MULTILINE)
        data = json.loads(output)

        normalized = {
            "applicant_types": data.get("applicant_types", []),
            "geographic_scope": data.get("geographic_scope", ""),
            "beneficiary_focus": data.get("beneficiary_focus", []),
            "funding_range": data.get("funding_range", ""),
            "restrictions": data.get("restrictions", []),
            "application_status": data.get("application_status", "unclear"),
            "deadline": data.get("deadline", ""),
            "notes": data.get("notes", ""),
            "eligibility": data.get("eligibility", "Low Match"),
            "evidence": data.get("evidence", ""),
        }

        if normalized["eligibility"] not in ELIGIBILITY_ORDER:
            _log(f"Invalid eligibility value from LLM: {normalized['eligibility']}; defaulting to 'Low Match'", "warning")
            normalized["eligibility"] = "Low Match"

        for key in ["applicant_types", "beneficiary_focus", "restrictions"]:
            if isinstance(normalized[key], list):
                normalized[key] = "; ".join(normalized[key]) if normalized[key] else ""

        return normalized

    except json.JSONDecodeError as e:
        _log(f"Invalid JSON from LLM: {e}", "error")
        return {
            "applicant_types": "",
            "geographic_scope": "",
            "beneficiary_focus": "",
            "funding_range": "",
            "restrictions": "",
            "application_status": "unclear",
            "deadline": "",
            "notes": f"JSON parsing error: {str(e)}",
            "eligibility": "Low Match",
            "evidence": f"Error: Could not parse LLM response - {str(e)}",
        }
    except Exception as e:
        _log(f"LLM extraction failed: {e}", "error")
        return {
            "applicant_types": "",
            "geographic_scope": "",
            "beneficiary_focus": "",
            "funding_range": "",
            "restrictions": "",
            "application_status": "unclear",
            "deadline": "",
            "notes": f"Extraction failed: {str(e)}",
            "eligibility": "Low Match",
            "evidence": f"Error: LLM extraction failed - {str(e)}",
        }


def folder_name_for_url(u: str) -> str:
    return safe_filename_from_url(normalize_url(u))


def _require_google_config() -> Tuple[dict, str]:
    if not _SETTINGS.google_service_account or not _SETTINGS.google_sheet_id:
        raise RuntimeError("Google Sheets credentials not configured. Call configure_tools(...) first.")
    return _SETTINGS.google_service_account, _SETTINGS.google_sheet_id


def _get_sheet(retries: int = 3, delay: int = 1):
    creds_info, sheet_id = _require_google_config()

    def try_connect():
        creds = Credentials.from_service_account_info(
            creds_info,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        client = gspread.authorize(creds)
        sh = client.open_by_key(sheet_id)
        return sh.sheet1

    for attempt in range(retries):
        try:
            return try_connect()
        except requests.exceptions.RequestException as exc:
            if attempt == retries - 1:
                _log(f"Network error while contacting Google Sheets: {exc}", "error")
                raise
            time.sleep(delay * (2**attempt))
        except Exception as exc:
            if attempt == retries - 1:
                _log(f"Failed to connect to Google Sheets: {exc}", "error")
                raise
            time.sleep(delay * (2**attempt))


def load_google_sheet_as_dataframe() -> pd.DataFrame:
    """Always loads the Google Sheet fresh, no caching."""
    try:
        ws = _get_sheet()
        data = ws.get_all_records()
        return pd.DataFrame(data)
    except Exception as exc:
        _log(f"Failed to load Google Sheet: {exc}", "error")
        return pd.DataFrame(columns=CSV_COLUMNS)


def canon_funder_url(url: str) -> str:
    """
    Canonical funder URL.

    Special rule:
    - For Charity Commission register URLs:
      Must match EXACT FULL URL (after basic normalization).
    - Otherwise:
      Canonicalize to base domain only.
    """
    if not url:
        return ""

    try:
        u = normalize_url(url)
        parts = urlparse(u)
        domain = parts.netloc.lower().replace("www.", "")

        # SPECIAL CASE: Charity Commission Register
        if domain.startswith("register-of-charities.charitycommission"):
            # exact match, no simplification
            return u.rstrip("/")

        # DEFAULT: return only the domain
        return f"https://{domain}"

    except Exception:
        return url.strip().lower()


def append_to_google_sheet(rows: List[dict]):
    """
    Permanently store results in Google Sheets.
    Each dict in `rows` is one funding record.
    """
    try:
        ws = _get_sheet()

        # Convert dicts to list-of-lists in column order
        data = []
        for r in rows:
            row = [r.get(col, "") for col in CSV_COLUMNS]
            data.append(row)

        ws.append_rows(data, value_input_option="RAW")
        clear_results_cache()
    except Exception as e:
        _log(f"Failed to write to Google Sheets: {e}", "error")


@lru_cache(maxsize=1)
def _load_results_csv_cached() -> pd.DataFrame:
    """Internal cached loader used by load_results_csv()."""
    try:
        ws = _get_sheet()
        values = ws.get_all_values()
        if not values:
            return pd.DataFrame(columns=CSV_COLUMNS)

        header = values[0]
        rows = values[1:]

        df = pd.DataFrame(rows, columns=header)
    except Exception as exc:
        _log(f"Error loading from Google Sheet: {exc}", "error")
        df = pd.DataFrame(columns=CSV_COLUMNS)

    for col in CSV_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    return df[CSV_COLUMNS]


def load_results_csv(force_refresh: bool = False) -> pd.DataFrame:
    """
    Load from Google Sheets (persistent).
    Returns a defensive copy so callers can modify freely.
    """
    if force_refresh:
        _load_results_csv_cached.cache_clear()
    return _load_results_csv_cached().copy()


@lru_cache(maxsize=1)
def _get_already_processed_urls_cached() -> Set[str]:
    df = load_results_csv()
    if "fund_url" in df.columns:
        return {normalize_url(u) for u in df["fund_url"].dropna().astype(str).tolist()}
    return set()


def get_already_processed_urls(force_refresh: bool = False) -> Set[str]:
    if force_refresh:
        clear_results_cache()
    return set(_get_already_processed_urls_cached())


@lru_cache(maxsize=4)
def _get_scraped_domains_cached(save_dir: str) -> Set[str]:
    if not os.path.exists(save_dir):
        return set()
    scraped = set()
    for item in os.listdir(save_dir):
        item_path = os.path.join(save_dir, item)
        if os.path.isdir(item_path):
            scraped.add(item.lower())
    return scraped


def get_scraped_domains(save_dir: str = SAVE_DIR, force_refresh: bool = False) -> Set[str]:
    if force_refresh:
        _get_scraped_domains_cached.cache_clear()
    return set(_get_scraped_domains_cached(save_dir))


def clear_results_cache() -> None:
    """Clear cached Google Sheet results and processed URL sets."""
    _load_results_csv_cached.cache_clear()
    _get_already_processed_urls_cached.cache_clear()


def clear_scraped_domains_cache() -> None:
    """Clear cached scraped-domain lookups."""
    _get_scraped_domains_cached.cache_clear()


def save_results_to_csv(results: List[dict], output_csv: str = OUTPUT_CSV):
    if not results:
        return
    results_df = pd.DataFrame(results)
    for col in CSV_COLUMNS:
        if col not in results_df.columns:
            results_df[col] = ""
    results_df = results_df[CSV_COLUMNS]
    os.makedirs(os.path.dirname(output_csv) or ".", exist_ok=True)
    if os.path.exists(output_csv):
        results_df.to_csv(output_csv, mode="a", index=False, header=False)
    else:
        results_df.to_csv(output_csv, index=False)
    clear_results_cache()


def load_text_from_folder(folder_path: str) -> tuple[str, int, str]:
    txt_files = list(Path(folder_path).glob("*.txt"))
    if not txt_files:
        return "", 0, ""
    combined_text = []
    fund_url = ""
    txt_files.sort()
    for txt_file in txt_files:
        try:
            with open(txt_file, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
                combined_text.append(f"\n=== {txt_file.name} ===\n{content}")
                if not fund_url and "_" in txt_file.name:
                    domain = txt_file.name.split("_")[0]
                    fund_url = f"https://{domain}"
        except Exception as e:
            _log(f"Could not read {txt_file}: {e}", "warning")
            continue
    full_text = "\n".join(combined_text)
    return full_text, len(txt_files), fund_url


# =========================================
# ========== BATCH / SCRAPE FLOW ==========
# =========================================


def process_single_fund(url: str, fund_name: Optional[str] = None) -> dict:
    fund_name = fund_name or urlparse(url).netloc
    result = {"fund_url": url, "fund_name": fund_name, "extraction_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    # TODO: Emit structured log/metric events for each scrape stage to aid backend observability.
    try:
        text, folder, pages_scraped, visited_urls = prioritized_crawl(url)
        if not text or len(text) < 100:
            result["error"] = "Insufficient text extracted"
            _log(f"Insufficient text extracted for {url}", "warning")
            return result
        data = call_llm_extract(text)
        result.update(data)
        result["pages_scraped"] = pages_scraped
        result["visited_urls_count"] = len(visited_urls)
        result["error"] = ""
        try:
            single_df = pd.DataFrame([result])
            for col in CSV_COLUMNS:
                if col not in single_df.columns:
                    single_df[col] = ""
            single_df = single_df[CSV_COLUMNS]
            domain_folder = os.path.join(SAVE_DIR, safe_filename_from_url(url))
            os.makedirs(domain_folder, exist_ok=True)
            single_df.to_csv(os.path.join(domain_folder, "fund_result.csv"), index=False)
        except Exception as e:
            _log(f"Could not write individual CSV for {fund_name}: {e}", "warning")
    except Exception as e:
        # Friendly error mapping
        msg = str(e)
        if "Name or service not known" in msg or "Failed to establish a new connection" in msg:
            _log(f"Network error contacting {url}: {msg}", "error")
        else:
            _log(f"Processing failed for {url}: {msg}", "error")
        result["error"] = msg
    finally:
        # Persist to Google Sheet as soon as each fund finishes (even if errored)
        try:
            append_to_google_sheet([result])
            clear_results_cache()
        except Exception as e:  # pragma: no cover - external dependency
            _log(f"Failed to persist {url} to Google Sheets: {e}", "error")
    return result


# -----------------------------
# BACKGROUND SCRAPE WORKER
# -----------------------------
def start_background_scrape(urls: List[str]) -> ScrapeProgress:
    """
    Kick off a background scrape for the provided URLs.
    Returns a ScrapeProgress object that callers can poll.
    """

    progress = ScrapeProgress(started_at=time.time())
    total = max(len(urls), 1)

    # TODO: push incremental progress updates to the API layer (webhooks/websockets) instead of only polling.
    def worker():
        for idx, url in enumerate(urls, start=1):
            try:
                progress.current_url = url
                progress.current_started_at = time.time()
                res = process_single_fund(url)
                progress.results.append(res)
                if res.get("error"):
                    progress.errors.append((url, res["error"]))
            except Exception as exc:
                progress.errors.append((url, str(exc)))
                res = {"fund_url": url, "error": str(exc)}
            finally:
                if progress.current_started_at:
                    duration = max(0.0, time.time() - progress.current_started_at)
                else:
                    duration = 0.0
                progress.url_timings.append(
                    {
                        "url": url,
                        "duration_seconds": duration,
                        "started_at": progress.current_started_at or time.time(),
                        "finished_at": time.time(),
                        "error": res.get("error") if isinstance(res, dict) else None,
                    }
                )
                progress.current_url = None
                progress.current_started_at = None

            progress.progress_percent = int(idx / total * 100)

        progress.done = True
        progress.finished_at = time.time()

    threading.Thread(target=worker, daemon=True).start()
    return progress
