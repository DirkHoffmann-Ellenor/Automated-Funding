# app.py
# Streamlit web app for ellenor Hospice funding discovery tool
# (Refined navigation, API key vault, login/unlock, improved results table & error handling)

import os, re, csv, time, json, requests, pandas as pd
import streamlit as st
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from openai import OpenAI
from typing import Dict, List, Tuple, Set, Optional
from pathlib import Path
from datetime import datetime
from base64 import urlsafe_b64encode, urlsafe_b64decode
from hashlib import sha256

# Try to use proper encryption if available
try:
    from cryptography.fernet import Fernet
    CRYPTO_OK = True
except Exception:
    CRYPTO_OK = False

# ==============================
# ========== CONSTANTS =========
# ==============================

SAVE_DIR = "Scraped"
DISCOVERY_DEPTH = 2
MAX_PAGES = 15
MAX_DISCOVERY_PAGES = 200
PAUSE_BETWEEN_REQUESTS = 1.0
HEADERS = {"User-Agent": "ellenor-funding-bot/priority/1.0 (+https://ellenor.org)"}
OUTPUT_CSV = "funds_results_reprocessed.csv"
INPUT_CSV = "funds.csv"  # optional helper
ELIGIBILITY_ORDER = ["Highly Eligible", "Eligible", "Possibly Eligible", "Low Match", "Not Eligible"]

# Local Vault location (for API key persistence)
VAULT_DIR = Path.home() / ".ellenor_funding"
VAULT_PATH = VAULT_DIR / "vault.json"

ELLENOR_PROFILE = {
    "name": "ellenor Hospice",
    "location": "Coldharbour Road, Gravesend, Kent, DA11 7HQ, UK",
    "service_area": [
        "Dartford", "Gravesham", "Swanley",
        "London Borough of Bexley (children's services)"
    ],
    "beneficiaries": "Babies, children, young people, adults, and families receiving palliative and end-of-life care",
    "core_services": [
        "Inpatient Ward",
        "Hospice at Home (90% of patients cared for at home)",
        "Living Well programme",
        "Children's Hospice at Home",
        "Bereavement and counselling support",
        "Music and play therapy",
        "Complementary therapies",
        "Spiritual and emotional support",
        "Support for carers and families"
    ],
    "staff": 152,
    "volunteers": 333,
    "annual_income": 8209780,
    "income_sources": {
        "Donations and legacies": 3400000,
        "Charitable activities": 2420000,
        "Trading (shops, lottery, etc.)": 2260000,
        "Investments": 62200,
        "Other": 66800,
        "Government grants": 2276252
    },
    "annual_expenditure": 7809224,
    "expenditure_breakdown": {
        "Charitable activities": 5990000,
        "Fundraising": 1810000,
        "Other": 8880
    },
    "strategic_priorities": [
        "Develop a new Wellbeing Centre",
        "Advance Equality, Diversity and Inclusion",
        "Improve service delivery and access",
        "Expand community engagement and partnerships"
    ],
}

LLM_PROMPT = """
You are a precise information extractor for charity funding opportunities, specifically evaluating eligibility for ellenor Hospice.

=== ELLENOR HOSPICE PROFILE ===
- Name: ellenor Hospice
- Location: Gravesend, Kent, UK (serves Dartford, Gravesham, Swanley, Bexley)
- Services: Palliative and end-of-life care for babies, children, young people, adults, and families
- Core activities: Inpatient Ward, Hospice at Home, Living Well programme, Children's Hospice at Home, Bereavement support, Music/play therapy, Complementary therapies
- Staff: 152 | Volunteers: 333
- Annual income: £8.2M (donations, charitable activities, trading, government grants)
- Type: Registered UK charity providing hospice and palliative care services

=== YOUR TASK ===
Extract funding information AND determine eligibility for ellenor Hospice.

Return ONLY valid JSON with this exact structure:

{
  "applicant_types": ["list", "of", "eligible", "applicant", "types"],
  "geographic_scope": "geographic area covered (e.g., UK, England, Kent, London)",
  "beneficiary_focus": ["target", "beneficiary", "groups"],
  "funding_range": "minimum and maximum amounts if stated (e.g., £1,000 - £10,000)",
  "restrictions": ["explicit", "exclusions", "or", "restrictions"],
  "application_status": "open|closed|paused|rolling|seasonal|unclear",
  "deadline": "application deadline if mentioned",
  "notes": "any other critical eligibility requirements",
  "eligibility": "Highly Eligible|Eligible|Possibly Eligible|Low Match|Not Eligible",
  "evidence": "detailed explanation of eligibility determination with specific reasons"
}

=== EVIDENCE GUIDELINES ===
In the "evidence" field, provide a clear explanation including:
1. Key matching factors
2. Concerns or limitations
3. Specific quotes or facts from the page that support your assessment
4. Overall recommendation

=== PAGE TEXT START ===
{text}
=== PAGE TEXT END ===

Return ONLY the JSON object, no additional text.
"""

CSV_COLUMNS = [
    "fund_url",
    "fund_name",
    "applicant_types",
    "geographic_scope",
    "beneficiary_focus",
    "funding_range",
    "restrictions",
    "application_status",
    "deadline",
    "notes",
    "eligibility",
    "evidence",
    "pages_scraped",
    "visited_urls_count",
    "extraction_timestamp",
    "error"
]

KEYWORDS = ["grant", "grants", "apply", "fund", "funding", "eligible", "eligibility",
            "criteria", "who-can-apply", "what-we-fund", "apply-for", "apply-for-funding",
            "support", "programme", "award", "awarded", "application", "guidelines"]


# ==========================================
# ========== SESSION / NAVIGATION ==========
# ==========================================

def init_session():
    if "api_key" not in st.session_state:
        st.session_state.api_key = os.getenv("APIKEY") or os.getenv("OPENAI_API_KEY") or ""
    if "logs" not in st.session_state:
        st.session_state.logs = []
    if "last_run_results" not in st.session_state:
        st.session_state.last_run_results = []
    if "page" not in st.session_state:
        st.session_state.page = "Scrape & Analyze"
    if "unlocked" not in st.session_state:
        st.session_state.unlocked = False  # “login” (passphrase unlock) state


def set_sidebar_nav():
    with st.sidebar:
        st.title("ellenor Funding")
        st.caption("Scrape → Analyze → Review results")

        # Clear, simple nav (no radio widgets)
        if st.button("🌐 Scrape & Analyze", use_container_width=True):
            st.session_state.page = "Scrape & Analyze"
        if st.button("📊 Results", use_container_width=True):
            st.session_state.page = "Results"
        if st.button("⚙️ Settings", use_container_width=True):
            st.session_state.page = "Settings"

        st.markdown("---")
        # API Key status
        key_status = "Loaded" if st.session_state.api_key.strip() else "Not set"
        st.metric("API key", key_status)

        # CSV location preview
        st.caption("Results CSV")
        st.code(str(Path(OUTPUT_CSV).resolve()), language="text")
        if Path(OUTPUT_CSV).exists():
            st.success("Results file found.")
        else:
            st.info("No results file yet.")
        if st.button("Reveal results folder", use_container_width=True):
            st.info(f"Directory: {Path(OUTPUT_CSV).resolve().parent}")


# ==================================
# ========== API KEY VAULT =========
# ==================================

def _derive_fernet_key(passphrase: str) -> bytes:
    # Simple deterministic derivation (not PBKDF2, but avoids extra deps).
    # We hash passphrase into 32 bytes and base64-url encode for Fernet.
    h = sha256(passphrase.encode("utf-8")).digest()
    return urlsafe_b64encode(h)

def vault_save_api_key(passphrase: str, api_key: str):
    VAULT_DIR.mkdir(parents=True, exist_ok=True)
    payload: Dict[str, str]
    if CRYPTO_OK:
        fkey = _derive_fernet_key(passphrase)
        f = Fernet(fkey)
        token = f.encrypt(api_key.encode("utf-8")).decode("utf-8")
        payload = {"method": "fernet", "ciphertext": token}
    else:
        # Fallback (not secure) – warn loudly
        payload = {"method": "plain", "api_key": api_key}
    VAULT_PATH.write_text(json.dumps(payload))

def vault_load_api_key(passphrase: str) -> Optional[str]:
    if not VAULT_PATH.exists():
        return None
    data = json.loads(VAULT_PATH.read_text())
    method = data.get("method")
    if method == "fernet" and CRYPTO_OK:
        try:
            fkey = _derive_fernet_key(passphrase)
            f = Fernet(fkey)
            token = data.get("ciphertext", "")
            return f.decrypt(token.encode("utf-8")).decode("utf-8")
        except Exception:
            return None
    elif method == "plain":
        return data.get("api_key")
    return None

def vault_exists() -> bool:
    return VAULT_PATH.exists()

def get_client() -> Optional[OpenAI]:
    api_key = st.session_state.get("api_key", "").strip()
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
    parsed = urlparse(url)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc
    path = parsed.path.rstrip("/")
    qs = ("?" + parsed.query) if parsed.query else ""
    return f"{scheme}://{netloc}{path}{qs}"

def initial_normalize_url(url: str) -> str:
    """Normalize a URL for consistent crawling & deduplication."""
    url = url.strip()
    if not url:
        return url

    parsed = urlparse(url)

    # --- fix scheme ---
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc.lower().replace("www.", "")

    # --- clean up path ---
    path = parsed.path or "/"
    path = re.sub(r"/+$", "", path)  # remove trailing slashes

    # Special case: Charity Commission URLs often have an ID after 'charity-details/'
    # e.g. .../charity-details/1010625/charity-overview -> keep only /charity-details/1010625
    if "charitycommission.gov.uk" in netloc and "/charity-details/" in path:
        match = re.search(r"(/charity-details/\d+)", path)
        if match:
            path = match.group(1)

    # --- strip fragments ---
    fragment = ""

    # --- remove tracking query params ---
    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    clean_query = [(k, v) for (k, v) in query_pairs if not k.lower().startswith(("utm_", "fbclid", "gclid"))]
    query = urlencode(clean_query)

    # --- reconstruct canonical URL ---
    normalized = urlunparse((scheme, netloc, path, "", query, fragment))

    # For most cases, if the query is very long or mostly numeric noise, drop it entirely
    if len(query) > 80 or any(ch in query for ch in ["=", "_"]):
        normalized = f"{scheme}://{netloc}{path}"

    return normalized


def _log(msg: str, level: str = "info"):
    st.session_state.logs.append((level, msg))
    if level == "error":
        st.error(msg)
    elif level == "warning":
        st.warning(msg)
    elif level == "success":
        st.success(msg)
    else:
        st.write(msg)

def fetch_page(url: str, retries: int = 4, backoff_factor: int = 2) -> Optional[str]:
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", (backoff_factor ** attempt) * 5))
                _log(f"Rate limited ({resp.status_code}) – pausing {wait}s", "warning")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.text
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                pause = (backoff_factor ** attempt) * 2
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

def discover_links(seed_url: str, discovery_depth: int = DISCOVERY_DEPTH,
                   max_pages: int = MAX_DISCOVERY_PAGES) -> Dict:
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
        if (p := soup.find("p")):
            snippet = p.get_text(" ", strip=True)[:300]

        for a in soup.find_all("a", href=True):
            href = urljoin(url, a["href"].split("#")[0])
            if not href.startswith("http"):
                continue
            if base_domain not in urlparse(href).netloc:
                continue
            if any(href.lower().endswith(ext) for ext in [".pdf", ".jpg", ".jpeg", ".png",
                                                           ".zip", ".mp4", ".doc", ".docx"]):
                continue
            hnorm = normalize_url(href)
            anchor = (a.get_text(" ", strip=True) or "").strip()
            meta = candidates.setdefault(hnorm, {
                "anchor_texts": set(),
                "source_titles": set(),
                "source_snippets": set()
            })
            if anchor:
                meta["anchor_texts"].add(anchor)
            if title:
                meta["source_titles"].add(title)
            if snippet:
                meta["source_snippets"].add(snippet)
            if hnorm not in visited and depth + 1 <= discovery_depth:
                queue.append((hnorm, depth + 1))

        time.sleep(PAUSE_BETWEEN_REQUESTS)

    _log(f"➕ Found {len(candidates)} internal links (visited {pages_visited} pages)")
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
    seed_norm = normalize_url(seed_url)
    domain_folder = os.path.join(SAVE_DIR, safe_filename_from_url(seed_norm))
    os.makedirs(domain_folder, exist_ok=True)

    candidates = discover_links(seed_norm)
    candidates.setdefault(seed_norm, {
        "anchor_texts": set(),
        "source_titles": set(),
        "source_snippets": set()
    })
    scored = [(score_candidate(url, meta), url) for url, meta in candidates.items()]
    scored.sort(reverse=True)

    top_links = [url for _, url in scored[:MAX_PAGES]]
    _log(f"🌐 Fetching top {len(top_links)} links from {seed_norm}")

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
            "evidence": "LLM extraction skipped (no API key)."
        }

    max_chars = 50000
    if len(text) > max_chars:
        _log(f"Text truncated from {len(text)} to {max_chars} chars", "warning")
        text = text[:max_chars//2] + "\n...[content truncated]...\n" + text[-max_chars//2:]

    prompt = LLM_PROMPT.replace("{text}", text)

    try:
        resp = client.chat.completions.create(
            model="gpt-4.1",
            messages=[
                {"role": "system", "content": "You are an expert at evaluating charity funding eligibility. You extract structured data and provide accurate eligibility assessments based on specific criteria."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            response_format={"type": "json_object"}
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
            "evidence": data.get("evidence", "")
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
            "evidence": f"Error: Could not parse LLM response - {str(e)}"
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
            "evidence": f"Error: LLM extraction failed - {str(e)}"
        }

# --- Scraped manager helpers ---

def list_scraped_folders(save_dir: str = SAVE_DIR) -> List[str]:
    if not os.path.exists(save_dir):
        return []
    return sorted([d for d in os.listdir(save_dir) if os.path.isdir(os.path.join(save_dir, d))])

def delete_scraped_folder(folder_name: str, save_dir: str = SAVE_DIR) -> bool:
    try:
        import shutil
        path = os.path.join(save_dir, folder_name)
        if os.path.isdir(path):
            shutil.rmtree(path)
            # Clear caches that depend on this listing
            get_scraped_domains.clear()
            return True
    except Exception:
        pass
    return False

def folder_name_for_url(u: str) -> str:
    # consistent with safe_filename_from_url(normalized_url)
    return safe_filename_from_url(normalize_url(u))


# ====================================
# ========== CSV / CACHE LAYER =======
# ====================================

@st.cache_data(show_spinner=False)
def load_results_csv(path: str = OUTPUT_CSV) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=CSV_COLUMNS)
    try:
        df = pd.read_csv(path)
        for col in CSV_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        return df[CSV_COLUMNS]
    except Exception:
        return pd.DataFrame(columns=CSV_COLUMNS)

@st.cache_data(show_spinner=False)
def get_already_processed_urls(output_csv: str = OUTPUT_CSV) -> Set[str]:
    df = load_results_csv(output_csv)
    if "fund_url" in df.columns:
        return {normalize_url(u) for u in df["fund_url"].dropna().astype(str).tolist()}
    return set()

@st.cache_data(show_spinner=False)
def get_scraped_domains(save_dir: str = SAVE_DIR) -> Set[str]:
    if not os.path.exists(save_dir):
        return set()
    scraped = set()
    for item in os.listdir(save_dir):
        item_path = os.path.join(save_dir, item)
        if os.path.isdir(item_path):
            scraped.add(item.lower())
    return scraped

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
    load_results_csv.clear()
    get_already_processed_urls.clear()

def load_text_from_folder(folder_path: str) -> tuple[str, int, str]:
    txt_files = list(Path(folder_path).glob("*.txt"))
    if not txt_files:
        return "", 0, ""
    combined_text = []
    fund_url = ""
    txt_files.sort()
    for txt_file in txt_files:
        try:
            with open(txt_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                combined_text.append(f"\n=== {txt_file.name} ===\n{content}")
                if not fund_url and "_" in txt_file.name:
                    domain = txt_file.name.split('_')[0]
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
    result = {
        "fund_url": url,
        "fund_name": fund_name,
        "extraction_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
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
    return result


# ===============================
# ========== UI PAGES ===========
# ===============================

def _eligibility_color(val: str) -> str:
    palette = {
        "Highly Eligible": "#1f9d55",  # green
        "Eligible": "#2d7dff",         # blue
        "Possibly Eligible": "#b7791f",# amber
        "Low Match": "#6b7280",        # gray
        "Not Eligible": "#dc2626"      # red
    }
    return palette.get(val, "#6b7280")

def _results_metrics(df: pd.DataFrame):
    total = len(df)
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1: st.metric("Total processed", total)
    for idx, (col, label) in enumerate([
        ("Highly Eligible", "Highly Eligible"),
        ("Eligible", "Eligible"),
        ("Possibly Eligible", "Possibly Eligible"),
        ("Not Eligible", "Not Eligible")
    ]):
        count = (df["eligibility"] == col).sum() if "eligibility" in df.columns else 0
        [col2, col3, col4, col5][idx].metric(label, count)

def page_results():
    st.title("📊 Results")
    st.caption("Browse, filter, and export analyzed funds. Click URLs to open funding pages.")

    df = load_results_csv(OUTPUT_CSV)
    if df.empty:
        st.info("No results yet. Use **Scrape & Analyze** to add funds.")
        return

    # Reorder: fund_url → eligibility → fund_name → rest
    ordered_cols = (
        ["fund_url", "eligibility", "fund_name",
         "application_status", "deadline", "funding_range",
         "geographic_scope", "applicant_types", "beneficiary_focus",
         "restrictions", "notes", "evidence",
         "pages_scraped", "visited_urls_count", "extraction_timestamp", "error"]
    )
    df = df[[c for c in ordered_cols if c in df.columns]]

    # Filters
    with st.expander("Filters", expanded=True):
        elig_filter = st.multiselect("Eligibility", ELIGIBILITY_ORDER, default=ELIGIBILITY_ORDER)
        keyword = st.text_input("Keyword (URL, name, notes, evidence)")
        f = df.copy()
        if elig_filter:
            f = f[f["eligibility"].isin(elig_filter)]
        if keyword.strip():
            kw = keyword.lower()
            mask = (
                f["fund_url"].fillna("").str.lower().str.contains(kw) |
                f["fund_name"].fillna("").str.lower().str.contains(kw) |
                f["notes"].fillna("").str.lower().str.contains(kw) |
                f["evidence"].fillna("").str.lower().str.contains(kw)
            )
            f = f[mask]

    _results_metrics(f)

    # Clickable links & nicer column labels
    colcfg = {
        "fund_url": st.column_config.LinkColumn("Fund URL"),
        "eligibility": st.column_config.TextColumn("Eligibility"),
        "fund_name": st.column_config.TextColumn("Fund Name"),
        "application_status": st.column_config.TextColumn("Status"),
        "deadline": st.column_config.TextColumn("Deadline"),
        "funding_range": st.column_config.TextColumn("Funding Range"),
        "geographic_scope": st.column_config.TextColumn("Scope"),
        "applicant_types": st.column_config.TextColumn("Applicant Types"),
        "beneficiary_focus": st.column_config.TextColumn("Beneficiaries"),
        "restrictions": st.column_config.TextColumn("Restrictions"),
        "notes": st.column_config.TextColumn("Notes"),
        "evidence": st.column_config.TextColumn("Evidence"),
        "pages_scraped": st.column_config.NumberColumn("Pages"),
        "visited_urls_count": st.column_config.NumberColumn("Links Visited"),
        "extraction_timestamp": st.column_config.TextColumn("Extracted At"),
        "error": st.column_config.TextColumn("Error")
    }

    st.dataframe(f, use_container_width=True, height=480, column_config=colcfg)

    st.download_button(
        "Download Filtered CSV",
        data=f.to_csv(index=False).encode("utf-8"),
        file_name="funds_results_filtered.csv",
        mime="text/csv"
    )

    with st.expander("Evidence details"):
        for _, row in f.iterrows():
            color = _eligibility_color(row.get("eligibility",""))
            st.markdown(
                f"**{row.get('fund_name','(unknown)')}** — "
                f"[{row.get('fund_url','')}]({row.get('fund_url','')}) · "
                f"<span style='color:{color};font-weight:600'>{row.get('eligibility','')}</span>",
                unsafe_allow_html=True
            )
            st.caption(f"Status: {row.get('application_status','')} · Deadline: {row.get('deadline','')}")
            st.write(row.get("evidence","") or "_No evidence captured_")
            st.divider()


def page_scrape():
    st.title("🌐 Scrape & Analyze")
    st.caption("Paste URLs or upload a CSV with `fund_url`. We’ll skip items already processed or scraped.")

    # API key helper
    if not st.session_state.api_key.strip():
        st.warning("OpenAI API key not set. Scraping will run, but LLM extraction will be skipped (eligibility = low-confidence).")
        with st.expander("How to enable LLM extraction"):
            st.write("Go to **Settings → API Key** to enter a key, or unlock from your Local Vault.")

    st.subheader("Provide funding URLs")
    urls_text = st.text_area("One per line", height=120, placeholder="https://funder.org/grants\nhttps://another.org/funding-programme")
    st.write("**Or** upload a CSV that contains a `fund_url` column.")
    up = st.file_uploader("Upload CSV", type=["csv"])

    input_urls: List[str] = []
    if urls_text.strip():
        input_urls.extend([u.strip() for u in urls_text.strip().splitlines() if u.strip()])
    if up is not None:
        try:
            df_in = pd.read_csv(up)
            if "fund_url" not in df_in.columns:
                st.error("Uploaded CSV must include a `fund_url` column.")
            else:
                input_urls.extend([str(u).strip() for u in df_in["fund_url"].dropna().tolist()])
        except Exception as e:
            st.error(f"Could not read uploaded CSV: {e}")

    input_urls = [normalize_url(u) for u in input_urls]
    input_urls = list(dict.fromkeys(input_urls))  # dedupe

    if not input_urls:
        st.info("Add URLs or upload a CSV to proceed.")
        return
    

    force_rescrape = st.checkbox("Force re-scrape even if a folder exists", value=False, help="Ignores the 'already scraped' skip. Useful when you want a fresh crawl.")
    purge_before_rescrape = st.checkbox("Purge existing folder before force re-scrape", value=False, help="Deletes the old scraped folder first (safer to avoid mixing old/new pages).")

    st.subheader("Duplicate check")
    processed_urls = get_already_processed_urls(OUTPUT_CSV)
    scraped_domains = get_scraped_domains(SAVE_DIR)

    will_skip, will_process = [], []
    for u in input_urls:
        domain_key = folder_name_for_url(u).lower()
        if u in processed_urls and not force_rescrape:
            will_skip.append((u, "Already in results CSV"))
        elif (domain_key in scraped_domains) and not force_rescrape:
            will_skip.append((u, "Already scraped (folder exists)"))
        else:
            # If forcing, optionally purge the old folder
            if force_rescrape and (domain_key in scraped_domains) and purge_before_rescrape:
                if delete_scraped_folder(domain_key, SAVE_DIR):
                    st.info(f"Purged existing scraped folder for {u}")
                else:
                    st.warning(f"Could not purge scraped folder for {u}; continuing anyway.")
            will_process.append(u)


    if will_skip:
        with st.expander("Skipped (to save time & cost)", expanded=True):
            for u, reason in will_skip:
                st.write(f"⏭️  {u} — {reason}")

    if will_process:
        st.success(f"{len(will_process)} URL(s) ready to process.")
        with st.expander("Show list to be processed"):
            for u in will_process:
                st.write(f"- {u}")

    go = st.button("🚀 Start", type="primary", use_container_width=True, disabled=len(will_process)==0)
    if not go:
        return

    # Clear logs and prep trackers
    st.session_state.logs = []
    progress = st.progress(0)
    status = st.empty()
    results: List[dict] = []
    errs = []

    os.makedirs(SAVE_DIR, exist_ok=True)

    for i, url in enumerate(will_process, start=1):
        status.info(f"Processing {i}/{len(will_process)} — {url}")
        try:
            with st.spinner(f"Crawling and analyzing: {url}"):
                res = process_single_fund(url)
                results.append(res)
                if res.get("error"):
                    errs.append((url, res["error"]))

                # Per-fund preview
                with st.expander(f"Result: {res.get('fund_name') or url}", expanded=False):
                    if res.get("error"):
                        st.error(res["error"])
                    else:
                        c1, c2, c3 = st.columns([2,1,1])
                        with c1:
                            st.markdown(f"**URL:** [{res.get('fund_url','')}]({res.get('fund_url','')})")
                            st.markdown(f"**Funding range:** {res.get('funding_range','')}")
                            st.markdown(f"**Scope:** {res.get('geographic_scope','')}")
                        with c2:
                            st.metric("Pages scraped", int(res.get("pages_scraped") or 0))
                            st.metric("Links visited", int(res.get("visited_urls_count") or 0))
                        with c3:
                            color = _eligibility_color(res.get("eligibility",""))
                            st.markdown("**Eligibility**")
                            st.markdown(f"<span style='color:{color};font-weight:700'>{res.get('eligibility','')}</span>", unsafe_allow_html=True)

                        with st.expander("Evidence", expanded=False):
                            st.write(res.get("evidence", "") or "_No evidence recorded_")
                        with st.expander("Notes / Restrictions / Applicant types", expanded=False):
                            st.write(f"**Notes:** {res.get('notes','')}")
                            st.write(f"**Restrictions:** {res.get('restrictions','')}")
                            st.write(f"**Applicant types:** {res.get('applicant_types','')}")

                # Append to master CSV as we go
                try:
                    save_results_to_csv([res], OUTPUT_CSV)
                except Exception as e:
                    st.error(f"Could not save to master CSV: {e}")

        except Exception as e:
            errs.append((url, str(e)))
            st.error(f"Unexpected error on {url}: {e}")

        progress.progress(int(i/len(will_process)*100))

    status.success("Done.")

    # Error summary (cleaner, grouped)
    if errs:
        st.subheader("Issues encountered")
        grouped = {}
        for u, e in errs:
            key = "Network" if any(k in e for k in ["Name or service", "Failed to establish", "timeout"]) else \
                  "Access/HTTP" if any(k in e for k in ["403", "404", "429", "5"]) else \
                  "Other"
            grouped.setdefault(key, []).append((u, e))
        for g, items in grouped.items():
            with st.expander(f"{g} errors ({len(items)})", expanded=False):
                for u, e in items:
                    st.write(f"• **{u}** — {e}")

    # Batch table
    st.subheader("This batch")
    df_new = pd.DataFrame(results)
    if not df_new.empty:
        for col in CSV_COLUMNS:
            if col not in df_new.columns:
                df_new[col] = ""
        # reorder like Results page
        ordered_cols = ["fund_url", "eligibility", "fund_name", "application_status", "deadline",
                        "funding_range", "geographic_scope", "applicant_types", "beneficiary_focus",
                        "restrictions", "notes", "evidence",
                        "pages_scraped", "visited_urls_count", "extraction_timestamp", "error"]
        df_new = df_new[[c for c in ordered_cols if c in df_new.columns]]

        colcfg = {
            "fund_url": st.column_config.LinkColumn("Fund URL", display_text="Open"),
            "eligibility": st.column_config.TextColumn("Eligibility"),
            "fund_name": st.column_config.TextColumn("Fund Name"),
            "application_status": st.column_config.TextColumn("Status"),
            "deadline": st.column_config.TextColumn("Deadline"),
            "funding_range": st.column_config.TextColumn("Funding Range"),
            "geographic_scope": st.column_config.TextColumn("Scope"),
            "applicant_types": st.column_config.TextColumn("Applicant Types"),
            "beneficiary_focus": st.column_config.TextColumn("Beneficiaries"),
            "restrictions": st.column_config.TextColumn("Restrictions"),
            "notes": st.column_config.TextColumn("Notes"),
            "evidence": st.column_config.TextColumn("Evidence"),
            "pages_scraped": st.column_config.NumberColumn("Pages"),
            "visited_urls_count": st.column_config.NumberColumn("Links Visited"),
            "extraction_timestamp": st.column_config.TextColumn("Extracted At"),
            "error": st.column_config.TextColumn("Error")
        }
        st.dataframe(df_new, use_container_width=True, height=420, column_config=colcfg)
        st.download_button(
            "Download This Batch (CSV)",
            data=df_new.to_csv(index=False).encode("utf-8"),
            file_name="funds_results_batch.csv",
            mime="text/csv"
        )
    else:
        st.info("No results produced.")
            
    st.markdown("---")
    st.subheader("🔁 Reprocess from scraped text (LLM only)")
    st.caption("Reads text files already in `Scraped/` and re-runs the LLM extractor without re-crawling.")

    folders = list_scraped_folders(SAVE_DIR)
    if not folders:
        st.info("No scraped folders found yet.")
    else:
        pick = st.selectbox("Choose a scraped folder", folders, index=0)
        r1, r2 = st.columns([1,1])
        with r1:
            reprocess_now = st.button("Run LLM Extraction on Selected Folder", use_container_width=True)
        with r2:
            del_now = st.button("Delete Selected Folder", type="secondary", use_container_width=True)

        if reprocess_now:
            if not st.session_state.api_key.strip():
                st.error("Please set your OpenAI API key first (Settings).")
            else:
                full_path = os.path.join(SAVE_DIR, pick)
                combined_text, file_count, url_guess = load_text_from_folder(full_path)
                if not combined_text or len(combined_text) < 100:
                    st.warning(f"Insufficient text in {pick} ({len(combined_text)} chars).")
                else:
                    data = call_llm_extract(combined_text)
                    row = {
                        "fund_url": url_guess or "",
                        "fund_name": pick,
                        "extraction_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "pages_scraped": file_count,
                        "visited_urls_count": file_count,
                        "error": ""
                    }
                    row.update(data)
                    save_results_to_csv([row], OUTPUT_CSV)
                    st.success(f"Saved reprocessed result to {OUTPUT_CSV}")
                    st.json({k: row.get(k) for k in ["fund_url","eligibility","application_status","deadline","funding_range","notes"]})

        if del_now:
            if delete_scraped_folder(pick, SAVE_DIR):
                st.success(f"Deleted folder: {pick}")
            else:
                st.error("Could not delete folder. Check permissions.")



def page_settings():
    st.title("⚙️ Settings")
    st.caption("Manage API key, Local Vault, and utilities.")

    st.subheader("API Key")
    st.write("Set an OpenAI API key for LLM extraction.")
    key = st.text_input("OpenAI API key", value=st.session_state.api_key, type="password", help="Used only in this session unless saved to Local Vault.")
    b1, b2 = st.columns([1,1])
    with b1:
        if st.button("Save to Session", use_container_width=True):
            st.session_state.api_key = key.strip()
            st.success("API key saved to session.")
    with b2:
        if st.button("Clear from Session", type="secondary", use_container_width=True):
            st.session_state.api_key = ""
            st.warning("API key cleared from session.")

    st.markdown("---")
    st.subheader("🔐 Local Vault (persist key across refresh/restart)")
    if CRYPTO_OK:
        st.caption("Your key is encrypted using a passphrase (Fernet).")
    else:
        st.caption("cryptography not installed — will store **plain text** (not recommended). `pip install cryptography` to enable encryption.")

    colA, colB = st.columns(2)
    with colA:
        passphrase = st.text_input("App Passphrase", type="password", help="Used to encrypt/decrypt the API key on this machine.")
    with colB:
        st.write("")
        if st.button("Save API key to Local Vault", use_container_width=True, disabled=not passphrase or not key.strip()):
            try:
                vault_save_api_key(passphrase, key.strip())
                st.success(f"Saved to {VAULT_PATH}")
            except Exception as e:
                st.error(f"Failed to save: {e}")

    colC, colD = st.columns(2)
    with colC:
        unlock_pw = st.text_input("Unlock with Passphrase", type="password")
    with colD:
        st.write("")
        if st.button("Unlock & Load Key", use_container_width=True, disabled=not unlock_pw or not vault_exists()):
            loaded = vault_load_api_key(unlock_pw)
            if loaded:
                st.session_state.api_key = loaded
                st.session_state.unlocked = True
                st.success("Key loaded into session.")
            else:
                st.error("Unlock failed. Check your passphrase or vault availability.")

    if vault_exists():
        st.info(f"Vault file: `{VAULT_PATH}`")
        if st.button("Delete Local Vault", use_container_width=True):
            try:
                VAULT_PATH.unlink(missing_ok=True)
                st.success("Vault deleted.")
            except Exception as e:
                st.error(f"Could not delete vault: {e}")
    else:
        st.info("No Local Vault found yet.")

    st.markdown("---")
    st.subheader("Data Locations")
    st.write(f"- Results CSV: `{Path(OUTPUT_CSV).resolve()}`")
    st.write(f"- Scraped text folder: `{Path(SAVE_DIR).resolve()}/`")
    st.caption("Paths are on the machine where Streamlit is running.")


# ===============================
# ========== NAV / MAIN =========
# ===============================

def main():
    st.set_page_config(page_title="ellenor Auto Funding Discovery", page_icon="Logo.png", layout="wide")
    init_session()
    set_sidebar_nav()

    page = st.session_state.page
    if page == "Results":
        page_results()
    elif page == "Settings":
        page_settings()
    else:
        page_scrape()  # default / main flow

if __name__ == "__main__":
    main()
