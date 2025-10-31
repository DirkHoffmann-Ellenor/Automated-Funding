# app.py
# Streamlit web app for ellenor Hospice funding discovery tool
# Converts the provided CLI script into a reactive, user-friendly UI.

import os, re, csv, time, json, requests, pandas as pd
import streamlit as st
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from openai import OpenAI
from typing import Dict, List, Tuple, Set, Callable, Optional
from pathlib import Path
from datetime import datetime

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
INPUT_CSV = "funds.csv"  # still supported on Settings tab (optional helper)
ELIGIBILITY_ORDER = ["Highly Eligible", "Eligible", "Possibly Eligible", "Low Match", "Not Eligible"]

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

=== ELIGIBILITY ASSESSMENT GUIDELINES ===

**Highly Eligible** - Use when 4+ of these are true:
- Hospices/palliative care organisations explicitly mentioned OR charities in health/care sector eligible
- Geographic scope includes Kent/South East/UK-wide
- Beneficiaries include palliative care patients, people with life-limiting conditions, children, families, or bereaved people
- Funding amount suitable (£1k-£500k range)
- Applications are open/rolling
- No restrictions excluding hospices or healthcare charities

**Eligible** - Use when 3+ criteria match:
- Registered charities eligible (even if hospices not specifically mentioned)
- Geographic scope includes England or broader regions including Kent
- Beneficiaries include health, wellbeing, or vulnerable people
- Reasonable funding available
- Applications open or status unclear but likely available
- No major restrictions

**Possibly Eligible** - Use when 2+ criteria match or when:
- Applicant type unclear but could include charities
- Geographic scope broad enough to potentially include Kent
- Beneficiary focus tangentially related (e.g., general community support)
- Some restrictions but not directly excluding hospices

**Low Match** - Use when:
- Only 1 criterion matches
- Geographic scope excludes Kent but includes UK
- Beneficiary focus is very different but not explicitly excluding palliative care
- Funding too small (<£1k) but still available

**Not Eligible** - Use when ANY of these are true:
- Applications closed/paused with no reopening date
- Geographic scope explicitly excludes Kent/South East
- Applicant types exclude charities or healthcare organizations
- Restrictions explicitly exclude hospices, palliative care, or health services
- Only for-profit organizations or individuals eligible

=== EVIDENCE GUIDELINES ===
In the "evidence" field, provide a clear explanation including:
1. Key matching factors (e.g., "Geographic scope includes Kent", "Explicitly welcomes hospice applications")
2. Any concerns or limitations (e.g., "Competitive funding", "Deadline approaching")
3. Specific quotes or facts from the page that support your assessment
4. Overall recommendation or next steps

Be specific and factual. Avoid generic statements.

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
# ========== SESSION / API KEY UX ==========
# ==========================================

def init_session():
    if "api_key" not in st.session_state:
        # try environment only once for convenience; UI still preferred
        st.session_state.api_key = os.getenv("APIKEY") or os.getenv("OPENAI_API_KEY") or ""
    if "logs" not in st.session_state:
        st.session_state.logs = []
    if "last_run_results" not in st.session_state:
        st.session_state.last_run_results = []  # list of dict rows


def set_api_key_ui():
    with st.sidebar:
        st.subheader("🔐 API Key")
        st.write("Provide your OpenAI API key to enable LLM extraction.")
        key = st.text_input("Enter your OpenAI API key", value=st.session_state.api_key, type="password")
        col1, col2 = st.columns([1,1])
        with col1:
            if st.button("Save Key", use_container_width=True):
                st.session_state.api_key = key.strip()
                st.success("API key saved for this session.")
        with col2:
            if st.button("Clear Key", type="secondary", use_container_width=True):
                st.session_state.api_key = ""
                st.warning("API key cleared. Scraping will run without LLM extraction.")


def get_client() -> Optional[OpenAI]:
    """Create a fresh OpenAI client for each call, avoiding stale globals."""
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

def _log(msg: str, level: str = "info"):
    """Accumulate logs & render immediately."""
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
                _log(f"Fetch failed for {url} ({e}); retrying in {pause}s", "warning")
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
    """Extract structured data including LLM-determined eligibility."""
    client = get_client()
    if client is None:
        # LLM disabled – return placeholder
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

        # Convert lists to strings for CSV storage
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


# ====================================
# ========== CSV / CACHE LAYER =======
# ====================================

@st.cache_data(show_spinner=False)
def load_results_csv(path: str = OUTPUT_CSV) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=CSV_COLUMNS)
    try:
        df = pd.read_csv(path)
        # ensure all expected columns exist
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
    """Append-safe write with column alignment."""
    if not results:
        return
    results_df = pd.DataFrame(results)
    for col in CSV_COLUMNS:
        if col not in results_df.columns:
            results_df[col] = ""
    results_df = results_df[CSV_COLUMNS]
    # Basic single-writer semantics for Streamlit:
    os.makedirs(os.path.dirname(output_csv) or ".", exist_ok=True)
    if os.path.exists(output_csv):
        results_df.to_csv(output_csv, mode="a", index=False, header=False)
    else:
        results_df.to_csv(output_csv, index=False)
    # Invalidate cache
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
        _log(f"✅ Crawled {pages_scraped} pages. Saved in `{folder}`", "success")
        if not text or len(text) < 100:
            result["error"] = "Insufficient text extracted"
            return result
        data = call_llm_extract(text)
        result.update(data)
        result["pages_scraped"] = pages_scraped
        result["visited_urls_count"] = len(visited_urls)
        result["error"] = ""
        # Save per-fund CSV
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
        _log(f"Processing failed for {url}: {e}", "error")
        result["error"] = str(e)
    return result


# ===============================
# ========== UI PAGES ===========
# ===============================

def page_home():
    st.title("ellenor Funding Discovery")
    st.caption("Scrape funding sites → analyze eligibility → capture evidence.")
    st.markdown("""
    **What this app does**
    - Crawls funding programme websites (with focused, keyword-led link discovery).
    - Extracts page text and asks an LLM to *structure* key details and classify ellenor Hospice’s **eligibility**.
    - Stores and visualises results so your team can quickly spot strong matches.

    **Features**
    - 📊 Past results with filters, search, summary metrics, and CSV download  
    - 🌐 Smart scraping of new URLs or a CSV upload (skips duplicates automatically)  
    - 🤖 LLM-powered extraction & eligibility classification, with on-page evidence  
    - 🧰 JSON preview (optional) & per-fund text preview (optional)  
    - 🔒 API key managed safely in your session (not saved to disk)

    **Disclaimer**
    - This tool uses the OpenAI API for text extraction & analysis **only when you provide a key**.  
    - No fund page content or results are sent anywhere else; results are stored locally in CSVs and in the `Scraped/` folder.  
    - Always validate eligibility against the original funder guidance before applying.
    """)
    with st.expander("About ellenor profile used for matching"):
        st.json(ELLENOR_PROFILE)


def _eligibility_color(val: str) -> str:
    palette = {
        "Highly Eligible": "#1f9d55",  # green
        "Eligible": "#2d7dff",         # blue
        "Possibly Eligible": "#b7791f",# amber
        "Low Match": "#6b7280",        # gray
        "Not Eligible": "#dc2626"      # red
    }
    return palette.get(val, "#6b7280")

def _style_eligibility(df: pd.DataFrame) -> pd.DataFrame:
    if "eligibility" not in df.columns: return df
    return df.style.map(lambda v: f"color: {_eligibility_color(v)}; font-weight:600" if v in ELIGIBILITY_ORDER else "" , subset=["eligibility"])

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
    st.title("📊 Past Results")
    df = load_results_csv(OUTPUT_CSV)

    if df.empty:
        st.info("No results found yet. Run a scrape from the **Scrape & Analyze** page.")
        return

    # Quick filters
    with st.expander("Filters", expanded=True):
        elig_filter = st.multiselect("Filter by eligibility", ELIGIBILITY_ORDER, default=ELIGIBILITY_ORDER)
        keyword = st.text_input("Search keyword (URL, name, notes, evidence)")
        filtered = df.copy()
        if elig_filter:
            filtered = filtered[filtered["eligibility"].isin(elig_filter)]
        if keyword.strip():
            kw = keyword.strip().lower()
            mask = (
                filtered["fund_url"].fillna("").str.lower().str.contains(kw) |
                filtered["fund_name"].fillna("").str.lower().str.contains(kw) |
                filtered["notes"].fillna("").str.lower().str.contains(kw) |
                filtered["evidence"].fillna("").str.lower().str.contains(kw)
            )
            filtered = filtered[mask]

    _results_metrics(filtered)

    st.dataframe(filtered, use_container_width=True, height=420)
    st.download_button(
        "Download CSV",
        data=filtered.to_csv(index=False).encode("utf-8"),
        file_name="funds_results_filtered.csv",
        mime="text/csv"
    )

    # Optional evidence preview
    with st.expander("Show evidence details per fund"):
        for _, row in filtered.iterrows():
            color = _eligibility_color(row.get("eligibility",""))
            st.markdown(f"**{row.get('fund_name','(unknown)')}** — "
                        f"[{row.get('fund_url','')}]({row.get('fund_url','')}) "
                        f"· <span style='color:{color};font-weight:600'>{row.get('eligibility','')}</span>",
                        unsafe_allow_html=True)
            st.caption(f"Application status: {row.get('application_status','')} · Deadline: {row.get('deadline','')}")
            st.write(row.get("evidence","") or "_No evidence captured_")
            st.divider()


def page_scrape():
    st.title("🌐 Scrape & Analyze")

    api_ok = bool(st.session_state.get("api_key", "").strip())
    if not api_ok:
        st.warning("An OpenAI API key is not set. Scraping can still run, but LLM extraction will be **skipped** and eligibility will default to a low-confidence placeholder.")
        st.info("Set your key from the left sidebar.")

    st.subheader("Provide funding URLs")
    urls_text = st.text_area("Paste one or more URLs (one per line)", height=130, placeholder="https://funder.org/grants\nhttps://another.org/funding-programme")
    st.write("OR upload a CSV that contains a `fund_url` column.")
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
    input_urls = list(dict.fromkeys(input_urls))  # de-duplicate order-preserving

    if not input_urls:
        st.info("Add URLs or upload a CSV to proceed.")
        return

    st.subheader("De-duplication check")
    processed_urls = get_already_processed_urls(OUTPUT_CSV)
    scraped_domains = get_scraped_domains(SAVE_DIR)

    will_skip = []
    will_process = []
    for u in input_urls:
        domain_key = safe_filename_from_url(u).lower()
        if u in processed_urls:
            will_skip.append((u, "Already in results CSV"))
        elif domain_key in scraped_domains:
            will_skip.append((u, "Already scraped (folder exists)"))
        else:
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

    start = st.button("🚀 Start Scraping", type="primary", use_container_width=True, disabled=len(will_process)==0)

    if not start:
        return

    # Run scraping + LLM extraction with visible progress and logs
    st.session_state.logs = []  # reset logs
    progress = st.progress(0)
    status = st.empty()
    results: List[dict] = []

    os.makedirs(SAVE_DIR, exist_ok=True)

    for i, url in enumerate(will_process, start=1):
        status.info(f"Processing {i}/{len(will_process)} — {url}")
        with st.spinner(f"Crawling and analyzing: {url}"):
            res = process_single_fund(url)
            results.append(res)
            # Per-fund result preview
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
                        st.markdown(f"**Eligibility**")
                        st.markdown(f"<span style='color:{color};font-weight:700'>{res.get('eligibility','')}</span>", unsafe_allow_html=True)

                    with st.expander("Evidence", expanded=False):
                        st.write(res.get("evidence", "") or "_No evidence recorded_")
                    with st.expander("Notes / Restrictions / Applicant types", expanded=False):
                        st.write(f"**Notes:** {res.get('notes','')}")
                        st.write(f"**Restrictions:** {res.get('restrictions','')}")
                        st.write(f"**Applicant types:** {res.get('applicant_types','')}")
                    with st.expander("Raw JSON (from LLM)", expanded=False):
                        raw = {k: res.get(k, "") for k in [
                            "applicant_types","geographic_scope","beneficiary_focus",
                            "funding_range","restrictions","application_status",
                            "deadline","notes","eligibility","evidence"
                        ]}
                        st.json(raw)

            # Append to master CSV as we go (also populates individual per-fund CSV in process_single_fund)
            try:
                save_results_to_csv([res], OUTPUT_CSV)
            except Exception as e:
                st.error(f"Could not save to master CSV: {e}")

        progress.progress(int(i/len(will_process)*100))

    status.success("Scraping & analysis complete.")
    st.session_state.last_run_results = results

    # Display batch summary and table of new results
    st.subheader("Batch Summary")
    df_new = pd.DataFrame(results)
    if not df_new.empty:
        # Ensure columns present
        for col in CSV_COLUMNS:
            if col not in df_new.columns:
                df_new[col] = ""
        df_new = df_new[CSV_COLUMNS]
        # Metrics
        _results_metrics(df_new)
        st.dataframe(df_new, use_container_width=True, height=420)
        st.download_button(
            "Download This Batch (CSV)",
            data=df_new.to_csv(index=False).encode("utf-8"),
            file_name="funds_results_batch.csv",
            mime="text/csv"
        )
    else:
        st.info("No results produced.")

    # Live logs
    if st.session_state.logs:
        with st.expander("Logs"):
            for level, msg in st.session_state.logs:
                if level == "error": st.error(msg)
                elif level == "warning": st.warning(msg)
                elif level == "success": st.success(msg)
                else: st.write(msg)


def page_settings():
    st.title("⚙️ Settings & Utilities")

    st.subheader("API Key")
    st.write("You can also manage your key from the sidebar.")
    key = st.text_input("OpenAI API key", value=st.session_state.api_key, type="password")
    b1, b2 = st.columns([1,1])
    with b1:
        if st.button("Save Key", use_container_width=True):
            st.session_state.api_key = key.strip()
            st.success("API key saved for this session.")
    with b2:
        if st.button("Clear Key", type="secondary", use_container_width=True):
            st.session_state.api_key = ""
            st.warning("API key cleared.")

    st.subheader("Data Locations")
    st.write(f"- Results CSV: `{OUTPUT_CSV}`")
    st.write(f"- Scraped text folder: `{SAVE_DIR}/`")
    st.caption("These paths are on your local machine / server where Streamlit runs.")

    st.subheader("Optional: Process pre-scraped folders (LLM-only reprocess)")
    st.write("If you previously scraped sites (TXT files saved in `Scraped/`), you can run LLM extraction again without crawling.")
    if not os.path.exists(SAVE_DIR):
        st.info("No `Scraped/` directory found yet.")
        return

    # Aid: Load from INPUT_CSV (if present) to select subset
    input_csv_exists = os.path.exists(INPUT_CSV)
    use_input = st.checkbox("Use input list from funds.csv (if present)", value=input_csv_exists)
    selected_folders = []

    if use_input and input_csv_exists:
        try:
            df_in = pd.read_csv(INPUT_CSV)
            if "fund_url" in df_in.columns:
                st.caption(f"Loaded {len(df_in)} rows from {INPUT_CSV}. Only those with scraped folders will be considered.")
                # Filter to those with folders
                candidates = []
                for u in df_in["fund_url"].dropna().astype(str).tolist():
                    folder = os.path.join(SAVE_DIR, safe_filename_from_url(u))
                    if os.path.isdir(folder):
                        candidates.append((u, folder))
                urls_to_reprocess = [c[0] for c in candidates]
                pick = st.multiselect("Choose funds to reprocess", urls_to_reprocess, default=urls_to_reprocess[:10])
                selected_folders = [os.path.join(SAVE_DIR, safe_filename_from_url(u)) for u in pick]
            else:
                st.warning("`funds.csv` must include a `fund_url` column.")
        except Exception as e:
            st.error(f"Could not read {INPUT_CSV}: {e}")
    else:
        # list all scraped folders
        all_folders = [f for f in os.listdir(SAVE_DIR) if os.path.isdir(os.path.join(SAVE_DIR, f))]
        if not all_folders:
            st.info("No scraped folders to reprocess.")
        else:
            pick = st.multiselect("Choose scraped folders", all_folders, default=all_folders[:10])
            selected_folders = [os.path.join(SAVE_DIR, f) for f in pick]

    if st.button("Run LLM extraction on selected folders", disabled=len(selected_folders)==0):
        if not st.session_state.api_key.strip():
            st.error("Please set your OpenAI API key first.")
            return
        results = []
        progress = st.progress(0)
        for i, folder in enumerate(selected_folders, start=1):
            combined_text, file_count, fund_url_guess = load_text_from_folder(folder)
            if not combined_text or len(combined_text) < 100:
                st.warning(f"Insufficient text in {folder} ({len(combined_text)} chars)")
                continue
            data = call_llm_extract(combined_text)
            row = {
                "fund_url": fund_url_guess or "",
                "fund_name": os.path.basename(folder),
                "extraction_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "pages_scraped": file_count,
                "visited_urls_count": file_count,
                "error": ""
            }
            row.update(data)
            results.append(row)
            # Write per-folder CSV
            try:
                df_one = pd.DataFrame([row])
                for col in CSV_COLUMNS:
                    if col not in df_one.columns:
                        df_one[col] = ""
                df_one = df_one[CSV_COLUMNS]
                Path(folder, "fund_result.csv").write_text(df_one.to_csv(index=False))
            except Exception as e:
                st.warning(f"Could not write individual CSV in {folder}: {e}")
            progress.progress(int(i/len(selected_folders)*100))
        if results:
            save_results_to_csv(results, OUTPUT_CSV)
            st.success(f"Saved {len(results)} new LLM result(s) to {OUTPUT_CSV}")
            st.dataframe(pd.DataFrame(results), use_container_width=True, height=420)
        else:
            st.info("No results created.")


# ===============================
# ========== NAV / MAIN =========
# ===============================

def main():
    st.set_page_config(page_title="ellenor Funding Discovery", page_icon="🌿", layout="wide")
    init_session()
    set_api_key_ui()

    with st.sidebar:
        st.header("Navigation")
        page = st.radio("Go to", ["Home", "Results", "Scrape & Analyze", "Settings"], index=0)
        st.markdown("---")
        st.caption("Tip: Use the **Scrape & Analyze** page to add new funds. "
                   "Processed results appear instantly on the **Results** page.")

    if page == "Home":
        page_home()
    elif page == "Results":
        page_results()
    elif page == "Scrape & Analyze":
        page_scrape()
    elif page == "Settings":
        page_settings()

if __name__ == "__main__":
    main()
