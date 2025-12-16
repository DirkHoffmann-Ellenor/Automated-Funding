import os, re, csv, time, json, requests, pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from openai import OpenAI
from typing import Dict, List, Tuple, Set
from pathlib import Path

# ========== CONFIG ==========
api_key = os.getenv("APIKEY") or os.getenv("OPENAI_API_KEY")
if not api_key:
    print("[WARN] No API key found yet. It can be set later from Streamlit.")
    api_key = None

client = OpenAI(api_key=api_key) if api_key else None

# Ellenor Hospice Profile for accurate matching
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

SAVE_DIR = "Scraped"
DISCOVERY_DEPTH = 2
MAX_PAGES = 15
MAX_DISCOVERY_PAGES = 200
PAUSE_BETWEEN_REQUESTS = 1.0
HEADERS = {"User-Agent": "ellenor-funding-bot/priority/1.0 (+https://ellenor.org)"}
OUTPUT_CSV = "funds_results_reprocessed.csv"
INPUT_CSV = "funds.csv"

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

# Define expected CSV columns for consistency
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

# ========== HELPERS ==========

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

def fetch_page(url: str, retries: int = 4, backoff_factor: int = 2) -> str:
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", (backoff_factor ** attempt) * 5))
                print(f"[WAIT] Rate limited ({resp.status_code}) – sleeping {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.text
        except requests.exceptions.RequestException as e:
            print(f"[WARN] fetch failed ({e})")
            if attempt < retries - 1:
                time.sleep((backoff_factor ** attempt) * 2)
    return None

def extract_visible_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "svg", "footer", "nav", "form", "header"]):
        tag.decompose()
    text = " ".join(t.get_text(" ", strip=True) for t in soup.find_all(["h1", "h2", "h3", "p", "li", "td", "th"]))
    return re.sub(r"\s+", " ", text).strip()

# ========== LINK DISCOVERY + SCORING ==========

KEYWORDS = ["grant", "grants", "apply", "fund", "funding", "eligible", "eligibility", 
            "criteria", "who-can-apply", "what-we-fund", "apply-for", "apply-for-funding", 
            "support", "programme", "award", "awarded", "application", "guidelines"]

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

    print(f"   ➕ Found {len(candidates)} internal links (visited {pages_visited} pages)")
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

# ========== PRIORITIZED CRAWL ==========

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
    print(f"🌐 Fetching top {len(top_links)} links from {seed_norm}")

    all_text = []
    for i, url in enumerate(top_links, 1):
        print(f"   ↳ ({i}/{len(top_links)}) {url}")
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

# ========== LLM + CSV HANDLING ==========

def call_llm_extract(text: str) -> Dict:
    """Extract structured data including LLM-determined eligibility."""
    # Use token-aware truncation (roughly 4 chars per token)
    max_chars = 50000
    if len(text) > max_chars:
        print(f"[INFO] Text truncated from {len(text)} to {max_chars} chars")
        # Prioritize beginning and end (often contains key info)
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
        # Clean up potential markdown code blocks
        output = re.sub(r"^```json\s*|\s*```$", "", output, flags=re.MULTILINE)
        data = json.loads(output)
        
        # Validate and normalize structure
        normalized = {
            "applicant_types": data.get("applicant_types", []),
            "geographic_scope": data.get("geographic_scope", ""),
            "beneficiary_focus": data.get("beneficiary_focus", []),
            "funding_range": data.get("funding_range", ""),
            "restrictions": data.get("restrictions", []),
            "application_status": data.get("application_status", "unclear"),
            "deadline": data.get("deadline", ""),
            "notes": data.get("notes", ""),
            "eligibility": data.get("eligibility", "Low Match"),  # NEW
            "evidence": data.get("evidence", "")  # NEW
        }
        
        # Validate eligibility value
        valid_eligibility = ["Highly Eligible", "Eligible", "Possibly Eligible", "Low Match", "Not Eligible"]
        if normalized["eligibility"] not in valid_eligibility:
            print(f"[WARNING] Invalid eligibility value: {normalized['eligibility']}, defaulting to 'Low Match'")
            normalized["eligibility"] = "Low Match"
        
        # Convert lists to strings for CSV storage
        for key in ["applicant_types", "beneficiary_focus", "restrictions"]:
            if isinstance(normalized[key], list):
                normalized[key] = "; ".join(normalized[key]) if normalized[key] else ""
        
        return normalized
        
    except json.JSONDecodeError as e:
        print(f"[ERROR] Invalid JSON from LLM: {e}")
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
        print(f"[ERROR] LLM extraction failed: {e}")
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

# ========== BATCH ORCHESTRATION ==========


def get_already_processed_urls(output_csv: str) -> Set[str]:
    """Get set of URLs that have already been processed and saved to CSV."""
    if not os.path.exists(output_csv):
        return set()
    
    try:
        existing_df = pd.read_csv(output_csv)
        if "fund_url" in existing_df.columns:
            processed_urls = {normalize_url(url) for url in existing_df["fund_url"].dropna()}
            return processed_urls
    except Exception as e:
        print(f"[WARN] Could not read existing CSV: {e}")
    
    return set()


def get_scraped_domains(save_dir: str) -> Set[str]:
    """Get set of domains that have already been scraped (have folders)."""
    if not os.path.exists(save_dir):
        return set()
    
    scraped = set()
    for item in os.listdir(save_dir):
        item_path = os.path.join(save_dir, item)
        if os.path.isdir(item_path):
            scraped.add(item.lower())
    
    return scraped


def process_new_funds(input_csv: str, output_csv: str, batch_size: int = 5):
    """
    MODE 1: Process new funding URLs from input CSV.
    Scrapes websites and runs LLM extraction.
    """
    print("\n" + "="*60)
    print("MODE 1: Processing New Funds from CSV")
    print("="*60 + "\n")
    
    df = pd.read_csv(input_csv)
    
    if "fund_name" not in df.columns:
        df["fund_name"] = df["fund_url"].apply(lambda x: urlparse(x).netloc)
    
    os.makedirs(SAVE_DIR, exist_ok=True)
    
    scraped_domains = get_scraped_domains(SAVE_DIR)
    processed_urls = get_already_processed_urls(output_csv)
    
    print(f"🔎 Already scraped folders: {len(scraped_domains)}")
    print(f"📋 Already in results CSV: {len(processed_urls)}")

    batch = []
    skipped_already_scraped = 0
    skipped_already_in_csv = 0
    
    for _, row in df.iterrows():
        url = row["fund_url"]
        normalized_url = normalize_url(url)
        
        if normalized_url in processed_urls:
            skipped_already_in_csv += 1
            continue
        
        domain_folder_name = safe_filename_from_url(url)
        if domain_folder_name.lower() in scraped_domains:
            skipped_already_scraped += 1
            continue
        
        batch.append(row)
        
        if len(batch) >= batch_size:
            break
    
    if skipped_already_in_csv > 0:
        print(f"⏭️  Skipped {skipped_already_in_csv} URLs (already in results CSV)")
    if skipped_already_scraped > 0:
        print(f"⏭️  Skipped {skipped_already_scraped} URLs (already scraped)")

    if not batch:
        print("✅ No new funds to process. All URLs have been scraped or are in results.")
        return

    print(f"🚀 Processing {len(batch)} new funds...")
    results = []

    for i, row in enumerate(batch, 1):
        url = row["fund_url"]
        fund_name = row.get("fund_name", urlparse(url).netloc)
        
        domain_folder = os.path.join(SAVE_DIR, safe_filename_from_url(url))
        os.makedirs(domain_folder, exist_ok=True)

        if os.path.exists(domain_folder) and os.listdir(domain_folder):
            print(f"\n⏭️  [{i}/{len(batch)}] SKIPPING {fund_name} (folder exists)")
            print(f"    {url}")
            continue
        
        print(f"\n🔍 [{i}/{len(batch)}] {fund_name}")
        print(f"    {url}")
        
        result = {
            "fund_url": url,
            "fund_name": fund_name,
            "extraction_timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        try:
            text, folder, pages_scraped, visited_urls = prioritized_crawl(url)
            print(f"✅ Crawled {pages_scraped} pages. Saved in {folder}")
            
            if not text or len(text) < 100:
                result["error"] = "Insufficient text extracted"
                results.append(result)
                continue
            
            data = call_llm_extract(text)
            result.update(data)
            
            result["pages_scraped"] = pages_scraped
            result["visited_urls_count"] = len(visited_urls)
            result["error"] = ""
            
        except Exception as e:
            print(f"[ERROR] Processing failed for {url}: {e}")
            result["error"] = str(e)
        
        results.append(result)

        try:
            single_result_df = pd.DataFrame([result])
            for col in CSV_COLUMNS:
                if col not in single_result_df.columns:
                    single_result_df[col] = ""
            single_result_df = single_result_df[CSV_COLUMNS]

            individual_csv_path = os.path.join(domain_folder, "fund_result.csv")
            single_result_df.to_csv(individual_csv_path, index=False)
            print(f"💾 Saved individual result CSV to {individual_csv_path}")
        except Exception as e:
            print(f"[WARNING] Could not write individual CSV for {fund_name}: {e}")

    save_results_to_csv(results, output_csv)
    print_summary(results)


def load_text_from_folder(folder_path: str) -> tuple[str, int, str]:
    """
    Load and combine all .txt files from a scraped folder.
    Returns: (combined_text, file_count, fund_url_guess)
    """
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
            print(f"[WARNING] Could not read {txt_file}: {e}")
            continue
    
    full_text = "\n".join(combined_text)
    return full_text, len(txt_files), fund_url


def get_already_processed_folders(output_csv: str) -> set:
    """Get set of fund_names already in the output CSV."""
    if not os.path.exists(output_csv):
        return set()
    
    try:
        df = pd.read_csv(output_csv)
        if "fund_name" in df.columns:
            return set(df["fund_name"].dropna().unique())
        return set()
    except Exception as e:
        print(f"[WARNING] Could not read existing CSV: {e}")
        return set()


def reprocess_scraped_funds(input_csv: str, output_csv: str, batch_size: int = 10):
    """
    MODE 2: Reprocess already-scraped funds by reading their txt files 
    and running LLM extraction.
    Only processes funds that are in input CSV but NOT in output CSV.
    """
    print("\n" + "="*60)
    print("MODE 2: Reprocessing Already-Scraped Funds")
    print("="*60 + "\n")
    
    # Check if input CSV exists
    if not os.path.exists(input_csv):
        print(f"[ERROR] Input CSV not found: {input_csv}")
        return
    
    if not os.path.exists(SAVE_DIR):
        print(f"[ERROR] Scraped directory not found: {SAVE_DIR}")
        return
    
    # Read input CSV to get the list of funds we care about
    input_df = pd.read_csv(input_csv)
    if "fund_url" not in input_df.columns:
        print(f"[ERROR] Input CSV must have 'fund_url' column")
        return
    
    # Get already processed URLs from output CSV
    processed_urls = get_already_processed_urls(output_csv)
    print(f"✅ Already in output CSV: {len(processed_urls)}")
    
    # Build list of funds to process: in input CSV but NOT in output CSV
    to_process = []
    
    for _, row in input_df.iterrows():
        url = row["fund_url"]
        normalized_url = normalize_url(url)
        
        # Skip if already in output CSV
        if normalized_url in processed_urls:
            continue
        
        # Check if scraped folder exists
        domain_folder_name = safe_filename_from_url(url)
        folder_path = os.path.join(SAVE_DIR, domain_folder_name)
        
        if os.path.exists(folder_path) and os.path.isdir(folder_path):
            to_process.append({
                'url': url,
                'fund_name': row.get('fund_name', urlparse(url).netloc),
                'folder_name': domain_folder_name,
                'folder_path': folder_path
            })
    
    print(f"📁 Found {len(to_process)} scraped funds in input CSV that need processing")
    
    if not to_process:
        print("🎉 All input CSV funds are either already processed or not yet scraped!")
        return
    
    # Limit to batch size
    to_process = to_process[:batch_size]
    print(f"🚀 Processing {len(to_process)} funds...\n")
    
    results = []
    
    for i, fund_info in enumerate(to_process, 1):
        folder_path = fund_info['folder_path']
        fund_name = fund_info['fund_name']
        url = fund_info['url']
        
        print(f"🔍 [{i}/{len(to_process)}] {fund_name}")
        print(f"    {url}")
        
        result = {
            "fund_url": url,
            "fund_name": fund_name,
            "extraction_timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        try:
            combined_text, file_count, fund_url = load_text_from_folder(folder_path)
            
            if not combined_text or len(combined_text) < 100:
                result["error"] = f"Insufficient text extracted (only {len(combined_text)} chars from {file_count} files)"
                result["pages_scraped"] = file_count
                results.append(result)
                print(f"   ⚠️  Insufficient text ({len(combined_text)} chars)")
                continue
            
            result["pages_scraped"] = file_count
            
            print(f"   📄 Loaded {file_count} text files ({len(combined_text):,} chars)")
            print(f"   🤖 Running LLM extraction...")
            
            data = call_llm_extract(combined_text)
            result.update(data)
            
            result["visited_urls_count"] = file_count
            result["error"] = ""
            
            print(f"   ✅ {data['eligibility']}")
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            result["error"] = str(e)
        
        results.append(result)
        
        try:
            single_result_df = pd.DataFrame([result])
            for col in CSV_COLUMNS:
                if col not in single_result_df.columns:
                    single_result_df[col] = ""
            single_result_df = single_result_df[CSV_COLUMNS]
            
            individual_csv_path = os.path.join(folder_path, "fund_result.csv")
            single_result_df.to_csv(individual_csv_path, index=False)
            print(f"   💾 Saved to {individual_csv_path}")
        except Exception as e:
            print(f"   [WARNING] Could not write individual CSV: {e}")
    
    save_results_to_csv(results, output_csv)
    print_summary(results)
    
    # Calculate remaining based on input CSV
    input_df = pd.read_csv(input_csv)
    total_in_input = len(input_df)
    remaining = total_in_input - len(processed_urls) - len(to_process)
    
    if remaining > 0:
        print(f"\n⏳ {remaining} funds from input CSV remaining to process (run again to continue)")
    else:
        print(f"\n✅ All funds from input CSV have been processed!")


def save_results_to_csv(results: list, output_csv: str):
    """Save results to the output CSV file."""
    if not results:
        return
    
    results_df = pd.DataFrame(results)
    
    for col in CSV_COLUMNS:
        if col not in results_df.columns:
            results_df[col] = ""
    
    results_df = results_df[CSV_COLUMNS]
    
    try:
        if os.path.exists(output_csv):
            results_df.to_csv(output_csv, mode="a", index=False, header=False)
        else:
            results_df.to_csv(output_csv, index=False)
        print(f"\n✅ Batch saved to {output_csv}")
    except Exception as e:
        print(f"\n[CRITICAL] Could not save to CSV: {e}")


def print_summary(results: list):
    """Print processing summary statistics."""
    print(f"\n📊 Summary:")
    print(f"   - Highly Eligible: {sum(1 for r in results if r.get('eligibility') == 'Highly Eligible')}")
    print(f"   - Eligible: {sum(1 for r in results if r.get('eligibility') == 'Eligible')}")
    print(f"   - Possibly Eligible: {sum(1 for r in results if r.get('eligibility') == 'Possibly Eligible')}")
    print(f"   - Low Match: {sum(1 for r in results if r.get('eligibility') == 'Low Match')}")
    print(f"   - Not Eligible: {sum(1 for r in results if r.get('eligibility') == 'Not Eligible')}")
    print(f"   - Errors: {sum(1 for r in results if r.get('error'))}")


def display_menu():
    """Display the main menu and get user choice."""
    print("\n" + "="*60)
    print("FUND PROCESSING SYSTEM")
    print("="*60)
    print("\nChoose an option:")
    print("  1. Process new funds from CSV (scrape + LLM extraction)")
    print("  2. Reprocess already-scraped funds (LLM extraction only)")
    print("  3. Exit")
    print("="*60)
    
    while True:
        choice = input("\nEnter your choice (1-3): ").strip()
        if choice in ['1', '2', '3']:
            return choice
        print("❌ Invalid choice. Please enter 1, 2, or 3.")


def main():
    """Main entry point with interactive menu."""
    
    while True:
        choice = display_menu()
        
        if choice == '1':
            try:
                batch_size = input("\nEnter batch size (default 5): ").strip()
                batch_size = int(batch_size) if batch_size else 5
            except ValueError:
                print("⚠️  Invalid batch size. Using default: 5")
                batch_size = 5
            
            process_new_funds(INPUT_CSV, OUTPUT_CSV, batch_size)
            
        elif choice == '2':
            try:
                batch_size = input("\nEnter batch size (default 10): ").strip()
                batch_size = int(batch_size) if batch_size else 10
            except ValueError:
                print("⚠️  Invalid batch size. Using default: 10")
                batch_size = 10
            
            reprocess_scraped_funds(INPUT_CSV, OUTPUT_CSV, batch_size)
            
        elif choice == '3':
            print("\n👋 Exiting program. Goodbye!")
            break
        
        cont = input("\n\nPress Enter to return to menu (or 'q' to quit): ").strip().lower()
        if cont == 'q':
            print("\n👋 Exiting program. Goodbye!")
            break


if __name__ == "__main__":
    main()