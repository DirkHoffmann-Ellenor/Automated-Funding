import requests
import time
import csv
import logging
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from datetime import datetime

BASE_URL = "https://www.getgrants.org.uk"
CATEGORY_PATH = "/category/health-wellbeing/"
LISTING_URL = urljoin(BASE_URL, CATEGORY_PATH)
OUTPUT_CSV = "health_wellbeing_results.csv"

# -----------------------
# Setup Logging
# -----------------------
log_filename = f"scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    filename=log_filename,
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def log_and_print(message, level="info"):
    """Write to log file and print to terminal."""
    print(message)

    if level == "info":
        logging.info(message)
    elif level == "warning":
        logging.warning(message)
    elif level == "error":
        logging.error(message)
    else:
        logging.info(message)


HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; GrantScraper/1.0)"
}


def get_listing_pages():
    """Yield BeautifulSoup objects for each listing page until none remain."""
    page = 1
    while True:
        url = LISTING_URL if page == 1 else urljoin(BASE_URL, f"{CATEGORY_PATH}page/{page}/")
        log_and_print(f"Fetching listing page: {url}")

        r = requests.get(url, headers=HEADERS)
        if r.status_code != 200:
            log_and_print(f"Failed to fetch listing page {url} (status {r.status_code})", "warning")
            break

        soup = BeautifulSoup(r.text, "html.parser")
        entries = soup.find_all("a", class_="entry-title-link", href=True)

        if not entries:
            log_and_print(f"No more entries found on listing page {url}. Stopping pagination.")
            break

        yield soup
        page += 1
        time.sleep(1)


def extract_fund_links(soup):
    links = [a["href"] for a in soup.find_all("a", class_="entry-title-link", href=True)]
    log_and_print(f"Found {len(links)} fund links on this listing page.")
    return links


def get_fund_name(soup):
    h1 = soup.find("h1", class_="entry-title")
    if h1:
        return h1.get_text(strip=True)
    return None


def extract_org_websites(soup):
    """Extract all external 'Website' links and remove getgrants internal links."""
    links = []
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True).lower()
        href = a["href"].strip()

        if "website" in text:
            if href.startswith("/"):
                href = urljoin(BASE_URL, href)
            if href.startswith("https://www.getgrants.org.uk"):
                continue
            links.append(href)

    return links


def process_fund_page(fund_url):
    log_and_print(f"  → Visiting fund page: {fund_url}")

    try:
        r = requests.get(fund_url, headers=HEADERS)
    except Exception as e:
        log_and_print(f"Request exception for {fund_url}: {e}", "error")
        return None, []

    if r.status_code != 200:
        log_and_print(f"Failed to load fund page {fund_url} (status {r.status_code})", "error")
        return None, []

    soup = BeautifulSoup(r.text, "html.parser")
    fund_name = get_fund_name(soup)

    if not fund_name:
        log_and_print(f"Could not find fund name on {fund_url}", "warning")
    else:
        log_and_print(f"Extracted fund name: {fund_name}")

    websites = extract_org_websites(soup)
    log_and_print(f"Found {len(websites)} website links for: {fund_name or fund_url}")

    return fund_name, websites


def main():
    log_and_print("Scraper started.")
    results = []

    for soup in get_listing_pages():
        fund_links = extract_fund_links(soup)

        for fund_url in fund_links:
            fund_name, websites = process_fund_page(fund_url)

            if not fund_name:
                log_and_print(f"Skipping fund (no name found): {fund_url}", "warning")
                continue

            if not websites:
                log_and_print(f"No website links found for fund: {fund_name}", "warning")

            for site in websites:
                results.append({
                    "fund_name": fund_name,
                    "organisation_website": site
                })

            time.sleep(1)

    # Save CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["fund_name", "organisation_website"])
        writer.writeheader()
        writer.writerows(results)

    log_and_print(f"Scraper finished. Saved {len(results)} rows to {OUTPUT_CSV}")
    log_and_print(f"Log file saved as {log_filename}")

    print("\nDone!\n")


if __name__ == "__main__":
    main()
