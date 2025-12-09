import requests
from bs4 import BeautifulSoup
import csv
import time

BASE_URL = "https://grantnav.threesixtygiving.org"
PAGE_URL = BASE_URL + "/funders"

def get_page(url):
    """Download a page and return a BeautifulSoup object."""
    response = requests.get(url)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")

def extract_funders(soup):
    """Extract funder names and links from a page."""
    funders = []

    # find all funder "title" links
    results = soup.find_all("a", class_="grant-search-result__title")

    for a in results:
        name = a.get_text(strip=True)
        href = a.get("href")
        full_url = BASE_URL + href if href.startswith("/") else href

        funders.append({
            "name": name,
            "url": full_url
        })

    return funders


def scrape_all_pages():
    all_funders = []

    TOTAL_PAGES = 17  # hardcoded since pagination HTML does not render serverside

    for page in range(1, TOTAL_PAGES + 1):
        if page == 1:
            url = PAGE_URL
        else:
            url = f"{PAGE_URL}?query=%2A&default_field=%2A&sort=_score+desc&page={page}"

        print(f"Scraping page {page}/{TOTAL_PAGES}: {url}")
        
        soup = get_page(url)
        page_funders = extract_funders(soup)
        all_funders.extend(page_funders)

        time.sleep(1)  # Be polite

    return all_funders


def save_to_csv(funders, filename="funders.csv"):
    print(f"Saving {len(funders)} funders to {filename}...")
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "url"])
        writer.writeheader()
        writer.writerows(funders)

    print("Done!")


# Run scraper
if __name__ == "__main__":
    funders = scrape_all_pages()
    save_to_csv(funders)
