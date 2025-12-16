import requests
from bs4 import BeautifulSoup
import csv
import time

BASE_URL = "https://grantnav.threesixtygiving.org"

def get_page(url):
    r = requests.get(url)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def extract_text(el):
    """Extract text safely."""
    return el.get_text(strip=True) if el else ""

import os

def parse_org_page(url):
    print("\n==============================")
    print(f"FETCHING URL: {url}")
    print("==============================")

    soup = get_page(url)

    # -------------------------------------------------
    # SAVE RAW HTML FOR INSPECTION
    # -------------------------------------------------
    # Try to extract something unique for filename
    filename_id = url.rstrip("/").split("/")[-1]
    os.makedirs("debug_html", exist_ok=True)
    debug_path = f"debug_html/{filename_id}.txt"

    with open(debug_path, "w", encoding="utf-8") as f:
        f.write(str(soup))

    print(f"✓ Saved raw HTML to {debug_path}")
    print("Preview of HTML (first 500 chars):")
    print(str(soup)[:500])
    print("\n-------------------------------------------------\n")

    data = {"grantnav_url": url}

    # ============================================================
    # 1. ORGANISATION INFORMATION
    # ============================================================
    print("Looking for <div class='media-card__content_no_image'> ...")
    org_card = soup.find("div", class_="media-card__content_no_image")
    print("Found org_card?", bool(org_card))

    if org_card:
        # Appears in the data as:
        appears_box = org_card.find("strong", string="Appears in the data as")
        print("Found Appears in the data as?", bool(appears_box))

        if appears_box:
            parent = appears_box.parent
            roles = [a.get_text(strip=True) for a in parent.find_all("a")]
            print("Roles detected:", roles)
            data["appears_as"] = ", ".join(roles)

        # Org IDs
        orgid_box = org_card.find("strong", string="Org IDs")
        print("Found Org IDs?", bool(orgid_box))

        if orgid_box:
            data["org_ids"] = orgid_box.parent.get_text(strip=True)

        # Other names
        other_box = org_card.find("strong", string="Other Names used in the data")
        print("Found Other Names?", bool(other_box))

        if other_box:
            data["other_names"] = other_box.parent.get_text(strip=True)

        # Main table inside org_card
        table = org_card.find("table")
        print("Found main table inside org_card?", bool(table))

        if table:
            print("---- Parsing main table rows ----")
            for row in table.find_all("tr"):
                cols = row.find_all("td")
                if len(cols) == 2:
                    raw_key = extract_text(cols[0])
                    val = extract_text(cols[1])
                    print(f" MAIN TABLE ROW → {raw_key} = {val}")

                    key = raw_key.strip().replace(" ", "_").replace(":", "").lower()
                    if key == "url":
                        key = "website_url"
                        if not val.startswith(("http://", "https://")):
                            val = "https://" + val

                    data[key] = val
                    
    # ============================================================
    # 2. ADDITIONAL DATA SECTION
    # ============================================================
    print("\nLooking for #additional-data-section ...")
    additional_section = soup.find("div", id="additional-data-section")
    print("Found additional-data-section?", bool(additional_section))

    if additional_section:
        # Find the tbody element specifically
        tbody = additional_section.find("tbody")
        print("Found tbody?", bool(tbody))
        
        if tbody:
            print("---- Parsing ADDITIONAL DATA rows ----")
            # Find all tr elements directly, regardless of nesting
            all_trs = tbody.find_all("tr")
            print(f"Found {len(all_trs)} tr elements")
            
            for row in all_trs:
                cols = row.find_all("td", recursive=False)  # Only direct children
                if len(cols) != 2:
                    continue

                key_td, val_td = cols

                # Get <b> key inside <td>
                b_tag = key_td.find("b")
                if not b_tag:
                    continue

                raw_key = extract_text(b_tag)
                val = extract_text(val_td)

                print(f" ADDITIONAL DATA ROW → {raw_key} = {val}")

                # Build clean key
                key = "additional_" + raw_key.strip().replace(" ", "_").replace(":", "").lower()

                if "url" in raw_key.lower():
                    print("🔥 Found WEBSITE inside additional-data:", val)
                    if not val.startswith(("http://", "https://")):
                        val = "https://" + val
                    data["org_website"] = val  # Force a stable key
                else:
                    data[key] = val

    # ============================================================
    # 3. FUNDER SUMMARY
    # ============================================================
    print("\nLooking for Funer summary <h3 id='Funder'> ...")
    funder_section = soup.find("h3", id="Funder")
    print("Found funder section?", bool(funder_section))

    if funder_section:
        container = funder_section.find_parent("div")
        print("Funder container exists?", bool(container))

        if container:
            print("---- Parsing FUNDER SUMMARY rows ----")
            for box in container.find_all("div", class_="media-card__box"):
                strong_tag = box.find("strong")
                if strong_tag:
                    key = extract_text(strong_tag)
                    val = box.get_text(separator=" ", strip=True).replace(key, "").strip()
                    print(f" FUNDER SUMMARY → {key} = {val}")
                    data[key] = val

    print("Final extracted data keys:", list(data.keys()))
    print("============================================================\n")

    return data



# -------------------------------------------------------
# PROCESS ALL FUNDERS FROM funders.csv (first scraper)
# -------------------------------------------------------

def load_funders(filename="funders.csv"):
    funders = []
    with open(filename, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            funders.append(row)
    return funders


def save_full_data(results, filename="funder_details.csv"):
    # Collect all unique keys so CSV columns include everything
    all_keys = set()
    for row in results:
        all_keys.update(row.keys())

    all_keys = sorted(all_keys)

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys)
        writer.writeheader()
        for row in results:
            writer.writerow(row)


if __name__ == "__main__":
    funders = load_funders()

    print(f"Loaded {len(funders)} funders.")
    results = []

    for i, funder in enumerate(funders, start=1):
        print(f"[{i}/{len(funders)}] Scraping {funder['name']} → {funder['url']}")
        try:
            data = parse_org_page(funder["url"])
            data["name"] = funder["name"]
            results.append(data)
            
            # Debug: print what was found
            if "additional_url" in data:
                print(f"  ✓ Found URL: {data['additional_url']}")
            else:
                print(f"  ✗ No additional URL found")
                
        except Exception as e:
            print(f"  ERROR: {e}")
            data = {"grantnav_url": funder["url"], "name": funder["name"], "error": str(e)}
            results.append(data)

        time.sleep(1)  # Be polite

    save_full_data(results)
    print(f"\nFinished! Full data saved to funder_details.csv.")
    print(f"Successfully scraped {len([r for r in results if 'additional_url' in r])} organizations with URLs.")