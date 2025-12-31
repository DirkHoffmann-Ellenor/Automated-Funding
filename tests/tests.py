import time, requests, json
BASE = "https://automated-funding-api.calmsmoke-5afc5b29.eastus.azurecontainerapps.io"

def show(resp):
    print(resp.status_code, resp.text[:500])

print("health"); show(requests.get(f"{BASE}/health", timeout=30))
print("results"); show(requests.get(f"{BASE}/results/", timeout=30))

prep = {"fund_urls": ["https://example.com/funding"]}
print("prepare"); show(requests.post(f"{BASE}/scrape/prepare", json=prep, timeout=60))

single = {"fund_url": "https://example.com/funding", "fund_name": "Example Fund"}
print("single"); show(requests.post(f"{BASE}/scrape/single", json=single, timeout=120))

batch = {"fund_urls": ["https://example.com/funding", "https://example.org/grants"]}
print("batch"); b = requests.post(f"{BASE}/scrape/batch", json=batch, timeout=60); show(b)
job_id = b.json().get("job_id")
if job_id:
    time.sleep(5)
    print("job status"); show(requests.get(f"{BASE}/scrape/jobs/{job_id}", timeout=30))
