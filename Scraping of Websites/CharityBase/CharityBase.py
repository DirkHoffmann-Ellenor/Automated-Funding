import requests

API_KEY = "28cf0d20-5494-4bea-8546-d49c28495d87"

URL = "https://charitybase.uk/api/graphql"
HEADERS = {
    "Authorization": f"Apikey {API_KEY}",
    "Content-Type": "application/json"
}

QUERY = """
query GetFunders($limit: PageLimit, $skip: Int) {
  CHC {
    getCharities(filters: {}) {
      list(limit: $limit, skip: $skip) {
        id
        names { value }
        funding {
          funders {
            name
          }
        }
      }
    }
  }
}
"""

TEST_QUERY = """
{
  CHC {
    getCharities(filters: {}) {
      count
    }
  }
}
"""


def fetch_funders(limit=30):
    skip = 0
    all_funders = set()

    while True:
        variables = {"limit": limit, "skip": skip}

        response = requests.post(
            URL,
            headers=HEADERS,
            json={"query": TEST_QUERY, "variables": variables}
        ).json()

        if "errors" in response:
            print("GraphQL Errors:", response["errors"])
            return []

        charities = response["data"]["CHC"]["getCharities"]["list"]
        if not charities:
            break

        for charity in charities:
            funding = charity.get("funding", {})
            funders = funding.get("funders", [])
            for f in funders:
                if f and f.get("name"):
                    all_funders.add(f["name"])

        skip += limit  # skip is a normal integer

    return sorted(all_funders)


if __name__ == "__main__":
    funders = fetch_funders(limit=30)
    print("Total unique funders found:", len(funders))
    for name in funders:
        print(name)
