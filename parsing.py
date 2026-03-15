import re
from typing import Any

import requests
from bs4 import BeautifulSoup


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )
}


def extract_immoweb_listing_candidates(
    html: str,
    source_name: str,
    search_target_id: Any,
) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    found: list[dict[str, Any]] = []
    links = soup.find_all("a", href=True)
    print(f"TOTAL LINKS FOUND: {len(links)}")

    for anchor in links:
        href = anchor["href"]
        if "/fr/annonce/" not in href:
            continue

        if href.startswith("/"):
            full_url = f"https://www.immoweb.be{href}"
        elif href.startswith("http"):
            full_url = href
        else:
            continue

        match = re.search(r"/(\d{8,})", full_url)
        listing_id = match.group(1) if match else None
        print(f"DISCOVERED CANDIDATE: {full_url}")

        found.append(
            {
                "source_name": source_name,
                "search_target_id": search_target_id,
                "source_url": full_url,
                "source_listing_id": listing_id,
            }
        )

    unique: dict[str, dict[str, Any]] = {}
    for item in found:
        unique[item["source_url"]] = item

    print(f"UNIQUE DISCOVERED URLS: {len(unique)}")
    print("=" * 80)
    return list(unique.values())


def discover_immoweb_urls(search_target: dict[str, Any]) -> list[dict[str, Any]]:
    url = search_target["search_url"]
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()

    print("=" * 80)
    print(f"SEARCH TARGET: {search_target.get('target_name')}")
    print(f"FETCHED URL: {url}")
    print(f"STATUS CODE: {response.status_code}")
    print(f"FINAL URL: {response.url}")
    print(f"HTML LENGTH: {len(response.text)}")

    return extract_immoweb_listing_candidates(
        response.text,
        source_name=search_target["source_name"],
        search_target_id=search_target["id"],
    )
