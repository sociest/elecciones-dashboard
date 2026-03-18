import os
import time
from typing import Optional, Tuple

import pandas as pd
import requests
import json


DEFAULT_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "DNT": "1",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}


def _fetch_page(
    endpoint: str,
    query_template: str,
    offset: int,
    limit: int,
    headers: dict,
    max_retries: int,
    timeout: int,
    retry_delay: int,
) -> Tuple[Optional[dict], Optional[str]]:
    query_with_offset = query_template.replace("{{OFFSET}}", str(offset)).replace(
        "{{LIMIT}}", str(limit)
    )
    payload = {"query": query_with_offset}

    for attempt in range(max_retries):
        response = None
        try:
            print(
                f"Request {attempt + 1}/{max_retries} | offset={offset} | limit={limit}"
            )
            response = requests.post(
                endpoint,
                json=payload,
                headers=headers,
                timeout=timeout,
                allow_redirects=False,
            )

            if response.status_code in [200, 201]:
                try:
                    return response.json(), None
                except ValueError:
                    print("JSON inválido en respuesta exitosa.")
                    return None, "invalid_json"

            body_preview = (response.text or "")[:250]
            print(f"Error HTTP {response.status_code}: {body_preview}")

            if attempt < max_retries - 1:
                time.sleep(retry_delay)

        except Exception as exc:
            print(f"Exception: {exc}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)

    return None, "request_failed"


def collect_query_to_csv(
    query_template: str,
    format: str = "csv",
    output_path: str = "",
    endpoint: Optional[str] = None,
    initial_limit: int = 100,
    max_retries: int = 3,
    request_timeout: int = 900,
    retry_delay: int = 5,
    sleep_between_pages: float = 1.0,
    max_records: Optional[int] = None,
) -> None:
    endpoint = endpoint or os.getenv("QUERY_ENDPOINT", "https://query.sociest.org/")

    offset = 0
    all_data = []
    page_limit = max(1, initial_limit)
    consecutive_skips = 0

    print("Start Extraction")
    print(f"Endpoint: {endpoint}")
    if max_records is not None:
        print(f"Max records configurado: {max_records}")

    while True:
        response_data, error = _fetch_page(
            endpoint=endpoint,
            query_template=query_template,
            offset=offset,
            limit=page_limit,
            headers=DEFAULT_HEADERS,
            max_retries=max_retries,
            timeout=request_timeout,
            retry_delay=retry_delay,
        )

        if error is None and response_data is not None:
            results = response_data.get("results", {}).get("bindings", [])

            if not results:
                print(f"No more records at offset {offset}")
                break

            print(f"Offset {offset}: Retrieved {len(results)} records")
            all_data.extend(results)
            offset += len(results)
            consecutive_skips = 0
            page_limit = max(1, initial_limit)

            if max_records is not None and len(all_data) >= max_records:
                all_data = all_data[:max_records]
                print(f"Reached max_records={max_records}")
                break

            time.sleep(sleep_between_pages)
            continue

        print(f"Request failed at offset {offset} with limit={page_limit}")

        if page_limit > 1:
            page_limit = max(1, page_limit // 2)
            print(f"Reducing page size and retrying with limit={page_limit}...")
            continue

        print(f"Skipping problematic record at offset {offset}")
        offset += 1
        page_limit = max(1, initial_limit)
        consecutive_skips += 1

        if consecutive_skips >= 100:
            print(
                "Too many consecutive skipped records. Stopping to avoid infinite loop."
            )
            break

    print(f"\nTotal records collected: {len(all_data)}")

    if all_data:
        if format=="json":
            with open(output_path, "w") as file:
                json.dump(all_data, file, ensure_ascii=False, indent=2)
        if format=="csv":
            pd.DataFrame(all_data).to_csv(output_path, index=False)
        print(f"Data saved to {output_path} in format {format}")
    else:
        print("No data collected. Check endpoint/query permissions.")
