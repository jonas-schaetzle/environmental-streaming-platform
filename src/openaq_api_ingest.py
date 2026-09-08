from typing import Any

import requests


def fetch_json(
    url: str,
    api_key: str,
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    if not api_key:
        raise ValueError("OpenAQ API key must not be empty.")

    response = requests.get(
        url,
        headers={"X-API-Key": api_key},
        timeout=timeout_seconds,
    )
    response.raise_for_status()

    return response.json()
