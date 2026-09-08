import os
from typing import Any

import requests


def get_required_env_var(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Environment variable {name} must be set.")

    return value


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
