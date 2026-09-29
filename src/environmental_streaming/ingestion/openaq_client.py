import os
import time
from typing import Any

import requests


OPENAQ_API_BASE_URL = "https://api.openaq.org/v3"
DEFAULT_REQUEST_MAX_ATTEMPTS = 4
DEFAULT_REQUEST_BACKOFF_SECONDS = 1.0
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def get_required_env_var(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Environment variable {name} must be set.")

    return value


def fetch_json(
    url: str,
    api_key: str,
    timeout_seconds: int = 30,
    max_attempts: int = DEFAULT_REQUEST_MAX_ATTEMPTS,
    backoff_seconds: float = DEFAULT_REQUEST_BACKOFF_SECONDS,
) -> dict[str, Any]:
    if not api_key:
        raise ValueError("OpenAQ API key must not be empty.")
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1.")
    if backoff_seconds < 0:
        raise ValueError("backoff_seconds must not be negative.")

    for attempt in range(max_attempts):
        try:
            response = requests.get(
                url,
                headers={"X-API-Key": api_key},
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as error:
            status_code = getattr(error.response, "status_code", None)
            retryable = status_code is None or status_code in RETRYABLE_STATUS_CODES
            if not retryable or attempt == max_attempts - 1:
                raise

            time.sleep(backoff_seconds * (2**attempt))

    raise RuntimeError("OpenAQ request retry loop ended unexpectedly.")


def latest_measurements_url(location_id: int) -> str:
    return f"{OPENAQ_API_BASE_URL}/locations/{location_id}/latest?limit=100"


def sensor_metadata_url(sensor_id: int) -> str:
    return f"{OPENAQ_API_BASE_URL}/sensors/{sensor_id}"


def extract_sensor_ids(latest_payload: dict[str, Any]) -> list[int]:
    return sorted(
        {
            result["sensorsId"]
            for result in latest_payload.get("results", [])
            if result.get("sensorsId") is not None
        }
    )
