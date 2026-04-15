from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from src.utils.helpers import get_logger

BASE_URL = "https://www.fema.gov/api/open/v2"

log = get_logger(__name__)

ENDPOINTS = {
    "declarations": f"{BASE_URL}/DisasterDeclarationsSummaries",
    "public_assistance": f"{BASE_URL}/PublicAssistanceGrantAwardActivities",
}

FIELDS = {
    "declarations": [
        "disasterNumber",
        "state",
        "declarationDate",
        "incidentType",
        "declarationTitle",
        "incidentBeginDate",
        "incidentEndDate",
        "fyDeclared",
        "designatedArea",
        "declarationRequestNumber",
    ],
    "public_assistance": [
        "disasterNumber",
    ],
}

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"


def fetch_paginated_data(
    endpoint: str,
    fields: list[str],
    max_records: Optional[int] = None,
    page_size: int = 1000,
) -> pd.DataFrame:
    """
    Fetch paginated data from a FEMA Open API endpoint and return as a DataFrame.
    """
    records = []
    skip = 0

    while True:
        params = {
            "$format": "json",
            "$top": page_size,
            "$skip": skip,
            "$select": ",".join(fields),
            "$inlinecount": "allpages",
        }

        try:
            response = requests.get(endpoint, params=params, timeout=60)
            response.raise_for_status()
        except requests.RequestException as exc:
            log.error("Request failed: %s", exc)
            if "response" in locals() and response is not None:
                log.error("Request URL: %s", response.url)
                log.error("Response text: %s", response.text[:2000])  # Log first 2000 chars of response    
            raise

        payload = response.json()

        if "value" in payload:
            batch = payload["value"]
        else:
            list_values = [v for v in payload.values() if isinstance(v, list)]
            batch = list_values[0] if list_values else []

        log.info("Fetched %s records from %s", len(batch), endpoint)

        if not batch:
            break

        records.extend(batch)

        if max_records is not None and len(records) >= max_records:
            records = records[:max_records]
            break

        if len(batch) < page_size:
            break

        skip += page_size

    return pd.DataFrame(records)


def fetch_declarations(max_records: Optional[int] = None) -> pd.DataFrame:
    return fetch_paginated_data(
        endpoint=ENDPOINTS["declarations"],
        fields=FIELDS["declarations"],
        max_records=max_records,
    )


def fetch_public_assistance(max_records: Optional[int] = None) -> pd.DataFrame:
    return fetch_paginated_data(
        endpoint=ENDPOINTS["public_assistance"],
        fields=FIELDS["public_assistance"],
        max_records=max_records,
    )


#def fetch_disaster_summaries(max_records: Optional[int] = None) -> pd.DataFrame:
    #return fetch_paginated_data(
        #endpoint=ENDPOINTS["disaster_summaries"],
        ##fields=FIELDS["disaster_summaries"],
       #max_records=max_records,
    #)


def save_data(df: pd.DataFrame, name: str) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    output_path = RAW_DIR / f"{name}.csv"
    df.to_csv(output_path, index=False)
    log.info("Saved %s records to %s", len(df), output_path)


def run_ingestion() -> None:
    log.info("Starting data ingestion")

    declarations_df = fetch_declarations()
    save_data(declarations_df, "declarations")

    public_assistance_df = fetch_public_assistance()
    save_data(public_assistance_df, "public_assistance")

    log.info("Data ingestion complete")


if __name__ == "__main__":
    run_ingestion()