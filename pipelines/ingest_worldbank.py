# pipelines/ingest_worldbank.py
import sys
from pathlib import Path
import requests
from loguru import logger
from config import settings

# World Bank bulk CSV-by-indicator endpoint (returns a ZIP of CSVs)
BASE = "https://api.worldbank.org/v2/country/all/indicator/{code}?downloadformat=csv"


def download_indicator(code: str) -> Path:
    url = BASE.format(code=code)
    out_zip = settings.data_dir / "raw" / f"wb_{code}.zip"
    out_zip.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Downloading {code} from World Bank: {url}")
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    out_zip.write_bytes(r.content)
    logger.success(f"Saved {out_zip}")
    return out_zip


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Usage: python pipelines/ingest_worldbank.py <INDICATOR_CODE>")
        sys.exit(1)
    code = sys.argv[1]
    download_indicator(code)
