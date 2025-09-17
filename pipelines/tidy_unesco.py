# pipelines/tidy_unesco.py
from pathlib import Path
import pandas as pd

try:
    import country_converter as coco

    CC = coco.CountryConverter()
except Exception:
    CC = None  # we'll fallback if not installed

SCHEMA = [
    "country_iso3",
    "country_name",
    "year",
    "indicator_id",
    "value",
    "unit",
    "source",
    "disagg_type",
    "disagg_value",
    "is_imputed",
    "obs_status",
]


def _col(df, options):
    for c in options:
        if c in df.columns:
            return c
    return None


def _read_any(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext in (".xls", ".xlsx"):
        return pd.read_excel(path)
    elif ext == ".csv":
        return pd.read_csv(path)
    else:
        raise ValueError(f"Unsupported file type: {ext}")


def _melt_years(df: pd.DataFrame) -> pd.DataFrame:
    # Any column that is a 4-digit year becomes a value column
    year_cols = [c for c in df.columns if str(c).isdigit() and len(str(c)) == 4]
    id_cols = [c for c in df.columns if c not in year_cols]
    long = df.melt(
        id_vars=id_cols, value_vars=year_cols, var_name="year", value_name="value"
    )
    long["year"] = pd.to_numeric(long["year"], errors="coerce")
    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    return long


def tidy_unesco_file(path: Path, indicator_id: str, unit: str) -> pd.DataFrame:
    df = _read_any(path)

    # --- CASE A: SDG portal schema (wide years + GeoAreaName/GeoAreaCode) ---
    if "GeoAreaName" in df.columns:
        df_long = _melt_years(df)

        country_name_col = "GeoAreaName"
        # Prefer ISO3; SDG portal gives M49 numeric codes, not ISO3
        if CC is not None:
            iso3_series = CC.convert(
                df_long[country_name_col].astype(str), to="ISO3", not_found=None
            )
        else:
            iso3_series = None

        out = pd.DataFrame(
            {
                "country_iso3": iso3_series if iso3_series is not None else pd.NA,
                "country_name": df_long[country_name_col],
                "year": df_long["year"],
                "indicator_id": indicator_id,
                "value": df_long["value"],
                "unit": (
                    df_long[_col(df_long, ["Units", "Unit"])]
                    if _col(df_long, ["Units", "Unit"])
                    else unit
                ),
                "source": "UNESCO",
                "disagg_type": pd.NA,  # You can later wire Sex/Education level if you want
                "disagg_value": pd.NA,
                "is_imputed": False,
                "obs_status": pd.NA,
            }
        )
        return out[SCHEMA]

    # --- CASE B: UIS-style (already long or tidy-ish) ---
    # Try flexible mapping
    ctry = _col(df, ["Country", "COUNTRY", "country", "Ref_Area", "LOCATION_NAME"])
    iso3 = _col(df, ["ISO3", "Code", "Country Code", "REF_AREA", "LOCATION"])
    year = _col(df, ["Year", "Time", "TIME_PERIOD", "Year_Code"])
    val = _col(df, ["Value", "OBS_VALUE", "Observation Value", "obs_value"])

    # If it's wide but not SDG schema, try melting by year too
    if year is None and any(str(c).isdigit() and len(str(c)) == 4 for c in df.columns):
        df_long = _melt_years(df)
        year = "year"
        val = "value"
        # Recompute ctry/iso3 in long table if needed
        if ctry is None:
            ctry = _col(df_long, ["Country", "COUNTRY", "Ref_Area", "LOCATION_NAME"])
        if iso3 is None and CC is not None and ctry is not None:
            iso3_series = CC.convert(
                df_long[ctry].astype(str), to="ISO3", not_found=None
            )
        else:
            iso3_series = df_long[iso3] if iso3 in df_long.columns else pd.NA
        out = pd.DataFrame(
            {
                "country_iso3": iso3_series,
                "country_name": df_long[ctry] if ctry else pd.NA,
                "year": df_long[year],
                "indicator_id": indicator_id,
                "value": df_long[val],
                "unit": unit,
                "source": "UNESCO",
                "disagg_type": pd.NA,
                "disagg_value": pd.NA,
                "is_imputed": False,
                "obs_status": pd.NA,
            }
        )
        return out[SCHEMA]

    # Fallback simple mapping if columns are present
    out = pd.DataFrame(
        {
            "country_iso3": df[iso3] if iso3 else pd.NA,
            "country_name": df[ctry] if ctry else pd.NA,
            "year": pd.to_numeric(df[year], errors="coerce") if year else pd.NA,
            "indicator_id": indicator_id,
            "value": pd.to_numeric(df[val], errors="coerce") if val else pd.NA,
            "unit": unit,
            "source": "UNESCO",
            "disagg_type": pd.NA,
            "disagg_value": pd.NA,
            "is_imputed": False,
            "obs_status": pd.NA,
        }
    )
    return out[SCHEMA]
