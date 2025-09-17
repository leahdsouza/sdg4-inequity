# pipelines/harmonize.py
import pandas as pd
from pathlib import Path
from loguru import logger
from config import settings
import zipfile
from tidy_unesco import tidy_unesco_file  # ensure this import matches the new helper

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


def tidy_wb_zip(zip_path: Path, indicator_id: str, unit: str):
    """Take a World Bank ZIP and convert it to tidy dataframe."""
    with zipfile.ZipFile(zip_path) as z:
        csv_name = [
            f for f in z.namelist() if f.endswith(".csv") and f.startswith("API_")
        ][0]
        with z.open(csv_name) as f:
            df = pd.read_csv(f, header=2)

    # Reshape wide (years as columns) → long (one row per country-year)
    df = df.melt(
        id_vars=["Country Name", "Country Code"], var_name="year", value_name="value"
    )
    df = df.rename(
        columns={"Country Code": "country_iso3", "Country Name": "country_name"}
    )

    # Clean and convert data types
    # Convert year to integer
    df["year"] = pd.to_numeric(df["year"], errors="coerce")

    # Clean the value column - convert ".." to NaN and ensure numeric type
    df["value"] = df["value"].replace("..", pd.NA)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Ensure proper data types for all columns
    df["country_iso3"] = df["country_iso3"].astype("string")
    df["country_name"] = df["country_name"].astype("string")
    df["year"] = df["year"].astype("Int64")  # Nullable integer type
    df["indicator_id"] = indicator_id
    df["unit"] = unit
    df["source"] = "WorldBank"
    df["disagg_type"] = pd.NA
    df["disagg_value"] = pd.NA
    df["is_imputed"] = False
    df["obs_status"] = pd.NA

    df = df[SCHEMA]
    return df


def _first_existing(base_dir, stem):
    """Return Path to the first existing file among .xlsx, .xls, .csv for given stem name."""
    for ext in (".xlsx", ".xls", ".csv"):
        p = base_dir / f"{stem}{ext}"
        if p.exists():
            return p
    return None


def main():
    raw = settings.data_dir / "raw"
    interim = settings.data_dir / "interim"
    interim.mkdir(parents=True, exist_ok=True)

    # 1) World Bank as before...
    wb_zip = raw / "wb_SE.PRM.CMPT.ZS.zip"
    if wb_zip.exists():
        df_wb = tidy_wb_zip(wb_zip, "SE.PRM.CMPT.ZS", "percent")
        out = interim / "SE.PRM.CMPT.ZS.parquet"
        df_wb.to_parquet(out, index=False)
        logger.success(f"Wrote {out} with {len(df_wb):,} rows")

    # 2) UNESCO tasks (stems without extension)
    tasks = [
        ("SDG_4.1.1_read", "SDG_4.1.1_read", "percent"),
        ("SDG_4.2.2", "SDG_4.2.2", "percent"),
        ("SDG_4.5.1_GPI_SEC", "SDG_4.5.1_GPI_SEC", "ratio"),
        ("SDG_4.a.1_elec", "SDG_4.a.1_elec", "percent"),
        ("SDG_4.c.1_prim", "SDG_4.c.1_prim", "percent"),
    ]
    base = raw / "unesco"
    for ind, stem, unit in tasks:
        fpath = _first_existing(base, stem)
        if fpath:
            df_u = tidy_unesco_file(fpath, ind, unit)
            outp = interim / f"{ind}.parquet"
            df_u.to_parquet(outp, index=False)
            logger.success(f"Wrote {outp} with {len(df_u):,} rows")
        else:
            logger.warning(f"Missing UNESCO file for {stem} in {base}")


if __name__ == "__main__":
    main()
