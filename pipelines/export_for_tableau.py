# pipelines/export_for_tableau.py
from pathlib import Path
import pandas as pd
from loguru import logger
from country_converter import CountryConverter

BASE = Path("data/interim")
OUT = Path("data/public")
OUT.mkdir(parents=True, exist_ok=True)

# Map each indicator to a friendly Tableau name + bucket
INDICATORS = {
    "SE.PRM.CMPT.ZS": ("Participation_completion", "Participation"),
    "SDG_4.1.1_read": ("Learning_proficiency", "Learning"),
    "SDG_4.2.2": ("EarlyChildhood_participation", "EarlyChildhood"),
    "SDG_4.5.1_GPI_SEC": ("Equity_gender_parity", "Equity"),  # closer to 1 is better
    "SDG_4.a.1_elec": ("Infrastructure_electricity", "Infrastructure"),
    "SDG_4.c.1_prim": ("Teachers_trained", "Teachers"),
}

cc = CountryConverter()


def _filter_countries(df: pd.DataFrame) -> pd.DataFrame:
    iso = pd.Series(
        cc.convert(df["country_iso3"].astype(str), to="ISO3", not_found=None),
        index=df.index,
    )
    return df[iso.notna()].copy()


def export_index():
    p = BASE / "inequity_index.parquet"
    if not p.exists():
        logger.error(f"Missing {p}. Run pipelines/build_index.py first.")
        return
    df = pd.read_parquet(p)
    df = _filter_countries(df)
    df = df[(df["year"] >= 2015) & (df["year"] <= 2024)].copy()

    df["inequity_index"] = pd.to_numeric(df["inequity_index"], errors="coerce").round(3)
    df = df.rename(
        columns={
            "country_iso3": "ISO3",
            "country_name": "Country",
            "year": "Year",
            "inequity_index": "InequityIndex",
        }
    )
    df[["ISO3", "Country", "Year", "InequityIndex"]].to_csv(
        OUT / "inequity_index.csv", index=False
    )
    logger.success(f"Wrote {OUT/'inequity_index.csv'} ({len(df):,} rows)")

    # Latest year per country (handy for the opening map in Tableau)
    latest = df.sort_values("Year").groupby("ISO3", as_index=False).tail(1)
    latest.to_csv(OUT / "inequity_index_latest.csv", index=False)
    logger.success(f"Wrote {OUT/'inequity_index_latest.csv'} ({len(latest):,} rows)")


def export_indicators_long():
    frames = []
    for ind, (alias, bucket) in INDICATORS.items():
        p = BASE / f"{ind}.parquet"
        if not p.exists():
            logger.warning(f"Missing {p} — skipping")
            continue
        d = pd.read_parquet(p)[["country_iso3", "country_name", "year", "value"]].copy()
        d = _filter_countries(d)
        d = d[
            (pd.to_numeric(d["year"], errors="coerce") >= 2010)
            & (pd.to_numeric(d["year"], errors="coerce") <= 2024)
        ]
        d["IndicatorID"] = ind
        d["Indicator"] = alias
        d["Bucket"] = bucket
        d = d.rename(
            columns={
                "country_iso3": "ISO3",
                "country_name": "Country",
                "year": "Year",
                "value": "Value",
            }
        )
        d["Value"] = pd.to_numeric(d["Value"], errors="coerce").round(3)
        frames.append(d)

    if not frames:
        logger.error("No indicator files found to export.")
        return

    long_df = pd.concat(frames, ignore_index=True)
    long_df = long_df[
        ["ISO3", "Country", "Year", "Bucket", "Indicator", "IndicatorID", "Value"]
    ]
    long_df.to_csv(OUT / "indicators_long.csv", index=False)
    logger.success(f"Wrote {OUT/'indicators_long.csv'} ({len(long_df):,} rows)")


def export_coverage():
    # Optional: a quick coverage table for Tableau filters/labels
    from collections import defaultdict

    counts = defaultdict(int)
    for ind in INDICATORS:
        p = BASE / f"{ind}.parquet"
        if p.exists():
            d = pd.read_parquet(p)[
                ["country_iso3", "country_name", "year", "value"]
            ].copy()
            d = _filter_countries(d)
            d = d.dropna(subset=["value"])
            for tup in d[["country_iso3", "country_name", "year"]].itertuples(
                index=False, name=None
            ):
                counts[tup] += 1
    if not counts:
        return
    cov = pd.DataFrame(
        [(iso3, cname, yr, c) for (iso3, cname, yr), c in counts.items()],
        columns=["ISO3", "Country", "Year", "AvailableIndicators"],
    )
    cov.to_csv(OUT / "coverage.csv", index=False)
    logger.success(f"Wrote {OUT/'coverage.csv'} ({len(cov):,} rows)")


if __name__ == "__main__":
    export_index()
    export_indicators_long()
    export_coverage()
