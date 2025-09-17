# pipelines/build_index.py
import numpy as np
import pandas as pd
from pathlib import Path
from loguru import logger

# Buckets & indicators (MVP)
BUCKETS = {
    "Learning": ["SDG_4.1.1_read"],
    "EarlyChildhood": ["SDG_4.2.2"],
    "Participation": ["SE.PRM.CMPT.ZS"],
    "Equity": ["SDG_4.5.1_GPI_SEC"],  # GPI: ideal = 1
    "Infrastructure": ["SDG_4.a.1_elec"],
    "Teachers": ["SDG_4.c.1_prim"],
}
WEIGHTS = {k: 1 / len(BUCKETS) for k in BUCKETS}  # equal for now


def load_interim(ind_id: str, base: Path) -> pd.DataFrame:
    p = base / f"{ind_id}.parquet"
    if not p.exists():
        logger.warning(f"Missing {p}")
        return pd.DataFrame()
    df = pd.read_parquet(p)
    df["indicator_id"] = ind_id  # ensure
    return df[["country_iso3", "country_name", "year", "indicator_id", "value"]]


def normalize_series(x: pd.Series, higher_is_better: bool = True) -> pd.Series:
    x = pd.to_numeric(x, errors="coerce")
    x = x.replace([np.inf, -np.inf], np.nan)
    if x.notna().sum() <= 1:
        return pd.Series(np.zeros(len(x)), index=x.index)
    mn, mx = x.min(), x.max()
    if mn == mx:
        return pd.Series(np.zeros(len(x)), index=x.index)
    s01 = (x - mn) / (mx - mn)
    return s01 if higher_is_better else (1 - s01)

    base = Path("data/interim")
    frames = []
    for bucket, inds in BUCKETS.items():
        for ind in inds:
            df = load_interim(ind, base)
            if df.empty:
                continue
            df["bucket"] = bucket
            # Special handling for GPI (closer to 1 is better)
            if ind == "SDG_4.5.1_GPI_SEC":
                # convert to “goodness” = 1 - |GPI-1| then min-max across countries/years
                g = df["value"].astype(float)
                goodness = 1 - (g - 1.0).abs()
                # min-max scale goodness to [0,1]
                df["norm"] = normalize_series(goodness, True)
            else:
                df["norm"] = normalize_series(df["value"], True)
            frames.append(df)

    if not frames:
        logger.error("No indicators available. Did you run harmonize?")
        return

    df = pd.concat(frames, ignore_index=True)

    # Aggregate to bucket-level per country-year
    bucket_scores = df.groupby(
        ["country_iso3", "country_name", "year", "bucket"], as_index=False
    ).agg(bucket_score=("norm", "mean"))
    bucket_scores["weight"] = bucket_scores["bucket"].map(WEIGHTS)

    # Weighted average across buckets
    index_df = (
        bucket_scores.groupby(["country_iso3", "country_name", "year"], as_index=False)
        .apply(lambda g: np.average(g["bucket_score"], weights=g["weight"]))
        .rename(columns={None: "inequity_index"})
        .reset_index(drop=True)
    )

    out = base / "inequity_index.parquet"
    index_df.to_parquet(out, index=False)
    logger.success(f"Wrote {out} with {len(index_df):,} rows")


def main():
    from country_converter import CountryConverter
    import pandas as pd
    import numpy as np
    from pathlib import Path

    cc = CountryConverter()

    base = Path("data/interim")
    frames = []
    for bucket, inds in BUCKETS.items():
        for ind in inds:
            df = load_interim(ind, base)
            if df.empty:
                continue
            df["bucket"] = bucket

            # --- Drop regional aggregates (keep only true ISO3 countries) & silence prints ---
            import io
            from contextlib import redirect_stdout

            buf = io.StringIO()
            with redirect_stdout(buf):  # suppress "not found in ISO3" spam
                iso3_conv = pd.Series(
                    cc.convert(
                        df["country_iso3"].astype(str), to="ISO3", not_found=None
                    ),
                    index=df.index,
                )
            df = df[iso3_conv.notna()]  # keep only recognized countries

            # --- Normalize (special case for GPI) ---
            if ind == "SDG_4.5.1_GPI_SEC":
                g = pd.to_numeric(df["value"], errors="coerce")
                goodness = 1 - (g - 1.0).abs()
                df["norm"] = normalize_series(goodness, True)
            else:
                df["norm"] = normalize_series(df["value"], True)

            df = df.dropna(subset=["year", "norm"])
            frames.append(df)

    if not frames:
        logger.error("No indicators available. Did you run harmonize?")
        return

    df = pd.concat(frames, ignore_index=True)
    # Keep SDG era
    df = df[(df["year"] >= 2015) & (df["year"] <= 2024)]

    # --- Require at least 2 buckets present per country-year (global coverage bias) ---
    bucket_scores = df.groupby(
        ["country_iso3", "country_name", "year", "bucket"], as_index=False
    ).agg(bucket_score=("norm", "mean"))
    counts = bucket_scores.groupby(
        ["country_iso3", "country_name", "year"], as_index=False
    ).agg(n_buckets=("bucket", "nunique"))
    present = bucket_scores.merge(
        counts, on=["country_iso3", "country_name", "year"], how="left"
    )
    present = present[present["n_buckets"] >= 2]

    if present.empty:
        logger.error("No country-years with minimum coverage (>=2 buckets).")
        return

    present["weight"] = present["bucket"].map(WEIGHTS)
    index_df = (
        present.groupby(["country_iso3", "country_name", "year"], as_index=False)
        .apply(lambda g: np.average(g["bucket_score"], weights=g["weight"]))
        .rename(columns={None: "inequity_index"})
        .reset_index(drop=True)
    )

    out = base / "inequity_index.parquet"
    index_df.to_parquet(out, index=False)
    logger.success(f"Wrote {out} with {len(index_df):,} rows")


if __name__ == "__main__":
    main()
