# pipelines/check_coverage.py
import pandas as pd
from pathlib import Path

INDICATORS = [
    "SE.PRM.CMPT.ZS",
    "SDG_4.1.1_read",
    "SDG_4.2.2",
    "SDG_4.5.1_GPI_SEC",
    "SDG_4.a.1_elec",
    "SDG_4.c.1_prim",
]

base = Path("data/interim")
dfs = []
for ind in INDICATORS:
    p = base / f"{ind}.parquet"
    if p.exists():
        df = pd.read_parquet(p)[
            ["country_iso3", "country_name", "year", "indicator_id", "value"]
        ]
        dfs.append(df.assign(has_value=df["value"].notna()))
    else:
        print(f"Missing: {p}")

if not dfs:
    raise SystemExit("No interim files found. Run harmonize first.")

all_df = pd.concat(dfs, ignore_index=True)
coverage = all_df.groupby(
    ["country_iso3", "country_name", "year", "indicator_id"], as_index=False
).agg(has_value=("has_value", "max"))
pivot = coverage.pivot_table(
    index=["country_iso3", "country_name", "year"],
    columns="indicator_id",
    values="has_value",
    fill_value=False,
)
pivot["available_count"] = pivot.sum(axis=1)
pivot = pivot.reset_index().sort_values(
    ["year", "available_count"], ascending=[False, False]
)

out = Path("docs/coverage_by_country_year.csv")
pivot.to_csv(out, index=False)
print(f"Wrote {out} with {len(pivot):,} rows")
