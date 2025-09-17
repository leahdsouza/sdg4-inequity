import pandas as pd

df = pd.read_parquet("data/interim/inequity_index.parquet")
print(df.sort_values(["year", "inequity_index"], ascending=[False, False]).head(10))
