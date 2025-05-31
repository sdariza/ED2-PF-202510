import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def export_csv(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False)

def export_json(df: pd.DataFrame, path: str):
    df.to_json(path, orient="records", indent=2)

def export_parquet(df: pd.DataFrame, path: str):
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path)

def export_feather(df: pd.DataFrame, path: str):
    df.to_feather(path)


