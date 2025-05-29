import pandas as pd
import fastavro
import pyarrow as pa
import pyarrow.parquet as pq

def export_csv(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False)

def export_json(df: pd.DataFrame, path: str):
    df.to_json(path, orient="records", lines=True)

def export_parquet(df: pd.DataFrame, path: str):
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path)

def export_avro(df: pd.DataFrame, path: str, schema: dict):
    records = df.to_dict(orient="records")
    with open(path, "wb") as out:
        fastavro.writer(out, schema, records)
