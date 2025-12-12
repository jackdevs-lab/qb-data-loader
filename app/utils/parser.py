# app/utils/parser.py
import pandas as pd
def parse_csv(path: str):
    df = pd.read_csv(path)
    df = df.fillna("")
    return df.to_dict(orient="records")