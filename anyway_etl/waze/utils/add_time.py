import pandas as pd
from datetime import datetime


def add_now_column(df: pd.DataFrame, column_name: str):
    now = datetime.now()
    df[column_name] = now
