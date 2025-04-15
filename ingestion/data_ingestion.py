# ingestion/data_ingestion.py
import os
import pandas as pd
from datetime import datetime
from ingestion.db_utils import insert_upload_log

def ingest_data(file, selected_sheet=None, db_path='database/reporting.db'):
    ext = os.path.splitext(file.name)[1].lower()
    now = datetime.now().strftime('%Y%m%d_%H%M')
    table_name = f"raw_data_{now}"

    if ext == '.csv':
        df = pd.read_csv(file)
    elif ext in ['.xls', '.xlsx']:
        df = pd.read_excel(file, sheet_name=selected_sheet)
    else:
        raise ValueError("Unsupported file format.")

    # Upload to SQLite
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table_name, conn, index=False, if_exists='replace')

    insert_upload_log(file.name, table_name, df.shape[0], df.shape[1], db_path)

    return df, table_name
