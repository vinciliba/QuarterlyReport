import os
import sqlite3
from datetime import datetime

def init_db(db_path='database/reporting.db'):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)  # 🔐 make sure folder exists

    if not os.path.isfile(db_path):  # only create tables if DB is new
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS upload_log (
                    id INTEGER PRIMARY KEY,
                    filename TEXT,
                    table_name TEXT,
                    uploaded_at TEXT,
                    rows INTEGER,
                    cols INTEGER
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sheet_rules (
                    id INTEGER PRIMARY KEY,
                    filename TEXT,
                    sheet_name TEXT,
                    rule_created_at TEXT
                )
            """)
            conn.commit()

