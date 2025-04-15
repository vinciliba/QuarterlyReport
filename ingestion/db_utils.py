import os
import sqlite3
from datetime import datetime


def insert_upload_log(filename, table_name, rows, cols, db_path='database/reporting.db'):
    now = datetime.now().isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            INSERT INTO upload_log (filename, table_name, uploaded_at, rows, cols)
            VALUES (?, ?, ?, ?, ?)
        """, (filename, table_name, now, rows, cols))
        conn.commit()


def insert_sheet_rule(filename, sheet_name, db_path='database/reporting.db'):
    now = datetime.now().isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            INSERT INTO sheet_rules (filename, sheet_name, rule_created_at)
            VALUES (?, ?, ?)
        """, (filename, sheet_name, now))
        conn.commit()

def get_existing_rule(filename, db_path='database/reporting.db'):
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT sheet_name FROM sheet_rules WHERE filename = ?", (filename,))
        row = cur.fetchone()
        return row[0] if row else None

def init_db(db_path='database/reporting.db'):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    if not os.path.isfile(db_path):
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Upload log table
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

            # Sheet rule table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sheet_rules (
                    id INTEGER PRIMARY KEY,
                    filename TEXT,
                    sheet_name TEXT,
                    rule_created_at TEXT
                )
            """)

            # ✅ NEW: Wrangling/transform rule table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transform_rules (
                    id INTEGER PRIMARY KEY,
                    filename TEXT,
                    sheet TEXT,
                    original_column TEXT,
                    renamed_column TEXT,
                    included BOOLEAN,
                    created_at TEXT
                )
            """)

            conn.commit()
