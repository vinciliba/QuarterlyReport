import os
import sqlite3
from datetime import datetime

# ─────────────────────────────────────────
# Helper: initialize all tables, including 'reports'
# ─────────────────────────────────────────
def init_db(db_path='database/reporting.db'):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # 1) Upload log
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

        # 2) Sheet rules
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sheet_rules (
                id INTEGER PRIMARY KEY,
                filename TEXT,
                sheet_name TEXT,
                rule_created_at TEXT
                -- OPTIONAL: If you want to link to a 'report_name'
                -- report_name TEXT
            )
        """)

        # 3) Transform rules
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

        # 4) Reports table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                report_name TEXT UNIQUE,
                created_at TEXT
            )
        """)

        conn.commit()


# ─────────────────────────────────────────
# Upload log
# ─────────────────────────────────────────
def insert_upload_log(filename, table_name, rows, cols, db_path='database/reporting.db'):
    now = datetime.now().isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            INSERT INTO upload_log (filename, table_name, uploaded_at, rows, cols)
            VALUES (?, ?, ?, ?, ?)
        """, (filename, table_name, now, rows, cols))
        conn.commit()


# ─────────────────────────────────────────
# Sheet rules
# ─────────────────────────────────────────
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


# ─────────────────────────────────────────
# Transform rules
# ─────────────────────────────────────────
def get_transform_rules(filename, sheet, db_path='database/reporting.db'):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT original_column, renamed_column, included
            FROM transform_rules
            WHERE filename = ? AND sheet = ?
        """, (filename, sheet))
        results = cursor.fetchall()

        return [
            {
                "original_column": row[0],
                "renamed_column": row[1],
                "included": bool(row[2])
            }
            for row in results
        ]

def save_transform_rules(rules, db_path='database/reporting.db'):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        for rule in rules:
            cursor.execute("""
                DELETE FROM transform_rules 
                WHERE filename = ? AND sheet = ? AND original_column = ?
            """, (rule['filename'], rule['sheet'], rule['original_column']))
            cursor.execute("""
                INSERT INTO transform_rules 
                (filename, sheet, original_column, renamed_column, included, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                rule['filename'],
                rule['sheet'],
                rule['original_column'],
                rule['renamed_column'],
                int(rule['included']),
                rule['created_at']
            ))
        conn.commit()


# ─────────────────────────────────────────
# Reports
# ─────────────────────────────────────────
def create_new_report(report_name, db_path='database/reporting.db'):
    """Insert a new report record, if it doesn’t exist."""
    now = datetime.now().isoformat()
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO reports (report_name, created_at)
            VALUES (?, ?)
        """, (report_name, now))
        conn.commit()

def get_all_reports(db_path='database/reporting.db'):
    """Return all reports as a list of rows or a DataFrame if desired."""
    import pandas as pd
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query("SELECT * FROM reports ORDER BY created_at DESC", conn)
    return df
