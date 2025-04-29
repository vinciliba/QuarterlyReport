import os
import sqlite3, json
from datetime import datetime
# helper functions
from typing import Any
import pandas as pd 

# ─────────────────────────────────────────
# Init DB with all required tables
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
                cols INTEGER,
                report_name TEXT,
                table_alias TEXT
            )
        """)

        # 1.5) File-alias mapping
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS file_alias_map (
                id INTEGER PRIMARY KEY,
                filename TEXT UNIQUE,
                table_alias TEXT
            )
        """)

        # 1.6) Alias upload status
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alias_upload_status (
                id INTEGER PRIMARY KEY,
                alias TEXT UNIQUE,
                last_loaded_at TEXT,
                file_id INTEGER,
                FOREIGN KEY (file_id) REFERENCES file_alias_map(id)
            )
        """)

        # 2) Sheet rules
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sheet_rules (
                id INTEGER PRIMARY KEY,
                filename TEXT,
                sheet_name TEXT,
                start_row INTEGER,
                rule_created_at TEXT
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

        # 4) Reports
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                report_name TEXT UNIQUE,
                created_at TEXT
            )
        """)

        # 5) Expected report structure
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS report_structure (
                id INTEGER PRIMARY KEY,
                report_name TEXT,
                table_alias TEXT,
                required BOOLEAN,
                expected_cutoff TEXT
            )
        """)

        # 6) Report cutoff tracking
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS report_cutoff_log (
                id INTEGER PRIMARY KEY,
                report_name TEXT,
                cutoff_label TEXT,
                cutoff_date TEXT,
                validated BOOLEAN,
                validated_at TEXT
            )
        """)
        # 7) Report parameters
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS report_parameters (
            report_name   TEXT,
            param_key     TEXT,
            param_value   TEXT,
            PRIMARY KEY (report_name, param_key)
        )""")
        
        # 8) Generated reports
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS generated_reports (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            report_name   TEXT,
            cutoff_date   TEXT,
            generated_at  TEXT,
            file_path     TEXT,
            notes         TEXT
        )""")


        # UNIQUE index for report_structure
        cursor.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ux_report_structure_rn_alias
                        ON report_structure (report_name, table_alias);""")
        
        # Inside init_db function, after other CREATE TABLE statements:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS report_objects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                object_name TEXT UNIQUE NOT NULL,
                object_type TEXT NOT NULL, -- e.g., 'text', 'table', 'plotly_chart'
                description TEXT,
                sql_query TEXT,
                python_code TEXT,
                report_context TEXT, -- Optional: Link object to a specific report or make it global (NULL)
                created_at TEXT,
                updated_at TEXT
            )
        """)
        # Optional: Add an index for faster lookups
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_report_objects_name
            ON report_objects (object_name);
            """)

   
        conn.commit()

# ─────────────────────────────────────────
# Upload log
# ─────────────────────────────────────────
# Corrected function signature to accept table_alias
def insert_upload_log(filename, table_name, rows, cols, report_name, table_alias, db_path='database/reporting.db'):
    now = datetime.now().isoformat()
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        # Corrected INSERT statement to include table_alias
        cursor.execute("""
            INSERT INTO upload_log (filename, table_name, uploaded_at, rows, cols, report_name, table_alias)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (filename, table_name, now, rows, cols, report_name, table_alias))
        conn.commit()
        return cursor.lastrowid

# ─────────────────────────────────────────
# Sheet rules
# ─────────────────────────────────────────
def insert_sheet_rule(filename, sheet_name, start_row=0, db_path='database/reporting.db'):
    now = datetime.now().isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM sheet_rules WHERE filename = ?", (filename,))
        conn.execute("""
            INSERT INTO sheet_rules (filename, sheet_name, start_row, rule_created_at)
            VALUES (?, ?, ?, ?)
        """, (filename, sheet_name, start_row, now))
        conn.commit()

def get_existing_rule(filename, db_path='database/reporting.db'):
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT sheet_name, start_row FROM sheet_rules WHERE filename = ?", (filename,))
        row = cur.fetchone()
        return (row[0], row[1]) if row else (None, None)

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
    now = datetime.now().isoformat()
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM reports WHERE report_name = ?", (report_name,))
        exists = cursor.fetchone()[0]
        if exists:
            raise ValueError(f"Report name '{report_name}' already exists.")
        cursor.execute("""
            INSERT INTO reports (report_name, created_at)
            VALUES (?, ?)
        """, (report_name, now))
        conn.commit()

def get_all_reports(db_path='database/reporting.db'):
    import pandas as pd
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query("SELECT * FROM reports ORDER BY created_at DESC", conn)
    return df

# ─────────────────────────────────────────
# Report structure logic
# ─────────────────────────────────────────
def define_expected_table(report_name, table_alias, required=True, expected_cutoff=None, db_path='database/reporting.db'):
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            INSERT INTO report_structure (report_name, table_alias, required, expected_cutoff)
            VALUES (?, ?, ?, ?)
        """, (report_name, table_alias, int(required), expected_cutoff))
        conn.commit()

def get_expected_tables(report_name, db_path='database/reporting.db'):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_alias FROM report_structure
            WHERE report_name = ? AND required = 1
        """, (report_name,))
        return [row[0] for row in cursor.fetchall()]

def get_uploaded_tables(report_name, db_path='database/reporting.db'):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT table_alias FROM upload_log
            WHERE report_name = ?
        """, (report_name,))
        return [row[0] for row in cursor.fetchall()]

def is_report_complete(report_name, db_path='database/reporting.db'):
    expected = set(get_expected_tables(report_name, db_path))
    uploaded = set(get_uploaded_tables(report_name, db_path))
    missing = expected - uploaded
    return (len(missing) == 0, list(missing))

# ─────────────────────────────────────────
# Report cutoff logging
# ─────────────────────────────────────────
def log_cutoff(report_name, cutoff_label, cutoff_date, validated=False, db_path='database/reporting.db'):
    now = datetime.now().isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            INSERT INTO report_cutoff_log (report_name, cutoff_label, cutoff_date, validated, validated_at)
            VALUES (?, ?, ?, ?, ?)
        """, (report_name, cutoff_label, cutoff_date, int(validated), now))
        conn.commit()

# ─────────────────────────────────────────
# File ↔ Table Alias Mapping
# ─────────────────────────────────────────
def register_file_alias(filename, alias, db_path='database/reporting.db'):
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            INSERT OR REPLACE INTO file_alias_map (filename, table_alias)
            VALUES (?, ?)
        """, (filename, alias))
        conn.commit()

def get_alias_for_file(filename, db_path='database/reporting.db'):
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT table_alias FROM file_alias_map WHERE filename = ?", (filename,))
        row = cur.fetchone()
        return row[0] if row else None

# ─────────────────────────────────────────
# Alias freshness tracking
# ─────────────────────────────────────────
def update_alias_status(alias, filename, db_path='database/reporting.db'):
    now = datetime.now().isoformat()
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM file_alias_map WHERE filename = ?", (filename,))
        row = cur.fetchone()
        if row:
            file_id = row[0]
            cur.execute("""
                INSERT INTO alias_upload_status (alias, last_loaded_at, file_id)
                VALUES (?, ?, ?)
                ON CONFLICT(alias) DO UPDATE SET last_loaded_at=excluded.last_loaded_at, file_id=excluded.file_id
            """, (alias, now, file_id))
            conn.commit()

def get_alias_last_load(alias, db_path='database/reporting.db'):
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT last_loaded_at FROM alias_upload_status WHERE alias = ?", (alias,))
        row = cur.fetchone()
        return row[0] if row else None

def get_suggested_structure(report_name, db_path='database/reporting.db'):
    """
    Return aliases that exist in upload_log for this report but are
    NOT yet present in report_structure.
    """
    sql = """
        SELECT DISTINCT ul.table_alias
        FROM upload_log ul
        LEFT JOIN report_structure rs
          ON rs.report_name = ul.report_name
         AND rs.table_alias = ul.table_alias
        WHERE ul.report_name = ?
          AND rs.table_alias IS NULL
    """
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(sql, (report_name,))
        return [r[0] for r in cur.fetchall()]

# ─────────────────────────────────────────
# Report structure helpers  (ADD this)
# ─────────────────────────────────────────
def define_expected_table(
    report_name: str,
    table_alias: str,
    required: bool = True,
    expected_cutoff: str | None = None,
    db_path: str = "database/reporting.db",
):
    """
    Upsert a row in report_structure WITHOUT needing a UNIQUE index.
    """
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id FROM report_structure
            WHERE report_name = ? AND table_alias = ?
            """,
            (report_name, table_alias),
        )
        row = cur.fetchone()

        if row:  # --- update ---
            cur.execute(
                """
                UPDATE report_structure
                SET required = ?,
                    expected_cutoff = ?
                WHERE id = ?
                """,
                (int(required), expected_cutoff, row[0]),
            )
        else:    # --- insert ---
            cur.execute(
                """
                INSERT INTO report_structure
                      (report_name, table_alias, required, expected_cutoff)
                VALUES (?, ?, ?, ?)
                """,
                (report_name, table_alias, int(required), expected_cutoff),
            )
        conn.commit()

# helper functions

def upsert_report_param(report_name: str, key: str, value: Any,
                        db_path="database/reporting.db") -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            INSERT INTO report_parameters (report_name, param_key, param_value)
            VALUES (?,?,?)
            ON CONFLICT(report_name, param_key) DO UPDATE
            SET param_value = excluded.param_value
        """, (report_name, key, json.dumps(value)))
        conn.commit()

def load_report_params(report_name: str, db_path="database/reporting.db") -> dict:
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT param_key, param_value FROM report_parameters WHERE report_name = ?",
                    (report_name,))
        return {k: json.loads(v) for k, v in cur.fetchall()}
    

# ─────────────────────────────────────────
# Report Objects (Dynamic Content)
# ─────────────────────────────────────────

def save_report_object(
    object_name: str,
    object_type: str,
    description: str | None,
    sql_query: str | None,
    python_code: str | None,
    report_context: str | None = None,
    db_path: str = "database/reporting.db",
) -> int:
    """Saves or updates a report object definition."""
    now = datetime.now().isoformat()
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id FROM report_objects WHERE object_name = ?", (object_name,)
        )
        row = cursor.fetchone()
        if row:
            # Update existing object
            obj_id = row[0]
            cursor.execute(
                """
                UPDATE report_objects
                SET object_type = ?, description = ?, sql_query = ?,
                    python_code = ?, report_context = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    object_type,
                    description,
                    sql_query,
                    python_code,
                    report_context,
                    now,
                    obj_id,
                ),
            )
            print(f"Updated object: {object_name}")
        else:
            # Insert new object
            cursor.execute(
                """
                INSERT INTO report_objects (
                    object_name, object_type, description, sql_query,
                    python_code, report_context, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    object_name,
                    object_type,
                    description,
                    sql_query,
                    python_code,
                    report_context,
                    now,
                    now,
                ),
            )
            obj_id = cursor.lastrowid
            print(f"Inserted new object: {object_name} (ID: {obj_id})")
        conn.commit()
        return obj_id

def get_report_object(
    object_name: str, db_path: str = "database/reporting.db"
) -> dict | None:
    """Fetches a specific report object definition by name."""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row # Return results as dict-like rows
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM report_objects WHERE object_name = ?", (object_name,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None

def list_report_objects(
    report_context: str | None = None, db_path: str = "database/reporting.db"
) -> pd.DataFrame:
    """Lists all report objects, optionally filtered by report context."""
    with sqlite3.connect(db_path) as conn:
        query = "SELECT id, object_name, object_type, description, report_context, updated_at FROM report_objects"
        params = []
        if report_context:
            # Allows filtering for objects specific to a report OR global objects
            query += " WHERE report_context = ? OR report_context IS NULL"
            params.append(report_context)
        query += " ORDER BY object_name"
        df = pd.read_sql_query(query, conn, params=params)
        return df


def delete_report_object(
    object_name: str, db_path: str = "database/reporting.db"
) -> None:
    """Deletes a report object by name."""
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM report_objects WHERE object_name = ?", (object_name,))
        conn.commit()
        print(f"Deleted object: {object_name}")


