import os
import sqlite3, json
from datetime import datetime
# helper functions
from typing import Any
import pandas as pd 
from datetime import date, datetime, timedelta
import logging
from pathlib import Path
from typing import Any,  Dict, Type
import importlib.util
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service


service = Service(ChromeDriverManager().install())


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
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

        # 9) Which modules belong to which report (and in which order)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS report_modules (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                report_name   TEXT,
                module_name   TEXT,          -- e.g. 'Budget', must exist in MODULES dict
                run_order     INTEGER,       -- 1-based ordering
                enabled       BOOLEAN,       -- ticked/unticked in Admin UI
                UNIQUE (report_name, module_name)
            )
        """)

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
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS report_variables (
                report_name TEXT,
                module_name TEXT,
                var_name TEXT,
                value TEXT,
                gt_image BLOB,
                anchor_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(report_name, var_name)
            )
        ''')
 
       # Optional: Add an index for faster lookups
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_report_variables_name
            ON report_variables(var_name);
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

def alias_exists(alias: str, db_path: str = "database/reporting.db") -> bool:
    """
    Return True if <alias> appears in file_alias_map.alias, else False.
    """
    with sqlite3.connect(db_path) as con:
        cur = con.execute(
            "SELECT 1 FROM file_alias_map WHERE alias = ? LIMIT 1", (alias,)
        )
        return cur.fetchone() is not None
    
# ─────────────────────────────────────────
# Report structure helpers  (ADD this)
# ─────────────────────────────────────────

def ensure_report_modules_table(db_path: str) -> None:
    """Ensure the report_modules table exists in the database."""
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS report_modules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                report_name TEXT NOT NULL,
                module_name TEXT NOT NULL,
                run_order INTEGER NOT NULL,
                enabled BOOLEAN NOT NULL,
                UNIQUE(report_name, module_name)
            )
        """)
        conn.commit()



def crawl_for_modules_registry(reporting_root: str = "reporting") -> Dict[str, Dict[str, Type[Any]]]:
    """
    Crawl the reporting folder for modules_registry.py files and load their MODULES dictionaries.

    Args:
        reporting_root (str): Root directory to start the search (default: "reporting").

    Returns:
        Dict[str, Dict[str, Type[Any]]]: Mapping of report package paths to their MODULES dictionaries.
    """
    modules_mapping = {}
    reporting_path = Path(reporting_root)

    if not reporting_path.exists():
        logger.error(f"Reporting directory not found: {reporting_root}")
        return modules_mapping

    # Walk through the reporting directory
    for root, dirs, files in os.walk(reporting_path):
        if "modules_registry.py" in files:
            registry_path = Path(root) / "modules_registry.py"
            logger.debug(f"Found modules_registry.py at: {registry_path}")
            try:
                # Convert the file path to a module path
                relative_path = os.path.relpath(registry_path, reporting_path.parent)
                module_name = relative_path.replace(os.sep, ".").replace(".py", "")
                logger.debug(f"Attempting to load module: {module_name}")
                
                spec = importlib.util.spec_from_file_location(module_name, registry_path)
                if spec is None:
                    logger.error(f"Could not create spec for {registry_path}")
                    continue
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                logger.debug(f"Successfully loaded module: {module_name}")

                # Extract the MODULES dictionary
                if hasattr(module, "MODULES"):
                    modules_mapping[module_name] = module.MODULES
                    logger.debug(f"Loaded MODULES from {module_name}: {list(module.MODULES.keys())}")
                else:
                    logger.warning(f"No MODULES dictionary found in {registry_path}")
            except Exception as e:
                logger.error(f"Error loading {registry_path}: {str(e)}", exc_info=True)
                continue

    logger.debug(f"Final modules registries: {list(modules_mapping.keys())}")
    return modules_mapping

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


# ─────────────────────────────────────────
# Report ⇢ Module mapping
# ─────────────────────────────────────────
def list_report_modules(report_name: str, db_path="database/reporting.db"):
    """Return DataFrame with id, module_name, run_order, enabled."""
    import pandas as pd
    with sqlite3.connect(db_path) as conn:
        return pd.read_sql_query("""
            SELECT id, module_name, run_order, enabled
            FROM report_modules
            WHERE report_name = ?
            ORDER BY run_order
        """, conn, params=(report_name,))

def upsert_report_module(report_name: str, module_name: str,
                         run_order: int = None, enabled: bool = True,
                         db_path="database/reporting.db"):
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO report_modules (report_name, module_name, run_order, enabled)
            VALUES (?,?,?,?)
            ON CONFLICT(report_name, module_name) DO UPDATE
            SET run_order = excluded.run_order,
                enabled   = excluded.enabled
        """, (report_name, module_name, run_order, int(enabled)))
        conn.commit()

def delete_report_module(row_id: int, db_path="database/reporting.db"):
    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM report_modules WHERE id = ?", (row_id,))
        conn.commit()

#-------------  Create Report Variables  ------------------
from pathlib import Path
import altair as alt
import altair_saver
import logging
import great_tables
import os
import sqlite3
import json
from typing import Any


def altair_chart_to_path(chart: alt.TopLevelMixin, var_name: str, folder: str = "charts_out") -> str:
    """
    Save an Altair chart as PNG to disk and return its file path.

    Args:
        chart: Altair chart object (Chart or LayerChart) to render.
        var_name: Name for the output PNG file.
        folder: Directory to save the PNG (default: 'charts_out').

    Returns:
        File path of the saved PNG as a string.

    Raises:
        ValueError: If chart is not an Altair chart object.
        RuntimeError: If chart rendering fails.
    """
  

    if not isinstance(chart, alt.TopLevelMixin):
        raise ValueError(f"Expected alt.TopLevelMixin (Chart or LayerChart), got {type(chart)}")

    # folder_path = Path(folder)
    # folder_path.mkdir(exist_ok=True)
    # out_path = folder_path / f"{var_name}.png"

    save_dir = "charts_out"
    os.makedirs(save_dir, exist_ok=True)
    out_path = os.path.join(save_dir, f"{var_name}_tta_chart.png")

    try:
        altair_saver.save(chart, out_path, method="selenium", webdriver="chrome")
        logging.debug(f"Saved Altair chart to {out_path}")
        return str(out_path)  # Store absolute path for consistency
    except Exception as e:
        logging.error(f"Failed to render Altair chart {var_name}: {str(e)}", exc_info=True)
        raise RuntimeError(f"Failed to render Altair chart {var_name}: {str(e)}")
    


def save_gt_table_smart(gt_table, file_path, var_name):
    """
    Intelligently save GT table with optimal window size based on content and table type.
    Balanced approach to prevent both cut-offs and excessive width.
    """
    from pathlib import Path
    import logging
    import time
    
    file_path = Path(file_path)
    file_path.parent.mkdir(exist_ok=True)
    
    # Delete existing file if it exists to avoid rename conflicts
    if file_path.exists():
        try:
            file_path.unlink()
            logging.debug(f"Deleted existing file: {file_path}")
        except Exception as e:
            logging.warning(f"Could not delete existing file {file_path}: {e}")
    
    # More balanced defaults - narrower width, adequate height
    default_width = 1200  # Reduced from 1800 - more appropriate for most tables
    default_height = 1500  # Good height to prevent cut-offs
    
    # Adjust dimensions based on table type
    if any(keyword in var_name.lower() for keyword in ['overview', 'summary', 'resources']):
        # Overview/summary tables are usually narrow
        default_width = 1000
        default_height = 1200
    elif any(keyword in var_name.lower() for keyword in ['table_1b', 'table_1c', 'table_1a']):
        # Budget tables might need more width for multiple columns
        default_width = 1400
        default_height = 2000
    elif any(keyword in var_name.lower() for keyword in ['external_audits', 'table_11']):
        # Audit tables - medium width
        default_width = 1200
        default_height = 1800
    elif any(keyword in var_name.lower() for keyword in ['he_', 'horizon', 'vobu']):
        # HE tables - based on your examples
        default_width = 1300
        default_height = 1500
    
    # Try sizes in order - start conservative, expand if needed
    window_sizes = [
        (default_width, default_height),
        (default_width + 200, default_height + 500),  # Slightly larger
        (1400, 2000),  # Fallback size
    ]
    
    last_exception = None
    
    for i, (width, height) in enumerate(window_sizes):
        try:
            start_time = time.time()
            logging.debug(f"Attempting GT save for {var_name} with size {width}x{height} (attempt {i+1}/{len(window_sizes)})")
            
            # Save the table
            gt_table.save(
                file_path, 
                web_driver='chrome', 
                window_size=(width, height)
            )
            
            # Small sleep to ensure file system catches up
            time.sleep(0.3)
            
            elapsed = time.time() - start_time
            
            if file_path.exists():
                file_size = file_path.stat().st_size
                logging.debug(f"GT table {var_name} saved in {elapsed:.1f}s: {width}x{height} = {file_size} bytes")
                
                # Check if file size seems reasonable
                if file_size > 15000:  # 15KB minimum for a complete table
                    return str(file_path)
                elif i == len(window_sizes) - 1:
                    # Last attempt - accept whatever we got
                    logging.warning(f"GT table {var_name} accepting small file ({file_size} bytes) on last attempt")
                    return str(file_path)
                else:
                    # Try next size
                    logging.warning(f"GT table {var_name} file size only {file_size} bytes, trying larger dimensions")
                    file_path.unlink()
                    continue
                    
            else:
                logging.warning(f"GT table {var_name} save failed - no file created")
                
        except Exception as e:
            last_exception = e
            logging.error(f"GT table {var_name} save attempt {i+1} failed: {e}")
            if file_path.exists():
                try:
                    file_path.unlink()
                except:
                    pass
    
    # Final attempt with moderate dimensions
    try:
        logging.debug(f"Final fallback attempt for GT table {var_name}")
        gt_table.save(
            file_path, 
            web_driver='chrome', 
            window_size=(1200, 1800)
        )
        if file_path.exists():
            return str(file_path)
    except Exception as e:
        last_exception = e
    
    if last_exception:
        raise Exception(f"Failed to save GT table {var_name} after all attempts: {last_exception}")
    else:
        raise Exception(f"Failed to save GT table {var_name} - file not created")

def insert_variable(
    report: str,
    module: str,
    var: str,
    value: Any,
    db_path: str,
    anchor: str | None = None,
    gt_table: great_tables.GT | None = None,
    altair_chart: alt.TopLevelMixin | None = None,
) -> None:
    import time
    
   
    """
    Overwrite the row (report_name, var_name) with a new value (and picture path).

    Exactly ONE row per variable is kept. Either gt_table or altair_chart can be provided, not both.
    Stores the file path to the rendered PNG in gt_image instead of the image bytes.

    Args:
        report: Report name.
        module: Module name.
        var: Variable name.
        value: Value to serialize (e.g., DataFrame, dict).
        db_path: Path to SQLite database.
        anchor: Anchor name (defaults to var).
        gt_table: great_tables.GT object to render as PNG.
        altair_chart: Altair chart object (Chart or LayerChart) to render as PNG.

    Raises:
        ValueError: If both gt_table and altair_chart are provided or invalid types.
        sqlite3.Error: If database operations fail.
    """
    if gt_table is not None and altair_chart is not None:
        raise ValueError("Cannot provide both gt_table and altair_chart")
    if gt_table is not None and not isinstance(gt_table, great_tables.GT):
        raise ValueError(f"Expected great_tables.GT, got {type(gt_table)}")
    if altair_chart is not None and not isinstance(altair_chart, alt.TopLevelMixin):
        raise ValueError(f"Expected alt.TopLevelMixin (Chart or LayerChart), got {type(altair_chart)}")

    con = sqlite3.connect(db_path)
    cur = con.cursor()
    try:
        # 1) Remove any previous copy of this variable
        cur.execute(
            "DELETE FROM report_variables WHERE report_name = ? AND var_name = ?",
            (report, var),
        )

        # 2) Serialize the Python value
        val_json = json.dumps(value, default=str)

        # 3) Optional: Render great-tables or Altair chart to PNG and store the path
        gt_image = None
        if gt_table is not None:
            logging.debug(f"Rendering gt_table for {var}")
            tmp = Path(f"charts_out/{var}_gt.png")
            tmp.parent.mkdir(exist_ok=True)

            # gt_table.save(tmp, web_driver='chrome', window_size=(8000, 8000))  # Playwright renders PNG
            # gt_image = str(tmp)
          

            # # Improved GT table save with better parameters for styling preservation
            # gt_table.save(
            #     tmp, web_driver='chrome', window_size=(1400, 1000)
            # )
            # gt_image = str(tmp)
            # logging.debug(f"Saved great_tables to {gt_image}")

             # Smart GT table save with automatic size detection
            gt_image = save_gt_table_smart(gt_table, tmp, var)
            time.sleep(0.5)
            logging.debug(f"Saved great_tables to {gt_image}")
        elif altair_chart is not None:
            logging.debug(f"Rendering altair_chart for {var}")
            gt_image = altair_chart_to_path(altair_chart, var)
            logging.debug(f"Saved Altair chart path: {gt_image}")

        # 4) Insert the fresh row
        cur.execute(
            """
            INSERT INTO report_variables
                  (report_name, module_name, var_name,
                   anchor_name, value, gt_image, created_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (report, module, var, anchor or var, val_json, gt_image),
        )

        con.commit()
        logging.debug("Stored variable %s for report %s (rowid=%s)",
                      var, report, cur.lastrowid)

    except Exception as exc:
        con.rollback()
        logging.error("insert_variable failed for %s/%s: %s", report, var, exc, exc_info=True)
        raise
    finally:
        con.close()

def fetch_vars_for_report(report_name, db_path):
    con = sqlite3.connect(db_path)
    df = pd.read_sql_query('''
        SELECT anchor_name, value FROM report_variables
        WHERE report_name = ?
    ''', con, params=(report_name,))
    con.close()
    context = {}
    for _, row in df.iterrows():
        try:
            context[row["anchor_name"]] = json.loads(row["value"])
        except (json.JSONDecodeError, TypeError, ValueError):
            context[row["anchor_name"]] = row["value"]
    return context

def fetch_gt_image(report_name, var_name, db_path):
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute('''
            SELECT gt_image, anchor_name
            FROM report_variables
            WHERE report_name = ? AND var_name = ?
            ORDER BY created_at DESC
            LIMIT 1
        ''', (report_name, var_name))
        result = cursor.fetchone()
        logging.debug(f"fetch_gt_image result for {var_name}: {result}")
        if result:
            gt_image, anchor_name = result
            return gt_image, anchor_name if anchor_name else var_name  # Fallback to var_name if anchor_name is None
        return None, None
    except Exception as e:
        logging.error(f"Error fetching gt_image for {var_name}: {str(e)}")
        raise
    finally:
        conn.close()

def get_variable_status(report_name, db_path):
    con = sqlite3.connect(db_path)
    try:
        # Query all columns except gt_image, which is a BLOB
        df = pd.read_sql_query('''
            SELECT var_name, value, created_at,
                   julianday('now') - julianday(created_at) as age_days
            FROM report_variables
            WHERE report_name = ?
        ''', con, params=(report_name,))

        # Debug: Log the raw DataFrame
        logging.debug(f"Raw DataFrame dtypes:\n{df.dtypes}")
        logging.debug(f"Raw DataFrame head:\n{df.head().to_string()}")

        # Ensure all columns are string-safe
        # Handle var_name
        df['var_name'] = df['var_name'].astype(str)

        # Handle value column: decode JSON and convert to string for display
        def safe_json_load(x):
            try:
                return json.loads(x) if x else None
            except json.JSONDecodeError as e:
                logging.warning(f"Failed to decode JSON in value column: {x[:100]}... Error: {str(e)}")
                return "Invalid JSON"

        def safe_str(x):
            try:
                if isinstance(x, (dict, list)):
                    return str(x)[:100] + "..."
                return str(x) if x is not None else "N/A"
            except Exception as e:
                logging.warning(f"Failed to convert to string: {x}. Error: {str(e)}")
                return "Unrepresentable Data"

        df['value'] = df['value'].apply(safe_json_load)
        df['value'] = df['value'].apply(safe_str)

        # Handle created_at
        df['created_at'] = df['created_at'].astype(str)

        # Handle age_days
        df['age_days'] = df['age_days'].astype(float).round(2)

        logging.debug(f"Processed DataFrame head:\n{df.head().to_string()}")
        logging.debug(f"Fetched variable status for report '{report_name}' with {len(df)} rows")
        return df
    except Exception as e:
        logging.error(f"Error fetching variable status for report '{report_name}': {str(e)}")
        raise
    finally:
        con.close()


# === workflow step: derived date computation ===
def compute_cutoff_related_dates(cutoff_date: date) -> dict:
    first = cutoff_date.replace(day=1)
    lastMonth = first - timedelta(days=1)
    a = date(cutoff_date.year, 1, 1)
    last_mont_Name = lastMonth.strftime("%b")

    lastYear_date = date(cutoff_date.year - 1, 12, 31)
    lastYear_year = lastYear_date.year
    lastYear = lastYear_date.strftime('%d/%m/%Y')
    previous_month_number = lastMonth.month
    previous_month_year = lastMonth.year

    if previous_month_number == 12:
        year = lastYear_year
        current_year = lastYear_year
        end_year_report = True
    else:
        year = cutoff_date.year
        current_year = cutoff_date.year
        end_year_report = False

    last_date = lastMonth.strftime("%d/%m/%Y")
    first_day = a.strftime("%d/%m/%Y")
    report_date = lastMonth.strftime("%B %Y")
    overviewDate = f"{lastMonth.strftime('%B')} {year}"
    overView_month = lastMonth.strftime("%B")
    two_Month_ago = first - timedelta(days=31)
    overview_two_Month_ago = two_Month_ago.strftime("%B")

    today = date.today()
    current_quarter = (today.month - 1) // 3 + 1
    if current_quarter == 1:
        previous_quarter = (4, today.year - 1)
    else:
        previous_quarter = (current_quarter - 1, today.year)
    quarter_period = f"Quarter {previous_quarter[0]} - {previous_quarter[1]}"

    return {
        "last_month_name": last_mont_Name,
        "lastYear": lastYear,
        "last_date": last_date,
        "first_day": first_day,
        "report_date": report_date,
        "overviewDate": overviewDate,
        "overView_month": overView_month,
        "overview_two_Month_ago": overview_two_Month_ago,
        "current_year": current_year,
        "end_year_report": end_year_report,
        "quarter_period": quarter_period,
    }

# ingestion/db_utils.py
def get_existing_rule_for_report(report, filename, db_path="database/reporting.db"):
    """
    Return (sheet_name, start_row) for <filename>.
    • If sheet_rules has a report_name column → use it.
    • Otherwise fall back to any rule that matches the filename only.
    """
    with sqlite3.connect(db_path) as con:
        # 1. detect columns
        cols = [c[1] for c in con.execute("PRAGMA table_info(sheet_rules)")]

        if "report_name" in cols:
            row = con.execute(
                """SELECT sheet_name, start_row
                     FROM sheet_rules
                    WHERE report_name = ? AND filename = ?
                    LIMIT 1""",
                (report, filename)
            ).fetchone()
            if row:        # exact (report+file) rule found
                return row

        # 2. fallback: any rule for this filename
        row = con.execute(
            """SELECT sheet_name, start_row
                 FROM sheet_rules
                WHERE filename = ?
                LIMIT 1""",
            (filename,)
        ).fetchone()
        return row if row else (None, None)


def fetch_latest_table_data(conn: sqlite3.Connection, table_alias: str, cutoff: pd.Timestamp) -> pd.DataFrame:
    cutoff_str = cutoff.isoformat()
    logging.debug(f"Fetching latest data for table_alias: {table_alias}, cutoff: {cutoff_str}")
    
    # Fetch all upload logs ordered by closeness to cutoff
    query = """
        SELECT uploaded_at, id
        FROM upload_log
        WHERE table_alias = ?
        ORDER BY ABS(strftime('%s', uploaded_at) - strftime('%s', ?))
    """
    results = conn.execute(query, (table_alias, cutoff_str)).fetchall()
    logging.debug(f"Upload log query results for {table_alias}: {results}")

    if not results:
        logging.warning(f"No uploads found for table alias '{table_alias}' near cutoff {cutoff_str}")
        return pd.DataFrame()

    # Iterate through upload_ids until we find one with data
    for uploaded_at, upload_id in results:
        logging.debug(f"Checking upload_id: {upload_id}, uploaded_at: {uploaded_at}")
        df = pd.read_sql_query(
            f"SELECT * FROM {table_alias} WHERE upload_id = ?",
            conn,
            params=(upload_id,)
        )
        if not df.empty:
            logging.debug(f"Fetched {len(df)} rows from {table_alias} with upload_id {upload_id}")
            return df
        logging.debug(f"No data found for upload_id {upload_id} in {table_alias}")

    logging.warning(f"No data found for any upload_id for table alias '{table_alias}'")
    return pd.DataFrame()