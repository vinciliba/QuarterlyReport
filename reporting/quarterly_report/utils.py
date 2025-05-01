## reporting/quarterly/utils.py   (simplified)
from typing import Any, Dict
from dataclasses import dataclass
import sqlite3, pandas as pd

class BaseModule:
    def run(self, ctx, cutoff, db_path):
        raise NotImplementedError
    
@dataclass
class RenderContext:
    db:  "Database"                 # wrapper around sqlite3 connection
    params: dict                    # report parameters (already looked up)
    cutoff: str                     # ISO date string
    out: Dict[str, Dict[str, Any]]  # artefacts collected along the way

class Database:                     # very thin helper
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
    def read_table(self, name) -> pd.DataFrame:
        return pd.read_sql_query(f"SELECT * FROM {name}", self.conn)

# In utils.py (updated)
def get_modules(report_name):
    from reporting.quarterly_report.modules_registry import MODULES
    return MODULES
