# In reporting/quarterly_report/modules/granting.py
from __future__ import annotations
from reporting.quarterly_report.utils import RenderContext, BaseModule
import streamlit as st
from docx import Document

class GrantsModule(BaseModule):
    name = "GAP"
    description = "GAP statistics, and tables"

    def run(self, ctx: RenderContext, cutoff, db_path: str):
        anchor_name = "TEST_GRANTING"
        message = "✅ Hello Daniele from GrantsModule!"
        print(message)

        if "staged_docx" in st.session_state:
            st.session_state.staged_docx.add_paragraph(f"GrantsModule: {message}")

        ctx[anchor_name] = message
        return ctx, (True, f"{anchor_name}:{message}")
