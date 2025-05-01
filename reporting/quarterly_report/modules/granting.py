# In reporting/quarterly_report/modules/granting.py
from __future__ import annotations
from reporting.quarterly_report.utils import RenderContext, BaseModule
import streamlit as st
from docx import Document

class GrantsModule(BaseModule):
    name = "GAP"
    description = "GAP statistics, and tables"

    def run(self, ctx: RenderContext, cutoff, db_path: str):
        message = "✅ Hello Daniele from GrantsModule!"
        print(message)

        # Add the message to the staged report (staged_docx)
        if "staged_docx" in st.session_state:
            doc = st.session_state.staged_docx
            doc.add_paragraph(f"GrantsModule: {message}")

        # Return a context dictionary to be used for final template rendering
        ctx["TEST_GRANTING"] = message
        return ctx, (True, "✅ GrantsModule ran successfully.")