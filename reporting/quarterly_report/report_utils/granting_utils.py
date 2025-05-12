# ---------------------------------------------------------------------------
# helpers for the GAP module  (put at top of granting.py or in utils)
# ---------------------------------------------------------------------------
import numpy as np
import pandas as pd

# 1) mappings ────────────────────────────────────────────────────────────────
_GAP_REPLACE = {
    "GAFile-OVA": "3_Under OVA",
    "JagateCreateCommitment": "3_Under OVA",
    "parallelFVAInclusiveGatewayOut": "4_Under FVA",
    "GAFile-FVA-OPT": "4_Under FVA",
    "GAFile-FVA": "4_Under FVA",
    "GAFile-ApproveMO": "5_Pending Approval",
    "GAFile-ApproveMD": "5_Pending Approval",
    "WaitGACooSignatureEG": "6_Approved - wait HI",
    "PrepareGrantDocumentsForCOOSignature": "6_Approved - wait HI",
    "PrepareGrantDocumentsForCOOSignature2": "6_Approved - wait HI",
    "SA-Wait": "7_Approved - wait SA",
    "WaitInitAgentVISAId": "8_Authorisation",
    "CL2-AO": "8_Authorisation",
    "CL2-AO-Autoclose": "8_Authorisation",
    "Decision-Verify": "8_Authorisation",
    "Decision-Encode": "5.1_Encode Commission Decision",
    "GASign-EC": "8_Authorisation",
    "AbacContractCreatedCatchEvent": "9_Completed",
    "Aforms-Wait": "9_Completed",
    "AForms-No-Sign": "9_Completed",
    "WaitECSignatureComplete": "9_Completed",
    "JagateCreateLegalCommitment": "9_Completed",
    "WaitPpgmsActivityGapGASignedCompleted": "9_Completed",
    "CL2_FINAL": "8_Authorisation",
    "ERL-RS-Parking": "0_Reserve list",
    "ERL-RJ-Parking": "00_Rejected Proposal",
    "Terminated": "10_Terminated",
    "N/A": "error",
}

_PANELS = {
    # LS
    "Applied Life Sciences and Non-Medical Biotechnology": "LS",
    "Applied Life Sciences, Biotechnology, and Molecular and Biosystems Engineering": "LS",
    "Applied Medical Technologies, Diagnostics, Therapies and Public Health": "LS",
    "Cellular and Developmental Biology": "LS",
    "Diagnostics, Therapies, Applied Medical Technology and Public Health": "LS",
    "Evolutionary, Population and Environmental Biology": "LS",
    "Ecology, Evolution and Environmental Biology": "LS",
    "Genetics, Genomics, Bioinformatics and Systems Biology": "LS",
    "Genetics, Omics, Bioinformatics and Systems Biology": "LS",
    "Immunity and Infection": "LS",
    "Molecular and Structural Biology and Biochemistry": "LS",
    "Molecular Biology, Biochemistry, Structural Biology and Molecular Biophysics": "LS",
    "Neurosciences and Neural Disorders": "LS",
    "Physiology, Pathophysiology and Endocrinology": "LS",
    "Neuroscience and Neural Disorders": "LS",
    # PE
    "Computer Science and Informatics": "PE",
    "Condensed Matter Physics": "PE",
    "Earth System Science": "PE",
    "Fundamental Constituents of Matter": "PE",
    "Mathematics": "PE",
    "Physical and Analytical Chemical Sciences": "PE",
    "Products and Processes Engineering": "PE",
    "Synthetic Chemistry and Materials": "PE",
    "Systems and Communication Engineering": "PE",
    "Universe Sciences": "PE",
    # SH
    "Cultures and Cultural Production": "SH",
    "Individuals, Markets and Organisations": "SH",
    "Institutions, Values, Environment and Space": "SH",
    "The Human Mind and Its Complexity": "SH",
    "The Social World, Diversity, Population": "SH",
    "The Study of the Human Past": "SH",
}

_DATE_COLS = [
    "GA Signature - Commission",
    "Call Closing Date",
    "Invitation Letter Sent",
    "Evaluation Result Letter Sent",
]

def _coerce_dates(out: pd.DataFrame) -> None:
    """Convert the date columns we need to pandas Timestamps (in-place)."""
    for col in _DATE_COLS:
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce")
        else:
            raise KeyError(f"Expected date column {col!r} missing in grants dataframe")
        
def _coerce_date_columns(df: pd.DataFrame) -> None:
    """
    In-place: turn every column whose name ends with 'date' / 'visa'
    / 'signature' (case-insensitive) into datetime64[ns].  Silently
    converts non-parsable values to NaT.
    """
    CANDIDATES = [c for c in df.columns
                  if c.lower().endswith(("date", "visa", "signature"))]

    for col in CANDIDATES:
        if not pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.to_datetime(df[col], errors="coerce")

def _ensure_timedelta_cols(out: pd.DataFrame) -> None:
    """Create TTG/TTS/TTI columns if absent (works whatever dtypes we start with)."""
    if {"TTG_timedelta", "TTS_timedelta", "TTI_timedelta"}.issubset(out.columns):
        return                # already there

    _coerce_dates(out)        # <── NEW: make sure date parsing happened

    main = out["Ranking Status"].eq("MAIN")

    out["TTG_timedelta"] = np.where(
        main,
        out["GA Signature - Commission"] - out["Call Closing Date"],
        (out["GA Signature - Commission"] - out["Invitation Letter Sent"])
        + (out["Evaluation Result Letter Sent"] - out["Call Closing Date"]),
    )

    out["TTS_timedelta"] = np.where(
        main,
        out["GA Signature - Commission"] - out["Evaluation Result Letter Sent"],
        (out["GA Signature - Commission"] - out["Evaluation Result Letter Sent"])
        - (out["Invitation Letter Sent"] - out["Evaluation Result Letter Sent"]),
    )

    out["TTI_timedelta"] = (
        out["Evaluation Result Letter Sent"] - out["Call Closing Date"]
    )
    
# 2) vectorised transform ────────────────────────────────────────────────────
def enrich_grants(df: pd.DataFrame) -> pd.DataFrame:
    """
    Vectorised re-labelling & helper-columns for the GAP pipeline.
    Assumes the original column names used in your notebook.
    Returns **new dataframe**; original is not modified.
    """
    out = df.copy()

    # --- simple flags -------------------------------------------------------
    out["SIGNED"] = (out["GA Signature - Commission"].notna()).astype(int)
    out["ACTIVE"] = (out["Project Status"] != "REJECTED").astype(int)

    # --- GAP_STEP normalisation  -------------------------------------------
    out["GAP_STEP "] = (
        out["GAP_STEP "]
        .replace(_GAP_REPLACE)                 # direct mapping
        .fillna(out["GAP_STEP "])              # keep existing values
    )

    # rule-based patching when NaN/odd values
    conditions = [
        # signed but ‘Exclusive Gateway’ → Completed
        (out["GAP_STEP "] == "Exclusive Gateway") & (out["Project Status"] == "SIGNED"),
        # CLOSED with NaN GAP_STEP
        out["Project Status"].eq("CLOSED") & out["GAP_STEP "].isna(),
        # UNDER_PREPARATION + GA_SIGNED flag
        (out["Project Status"].eq("UNDER_PREPARATION"))
        & (out["GAP_EXERCISE_STATUS "].eq("GA_SIGNED"))
        & out["GAP_STEP "].isna(),
        # plain SIGNED with NaN GAP_STEP
        out["Project Status"].eq("SIGNED") & out["GAP_STEP "].isna(),
        # REJECTED with NaN GAP_STEP
        out["Project Status"].eq("REJECTED") & out["GAP_STEP "].isna(),
        # UNDER_PREPARATION fallback
        out["Project Status"].eq("UNDER_PREPARATION") & out["GAP_STEP "].isna(),
        # TERMINATED fallback
        out["Project Status"].eq("TERMINATED") & out["GAP_STEP "].isna(),
        # SUSPENDED fallback
        out["Project Status"].eq("SUSPENDED") & out["GAP_STEP "].isna(),
    ]
    choices = [
        "9_Completed",
        "9_Completed",
        "8_Authorisation",
        "9_Completed",
        "01 Early termination",
        "2_Initiation with PO",
        "Terminated-after signature",
        "9_Completed",
    ]
    out["GAP_STEP "] = np.select(conditions, choices, default=out["GAP_STEP "])

    # --- ERC panel remap ----------------------------------------------------
    out["ERC_PANEL "] = (
        out["ERC_PANEL "]
        .replace(_PANELS)
        .mask(
            out["Instrument"].isin(["ERC-POC", "ERC-SyG", "ERC-POC-LS"]),
            "PC-SY",
        )
    )

    # --- Call-year & Instrument fix ----------------------------------------
    out["CALL_YEAR"] = out["Call"].str[4:8]

    # encode bug: Instrument derived from Call suffix
    out.loc[:, "Instrument"] = (
        "ERC-" + out["Call"].str[-3:]
    )

    # --- ethics status simplification --------------------------------------
    out["ETHIC_STATUS"] = out["ETHICS REVIEW OPINION"].replace(
        {"CONDITIONALLY_CLEARED": "CLEARED", "PENDING": "NOT CLEARED"}
    )
    out.loc[out["GAP_STEP "] == "01 Early termination", "ETHIC_STATUS"] = "CLEARED"
    out.loc[out["Project Status"] == "CLOSED",           "ETHIC_STATUS"] = "CLEARED"

    # --- KPI timedelta columns → numeric (days) ----------------------------
    out["TTG"] = (out["TTG_timedelta"] / np.timedelta64(1, "D")).round(2)
    out["TTS"] = (out["TTS_timedelta"] / np.timedelta64(1, "D")).round(2)
    out["TTI"] = (out["TTI_timedelta"] / np.timedelta64(1, "D")).round(2)

    # drop raw timedeltas if not needed anymore
    out.drop(columns=["TTG_timedelta", "TTS_timedelta", "TTI_timedelta"], inplace=True)

    return out

