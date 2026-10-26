import json
import sqlite3
import pandas as pd
import streamlit as st
from pipeline import VeritasPipeline

st.set_page_config(page_title="Veritas Claims — Medical Data Standardisation UI", layout="wide")
st.title("🏥 Veritas Claims — Medical Data Standardisation & Validation Console")

if st.sidebar.button("🔄 Run / Re-Run Pipeline (Idempotent)"):
    pipe = VeritasPipeline()
    stats = pipe.run("sample-data")
    st.sidebar.success(f"Processed {stats['files_processed']} files! Cross-file dups suppressed: {stats['cross_file_dups']}")

conn = sqlite3.connect("veritas_claims.db")
df_ideal = pd.read_sql("SELECT * FROM canonical_records_ideal_schema", conn)
df_pivot = pd.read_sql("SELECT * FROM canonical_pivoted_reports", conn)
df_audit = pd.read_sql("SELECT * FROM raw_audit_trail", conn)
df_dlq = pd.read_sql("SELECT * FROM error_dlq", conn)

# FR-5.1: Pipeline Dashboard
st.subheader("1. Daily Ingestion & Validation Overview (FR-5.1)")
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Files Ingested", len(df_audit))
c2.metric("Canonical Records", len(df_ideal))
c3.metric("Outliers Flagged", int((df_ideal["test_analytics"] == "Outlier").sum()))
c4.metric("Out-of-Range (Above/Below)", int(df_ideal["test_analytics"].isin(["Above Range", "Below Range"]).sum()))
c5.metric("Invalid / DLQ Errors", int((df_ideal["test_analytics"] == "Invalid").sum() + len(df_dlq)))

tab1, tab2, tab3, tab4 = st.tabs([
    "🚨 Flagged Records Queue (FR-5.3)",
    "🔍 Record Inspector: Raw vs Canonical (FR-5.2)",
    "🏥 Clinic / Source Quality Scorecard (FR-5.4)",
    "📊 Canonical Tables (78-Col Ideal & FR-2.2 Wide)"
])

with tab1:
    st.markdown("### Flagged Lab Results Requiring Ops Review (`Outlier`, `Invalid`, `Above Range`, `Below Range`)")
    flag_filter = st.multiselect(
        "Filter by Analytics Flag:",
        ["Outlier", "Invalid", "Above Range", "Below Range"],
        default=["Outlier", "Invalid"]
    )
    flagged_df = df_ideal[df_ideal["test_analytics"].isin(flag_filter)][[
        "document_id", "source_system", "claim_no", "test_name_original",
        "test_name_canonical", "result_text_original", "result_value",
        "unit_canonical", "range_text", "test_analytics", "normalization_method"
    ]]
    st.dataframe(flagged_df, use_container_width=True)

with tab2:
    st.markdown("### Side-by-Side Raw JSON vs. Standardised Canonical Output")
    selected_doc = st.selectbox("Select Document ID:", df_audit["document_id"].unique())
    col_raw, col_std = st.columns(2)
    with col_raw:
        st.markdown("**Raw Source JSON (Audit Trail — FR-4.3)**")
        raw_payload = df_audit[df_audit["document_id"] == selected_doc]["raw_json"].iloc[0]
        st.json(json.loads(raw_payload))
    with col_std:
        st.markdown("**Standardised Output Rows (FR-2 & FR-3)**")
        st.dataframe(df_ideal[df_ideal["document_id"] == selected_doc][[
            "record_type", "test_name_original", "test_name_canonical",
            "result_value", "unit_canonical", "test_analytics", "medicine", "dose"
        ]], use_container_width=True)

with tab3:
    st.markdown("### Source System / Clinic Data Quality Summary (FR-5.4)")
    clinic_summary = df_ideal.groupby("source_system").agg(
        total_records=("id", "count"),
        outlier_count=("test_analytics", lambda s: (s == "Outlier").sum()),
        invalid_count=("test_analytics", lambda s: (s == "Invalid").sum()),
        unmapped_count=("normalization_method", lambda s: (s == "UNMAPPED_PASSTHROUGH").sum())
    ).reset_index()
    st.dataframe(clinic_summary, use_container_width=True)

with tab4:
    st.markdown("#### Table A: `Ourput-table-ideal-schema` (78 Columns)")
    st.dataframe(df_ideal.head(50), use_container_width=True)
    st.markdown("#### Table B: `FR-2.2` Fixed 5-Column-Per-Test Schema")
    st.dataframe(df_pivot, use_container_width=True)