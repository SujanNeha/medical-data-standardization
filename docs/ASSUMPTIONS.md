# Veritas Claims Analytics — Structured Assumptions Document

> **Assignment Deliverable:** Section 3.3 (Structured Assumptions Document)[cite: 8]  
> **Evaluation Focus:** Engineering judgment, reasoning under ambiguity, and business awareness[cite: 8].

---

## 1. Business Assumptions
* **Duplicate Submission Context:** Duplicate reports occur when a clinic or hospital sub-system transmits the same patient encounter multiple times. Records sharing the same canonical tuple `(patient_id, clinic_id, report_date)` or identical content hash payloads are treated as duplicates, with the latest ingestion timestamp (or first complete record) retained[cite: 1, 2].
* **Reference & Outlier Thresholds:** Global reference ranges (`Test_Name_Range`) and outlier boundaries (`FR-3.1`, `FR-3.2`) are configured per canonical test in the metadata store, while production systems can optionally branch by patient age and gender where clinically mandated[cite: 3].
* **Handling of Flagged Records:** Records flagged as `Outlier` or `Invalid` are still loaded into the primary canonical database tables along with their respective `Test_Name_Analytics` classification so claims analysts and operations teams can review them in the UI, whereas structurally corrupted or unparsable JSONs are routed to the Dead-Letter Queue (DLQ)[cite: 3, 4, 5].

## 2. Technical Assumptions
* **Local Prototype vs. Cloud Production:** The prototype simulates a Google Cloud Storage (GCS) bucket and cloud object storage using a local `/sample-data` directory. **SQLite** is used as a zero-dependency local relational database that mirrors a production PostgreSQL or BigQuery canonical table schema[cite: 6, 7].
* **Operational Web UI:** **Streamlit** is chosen for the Operational UI (`FR-5.1` to `FR-5.4`) to deliver interactive filtering, JSON side-by-side inspection, and clinic metrics rapidly in pure Python without requiring a separate front-end web framework[cite: 3, 4, 7].
* **Idempotency & Upserts:** Deterministic primary keys and `INSERT OR REPLACE` / `MERGE` logic guarantee that re-running the pipeline on the same input files produces identical output rows without duplication[cite: 5, 7].

## 3. Data Assumptions
* **Combined / Mixed Text-Numeric Strings:** Mixed strings (e.g., `"120000 cells/cu.mm"`, `"116.21 mL/min/1.73m2"`) contain a primary numeric value extractable via regular expression parsing, while qualitative strings (`"POSITIVE"`, `"NEGATIVE"`) on quantitative lab tests are preserved in raw audit columns and flagged appropriately (`Invalid`)[cite: 1, 2, 3].
* **Missing Tests:** When a defined canonical test is absent from a patient report's JSON payload, all corresponding columns are populated as `NULL` / blank per `FR-2.2`[cite: 2].
* **Multi-Value Unpacking:** Composite strings such as differential counts (`"Neutrophil - 72.4, Lymphocyte - 23.5..."`) or liver panels (`"LFT ( SGOT - 38, SGPT -14...)"`) are programmatically unpacked into atomic canonical test records[cite: 2].

## 4. Scope Exclusions (Prototype vs. Production)
* **Real-Time Cloud Streaming:** Real-time cloud infrastructure (GCS Pub/Sub event notifications, Cloud Run autoscaling workers, and KMS PII encryption) is described via architecture diagrams and design notes rather than deployed cloud infrastructure, keeping the take-home repository self-contained and immediately runnable[cite: 4, 5, 6, 7].
* **Advanced PII Tokenisation:** While the architecture supports SHA-256 masking and tokenisation of patient identifiers (`FR-4.3`), the prototype retains sample fields securely with standard masking placeholders as provided in the test data[cite: 5].
