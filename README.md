# medical-data-standardization
A configurable Python pipeline for standardising medical report JSON data, normalising test names and units, validating laboratory results, and storing structured healthcare data in a database.

---

## 📋 1. Business Context & Problem Statement

Veritas Claims ingests over **200,000 medical reports daily** from over **500+ nationwide clinics and diagnostic labs**, each operating on disparate data conventions, custom field names, and local units[cite: 1]. 

### The Problem
* **Inconsistent Naming:** Labs report identical lab tests under variant or misspelled names (e.g., `Haemoglobin`, `Hemoglobin`, `aemoglobin`, `Hb`)[cite: 1, 2].
* **Mixed Data Types:** Result fields contain concatenated strings (e.g., `120000 cells/cu.mm`, `116.21 mL/min/1.73m2`, `POSITIVE`, or OCR row shifts)[cite: 1, 12, 13].
* **Brand Medications:** Discharge summaries list brand-name medications that require mapping to generic equivalents for clinical analytics[cite: 1, 3].
* **Severe Downstream Impact:** Automated adjudication rules fail, analysts cannot compare cross-clinic lab values, and outlier values slip through undetected[cite: 1].

### The Solution
A scalable, intelligent, metadata-driven data pipeline that ingests raw JSON reports, suppresses duplicate submissions, standardises test names and units, validates quantitative values against medically accepted reference ranges, and loads clean records into an analytics database with an operational web UI—with **zero manual intervention and zero code changes for new clinic onboarding**[cite: 2, 3, 4, 7].

---

## 2. Repository Structure

Your submission repository follows the required layout and file standards:

veritas-claims-/
  * config/
    * pipeline_config.yaml      # Metadata store: clinic mappings, test synonyms, unit conversions, reference ranges, & med dictionary
├── sample-data/                  # Input JSON files (Sample_JSON_file1.json to Sample_JSON_file5.json)
├── src/
│   ├── pipeline.py               # Core pipeline: Ingestion, deduplication, standardisation, validation, dual-schema DB loader
│   └── ui_app.py                 # Streamlit operational web dashboard (Stats, Flagged queue, Inspector, Clinic metrics)
├── tests/
│   └── test_pipeline.py          # Pytest unit tests verifying standardisation & validation logic
├── docs/
│   ├── architecture.drawio       # Draw.io solution architecture diagram
│   └── ASSUMPTIONS.md            # Structured business, technical, data assumptions & scope exclusions
└── README.md                     # Comprehensive project documentation 

## 3. Setup & Execution Instructions

Prerequisites
  Python 3.9+ installed on your system.

Step 1: Clone & Navigate to Repository
  
Step 2: Create Virtual Environment & Install Dependencies

  python -m venv venv
  source venv/bin/activate  # On Windows use: venv\Scripts\activate
  pip install pandas pyyaml streamlit pytest matplotlib

Step 3: Run the Data PipelineExecute the end-to-end pipeline to discover sample JSONs, run multi-tier deduplication, unpack multi-value lab strings, harmonise units, validate ranges/outliers, and write idempotently into SQLite (veritas_claims.db):
  python src/pipeline.py

  
Step 4: Launch the Operational UI (FR-5)
  streamlit run src/ui_app.py
  
Step 5: Run Unit Tests
  pytest tests/test_pipeline.py



## 4. Architecture Summary & Functional Compliance (FR-1 to FR-5)

The solution is architected as an event-driven, metadata-driven Lakehouse pipeline:
  * FR-1 (Ingestion & Deduplication): Ingests JSON reports partitioned by clinic ID and date. Suppresses multi-tier duplicates (cross-file duplicate discharge summaries transmitted from different systems, intra-file section repeats, and OCR page duplication) using content SHA-256 hashes.
  * FR-2 (Data Standardisation): Resolves test name variants using exact dictionary lookups and RapidFuzz similarity matching (aemoglobin $\rightarrow$ Hemoglobin). Extracts numeric values from mixed strings (132000 cells/cu.mm $\rightarrow$ 132000), harmonizes units (e.g., platelet counts in lakhs multiplied by 100,000), normalizes demographics (ISO dates, structured age, M/F gender), and maps brand medications to generics.
  * FR-3 (Validation & Analytics Flags): Validates quantitative lab results against clinical reference ranges and classifies every test into Within Range, Above Range, Below Range, Outlier (physiologically implausible values like Hb of 9700 resulting from OCR row shifts), or Invalid (qualitative strings like POSITIVE on quantitative tests or contradictory combined fields).
  * FR-4 (Storage & Audit Trail): Implements a Dual-Schema Strategy supporting both the 78-column denormalised Ideal Schema (Ourput-table-ideal-schema.csv) and the FR-2.2 wide table (Test_Name, Test_Name_Result, Test_Name_Range, Test_Name_Unit, Test_Name_Analytics), alongside raw JSON audit trails and a Dead-Letter Queue (DLQ) for malformed files.
  * FR-5 (Operational UI): Built with Streamlit to provide operations teams with a daily pipeline stats dashboard, a side-by-side Record Inspector, a Flagged Records Review queue, and clinic-level quality scorecards.


## 5. Key Design Decisions & Trade-Offs

  * Metadata-Driven Zero-Code Onboarding (NFR-2.1): Adding a new clinic or new test variant requires zero code changes; all field mappings, OCR synonym dictionaries, and harmonisation factors are maintained declaratively in config/pipeline_config.yaml.
  * Idempotent Upserts (NFR-3.2): Re-running the pipeline on the same input files produces identical database states without creating duplicate rows, guaranteed by deterministic primary keys and INSERT OR REPLACE transactions.
  * Decoupled Error Resilience (NFR-3.1): A malformed JSON file is caught in an isolated try/catch block, logged to the DLQ table with failure reasons, and does not block the processing of the remaining batch.   
