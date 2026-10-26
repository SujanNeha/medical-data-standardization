import os
import re
import json
import yaml
import sqlite3
import hashlib
import logging
from datetime import datetime, timezone
from difflib import get_close_matches
from typing import Dict, Any, List, Tuple, Optional
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

IDEAL_78_COLUMNS = [
    "id", "document_id", "record_type", "file_gcs_path", "trace_id", "correlation_id",
    "source_system", "claim_no", "nt_code", "consumer_client_id", "destination_identifier",
    "patient_name", "age", "gender", "uhid", "hospital_name", "doctor_name", "bill_date",
    "reports_date", "test_name_canonical", "test_name_original", "result_value", "result_text",
    "unit_canonical", "unit_original", "range_low", "range_high", "range_text", "test_analytics",
    "normalization_method", "normalization_confidence", "admission_date", "discharge_date",
    "diagnosis", "brief_history", "general_examinations", "recommendations", "hospital_address",
    "ward", "post_discharge_advice", "medicine", "dose", "frequency", "medicine_type",
    "processed_at", "ingested_at", "basic_info_age", "basic_info_bill_date",
    "medicine_injections_investigation", "discharge_medications_dose", "metadetails",
    "discharge_medications_frequency", "discharge_medications_medicine", "lab_or_hospital_name",
    "report_details_page_no", "report_details_range", "report_details_result",
    "report_details_test_analytics", "report_details_test_name", "report_details_unit",
    "age_text", "other_med_inj_investigations", "report_date", "course_during_hospitalisation",
    "page_number", "range_text_original", "medication_dose", "result_text_original",
    "medication_frequency", "course_during_hospitalization", "medication_name", "page_no",
    "test_name", "medication_medicine", "result", "unit", "age_years", "range"
]


class VeritasPipeline:
    def __init__(self, config_path: str = "config/pipeline_config.yaml", db_path: str = "veritas_claims.db"):
        with open(config_path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)
        self.db_path = db_path
        self.tests_cfg = self.config["canonical_tests"]
        self.meds_cfg = {k.lower(): v for k, v in self.config.get("medicine_mappings", {}).items()}
        self.synonym_map = {}
        for canon, meta in self.tests_cfg.items():
            self.synonym_map[canon.lower()] = canon
            for syn in meta.get("synonyms", []):
                self.synonym_map[syn.lower()] = canon
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS error_dlq (
                file_name TEXT,
                error_reason TEXT,
                logged_at TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_audit_trail (
                document_id TEXT PRIMARY KEY,
                file_name TEXT,
                source_system TEXT,
                raw_json TEXT,
                ingested_at TEXT
            )
        """)
        conn.commit()
        conn.close()

    @staticmethod
    def normalise_date(raw_date: Any) -> str:
        """FR-2.5: Normalise date strings to ISO-8601 YYYY-MM-DD."""
        if not raw_date or not isinstance(raw_date, str):
            return ""
        cleaned = raw_date.strip()
        for fmt in ("%d-%m-%Y", "%d/%b/%Y", "%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(cleaned, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return cleaned

    @staticmethod
    def normalise_age(raw_age: Any) -> Tuple[str, str]:
        """FR-2.5: Extract structured years from strings like '33Y11M265D'."""
        text = str(raw_age or "").strip()
        if not text or "REDACTED" in text.upper():
            return text, ""
        m = re.search(r"(\d+)\s*[Yy]?", text)
        return text, (m.group(1) if m else "")

    @staticmethod
    def normalise_gender(raw_gender: Any) -> str:
        """FR-2.5: Standardise M/Male/F/Female."""
        text = str(raw_gender or "").strip()
        if not text or "REDACTED" in text.upper():
            return text
        u = text.upper()
        if u in ("M", "MALE"):
            return "Male"
        if u in ("F", "FEMALE"):
            return "Female"
        return "Other"

    def normalise_test_name(self, raw_name: str) -> Tuple[Optional[str], str, float]:
        """FR-2.1: Configurable dictionary + fuzzy matching."""
        if not raw_name or not isinstance(raw_name, str):
            return None, "UNMAPPED", 0.0
        cleaned = raw_name.strip().lower()
        if cleaned in ("test_name", "comment", "diagnostics", "absent", "negative"):
            return None, "HEADER_OR_NOISE", 0.0
        if cleaned in self.synonym_map:
            return self.synonym_map[cleaned], "EXACT_DICTIONARY", 1.0
        matches = get_close_matches(cleaned, list(self.synonym_map.keys()), n=1, cutoff=0.78)
        if matches:
            return self.synonym_map[matches[0]], "FUZZY_MATCH", 0.85
        return raw_name.strip(), "UNMAPPED_PASSTHROUGH", 0.5

    def unpack_multi_value_row(self, raw_test_name: str, raw_result: str) -> Optional[List[Tuple[str, float]]]:
        """FR-2.3: Unpack composite strings like 'Neutrophil - 72.4, Lymphocyte - 23.5...'."""
        source = raw_result if (raw_result and "," in str(raw_result)) else raw_test_name
        if not source or "," not in str(source):
            return None
        pairs = re.findall(r"([A-Za-z][A-Za-z0-9\s\(\)/]+?)\s*(?:-|:)\s*([-+]?\d*\.?\d+)", str(source))
        if len(pairs) >= 2:
            unpacked = []
            for sub_name, sub_val in pairs:
                sub_clean = re.sub(r"^(LFT\s*\(|Serum Bilirubin\s*\()", "", sub_name).strip()
                canon, method, _ = self.normalise_test_name(sub_clean)
                unpacked.append((canon or sub_clean, float(sub_val)))
            return unpacked
        return None

    @staticmethod
    def parse_numeric_result(raw_result: Any, raw_test_name: str = "") -> Tuple[Optional[float], str, bool]:
        """FR-2.3 & FR-3.4: Extract numeric value, handle commas, H/L flags, and detect conflicting values."""
        text = str(raw_result or "").strip()
        if not text and raw_test_name:
            emb = re.findall(r"[-+]?\d*\.?\d+", raw_test_name)
            if len(emb) == 1:
                return float(emb[0]), text, False
        if not text or text.lower() in ("result", "n/a", "nil", "absent", "not detected"):
            return None, text, False
        if re.match(r"^\d+(\.\d+)?\s*-\s*\d+(\.\d+)?$", text) or ":" in text:
            return None, text, True
        cleaned = re.sub(r"(?<=\d),(?=\d{3}\b)", "", text)
        cleaned = re.sub(r"1\.73\s*m2", "", cleaned, flags=re.IGNORECASE)
        nums = re.findall(r"[-+]?\d*\.?\d+", cleaned)
        if len(nums) == 1:
            val = float(nums[0])
            if text.startswith(".0") and val < 0.1:
                val += 1.0
            return val, text, False
        elif len(nums) > 1:
            return None, text, True
        return None, text, False

    def harmonise_and_validate(
        self, canon_name: str, val: Optional[float], raw_unit: str, raw_text: str, is_contradictory: bool
    ) -> Tuple[Optional[float], str, Optional[float], Optional[float], str, str]:
        """FR-2.4 & FR-3.1–3.4: Unit conversion, range validation, outlier & invalid flagging."""
        if canon_name not in self.tests_cfg:
            return val, raw_unit or "", None, None, "", ("Invalid" if is_contradictory else "Unmapped")
        meta = self.tests_cfg[canon_name]
        c_unit = meta["canonical_unit"]
        r_low, r_high, r_text = meta["range_low"], meta["range_high"], meta["range_text"]

        if is_contradictory:
            return None, c_unit, r_low, r_high, r_text, "Invalid"
        if val is None:
            if raw_text and raw_text.upper() in ("POSITIVE", "NEGATIVE", "OTE"):
                return None, c_unit, r_low, r_high, r_text, "Invalid"
            return None, c_unit, r_low, r_high, r_text, ""

        u_clean = str(raw_unit or "").strip().lower()
        factor = meta.get("unit_conversions", {}).get(u_clean, 1.0)
        if canon_name == "Platelet Count" and val < 20.0:
            factor = 100000.0
        conv_val = round(val * factor, 4)

        if conv_val < meta["outlier_low"] or conv_val > meta["outlier_high"]:
            flag = "Outlier"
        elif conv_val < r_low:
            flag = "Below Range"
        elif conv_val > r_high:
            flag = "Above Range"
        else:
            flag = "Within Range"
        return conv_val, c_unit, r_low, r_high, r_text, flag

    def run(self, input_dir: str = "sample-data") -> Dict[str, Any]:
        now_iso = datetime.now(timezone.utc).isoformat()
        records_ideal = []
        pivoted_rows = []
        seen_section_hashes = set()
        stats = {
            "files_received": 0, "files_processed": 0, "files_failed": 0,
            "cross_file_dups": 0, "intra_file_section_dups": 0, "intra_report_row_dups": 0
        }

        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        for fname in sorted(os.listdir(input_dir)):
            if not fname.endswith(".json"):
                continue
            stats["files_received"] += 1
            fpath = os.path.join(input_dir, fname)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    raw_str = f.read()
                    payload = json.loads(raw_str)
            except Exception as exc:
                stats["files_failed"] += 1
                cur.execute("INSERT INTO error_dlq VALUES (?, ?, ?)", (fname, str(exc), now_iso))
                continue

            stats["files_processed"] += 1
            trace_id = payload.get("traceId", "")
            d = payload.get("data", {})
            doc_id = d.get("documentId", fname)
            corr_id = d.get("correlationId", "")
            meta_list = d.get("metaDetails", [])
            meta = {m["key"]: m["value"] for m in meta_list if isinstance(m, dict) and "key" in m}
            source_sys = meta.get("source_system", "UNKNOWN")

            cur.execute(
                "INSERT OR REPLACE INTO raw_audit_trail VALUES (?, ?, ?, ?, ?)",
                (doc_id, fname, source_sys, raw_str, now_iso)
            )

            for sec_idx, sec in enumerate(d.get("responseDetails", [])):
                classifier = sec.get("classifier", "")
                sdata = sec.get("data", {})
                sec_hash = hashlib.sha256(json.dumps(sdata, sort_keys=True).encode()).hexdigest()
                if sec_hash in seen_section_hashes:
                    stats["cross_file_dups" if sec_idx == 0 else "intra_file_section_dups"] += 1
                    continue
                seen_section_hashes.add(sec_hash)

                if classifier == "lab_report":
                    binfo = sdata.get("basic_info", {})
                    age_txt, age_yrs = self.normalise_age(binfo.get("age", ""))
                    gender_norm = self.normalise_gender(binfo.get("gender", ""))
                    rep_date_norm = self.normalise_date(binfo.get("reports_date", ""))
                    bill_date_norm = self.normalise_date(binfo.get("bill_date", ""))

                    pivot_row = {
                        "document_id": doc_id,
                        "file_name": fname,
                        "source_system": source_sys,
                        "claim_no": meta.get("claim_no", ""),
                        "reports_date": rep_date_norm
                    }
                    for c_test in self.tests_cfg:
                        pivot_row[f"{c_test}"] = ""
                        pivot_row[f"{c_test}_Result"] = ""
                        pivot_row[f"{c_test}_Range"] = ""
                        pivot_row[f"{c_test}_Unit"] = ""
                        pivot_row[f"{c_test}_Analytics"] = ""

                    seen_rows = set()
                    for r_idx, td in enumerate(sdata.get("report_details", [])):
                        sig = (
                            str(td.get("test_name", "")).strip().lower(),
                            str(td.get("result", "")).strip().lower(),
                            str(td.get("unit", "")).strip().lower(),
                            str(td.get("range", "")).strip().lower()
                        )
                        if sig in seen_rows:
                            stats["intra_report_row_dups"] += 1
                            continue
                        seen_rows.add(sig)

                        raw_tname = td.get("test_name", "")
                        raw_res = td.get("result", "")
                        canon_name, norm_method, norm_conf = self.normalise_test_name(raw_tname)
                        if norm_method == "HEADER_OR_NOISE":
                            continue

                        val, r_text, is_contra = self.parse_numeric_result(raw_res, raw_tname)
                        conv_val, c_unit, r_low, r_high, c_range_text, flag = self.harmonise_and_validate(
                            canon_name, val, td.get("unit", ""), r_text, is_contra
                        )

                        rec = {col: None for col in IDEAL_78_COLUMNS}
                        rec.update({
                            "id": f"{doc_id}_LAB_{r_idx}",
                            "document_id": doc_id,
                            "record_type": "lab_report",
                            "file_gcs_path": f"gs://veritas-claims-raw/{source_sys}/{fname}",
                            "trace_id": trace_id,
                            "correlation_id": corr_id,
                            "source_system": source_sys,
                            "claim_no": meta.get("claim_no", ""),
                            "nt_code": meta.get("nt_code", ""),
                            "consumer_client_id": meta.get("ConsumerClientId", ""),
                            "destination_identifier": meta.get("DestinationIdentifier", ""),
                            "patient_name": binfo.get("patient_name", ""),
                            "age": age_txt,
                            "age_text": age_txt,
                            "age_years": age_yrs,
                            "gender": gender_norm,
                            "uhid": binfo.get("uhid", ""),
                            "hospital_name": binfo.get("lab_or_hospital_name", ""),
                            "lab_or_hospital_name": binfo.get("lab_or_hospital_name", ""),
                            "bill_date": bill_date_norm,
                            "basic_info_bill_date": binfo.get("bill_date", ""),
                            "reports_date": rep_date_norm,
                            "report_date": rep_date_norm,
                            "test_name_canonical": canon_name,
                            "test_name_original": raw_tname,
                            "report_details_test_name": raw_tname,
                            "test_name": canon_name,
                            "result_value": conv_val,
                            "result_text": r_text,
                            "result_text_original": str(raw_res),
                            "report_details_result": str(raw_res),
                            "result": str(conv_val if conv_val is not None else r_text),
                            "unit_canonical": c_unit,
                            "unit_original": td.get("unit", ""),
                            "report_details_unit": td.get("unit", ""),
                            "unit": c_unit,
                            "range_low": r_low,
                            "range_high": r_high,
                            "range_text": c_range_text or td.get("range", ""),
                            "range_text_original": td.get("range", ""),
                            "report_details_range": td.get("range", ""),
                            "range": c_range_text or td.get("range", ""),
                            "test_analytics": flag,
                            "report_details_test_analytics": td.get("test_analytics", ""),
                            "normalization_method": norm_method,
                            "normalization_confidence": norm_conf,
                            "processed_at": now_iso,
                            "ingested_at": now_iso,
                            "metadetails": json.dumps(meta_list),
                            "page_no": str(td.get("page_no", "")),
                            "page_number": str(td.get("page_no", "")),
                            "report_details_page_no": str(td.get("page_no", ""))
                        })
                        records_ideal.append(rec)

                        if canon_name in self.tests_cfg and not pivot_row[f"{canon_name}"]:
                            pivot_row[f"{canon_name}"] = canon_name
                            pivot_row[f"{canon_name}_Result"] = conv_val if conv_val is not None else r_text
                            pivot_row[f"{canon_name}_Range"] = c_range_text
                            pivot_row[f"{canon_name}_Unit"] = c_unit
                            pivot_row[f"{canon_name}_Analytics"] = flag

                    pivoted_rows.append(pivot_row)

                elif classifier == "discharge_summary":
                    adm_date = self.normalise_date(sdata.get("admissionDate", ""))
                    dis_date = self.normalise_date(sdata.get("dischargeDate", ""))
                    inj_str = json.dumps(sdata.get("medicineInjectionsInvestigation", []))
                    for m_idx, med in enumerate(sdata.get("dischargeMedications", [])):
                        raw_med = str(med.get("medicine") or "").strip()
                        if not raw_med or raw_med.upper() == "N/A":
                            continue
                        gen_med = self.meds_cfg.get(raw_med.lower(), raw_med)
                        rec = {col: None for col in IDEAL_78_COLUMNS}
                        rec.update({
                            "id": f"{doc_id}_MED_{m_idx}",
                            "document_id": doc_id,
                            "record_type": "discharge_summary",
                            "file_gcs_path": f"gs://veritas-claims-raw/{source_sys}/{fname}",
                            "trace_id": trace_id,
                            "correlation_id": corr_id,
                            "source_system": source_sys,
                            "claim_no": meta.get("claim_no", ""),
                            "nt_code": meta.get("nt_code", ""),
                            "consumer_client_id": meta.get("ConsumerClientId", ""),
                            "destination_identifier": meta.get("DestinationIdentifier", ""),
                            "patient_name": sdata.get("patientName", ""),
                            "age": sdata.get("age", ""),
                            "gender": self.normalise_gender(sdata.get("gender", "")),
                            "hospital_name": sdata.get("hospitalName", ""),
                            "doctor_name": sdata.get("doctorName", ""),
                            "admission_date": adm_date,
                            "discharge_date": dis_date,
                            "diagnosis": sdata.get("diagnosis", ""),
                            "brief_history": sdata.get("briefHistory", ""),
                            "general_examinations": sdata.get("generalExaminations", ""),
                            "recommendations": sdata.get("recommendations", ""),
                            "hospital_address": sdata.get("hospitalAddress", ""),
                            "ward": sdata.get("ward", ""),
                            "medicine": gen_med,
                            "medication_name": gen_med,
                            "medication_medicine": raw_med,
                            "discharge_medications_medicine": raw_med,
                            "dose": med.get("dose", ""),
                            "medication_dose": med.get("dose", ""),
                            "discharge_medications_dose": med.get("dose", ""),
                            "frequency": med.get("frequency", ""),
                            "medication_frequency": med.get("frequency", ""),
                            "discharge_medications_frequency": med.get("frequency", ""),
                            "medicine_injections_investigation": inj_str,
                            "other_med_inj_investigations": inj_str,
                            "processed_at": now_iso,
                            "ingested_at": now_iso,
                            "metadetails": json.dumps(meta_list)
                        })
                        records_ideal.append(rec)

        df_ideal = pd.DataFrame(records_ideal, columns=IDEAL_78_COLUMNS)
        df_pivot = pd.DataFrame(pivoted_rows)

        # Idempotent load (FR-4.1, NFR-3.2)
        df_ideal.to_sql("canonical_records_ideal_schema", conn, if_exists="replace", index=False)
        df_pivot.to_sql("canonical_pivoted_reports", conn, if_exists="replace", index=False)
        conn.commit()
        conn.close()
        return stats


if __name__ == "__main__":
    pipeline = VeritasPipeline()
    summary = pipeline.run("sample-data")
    print("Pipeline Run Summary:", summary)