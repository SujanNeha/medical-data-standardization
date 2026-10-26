import pytest
from src.pipeline import VeritasPipeline

@pytest.fixture
def pipe():
    return VeritasPipeline("config/pipeline_config.yaml", ":memory:")

def test_test_name_normalisation(pipe):
    # Exact and OCR-clipped variants from Sample_JSON_file2 & Clinical_name_standardization.xlsx
    assert pipe.normalise_test_name("Haemoglobin")[0] == "Hemoglobin"
    assert pipe.normalise_test_name("aemoglobin")[0] == "Hemoglobin"
    assert pipe.normalise_test_name("tal WBC Count")[0] == "White Blood Cell Count"
    assert pipe.normalise_test_name("atelet Count")[0] == "Platelet Count"

def test_numeric_conversion_and_commas(pipe):
    val, _, is_contra = pipe.parse_numeric_result("4,290 cells/cu.mm")
    assert val == 4290.0 and not is_contra
    val2, _, is_contra2 = pipe.parse_numeric_result("116.21 mL/min/1.73m2")
    assert val2 == 116.21 and not is_contra2

def test_unit_harmonisation_and_analytics_flags(pipe):
    # Platelet count in lakhs/cumm -> cells/cu.mm (Within Range)
    val, unit, _, _, _, flag = pipe.harmonise_and_validate("Platelet Count", 1.90, "lakhs/cumm", "1.90 lakhs/cumm", False)
    assert val == 190000.0
    assert unit == "cells/cu.mm"
    assert flag == "Within Range"

    # Outlier detection (e.g., Hemoglobin = 9700 in Sample_JSON_file2.json)
    _, _, _, _, _, flag_outlier = pipe.harmonise_and_validate("Hemoglobin", 9700.0, "cell/cu.mm", "9700", False)
    assert flag_outlier == "Outlier"

    # Non-numeric where numeric is expected (CRP-QUANTITATIVE = POSITIVE in Sample_JSON_file2.json)
    _, _, _, _, _, flag_invalid = pipe.harmonise_and_validate("C-Reactive Protein (CRP)", None, "mg/L", "POSITIVE", False)
    assert flag_invalid == "Invalid"