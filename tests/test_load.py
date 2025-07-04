import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
import pandas as pd
import os
from scripts.load import *
import yaml

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "num_donneur_ordre": [1],
        "reference_remise": ["REM123"],
        "date_emission": ["2024-01-01"],
        "montant": ["000000000012345"],
        "date_execution": ["20240102"],
        "nom_donneur_ordre": ["Mamda"],
        "nom_benificiaire": ["Fatima"],
        "rib_donneur_ordre": ["007810000112345678901281"],
        "rib_beneficiaire": ["007123000198765432109845"],
        "motif_virement": ["Indemnisation"],
        "reference_virement": ["VIRJAN24AAA"],
        "reference_beneficiaire": ["100001"]
    })

@pytest.fixture
def yaml_path(tmp_path):
    structure = {
        "integration_file_structure": {
            "Header": {
                "Fields": [
                    {"num_donneur_ordre": {"starting position": 0, "obligatoire": True, "type": "integer", "longueur": 7, "default": 0}},
                    {"reference_remise": {"starting position": 7, "obligatoire": False, "type": "text", "longueur": 7, "default": ""}}
                ]
            },
            "Detail": {
                "Fields": [
                    {"montant": {"starting position": 0, "obligatoire": True, "type": "integer", "longueur": 16, "default": 0}}
                ]
            },
            "Footer": {
                "Fields": [
                    {"code_enregistrement": {"starting position": 0, "obligatoire": True, "type": "text", "longueur": 2, "default": "11"}},
                    {"nombre_total_virements": {"starting position": 2, "obligatoire": True, "type": "integer", "longueur": 5, "default": 0}},
                    {"montant_total_virements": {"starting position": 7, "obligatoire": True, "type": "integer", "longueur": 20, "default": 0}}
                ]
            }
        }
    }
    path = tmp_path / "structure.yaml"
    with open(path, "w") as f:
        yaml.dump(structure, f)
    return str(path)

def test_generate_simt_line_header(sample_df, yaml_path):
    with open(yaml_path) as f:
        structure = yaml.safe_load(f)["integration_file_structure"]
    header_fields = structure["Header"]["Fields"]
    line = generate_simt_line(header_fields, data_row=sample_df.iloc[0])
    assert line == '0000001REM123 ' + (" " * 486)
