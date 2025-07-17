import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
import pandas as pd
import os
from scripts.load import *
#import yaml
from utils.file_utils import *

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "num_donneur_ordre": [1],
        "reference_remise": ["REM123"],
        "date_emission": ["2024-01-01"],
       #"montant": ["000000000012345"],
        "montant": ["0000000012345.00"],
        "date_execution": ["2024-01-02"],
        "nom_donneur_ordre": ["Mamda"],
        "nom_benificiaire": ["Fatima"],
        "rib_donneur_ordre": ["007810000112345678901281"],
        "rib_beneficiaire": ["007123000198765432109845"],
        # "motif_virement": ["Indemnisation"],
        "reference_virement": ["VIRJAN24AAA"],
        "reference_beneficiaire": ["100001"]
    })

@pytest.fixture
def yaml_path(tmp_path):
    structure = {
        "integration_file_structure": {
            "Header": {
                "Fields": [
                    {"code_enregistrement": {"starting position": 0, "obligatoire": True, "type": "integer", "longueur": 2, "default": "10"}},
                    {"num_donneur_ordre": {"starting position": 2, "obligatoire": True, "type": "integer", "longueur": 7, "default": 0}},
                    {"reference_remise": {"starting position": 9, "obligatoire": False, "type": "text", "longueur": 7, "default": ""}}
                ]
            },
            "Detail": {
                "Fields": [
                    {"code_enregistrement": {"starting position": 0, "obligatoire": True, "type": "integer", "longueur": 2, "default": "04"}},
                    {"montant": {"starting position": 2, "obligatoire": True, "type": "integer", "longueur": 16, "default": 0}},
                    {"reference_virement": {"starting position": 18, "obligatoire": False, "type": "text", "longueur": 35, "default": ""}}
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
    structure = read_yaml_file(yaml_path)["integration_file_structure"]
    header_fields = structure["Header"]["Fields"]
    line = generate_simt_line(header_fields, data_row=sample_df.iloc[0])

    assert line[0:2] == '10'  # code_enregistrement
    assert line[2:9] == '0000001'  # num_donneur_ordre formaté
    assert line[9:16] == 'REM123 '  # reference_remise avec espace
    assert len(line) == 500

    assert line[16:].strip() == ''



def test_generate_simt_line_detail(sample_df, yaml_path):
    structure = read_yaml_file(yaml_path)["integration_file_structure"]
    detail_fielders = structure["Detail"]["Fields"]
    line = generate_simt_line(detail_fielders, data_row=sample_df.iloc[0])

    assert line[0:2] == '04' #code_enregistrement
    assert line[2:18] == '0000000012345.00' #montant
    assert line[18:53] == 'VIRJAN24AAA' + (" " * 24)
    assert line[53:].strip() == ''
    assert len(line)==500



def test_generate_simt_file(tmp_path, sample_df, yaml_path):
    output_dir = tmp_path / "output"
    os.makedirs(output_dir, exist_ok=True)

    result_path = generate_simt_file(yaml_path, sample_df, extension="txt", type_aux="A")

    # Verify that the file was created
    assert result_path is not None
    assert os.path.exists(result_path)

    with open(result_path, "r", encoding="utf-8") as f:
        lines = f.readlines()


    assert len(lines) == 3  # 1 Header, 1 Detail, 1 Footer

    # content of the Header
    expected_header = (
        "10"  # code_enregistrement
        + "0000001"  # num_donneur_ordre
        + "REM123 "  # reference_remise 
        + " " * 484 
    )
    assert lines[0] == expected_header + "\n"

    # content of the Detail
    expected_detail = (
        "04"  # code_enregistrement
        + "0000000012345.00"  # montant
        + "VIRJAN24AAA"  # reference_virement
        + " " * 24 
        + " " * 447
    )
    assert lines[1] == expected_detail + "\n"

    # content of the Footer
    expected_footer = (
        "11"  # code_enregistrement
        + "00001"  # nombre_total_virements
        + "000000000012345.0000"  # montant_total_virements
        + " " * 473
    )
    assert lines[2] == expected_footer + "\n"