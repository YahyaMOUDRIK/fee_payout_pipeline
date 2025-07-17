import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
import pandas as pd 
from scripts.parse import *


@pytest.fixture
def sample_ascii_file(tmp_path):
    """Fixture to create a sample ASCII file for testing."""
    content = (
        "10HEADER1234567890          \n"  # Header line
        "04DETAIL0012345.00VIRJAN24AAA          \n"  # Detail line 1
        "04DETAIL0023456.78VIRFEB24BBB          \n"  # Detail line 2
        "11FOOTER00002000123456.78            \n"  # Footer line
    )
    file_path = tmp_path / "sample_file.asc"
    file_path.write_text(content)
    return str(file_path)

@pytest.fixture
def sample_structure_yaml(tmp_path):
    """Fixture to create a sample YAML structure file."""
    structure = {
        "retour_sort_file_structure": {
            "Header": {
                "Fields": [
                    {"code_enregistrement": {"starting position": 0, "longueur": 2}},
                    {"type": {"starting position": 2, "longueur": 6}},
                    {"reference": {"starting position": 8, "longueur": 10}},
                ]
            },
            "Detail": {
                "Fields": [
                    {"code_enregistrement": {"starting position": 0, "longueur": 2}},
                    {"type": {"starting position": 2, "longueur": 6}},
                    {"montant": {"starting position": 8, "longueur": 10}},
                    {"reference_virement": {"starting position": 18, "longueur": 15}},
                ]
            },
            "Footer": {
                "Fields": [
                    {"code_enregistrement": {"starting position": 0, "longueur": 2}},
                    {"type": {"starting position": 2, "longueur": 6}},
                    {"nombre_total_virements": {"starting position": 8, "longueur": 5}},
                    {"montant_total": {"starting position": 13, "longueur": 12}},
                ]
            },
        }
    }
    yaml_path = tmp_path / "structure.yaml"
    with open(yaml_path, "w") as f:
        yaml.dump(structure, f)
    return str(yaml_path)

def test_parse_file(sample_ascii_file, sample_structure_yaml):
    """Test the parse_file function with a sample ASCII file."""
    # Call the parse_file function
    parsed_data = parse_file(sample_ascii_file, sample_structure_yaml)

    # Verify the parsed header
    assert parsed_data["header"] == {
        "code_enregistrement": "10",
        "type": "HEADER",
        "reference": "1234567890",
    }

    # Verify the parsed details
    assert len(parsed_data["details"]) == 2
    assert parsed_data["details"][0] == {
        "code_enregistrement": "04",
        "type": "DETAIL",
        "montant": "0012345.00",
        "reference_virement": "VIRJAN24AAA",
    }
    assert parsed_data["details"][1] == {
        "code_enregistrement": "04",
        "type": "DETAIL",
        "montant": "0023456.78",
        "reference_virement": "VIRFEB24BBB",
    }

    # Verify the parsed footer
    assert parsed_data["footer"] == {
        "code_enregistrement": "11",
        "type": "FOOTER",
        "nombre_total_virements": "00002",
        "montant_total": "000123456.78",
    }