import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# import pytest
# import pandas as pd
# from scripts.load import generate_simt_file

# def test_generate_simt_file(tmp_path, monkeypatch):
#     # Créer un mock de structure YAML
#     mock_structure = {
#         "integration_file_structure": {
#             "Header": {
#                 "Fields": [
#                     {"code": {"starting position": 0, "longueur": 2, "type": "integer", "default": "10"}}
#                 ]
#             },
#             "Detail": {
#                 "Fields": [
#                     {"code": {"starting position": 0, "longueur": 2, "type": "integer", "default": "04"}},
#                     {"nom": {"starting position": 2, "longueur": 6, "type": "text", "default": ""}}
#                 ]
#             },
#             "Footer": {
#                 "Fields": [
#                     {"code": {"starting position": 0, "longueur": 2, "type": "integer", "default": "11"}}
#                 ]
#             }
#         }
#     }

#     # Fake YAML path (ne sera pas lu car on mocke read_yaml_file)
#     yaml_path = tmp_path / "fake_structure.yaml"
#     yaml_path.write_text("fake")

#     # Données simulées
#     df = pd.DataFrame([{"code": 4, "nom": "Zineb"}])

#     # Créer un fichier temporaire
#     output_path = tmp_path / "output.txt"

#     # Mocker read_yaml_file pour retourner notre structure factice
#     import scripts.load
#     monkeypatch.setattr(scripts.load, "read_yaml_file", lambda _: mock_structure)

#     # Exécuter la fonction
#     generate_simt_file(yaml_path, df, output_path)

#     # Vérifications
#     assert output_path.exists()
#     content = output_path.read_text().splitlines()
#     assert content[0].startswith("10")
#     assert content[1].startswith("04Zineb")
#     assert content[2].startswith("11")
import pytest
import pandas as pd
import os
from scripts.load import *

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
    # Minimal YAML structure for test
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
    import yaml
    path = tmp_path / "structure.yaml"
    with open(path, "w") as f:
        yaml.dump(structure, f)
    return str(path)

def test_generate_simt_line_header(sample_df, yaml_path):
    # Test the actual line generated for the header
    import yaml
    with open(yaml_path) as f:
        structure = yaml.safe_load(f)["integration_file_structure"]
    header_fields = structure["Header"]["Fields"]
    line = generate_simt_line(header_fields, data_row=sample_df.iloc[0])
    # Check the line content and length
    assert line == '0000001REM123 ' + (" " * 486)
