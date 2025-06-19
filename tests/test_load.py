import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pytest
import pandas as pd
from scripts.load import generate_simt_file

def test_generate_simt_file(tmp_path, monkeypatch):
    # Créer un mock de structure YAML
    mock_structure = {
        "integration_file_structure": {
            "Header": {
                "Fields": [
                    {"code": {"starting position": 0, "longueur": 2, "type": "integer", "default": "10"}}
                ]
            },
            "Detail": {
                "Fields": [
                    {"code": {"starting position": 0, "longueur": 2, "type": "integer", "default": "04"}},
                    {"nom": {"starting position": 2, "longueur": 6, "type": "text", "default": ""}}
                ]
            },
            "Footer": {
                "Fields": [
                    {"code": {"starting position": 0, "longueur": 2, "type": "integer", "default": "11"}}
                ]
            }
        }
    }

    # Fake YAML path (ne sera pas lu car on mocke read_yaml_file)
    yaml_path = tmp_path / "fake_structure.yaml"
    yaml_path.write_text("fake")

    # Données simulées
    df = pd.DataFrame([{"code": 4, "nom": "Zineb"}])

    # Créer un fichier temporaire
    output_path = tmp_path / "output.txt"

    # Mocker read_yaml_file pour retourner notre structure factice
    import scripts.load
    monkeypatch.setattr(scripts.load, "read_yaml_file", lambda _: mock_structure)

    # Exécuter la fonction
    generate_simt_file(yaml_path, df, output_path)

    # Vérifications
    assert output_path.exists()
    content = output_path.read_text().splitlines()
    assert content[0].startswith("10")
    assert content[1].startswith("04Zineb")
    assert content[2].startswith("11")
