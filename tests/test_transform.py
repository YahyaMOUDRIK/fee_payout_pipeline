import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
import pandas as pd
from scripts.transform import map_tables, transform_fields

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "num_donneur": [1],
        "reference_remise": ["REM123"],
        "date_emission": ["2024-01-01"],
        "montant": [123.45]
    })

@pytest.fixture
def table_mapping_path(tmp_path):
    mapping = {
        "table_mapping": {
            "num_donneur": "num_donneur_ordre",
            "reference_remise": "reference_remise",
            "date_emission": "date_emission",
            "montant": "montant"
        }
    }
    path = tmp_path / "table_mapping.yaml"
    import yaml
    with open(path, "w") as f:
        yaml.dump(mapping, f)
    return str(path)

@pytest.fixture
def transformation_rules_path(tmp_path):
    rules = {
        "transformation_rules": {
            "montant": {
                "type": "int",
                "length": 16,
                "transformation": "str('{0:.2f}'.format(float(value)*100).zfill(length))"
            },
            "date_emission": {
                "type": "date",
                "form": "%Y%m%d"
            }
        }
    }
    path = tmp_path / "transformation_rules.yaml"
    import yaml
    with open(path, "w") as f:
        yaml.dump(rules, f)
    return str(path)

def test_map_tables(sample_df, table_mapping_path):
    mapped = map_tables(sample_df, table_mapping_path)
    assert "num_donneur_ordre" in mapped.columns
    assert "reference_remise" in mapped.columns

def test_transform_fields(sample_df, transformation_rules_path):
    df = sample_df.copy()
    df["montant"] = 123.45
    df["date_emission"] = "2024-01-01"
    transformed = transform_fields(df, transformation_rules_path)
    assert transformed['montant'].iloc[0] == "0000000012345.00"
    assert transformed["date_emission"].iloc[0] == "20240101"

    