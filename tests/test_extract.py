import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
import pandas as pd
from scripts.extract import extract_data_from_db

def test_extract_data_from_db(monkeypatch):
    # Simuler un curseur qui retourne deux lignes et deux colonnes
    class DummyCursor:
        description = [('id',), ('nom',)]
        def execute(self, query): pass
        def fetchall(self): return [(1, 'Ali'), (2, 'Fatima')]
        def close(self): pass

    class DummyConnection:
        def cursor(self): return DummyCursor()
        def close(self): pass

    def mock_connect_to_database(yaml_path):
        return DummyConnection()

    # On remplace la vraie fonction par la mock
    monkeypatch.setattr("scripts.extract.connect_to_database", mock_connect_to_database)

    # Exécution du test
    df = extract_data_from_db("fake_config.yaml", "SELECT * FROM fake")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 2)
    assert list(df.columns) == ['id', 'nom']
