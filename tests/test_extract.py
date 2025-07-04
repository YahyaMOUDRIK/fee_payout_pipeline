import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
import pandas as pd
from scripts.extract import extract_from_sql

@pytest.fixture
def db_config_path():
    return "config/db_config.yaml"

def test_extract_from_sql_success(monkeypatch, db_config_path):
    # Mock connect_to_dbs and generate_query to avoid real DB calls
    import scripts.extract as extract_mod

    class DummyCursor:
        def execute(self, query): pass
        @property
        def description(self): return [('col1',), ('col2',)]
        def fetchall(self): return [(1, 'a'), (2, 'b')]
        def close(self): pass

    class DummyConnection:
        def cursor(self): return DummyCursor()
        def close(self): pass

    monkeypatch.setattr(extract_mod, "connect_to_dbs", lambda config: {"mamda_app": DummyConnection()})
    monkeypatch.setattr(extract_mod, "generate_query", lambda db, schema: "SELECT * FROM test")

    df = extract_mod.extract_from_sql(db_config_path, "mamda_app", "acctra")
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["col1", "col2"]
    assert len(df) == 2

def test_extract_from_sql_failure(monkeypatch, db_config_path):
    import scripts.extract as extract_mod
    class DummyConnection:
        def cursor(self): raise Exception("DB error")
        def close(self): pass
    monkeypatch.setattr(extract_mod, "connect_to_dbs", lambda config: {"mamda_app": DummyConnection()})
    monkeypatch.setattr(extract_mod, "generate_query", lambda db, schema: "SELECT * FROM test")
    df = extract_mod.extract_from_sql(db_config_path, "mamda_app", "acctra")
    assert df is None