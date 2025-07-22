import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
import pyodbc
import pandas as pd
from scripts.extract import extract_from_sql

@pytest.fixture
def db_config_path(tmp_path):
    # Créer un fichier de config temporaire pour les tests
    db_config = {
        "connections": {
            "databases": {
                "test_db": {
                    "driver": "ODBC Driver 17 for SQL Server",
                    "server": "test_server"
                }
            }
        }
    }
    config_path = tmp_path / "test_db_config.yaml"
    import yaml
    with open(config_path, "w") as f:
        yaml.dump(db_config, f)
    return str(config_path)

def test_extract_from_sql_success(monkeypatch, db_config_path):
    # Mock connect_to_dbs et generate_query pour éviter les appels réels à la DB
    import scripts.extract as extract_mod

    class DummyCursor:
        def execute(self, query): 
            # Verify query format without checking specific type_aux value
            assert 'SELECT * FROM dbo.IMP_Etat_Virement_Aux' in query
            # Remove assertion for specific type_aux since we test multiple types
        
        @property
        def description(self): return [('col1',), ('col2',)]
        
        def fetchall(self): return [(1, 'a'), (2, 'b')]
        
        def close(self): pass

    class DummyConnection:
        def cursor(self): return DummyCursor()
        def close(self): pass

    # Mock the dependencies
    monkeypatch.setattr(extract_mod, "connect_to_dbs", lambda config: {"test_db": DummyConnection()})
    monkeypatch.setattr(extract_mod, "generate_query", lambda db, type_aux: f"SELECT * FROM dbo.IMP_Etat_Virement_Aux('{type_aux}')")

    # Test avec différents types d'auxiliaires
    for type_aux in ['A', 'E', 'M', 'C']:
        df = extract_mod.extract_from_sql(db_config_path, "test_db", type_aux)
        assert isinstance(df, pd.DataFrame), f"Failed for type_aux={type_aux}"
        assert list(df.columns) == ["col1", "col2"]
        assert len(df) == 2

def test_extract_from_sql_db_error(monkeypatch, db_config_path):
    # Test d'une erreur de connexion à la base de données
    import scripts.extract as extract_mod
    
    monkeypatch.setattr(extract_mod, "connect_to_dbs", lambda config: None)
    
    df = extract_mod.extract_from_sql(db_config_path, "test_db", "A")
    assert df is None

def test_extract_from_sql_cursor_error(monkeypatch, db_config_path):
    # Test d'une erreur avec le création du cursor 
    import scripts.extract as extract_mod
    
    class DummyConnection:
        def cursor(self): 
            raise pyodbc.Error("Erreur de connexion")
        def close(self): pass
    
    monkeypatch.setattr(extract_mod, "connect_to_dbs", lambda config: {"test_db": DummyConnection()})
    monkeypatch.setattr(extract_mod, "pyodbc", type('obj', (object,), {'Error': Exception}))
    
    df = extract_mod.extract_from_sql(db_config_path, "test_db", "A")
    assert df is None

def test_extract_from_sql_execution_error(monkeypatch, db_config_path):
    # Test d'une erreur lors de l'exécution de la requête
    import scripts.extract as extract_mod
    
    class DummyCursor:
        def execute(self, query): 
            raise Exception("Erreur SQL")
        def close(self): pass
    
    class DummyConnection:
        def cursor(self): return DummyCursor()
        def close(self): pass
    
    monkeypatch.setattr(extract_mod, "connect_to_dbs", lambda config: {"test_db": DummyConnection()})
    monkeypatch.setattr(extract_mod, "generate_query", lambda type_aux: f"SELECT * FROM test")
    
    df = extract_mod.extract_from_sql(db_config_path, "test_db", "A")
    assert df is None

def test_extract_from_sql_empty_results(monkeypatch, db_config_path):
    # Test avec des résultats vides
    import scripts.extract as extract_mod
    
    class DummyCursor:
        def execute(self, query): pass
        @property
        def description(self): return [('col1',), ('col2',)]
        def fetchall(self): return []
        def close(self): pass
    
    class DummyConnection:
        def cursor(self): return DummyCursor()
        def close(self): pass
    
    monkeypatch.setattr(extract_mod, "connect_to_dbs", lambda config: {"test_db": DummyConnection()})
    # Fix: Modify the lambda to accept db parameter
    monkeypatch.setattr(extract_mod, "generate_query", lambda db, type_aux: f"SELECT * FROM test")
    
    df = extract_mod.extract_from_sql(db_config_path, "test_db", "A")
    assert isinstance(df, pd.DataFrame)
    assert df.empty