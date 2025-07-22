import os
import sys
import pytest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.insert import insert_status_data

class MockCursor:
    def __init__(self, should_fail=False):
        self.execute_calls = []
        self.should_fail = should_fail
        
    def execute(self, query, values=None):
        if self.should_fail:
            raise Exception("SQL Error")
        self.execute_calls.append({'query': query, 'values': values})

class MockConnection:
    def __init__(self, should_fail=False):
        self.cursor_obj = MockCursor(should_fail)
        self.commit_called = False
        self.close_called = False
        
    def cursor(self):
        return self.cursor_obj
        
    def commit(self):
        self.commit_called = True
        
    def close(self):
        self.close_called = True

class MockDatabase:
    def __init__(self, connections=None, should_fail_connection=False):
        self.connections = connections or {}
        self.should_fail = should_fail_connection
        self.read_yaml_calls = []
        self.connect_to_dbs_calls = []
        
    def mock_read_yaml_file(self, path):
        self.read_yaml_calls.append(path)
        return {
            'table_mapping': {
                'Databases': {
                    'SinAuto_MAMDA_retour': {
                        'SinAutoReg_retour': {
                            'date_emission': 'DateEmission',
                            'date_traitement': 'DateTraitement',
                            'date_execution': 'DateExecution',
                            'montant': 'Montant',
                            'rib_beneficiaire': 'RibBeneficiaire',
                            'motif_virement': 'MotifVirement'
                        }
                    },
                    'SinAuto_MCMA_retour': {
                        'SinAutoReg_retour': {
                            'date_emission': 'DateEmission',
                            'montant': 'Montant',
                            'rib_beneficiaire' : 'RibBeneficiaire'
                        }
                    },
                    'SinAT_MAMDA_retour': {
                        'SinATReg_retour': {
                            'date_emission': 'DateEmission',
                            'montant': 'Montant',
                            'rib_beneficiaire' : 'RibBeneficiaire'
                        }
                    },
                    'SinRD_MAMDA_retour': {
                        'SinRDReg_retour': {
                            'date_emission': 'DateEmission',
                            'montant': 'Montant',
                            'rib_beneficiaire' : 'RibBeneficiaire'
                        }
                    }
                }
            }
        }
        
    def mock_connect_to_dbs(self, path):
        self.connect_to_dbs_calls.append(path)
        if self.should_fail:
            return None
        return self.connections

# Données de test
@pytest.fixture
def sample_parsed_data():
    return {
        'header': {
            'num_donneur_ordre': '0679812',
            'date_creation': '20240721'
        },
        'details': [
            {
                'reference_virement': '302-541242',
                'date_emission': '20240721',
                'date_traitement': '20240721',
                'date_execution': '20240722',
                'montant': '1000.00',
                'rib_beneficiaire': '123456789012345678901234',
                'motif_virement': 'Remboursement sinistre'
            },
            {
                'reference_virement': '302-541243',
                'date_emission': '20240721',
                'date_traitement': '20240721',
                'date_execution': '20240722',
                'montant': '2000.00',
                'rib_beneficiaire': '234567890123456789012345',
                'motif_virement': 'Indemnisation'
            }
        ],
        'footer': {
            'total_records': '2'
        }
    }

def test_insert_status_data_successful(sample_parsed_data, monkeypatch, capsys):
    mock_connection = MockConnection()
    mock_db = MockDatabase({'SinAuto_MCMA_retour': mock_connection})
    
    # Remplacer les fonctions dans le module insert
    monkeypatch.setattr('scripts.insert.read_yaml_file', mock_db.mock_read_yaml_file)
    monkeypatch.setattr('scripts.insert.connect_to_dbs', mock_db.mock_connect_to_dbs)
    
    # Exécuter la fonction
    result = insert_status_data(sample_parsed_data, "fake_db_config.yaml", "fake_mapping.yaml")
    
    # Vérifications
    assert len(mock_db.read_yaml_calls) == 1
    assert mock_db.read_yaml_calls[0] == "fake_mapping.yaml"
    
    assert len(mock_db.connect_to_dbs_calls) == 1
    assert mock_db.connect_to_dbs_calls[0] == "fake_db_config.yaml"
    
    # Vérifier que les requêtes ont été exécutées (2 détails)
    assert len(mock_connection.cursor_obj.execute_calls) == 2
    
    # Vérifier que commit et close ont été appelés
    assert mock_connection.commit_called == True
    assert mock_connection.close_called == True
    
    # Vérifier le message de succès
    captured = capsys.readouterr()
    assert "Data inserted successfully" in captured.out
    
    assert result is None

def test_insert_status_data_no_connection(sample_parsed_data, monkeypatch, capsys):
    # Créer un mock qui simule un échec de connexion
    mock_db = MockDatabase(should_fail_connection=True)
    
    # Remplacer les fonctions
    monkeypatch.setattr('scripts.insert.read_yaml_file', mock_db.mock_read_yaml_file)
    monkeypatch.setattr('scripts.insert.connect_to_dbs', mock_db.mock_connect_to_dbs)
    
    # Exécuter la fonction
    result = insert_status_data(sample_parsed_data, "fake_db_config.yaml", "fake_mapping.yaml")
    
    # Vérifications
    assert len(mock_db.connect_to_dbs_calls) == 1
    
    # Vérifier le message d'erreur
    captured = capsys.readouterr()
    assert "Failed to establish connection to the databases" in captured.out
    
    assert result is None

def test_insert_status_data_sql_error(sample_parsed_data, monkeypatch, capsys):
    # Créer un mock avec erreur SQL
    mock_connection = MockConnection(should_fail=True)
    mock_db = MockDatabase({'SinAuto_MCMA_retour': mock_connection})
    
    # Remplacer les fonctions
    monkeypatch.setattr('scripts.insert.read_yaml_file', mock_db.mock_read_yaml_file)
    monkeypatch.setattr('scripts.insert.connect_to_dbs', mock_db.mock_connect_to_dbs)
    
    # Exécuter la fonction
    result = insert_status_data(sample_parsed_data, "fake_db_config.yaml", "fake_mapping.yaml")
    
    # Vérifications - même avec erreur, commit et close doivent être appelés
    assert mock_connection.commit_called == True
    assert mock_connection.close_called == True
    
    # Vérifier qu'il n'y a pas de message de succès à cause des erreurs
    captured = capsys.readouterr()
    assert "Error executing query" in captured.out
    
    assert result is None

def test_insert_status_data_mcma_donneur_ordre(monkeypatch, capsys):
    # Données avec MCMA comme donneur d'ordre et type Auto (02)
    mcma_data = {
        'header': {'num_donneur_ordre': '0679812'},  # MCMA
        'details': [{'reference_virement': '202-541242'}],  # Type 02 = Auto
        'footer': {}
    }
    
    # Créer les mocks - MCMA + Auto = SinAuto_MCMA_retour
    mock_connection = MockConnection()
    mock_db = MockDatabase({'SinAuto_MCMA_retour': mock_connection})
    
    # Remplacer les fonctions
    monkeypatch.setattr('scripts.insert.read_yaml_file', mock_db.mock_read_yaml_file)
    monkeypatch.setattr('scripts.insert.connect_to_dbs', mock_db.mock_connect_to_dbs)
    
    # Exécuter la fonction
    result = insert_status_data(mcma_data, "fake_db_config.yaml", "fake_mapping.yaml")
    
    # Vérifications
    assert len(mock_connection.cursor_obj.execute_calls) == 1
    assert mock_connection.close_called == True
    
    # Vérifier le message de succès
    captured = capsys.readouterr()
    assert "Data inserted successfully" in captured.out
    
    assert result is None

def test_insert_status_data_different_sinistre_types(monkeypatch, capsys):
    # Test avec différents types de sinistres
    test_cases = [
        ('101-541242', 'SinAT_MAMDA_retour'),   # Type 01 = AT
        ('202-541243', 'SinAuto_MAMDA_retour'), # Type 02 = Auto
        ('303-541244', 'SinRD_MAMDA_retour')   # Autre = RD
    ]
    
    for ref_virement, expected_db in test_cases:
        # Préparer les données
        test_data = {
            'header': {'num_donneur_ordre': '0679814'},  # MAMDA
            'details': [{'reference_virement': ref_virement}],
            'footer': {}
        }
        
        # Créer un mock spécifique pour cette base de données
        mock_connection = MockConnection()
        mock_db = MockDatabase({expected_db: mock_connection})
        
        # Remplacer les fonctions
        monkeypatch.setattr('scripts.insert.read_yaml_file', mock_db.mock_read_yaml_file)
        monkeypatch.setattr('scripts.insert.connect_to_dbs', mock_db.mock_connect_to_dbs)
        
        # Exécuter la fonction
        insert_status_data(test_data, "fake_db_config.yaml", "fake_mapping.yaml")
        
        # Vérifier que la bonne base de données a été utilisée
        assert len(mock_connection.cursor_obj.execute_calls) == 1
        assert mock_connection.close_called == True
        
        # Vérifier le message de succès
        captured = capsys.readouterr()
        assert "Data inserted successfully" in captured.out

def test_insert_status_data_invalid_donneur_ordre(monkeypatch, capsys):
    # Test avec un donneur d'ordre non reconnu - la fonction gère l'erreur sans lever d'exception
    invalid_data = {
        'header': {'num_donneur_ordre': '9999999'},  # Non reconnu
        'details': [{'reference_virement': '302-541242'}],  # Type 02 = Auto
        'footer': {}
    }
    
    # Fournir des connexions valides mais aucune ne correspondra au critère
    mock_connections = {
        'SinAuto_MAMDA_retour': MockConnection(),
        'SinAuto_MCMA_retour': MockConnection()
    }
    mock_db = MockDatabase(mock_connections)
    
    # Remplacer les fonctions
    monkeypatch.setattr('scripts.insert.read_yaml_file', mock_db.mock_read_yaml_file)
    monkeypatch.setattr('scripts.insert.connect_to_dbs', mock_db.mock_connect_to_dbs)
    
    # Exécuter la fonction - elle gère l'erreur et retourne None
    result = insert_status_data(invalid_data, "fake_db_config.yaml", "fake_mapping.yaml")
    
    # Vérifier le message d'erreur
    captured = capsys.readouterr()
    assert "Error: Database doesn't exist" in captured.out
    
    assert result is None

def test_insert_status_data_empty_details(monkeypatch, capsys):
    # Test avec des détails vides - la fonction gère l'erreur sans lever d'exception
    empty_details_data = {
        'header': {'num_donneur_ordre': '0679814'},
        'details': [],
        'footer': {}
    }
    
    mock_connection = MockConnection()
    mock_db = MockDatabase({'SinAuto_MAMDA_retour': mock_connection})
    
    # Remplacer les fonctions
    monkeypatch.setattr('scripts.insert.read_yaml_file', mock_db.mock_read_yaml_file)
    monkeypatch.setattr('scripts.insert.connect_to_dbs', mock_db.mock_connect_to_dbs)
    
    # Exécuter la fonction - elle gère l'erreur et retourne None
    result = insert_status_data(empty_details_data, "fake_db_config.yaml", "fake_mapping.yaml")
    
    # Vérifier le message d'erreur
    captured = capsys.readouterr()
    assert "Error: list index out of range" in captured.out
    
    assert result is None

def test_insert_status_data_connection_failure(monkeypatch, capsys):
    # Test quand connect_to_dbs retourne None
    test_data = {
        'header': {'num_donneur_ordre': '0679814'},
        'details': [{'reference_virement': '302-541242'}],
        'footer': {}
    }
    
    mock_db = MockDatabase(should_fail_connection=True)
    
    # Remplacer les fonctions
    monkeypatch.setattr('scripts.insert.read_yaml_file', mock_db.mock_read_yaml_file)
    monkeypatch.setattr('scripts.insert.connect_to_dbs', mock_db.mock_connect_to_dbs)
    
    # Exécuter la fonction
    result = insert_status_data(test_data, "fake_db_config.yaml", "fake_mapping.yaml")
    
    # Vérifier le message d'erreur
    captured = capsys.readouterr()
    assert "Failed to establish connection to the databases" in captured.out
    
    # Devrait retourner None à cause de l'échec de connexion
    assert result is None

if __name__ == "__main__":
    pytest.main(["-v", "test_insert.py"])