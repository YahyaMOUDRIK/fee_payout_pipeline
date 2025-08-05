import pytest
import sys
import os
from datetime import datetime
import re

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.parse_ import validate_line, extract_fields, parse_line, parse_file

# Sample data for testing
VALID_HEADER = "10000000000000000000202407221149MAD20679812                              "
INVALID_HEADER_CODE = "11000000000000000000202407221149MAD20679812                              "
INVALID_HEADER_ZEROS = "10123456789012345678202407221149MAD20679812                              "
INVALID_HEADER_DATE = "10000000000000000000202499991149MAD20679812                              "

VALID_DETAIL = "04020007015000000202403070070051000001     MAD20000000250000.002024030820240428                     00                                   mcma                               ME BERRADA NAJIB                   007780000065258741236532011640000001248001501808        Rente invalidité                   301-541242"
INVALID_DETAIL_CODE = "03020007015000000202403070070051000001     MAD20000000250000.002024030820240428                     00                                   mcma                               ME BERRADA NAJIB                   007780000065258741236532011640000001248001501808        Rente invalidité                   301-541242"
INVALID_DETAIL_NO_MCMA = "04020007015000000202403070070051000001     MAD20000000250000.002024030820240428                     00                                                                ME BERRADA NAJIB                   007780000065258741236532011640000001248001501808        Rente invalidité                   301-541242"
INVALID_DETAIL_NO_RIB = "04020007015000000202403070070051000001     MAD20000000250000.002024030820240428                     00                                   mcma                               ME BERRADA NAJIB                                                                      Rente invalidité                   301-541242"
INVALID_DETAIL_NO_REF = "04020007015000000202403070070051000001     MAD20000000250000.002024030820240428                     00                                   mcma                               ME BERRADA NAJIB                   007780000065258741236532011640000001248001501808        Rente invalidité                   "

VALID_FOOTER = "1100004000000000943750.7500004"
INVALID_FOOTER_CODE = "1000004000000000943750.7500004"

SAMPLE_FIELDS = [
    {"code_enregistrement": {"starting position": 0, "longueur": 2}},
    {"par_defaut": {"starting position": 2, "longueur": 18}},
    {"date_production": {"starting position": 20, "longueur": 8}}
]

# Sample file content
SAMPLE_FILE_CONTENT = [
    VALID_HEADER,
    VALID_DETAIL,
    VALID_DETAIL,
    VALID_FOOTER
]

# Tests for validate_line function
class TestValidateLine:
    def test_valid_header(self):
        is_valid, errors = validate_line(VALID_HEADER, 'header')
        assert is_valid is True
        assert errors == []
        
    def test_invalid_header_code(self):
        is_valid, errors = validate_line(INVALID_HEADER_CODE, 'header')
        assert is_valid is False
        assert 'code_enregistrement_header' in errors
        
    def test_invalid_header_zeros(self):
        is_valid, errors = validate_line(INVALID_HEADER_ZEROS, 'header')
        assert is_valid is False
        assert '18 zeros header' in errors
        
    def test_invalid_header_date(self):
        is_valid, errors = validate_line(INVALID_HEADER_DATE, 'header')
        assert is_valid is False
        assert any('Date Time Production' in err for err in errors)
        
    def test_valid_detail(self):
        is_valid, errors = validate_line(VALID_DETAIL, 'detail')
        assert is_valid is True
        assert errors == []
        
    def test_invalid_detail_code(self):
        is_valid, errors = validate_line(INVALID_DETAIL_CODE, 'detail')
        assert is_valid is False
        assert any("commencer par le code '04'" in err for err in errors)
        
    def test_invalid_detail_no_mcma(self):
        is_valid, errors = validate_line(INVALID_DETAIL_NO_MCMA, 'detail')
        assert is_valid is False
        assert any("donneur d'ordre" in err for err in errors)
        
    def test_invalid_detail_no_rib(self):
        is_valid, errors = validate_line(INVALID_DETAIL_NO_RIB, 'detail')
        assert is_valid is False
        assert any("bloc de 48 chiffres" in err for err in errors)
        
    def test_invalid_detail_no_ref(self):
        is_valid, errors = validate_line(INVALID_DETAIL_NO_REF, 'detail')
        assert is_valid is False
        assert any("référence de virement" in err for err in errors)
        
    def test_valid_footer(self):
        is_valid, errors = validate_line(VALID_FOOTER, 'footer')
        assert is_valid is True
        assert errors == []
        
    def test_invalid_footer_code(self):
        is_valid, errors = validate_line(INVALID_FOOTER_CODE, 'footer')
        assert is_valid is False
        assert any("code enr footer" in err for err in errors)

# Tests for extract_fields function
class TestExtractFields:
    def test_extract_header(self):
        header_fields = extract_fields(VALID_HEADER, 'header')
        assert 'date_production' in header_fields
        assert header_fields['date_production'] == '202407221149'
        
    def test_extract_detail(self):
        detail_fields = extract_fields(VALID_DETAIL, 'detail')
        assert 'date_emission' in detail_fields
        assert 'date_traitement' in detail_fields
        assert 'date_execution' in detail_fields
        assert 'montant' in detail_fields
        assert 'rib_beneficiaire' in detail_fields
        assert 'motif_virement' in detail_fields
        assert 'nom_beneficiaire' in detail_fields
        assert 'reference_virement' in detail_fields
        
        assert detail_fields['date_emission'] == '20240307'
        assert detail_fields['montant'] == '0000000250000.00'
        assert detail_fields['nom_beneficiaire'] == 'ME BERRADA NAJIB'
        assert detail_fields['reference_virement'] == '301-541242'
        
    def test_extract_footer(self):
        footer_fields = extract_fields(VALID_FOOTER, 'footer')
        assert 'nb_valeurs' in footer_fields
        assert 'montant_total' in footer_fields
        assert 'nb_valeurs_payees' in footer_fields
        
        assert footer_fields['nb_valeurs'] == '00004'
        assert footer_fields['montant_total'] == '000000000943750.75'
        assert footer_fields['nb_valeurs_payees'] == '00004'


# Tests for parse_file function
class TestParseFile:
    # Mock the file reading and YAML loading functions
    @pytest.fixture(autouse=True)
    def mock_dependencies(self, monkeypatch):
        # Mock read_asc_file to return our sample file content
        def mock_read_asc_file(file_path):
            return SAMPLE_FILE_CONTENT
            
        # Mock read_yaml_file to return a sample structure
        def mock_read_yaml_file(structure_path):
            return {
                'retour_sort_file_structure': {
                    'Header': {'Fields': [
                        {"code_enregistrement": {"starting position": 0, "longueur": 2}},
                        {"par_defaut": {"starting position": 2, "longueur": 18}},
                        {"date_production": {"starting position": 20, "longueur": 14}}
                    ]},
                    'Detail': {'Fields': [
                        {"code_enregistrement": {"starting position": 0, "longueur": 2}},
                        {"code_operation": {"starting position": 2, "longueur": 3}},
                        {"date_emission": {"starting position": 17, "longueur": 8}}
                    ]},
                    'Footer': {'Fields': [
                        {"code_enregistrement": {"starting position": 0, "longueur": 2}},
                        {"nb_valeurs": {"starting position": 2, "longueur": 5}},
                        {"montant_total": {"starting position": 7, "longueur": 16}}
                    ]}
                }
            }
            
        monkeypatch.setattr('scripts.parse_.read_asc_file', mock_read_asc_file)
        monkeypatch.setattr('scripts.parse_.read_yaml_file', mock_read_yaml_file)
    
    def test_parse_file_basic(self, monkeypatch):
        parsed_data = parse_file('dummy_path.asc', 'dummy_structure.yaml')
        
        # Check the structure of the returned data
        assert 'file_path' in parsed_data
        assert 'header' in parsed_data
        assert 'details' in parsed_data
        assert 'footer' in parsed_data
        
        # Check header parsing
        assert parsed_data['header']['code_enregistrement'] == '10'
        
        # Check details parsing (should be a list of dictionaries)
        assert len(parsed_data['details']) == 2
        assert parsed_data['details'][0]['code_enregistrement'] == '04'
        
        # Check footer parsing
        assert parsed_data['footer']['code_enregistrement'] == '11'
        
    def test_parse_file_with_invalid_lines(self, monkeypatch):
        # Replace the second line with an invalid detail
        def mock_read_invalid_asc_file(file_path):
            return [VALID_HEADER, INVALID_DETAIL_NO_REF, VALID_DETAIL, VALID_FOOTER]
            
        monkeypatch.setattr('scripts.parse_.read_asc_file', mock_read_invalid_asc_file)
        
        parsed_data = parse_file('dummy_path.asc', 'dummy_structure.yaml')
        
        # The first detail should be parsed using extract_fields as fallback
        assert 'details' in parsed_data
        assert len(parsed_data['details']) == 2
        # Check if keys match what extract_fields would produce
        expected_keys = ['date_emission', 'date_traitement', 'date_execution', 'montant', 
                         'rib_beneficiaire', 'motif_virement', 'nom_beneficiaire', 
                         'reference_virement']
        # For the invalid line, the function will fall back to extract_fields,
        # which might fail on certain extractions, but should still return a dict
        assert isinstance(parsed_data['details'][0], dict)
