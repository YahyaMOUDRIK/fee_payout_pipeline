import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from scripts.transform import generate_simt_line

def test_generate_simt_line_with_data():
    fields = [
        {"code": {"starting position": 0, "longueur": 2, "type": "integer", "default": "10"}},
        {"nom": {"starting position": 2, "longueur": 6, "type": "text", "default": ""}}
    ]
    data = {"code": 5, "nom": "Rachid"}

    result = generate_simt_line(fields, data_row=data)

    assert result.startswith("05Rachid")
    assert len(result) <= 500

def test_generate_simt_line_with_defaults():
    fields = [
        {"code": {"starting position": 0, "longueur": 2, "type": "integer", "default": "10"}},
        {"nom": {"starting position": 2, "longueur": 6, "type": "text", "default": "TEST"}}
    ]
    result = generate_simt_line(fields)

    assert result.startswith("10TEST")
    assert len(result) <= 500
