''' Script for parsing the received file, takes a file path, and a structure path and it rturns a dataframe made of the fields of the file (definefd in the structure) '''
import pandas as pd

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.file_utils import *
import re
from datetime import datetime


def parse_line(line, fields):
    parsed_data = {}
    for field in fields:
        field_name = list(field.keys())[0]
        field_info = field[field_name]
        start = field_info["starting position"]
        length = field_info["longueur"]
        parsed_data[field_name] = line[start:start+length].strip()
    return parsed_data

def parse_file(file_path, structure_path):
    parsed_lines = {
        'file_path' : file_path,
        'header': None,
        'details': [],
        'footer': None
    }
    lines = read_asc_file(file_path)
    structure = read_yaml_file(structure_path)
    header_fields = structure['retour_sort_file_structure']['Header']['Fields'] 
    detail_fields = structure['retour_sort_file_structure']['Detail']['Fields']
    footer_fields = structure['retour_sort_file_structure']['Footer']['Fields']
    header_parsed_fields = parse_line(lines[0], header_fields)
    details_parsed_fields = [parse_line(line, detail_fields) for line in lines[1:-1]]
    footer_parsed_fields = parse_line(lines[-1], footer_fields)
    #populate parsed_lines
    parsed_lines['header'] = header_parsed_fields
    parsed_lines['details'] = details_parsed_fields
    parsed_lines['footer'] = footer_parsed_fields
    return parsed_lines
