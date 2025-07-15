''' Script for parsing the received file, takes a file path, and a structure path and it rturns a dataframe made of the fields of the file (definefd in the structure) '''
import pandas as pd

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.file_utils import *

 
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
    lines = read_asc_file(file_path)
    structure = read_yaml_file(structure_path)
    fields = structure['retour_sort_file_structure']['Detail']['Fields']
    parsed_lines = [parse_line(line, fields) for line in lines[1:-1]]
    df = pd.DataFrame(parsed_lines)
    return df

# file_path = "data/fee_payouts_status/sample.asc"
# structure_path = "config/file_structure/fee_payouts_status_structure.yaml"
# df = parse_file(file_path, structure_path)
# print(df)
