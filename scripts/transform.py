''' File for transforming data before writing to the SIMT file. '''
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from datetime import datetime
from utils.file_utils import read_yaml_file

def map_tables(df, table_mapping) :
    old_columns = df.columns
    mapper = read_yaml_file(table_mapping)
    new_df = df.copy()
    for column in old_columns :
        new_df.rename(columns = {column : mapper['table_mapping'][column]}, inplace = True) 
    return new_df


def transform_fields(df, rules):

    return None

def generate_simt_line(fields, data_row=None):

    line = [' '] * 500
    
    for field in fields:
        for name, props in field.items():
            pos = props["starting position"]
            length = props["longueur"]
            type = props["type"]
            default = props["default"]

            if data_row is not None and name in data_row and pd.notna(data_row[name]):
                value = str(data_row[name])
            else : 
                if type == 'text':
                    if default != "":
                        value = str(default)
                    else:
                        value = " " * length
                elif type == 'integer':
                    value = str(default)
                elif type == 'date' or type == 'time':
                    if default == "today":
                        value = datetime.today().strftime("%Y%m%d")
                    elif default == "now":
                        value = datetime.today().strftime("%H%M%S")


            if type == "integer":
                value = value.zfill(length)
            else:
                value = value.ljust(length)[:length]

            line[pos:pos+length] = list(value)

    return ''.join(line).rstrip()