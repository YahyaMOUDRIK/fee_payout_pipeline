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
    transformation_rules = read_yaml_file(rules)
    fields = transformation_rules['transformation_rules']
    columns = df.columns
    for column in columns : 
        for field in  fields :
            if column == field : 
                if fields[field]['type'] == 'int' :
                    length = fields[field]['length']
                    transformation = fields[field]['transformation']
                    df[column] = df[column].apply(lambda value : eval(transformation, {}, {"value": value, "length": length}))
                if fields[field]['type'] == 'date' or  fields[field]['type'] == 'time': 
                    df[column] = pd.to_datetime(df[column])
                    df[column] = df[column].dt.strftime(fields[field]['form'])
            else :
                pass                         
    return df

