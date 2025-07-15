# from datetime import datetime
# test = '0000000012500000'
# parsed = datetime.strptime(test, '%Y%M%d').strftime('%Y-%M-%d')
# print(parsed)

''' File for transforming data before writing loading to db '''
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from datetime import datetime
from utils.file_utils import read_yaml_file

def transform_fields(df, rules):
    transformation_rules = read_yaml_file(rules)
    fields = transformation_rules['transformation_rules']
    columns = df.columns
    for column in columns : 
        for field in  fields :
            if column == field :
                transformation = fields[field]['transformation']
                df[column] = df[column].apply(lambda value : eval(transformation, {}, {"value": value, "datetime": datetime}))
            else :
                pass                         
    return df