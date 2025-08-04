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

def transform_fields(data, rules):
    transformation_rules = read_yaml_file(rules)
    fields = transformation_rules['transformation_rules']
    
    # Transformer les champs du header
    if 'header' in data and data['header']:
        for field_name, field_value in data['header'].items():
            if field_name in fields:
                transformation = fields[field_name]['transformation']
                try:
                    data['header'][field_name] = eval(transformation, {}, {"value": field_value, "datetime": datetime})
                except Exception as e:
                    print(f"Erreur transformation header.{field_name}: {str(e)}")
    
    # Transformer les champs dans chaque détail
    if 'details' in data and data['details']:
        for i, detail in enumerate(data['details']):
            for field_name, field_value in detail.items():
                if field_name in fields:
                    transformation = fields[field_name]['transformation']
                    try:
                        if field_name in ['montant', 'montant_total']:
                            data['details'][i][field_name] = eval(transformation, {}, {"value": field_value})
                        else:
                            data['details'][i][field_name] = eval(transformation, {}, {"value": field_value, "datetime": datetime})
                    except Exception as e:
                        print(f"Erreur transformation details[{i}].{field_name}: {str(e)}")
    
    # Transformer les champs du footer
    if 'footer' in data and data['footer']:
        for field_name, field_value in data['footer'].items():
            if field_name in fields:
                transformation = fields[field_name]['transformation']
                try:
                    data['footer'][field_name] = eval(transformation, {}, {"value": field_value})
                except Exception as e:
                    print(f"Erreur transformation footer.{field_name}: {str(e)}")
                    
    return data