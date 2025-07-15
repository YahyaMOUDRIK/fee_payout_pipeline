''' Script for inserting the parsed data into the proper table in the proper database (for testing we will create a dataframe for each table and assume we only have one db)'''
import pandas as pd
import os  
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.file_utils import *
from utils.db_utils import connect_to_dbs

def insert_status_data(data, mapping_path):
    mapping = read_yaml_file(mapping_path)['table_mapping']
    databases = mapping['Databases'].keys()
    # tables = mapping['Databases'].values()
    df_AccTra_Donneur = pd.DataFrame()
    df_AccTra_Beneficiaire = pd.DataFrame()
    df_AccTra_Transations = pd.DataFrame()
    df_RD_Donneur = pd.DataFrame()
    df_RD_Beneficiaire = pd.DataFrame()
    df_RD_Transactions = pd.DataFrame()
    dataframes = {
        'df_Donneur' :  df_AccTra_Donneur,
        'df_Beneficiaire' : df_AccTra_Beneficiaire,
        'df_Transactions' : df_AccTra_Transations,
        # 'df_RD_Donneur' : df_RD_Donneur, 
        # 'df_RD_Beneficiaire' : df_RD_Beneficiaire,
        # 'df_RD_Transactions' : df_RD_Transactions
    }
    data_columns = data.columns
    for db in databases : 
        for table, columns in mapping['Databases'][db].items() :
            for df_name, df in dataframes.items(): 
                if df_name.split('df_', 1)[1] == table.replace('.', '_') : 
                    for key, value in columns.items():
                        for data_column in data_columns : 
                            if key == data_column :
                                df[value] = data[key]
                            else : pass
    
    return dataframes
