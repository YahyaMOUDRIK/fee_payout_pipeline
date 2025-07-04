''' File for database utilities : Connections, queries, etc. '''
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import yaml
import pyodbc
from utils.file_utils import read_yaml_file
import os
from dotenv import load_dotenv

load_dotenv()

def generate_query(db, schema = None):
    if schema :        
        QUERY = f"""
        SELECT 
            b.code_banque as code_banque_receptrice,
            t.date_emission,
            t.num_remise,
            b.reference as ref_ben,
            t.montant,
            t.date_execution,
            d.nom as nom_donneur,
            b.nom as nom_beneficiaire, 
            d.rib as rib_donneur,
            b.rib as rib_beneficiaire, 
            t.motif_virement,
            t.reference_virement,
            b.reference as reference_ben,
            t.reference_remise,
            d.num_donneur
        FROM 
            {schema}.Transactions t
        JOIN	
            {schema}.Donneur d ON t.rib_donneur = d.rib
        JOIN
            {schema}.Beneficiaire b ON t.rib_beneficiaire = b.rib
        """
    else :
        QUERY = f"""
        SELECT 
            b.code_banque as code_banque_receptrice,
            t.date_emission,
            t.num_remise,
            b.ref as ref_ben,
            t.montant,
            t.date_execution,
            d.nom as nom_donneur,
            b.nom as nom_beneficiaire, 
            d.rib as rib_donneur,
            b.rib as rib_beneficiaire, 
            t.motif_virement,
            t.reference_virement,
            b.reference as reference_ben,
            t.reference_remise,
            d.num_donneur
        FROM 
            Transactions t
        JOIN	
            Donneur d ON t.rib_donneur = d.rib
        JOIN
            Beneficiaire b ON t.rib_beneficiaire = b.rib
        """
    return QUERY


def connect_to_dbs(db_config_yaml):
    db_config = read_yaml_file(db_config_yaml)
    dbs = db_config["connections"]["databases"]
    username = os.getenv('user_name')
    password = os.getenv('password')
    connections = {}
    for key, value in dbs.items() :
        try:
            connections[key] = pyodbc.connect(f'Driver={value['driver']};'
                            f'UID={username};'
                            f'PWD={password};'
                            f'SERVER={value['server']};'
                            f'Database={key};')
                            # f'Trusted_Connection={value['trusted_connection']};')
            # print(f"Connection to the database was successful.")
        except pyodbc.Error as e:
            print(f"Error connecting to database: {e}")
            return None
    return connections    

def test_connection(db, db_config_yaml):
    connections = connect_to_dbs(db_config_yaml)
    connection = connections[db.lower()]
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM donneur")
            print("Connection test successful.")
        except pyodbc.Error as e:
            print(f"Error during connection test: {e}")
        finally:
            cursor.close()
            connection.close()
    else:
        print("Failed to connect to the database.")

# test_connection('mcma_aUTO', 'config/db_config.yaml')