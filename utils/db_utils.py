''' File for database utilities : Connections, queries, etc. '''
import yaml
import pyodbc
from utils.file_utils import read_yaml_file
import os
from dotenv import load_dotenv

load_dotenv()

# QUERY = """
#     SELECT 
#         '04' AS code_enregistrement,
#         '020' AS code_operation,  -- valeur fixe
#         d.CodeBanque AS code_banque,
#         d.CodeGuichet AS code_guichet,
#         p.ReferenceRemise AS ref_beneficiaire,
#         'MAD' AS code_devise,      -- valeur fixe
#         p.Montant AS montant,
#         FORMAT(p.DateExecution, 'yyyyMMdd') AS date_execution,
#         '00' AS par_defaut,        -- valeur fixe
#         d.Nom AS nom_donneur_ordre,
#         b.Nom AS nom_beneficiaire,
#         b.RIB AS rib_beneficiaire,
#         p.Motif AS motif_virement
#     FROM 
#         Paiements p
#     JOIN 
#         Benificiare b ON p.BenificiareID = b.BenificiareID
#     JOIN 
#         DonneursOrdre d ON p.DonneurID = d.DonneurID;
#     """  

def generate_query(schema):
    QUERY = f"""
    SELECT 
        b.code_banque,
        t.date_emission,
        t.num_remise,
        b.reference,
        t.montant,
        t.date_execution,
        d.nom as nom_donneur,
        b.nom as nom_beneficiaire, 
        d.rib as rib_donneur,
        b.rib as rib_beneficiaire, 
        t.motif_virement,
        t.reference_virement,
        t.reference_remise
    FROM 
        {schema}.Transactions t
    JOIN	
        {schema}.Donneur d ON t.rib_donneur = d.rib
    JOIN
        {schema}.Beneficiaire b ON t.rib_beneficiaire = b.rib
    """
    return QUERY

def connect_to_database(db_config_yaml):

    yaml_content = read_yaml_file(db_config_yaml)
    config = yaml_content["connections"]["local_test"]
    server = config["server"]
    database = config["database"]
    driver = config["driver"]
    username = os.getenv('username')
    password = os.getenv('password')
    trusted_connection = config["trusted_connection"]
    try:
        connection = pyodbc.connect(f'Driver={driver};'
                        f'UID={username};'
                        f'PWD={password};'
                        f'SERVER={server};'
                        f'Database={database};'
                        f'Trusted_Connection={trusted_connection};')
        print("Connection to the database was successful.")
        return connection
    except pyodbc.Error as e:
        print(f"Error connecting to database: {e}")
        return None
    
def test_connection(db_config_yaml):
    connection = connect_to_database(db_config_yaml)
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            print("Connection test successful.")
        except pyodbc.Error as e:
            print(f"Error during connection test: {e}")
        finally:
            cursor.close()
            connection.close()
    else:
        print("Failed to connect to the database.")


