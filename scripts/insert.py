''' Script for inserting the parsed data into the proper table in the proper database (for testing we will create a dataframe for each table and assume we only have one db)'''
import os  
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.file_utils import *
from utils.db_utils import connect_to_dbs



def insert_status_data(parsed_data, db_config_path, mapping_path):
    try :
        mapping = read_yaml_file(mapping_path)['table_mapping']
        databases = mapping['Databases']

        # Connect to all the dbs expressed in the db_config
        connections = connect_to_dbs(db_config_path)
        #check connections 
        if not connections : 
            print("Failed to establish connection to the databases")
            return None
        
        # specifier le donneur ordre
        parsed_header = parsed_data['header']
        if parsed_header['num_donneur_ordre'] == '0679814' :
            donneur_ordre = 'MAMDA'
        elif parsed_header['num_donneur_ordre'] == '0679812' :
            donneur_ordre = 'MCMA'
        else : 
            donneur_ordre = ""  #le scénario ou le donneur d'ordre n'est ni mamda ni mcma n'est pas pris en compte
        
        
        parsed_detail = parsed_data['details']
        # specifier le type de sinistre
        reference_virement = parsed_data['details'][0]['reference_virement']
        if reference_virement.split('-')[0][1:3] == '01': 
            type_sinistre = 'AT' #will be changed with the real value once i get acces to real db 
        elif reference_virement.split('-')[0][1:3] == '02':
            type_sinistre = 'Auto'
        else : 
            type_sinistre = 'RD' #only cases we will cover if other cases, i can add them later 
            
        
        # Connect to proper db
        for db in databases.keys(): 
            if (type_sinistre.lower() in db.lower() and donneur_ordre.lower() in db.lower()): 
                database = db
                break
            else :
                raise Exception(f"Database doesn't exist")

        connection = connections[database]
        if not connection :
            raise Exception(f"Connection to {database} failed")
        cursor = connection.cursor()

        has_errors = False
        for table, columns in databases[database].items():
            for detail in parsed_detail :  
                # Start inserting in proper tables :
                column_names = []
                values = []                  
                for key, value in detail.items():
                    if key in columns.keys():
                        column_names.append(columns[key])
                        values.append(value)
                    
                #Fill the missing columns that don't exist in data
                IdReglement = detail['reference_virement'].split('-')[1]
                if 'IdReglement' not in column_names:  # Avoid duplicate columns
                    column_names.append('IdReglement')
                    values.append(IdReglement)
                # RefReglement = reference_virement.split('-')[0][1] +                     
                if column_names and values:
                    placeholders = ", ".join(["?"] * len(values))
                    query = f"INSERT INTO {table} ({', '.join(column_names)}) VALUES ({placeholders})"
                    try:
                        cursor.execute(query, values)
                    except Exception as e:
                        print(f"Error executing query for table {table}: {e}")
                        has_errors = True
                        continue
            connection.commit()
            
            if not has_errors:
                print("Data inserted successfully")                     

            connection.close() 
            return None   
    except Exception as e : 
        print(f"Error: {e}")
        if 'connection' in locals() and connection:
            connection.close()
        return None    



