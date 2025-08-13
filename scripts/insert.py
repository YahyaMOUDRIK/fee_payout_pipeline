''' Script for inserting the parsed data into the proper table in the proper database (for testing we will create a dataframe for each table and assume we only have one db)'''
import os  
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.file_utils import *
from utils.db_utils import connect_to_dbs
from scripts.parse import *



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
            donneur_ordre = None  #le scénario ou le donneur d'ordre n'est ni mamda ni mcma n'est pas pris en compte
        
        
        parsed_detail = parsed_data['details']
        # specifier le type de sinistre
        reference_virement = parsed_data['details'][0]['reference_virement']
        if reference_virement.split('-')[0][1:3] == '01': 
            type_sinistre = 'AT' #will be changed with the real value once i get acces to real db 
        elif reference_virement.split('-')[0][1:3] == '02':
            type_sinistre = 'Auto'
        else : 
            type_sinistre = 'RD' #only cases we will cover if other cases, i can add them later 
            
        parsed_footer = parsed_data['footer']
        # Connect to proper db
        # for db in databases.keys(): 
        #     if (type_sinistre.lower() in db.lower() and donneur_ordre.lower() in db.lower()): 
        #         database = db
        #         print(f"db chosen is {db}")
        #         break
        #     else :
        #         raise Exception(f"Database doesn't exist")
        database = None
        for db in databases.keys(): 
            try : 
                if (type_sinistre.lower() in db.lower() and donneur_ordre.lower() in db.lower()): 
                    database = db
                    print(f"db chosen is {database}")
            except Exception as e: 
                # print(e)
                continue
        if not database : 
            raise Exception(f"Database doesn't exist for type_sinistre={type_sinistre} and donneur_ordre={donneur_ordre}")

        connection = connections[database]
        if not connection :
            raise Exception(f"Connection to {database} failed")
        cursor = connection.cursor()
        

        has_errors = False
        file_id = None

        for table, columns in databases[database].items() :
            if 'metadata' in table.lower() : 

                # Check if the file has already been processed
                name_file = os.path.basename(parsed_data['file_path'])
                cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE NameFile = ?", (name_file,))
                if cursor.fetchone()[0] > 0:
                    print(f"File {name_file} has already been processed. Skipping.")
                    return 
                
                metadata_columns = []
                metadata_values  = []
                for key, value in parsed_header.items() : 
                    if key in columns.keys():
                        metadata_columns.append(columns[key])
                        metadata_values.append(value)
                for key, value in parsed_footer.items() :
                    if key in columns.keys():
                        metadata_columns.append(columns[key])
                        metadata_values.append(value)
                name_file = os.path.basename(parsed_data['file_path'])
                metadata_columns.append('NameFile')
                metadata_values.append(name_file)

                # Construire et exécuter la requête d'insertion
                placeholders = ", ".join(["?"] * len(metadata_values))
                query = f"INSERT INTO {table} ({', '.join(metadata_columns)}) VALUES ({placeholders})"
                try:
                    cursor.execute(query, metadata_values)
                    connection.commit()

                    # Récupérer l'ID généré automatiquement
                    cursor.execute(f"SELECT IdFile FROM {table} WHERE NameFile = ?", 
                                (name_file))
                    file_id = cursor.fetchone()[0]
                    print(f"Metadata inserted successfully into {table} with ID: {file_id}")
                except Exception as e:
                    print(f"Error inserting metadata into {table}: {e}")
                    has_errors = True
                    file_id = None

            else : 

                if file_id is None:
                    print("Error: Metadata insertion failed, cannot proceed with detail insertion.")
                    has_errors = True
                    break

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

                    if 'IdFile' not in column_names:  # Assuming the column name in the table is IdFile
                        column_names.append('IdFile')
                        values.append(file_id)

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

    except Exception as e : 
        print(f"Error: {e}")
        if 'connection' in locals() and connection:
            connection.close()
        return None    
        
