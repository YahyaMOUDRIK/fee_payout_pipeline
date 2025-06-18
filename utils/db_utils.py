''' File for database utilities : Connections, queries, etc. '''
import yaml
import pyodbc
import os
from dotenv import load_dotenv

load_dotenv()

def connect_to_database(db_config_yaml):

    with open(db_config_yaml, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)["connections"]["local_test"]
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

