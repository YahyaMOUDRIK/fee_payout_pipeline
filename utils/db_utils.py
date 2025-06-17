''' File for database utilities : Connections, queries, etc. '''
import yaml
import pyodbc

def connect_to_database(db_config_yaml):

    with open(db_config_yaml, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)["connections"]["local_test"]
        server = config["server"]
        database = config["database"]
        driver = config["driver"]
        trusted_connection = config["trusted_connection"]
    try:
        connection = pyodbc.connect(f'Driver={driver};'
                        f'SERVER={server};'
                        f'Database={database};'
                        f'Trusted_Connection={trusted_connection};')
        print("Connection to the database was successful.")
        return connection
    except pyodbc.Error as e:
        print(f"Error connecting to database: {e}")
        return None