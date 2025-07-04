''' File that contains functions to extract data from the database. '''
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pyodbc
import pandas as pd
from utils.db_utils import connect_to_dbs, generate_query

def extract_from_sql(config, db, schema = None):
    try:
        connections = connect_to_dbs(config)
        connection = connections[db.lower()]
        cursor = connection.cursor()
        query = generate_query(db, schema)
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        data = [dict(zip(columns, row)) for row in rows]
        data = pd.DataFrame(data)
        return data
    except Exception as e: 
        print(f"Error executing query: {e}")
        return None
    finally:
        if 'cursor' in locals() :
            cursor.close()
        if 'connection' in locals() :
            connection.close()    
