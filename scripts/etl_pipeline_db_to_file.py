''' File for the whole ETL pipeline from database to file. '''
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.extract import extract_data_from_db
from scripts.load import generate_simt_file
import pandas as pd
from utils.db_utils import QUERY


if __name__ == "__main__":
    output_path = "generated_files/test_output.txt"
    db_config_yaml = "config/db_config.yaml"

    # Connect to the database and extract data
    data = extract_data_from_db(db_config_yaml, QUERY)

    if data is not None and not data.empty:
        print("Data extracted successfully:")
        df = pd.DataFrame(data)
    else:
        print("No data extracted or an error occurred.")

    # Generate the SIMT file
    yaml_path = "config/file_structure/fee_payouts_structure.yaml"
    generate_simt_file(yaml_path, df, output_path)