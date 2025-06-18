import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pyodbc
import pandas as pd
from datetime import datetime
from utils.db_utils import connect_to_database
from utils.file_utils import read_yaml_file
    
def extract_data_from_db(db_config_yaml, query):
    try:
        connection = connect_to_database(db_config_yaml)
        cursor = connection.cursor()
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        data = [dict(zip(columns, row)) for row in rows]
        data = pd.DataFrame(data)
        return data
    except pyodbc.Error as e:
        print(f"Error executing query: {e}")
        return None
    finally:
        cursor.close()
        connection.close()

def generate_simt_line(fields, data_row=None):

    line = [' '] * 500
    
    for field in fields:
        for name, props in field.items():
            pos = props["starting position"]
            length = props["longueur"]
            type = props["type"]
            default = props["default"]

            if data_row is not None and name in data_row and pd.notna(data_row[name]):
                value = str(data_row[name])
            else : 
                if type == 'text':
                    if default != "":
                        value = str(default)
                    else:
                        value = " " * length
                elif type == 'integer':
                    value = str(default)
                elif type == 'date' or type == 'time':
                    if default == "today":
                        value = datetime.today().strftime("%Y%m%d")
                    elif default == "now":
                        value = datetime.today().strftime("%H%M%S")


            if type == "integer":
                value = value.zfill(length)
            else:
                value = value.ljust(length)[:length]

            line[pos:pos+length] = list(value)

    return ''.join(line).rstrip()


def generate_simt_file(yaml_path, df, output_path):

    
    structure = read_yaml_file(yaml_path)["integration_file_structure"]

    lines = []
    # Header
    #lines.append("\nHEADER\n")
    lines.append(generate_simt_line(structure["Header"]["Fields"]))

    # Detail
    #lines.append("\nDETAIL\n")
    for _, row in df.iterrows():
        lines.append(generate_simt_line(structure["Detail"]["Fields"], data_row=row))

    # Footer
    #lines.append("\nFOOTER\n")
    lines.append(generate_simt_line(structure["Footer"]["Fields"]))

    # Write to txt
    with open(output_path, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")

    print(f"Fichier généré : {output_path}")

# Test the connection and data extraction
if __name__ == "__main__":
    output_path = "generated_files/test_output.txt"
    db_config_yaml = "config/db_config.yaml"
    query = """
    SELECT 
        '04' AS code_enregistrement,
        '020' AS code_operation,  -- valeur fixe
        d.CodeBanque AS code_banque,
        d.CodeGuichet AS code_guichet,
        p.ReferenceRemise AS ref_beneficiaire,
        'MAD' AS code_devise,      -- valeur fixe
        p.Montant AS montant,
        FORMAT(p.DateExecution, 'yyyyMMdd') AS date_execution,
        '00' AS par_defaut,        -- valeur fixe
        d.Nom AS nom_donneur_ordre,
        b.Nom AS nom_beneficiaire,
        b.RIB AS rib_beneficiaire,
        p.Motif AS motif_virement
    FROM 
        Paiements p
    JOIN 
        Benificiare b ON p.BenificiareID = b.BenificiareID
    JOIN 
        DonneursOrdre d ON p.DonneurID = d.DonneurID;
    """  
    # Connect to the database and extract data
    data = extract_data_from_db(db_config_yaml, query)

    if data is not None and not data.empty:
        print("Data extracted successfully:")
        df = pd.DataFrame(data)
    else:
        print("No data extracted or an error occurred.")

    # Generate the SIMT file
    yaml_path = "config/file_structure/fee_payouts_structure.yaml"
    generate_simt_file(yaml_path, df, output_path)
