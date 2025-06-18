''' File for writing the SIMT file from the extracted data. '''
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.file_utils import read_yaml_file
from scripts.transform import generate_simt_line


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