''' File for writing the SIMT file from the extracted data. '''
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.file_utils import read_yaml_file
from scripts.transform import generate_simt_line
from docx import Document
from docx.shared import Pt, Inches
from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
from datetime import datetime


def generate_simt_file(yaml_path, df, extension="txt"):

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if extension not in ["txt", "docx"]:
        raise ValueError("Extension must be either 'txt' or 'docx'")
    
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    output_dir = os.path.join(project_root, 'data', 'fee_payouts')
    os.makedirs(output_dir, exist_ok=True) 
    output_path = os.path.join(output_dir, f'fee_payouts_{timestamp}.{extension}')
    
    # Check if the YAML file exists and read its structure
    if not os.path.exists(yaml_path):
        raise FileNotFoundError(f"YAML file not found: {yaml_path}")
    else : structure = read_yaml_file(yaml_path)["integration_file_structure"]

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

    # Write to file
    if extension == "txt":
        with open(output_path, "w", encoding="utf-8") as f:
            for line in lines:
                f.write(line + "\n")
    elif extension == "docx":
        doc =Document()
        heading = doc.add_heading("SIMT File\n", level=1)
        heading.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
        
        heading_run = heading.runs[0]
        heading_run.font.size = Pt(20)
        heading_run.bold = True

        for line in lines:
            paragraph = doc.add_paragraph(line)
            paragraph.alignment = WD_PARAGRAPH_ALIGNMENT.LEFT
            run = paragraph.runs[0]
            run.font.size = Pt(12)

        sections = doc.sections
        for section in sections:
            section.top_margin = Inches(0.5)
            section.bottom_margin = Inches(0.5)
            section.left_margin = Inches(0.25)
            section.right_margin = Inches(0.25)

        doc.save(output_path)


    print(f"Fichier généré : {output_path}")