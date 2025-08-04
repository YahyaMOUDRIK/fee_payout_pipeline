''' Script for parsing the received file, takes a file path, and a structure path and it rturns a dataframe made of the fields of the file (definefd in the structure) '''
import pandas as pd

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.file_utils import *
import re
from datetime import datetime



def validate_line(line, line_type):
    is_valid = True
    errors = []
    if line_type == 'header':
        #code_enregistrement
        if not line.startswith('10'):
            is_valid = False
            errors.append('code_enregistrement_header')
        #18 zéros
        zero_fields = re.search(r"10(0+)", line)
        if zero_fields.group(1) != '0'*18:
            is_valid = False
            errors.append('18 zeros header')
        #DateTime 
        date_heure_prod = re.search(r'20.*?(?=MAD)', line)
        try:
            datetime.strptime(date_heure_prod.group(0), "%Y%m%d%H%M%S")
        except ValueError as e:
            is_valid = False
            errors.append(f'Date Time Production est fausse dans header: {date_heure_prod.group(0)}')

    if line_type == "detail" : 
        
        #code enregistrement detail
        if not line.startswith('04'):
            errors.append("La ligne doit commencer par le code '04'.")

        #nom donneur ordre
        if not re.search(r'\b(mcma|mamda)\b', line, re.IGNORECASE):
            errors.append("Le nom du donneur d'ordre ('mcma' ou 'mamda') est manquant.")

        #00
        if not re.search(r'\b00\b', line):
            errors.append("Le champ obligatoire '00' est manquant.")

        #MAD2
        if not re.search(r'MAD2\d{8,16}\.\d{1,2}', line):
            errors.append("Le format 'MAD2' suivi du montant est incorrect ou manquant.")

        #RIB ben et Rib donn ord
        rib_block_match = re.search(r'\b(\d{48})\b', line)
        if not rib_block_match:
            errors.append("Le bloc de 48 chiffres contenant les deux RIBs est manquant.")
        else:
            #rib donn ordre commence avec 007
            rib_combined = rib_block_match.group(1)
            first_rib = rib_combined[:24]
            second_rib = rib_combined[24:48]
            if not (first_rib.startswith('007') or second_rib.startswith('007')):
                errors.append("Aucun des deux RIBs ne commence par '007' (RIB donneur d'ordre invalide).")

        #nom benef
        if not re.search(r'mcma\s+[A-Z][A-Z\s\.&]{4,34}\s+\d{48}', line, re.IGNORECASE):
            errors.append("Le nom du bénéficiaire est manquant ou mal formaté.")

        #référence de virement au format 'XXX-XXXXXX'
        if not re.search(r'\b\d{3}-\d{6}\b', line):
            errors.append("La référence de virement (format XXX-XXXXXX) est manquante.")
            
        #date_emission : 
        date_emission = re.search(r"0{6}(\d{8})", line)
        if date_emission:
            date_str = date_emission.group(1)
            try:
                datetime.strptime(date_str, "%Y%m%d")
            except ValueError:
                is_valid = False
                errors.append(f"Date Time emission invalide (mauvais format ou date invalide): {date_str}")
        else:
            is_valid = False
            errors.append("Date d'emission non trouvée dans la ligne")
            
        #date_traitement
        date_traitement = re.search(r"\.\d{2}(\d{8})", line)
        if date_traitement:
            date_str = date_traitement.group(1)
            try:
                datetime.strptime(date_str, "%Y%m%d")
            except ValueError:
                is_valid = False
                errors.append(f"Date Time traitement invalide (mauvais format ou date invalide): {date_str}")
        else:
            is_valid = False
            errors.append("Date de traitement non trouvée dans la ligne")
            
        #date_execution
        date_execution = re.search(rf"{re.escape(date_traitement.group(1))}(\d{{8}})", line)
        if date_execution:
            date_str = date_execution.group(1)
            try:
                datetime.strptime(date_str, "%Y%m%d")
            except ValueError:
                is_valid = False
                errors.append(f"Date Time execution invalide (mauvais format ou date invalide): {date_str}")
        else:
            is_valid = False
            errors.append("Date de execution non trouvée dans la ligne")

    if line_type == 'footer' :
        if not line.startswith('11'):
            errors.append("error code enr footer")
    return is_valid, errors

    
def extract_fields(line, line_type) :
    header_fields = {}
    detail_fields = {}
    footer_fields = {}
    if line_type == 'header': 
        date_production = re.search(r'20.*?(?=MAD)', line)
        header_fields['date_production'] = date_production.group(0)
        return header_fields
    elif line_type == 'detail' : 
        reference_virement = re.search(r'\b\d{3}-\d{6}\b', line)
        date_emission = re.search(r"(\d{8})007", line)
        date_traitement = re.search(r"\.\d{2}(\d{8})", line)
        date_execution = re.search(rf"{re.escape(date_traitement.group(1))}(\d{{8}})", line)
        montant = re.search(r"MAD2(\d+\.\d{2})", line)
        rib_beneficiaire = re.search(r'\b(\d{48})\b', line).group(1)[24:48]
        motif_virement = re.search(r"\d{20,}\s+([A-Za-zÉéèàêâîïçûôë' -]+?)\s+\d{3}-\d{6}", line)
        nom_beneficiaire = re.search(r"(mcma|mamda)\s+([A-ZÉÈÀÂÎÏ'\- ]+?)\s+\d{20,}", line, re.IGNORECASE)
        detail_fields['date_emission'] = date_emission.group(1)
        detail_fields['date_traitement'] = date_traitement.group(1)
        detail_fields['date_execution'] = date_execution.group(1)
        detail_fields['montant'] = montant.group(1)
        detail_fields['rib_beneficiaire'] = rib_beneficiaire
        detail_fields['motif_virement'] = motif_virement.group(1).strip()
        detail_fields['nom_beneficiaire'] = nom_beneficiaire.group(2).strip()
        detail_fields['reference_virement'] = reference_virement.group(0)
        return detail_fields
    elif line_type == 'footer':
        nb_valeurs = re.search(r"11(\d{5})", line).group(1)
        montant_total = re.search(r"11\d{5}(\d+\.\d{2})", line).group(1)
        nb_valeurs_payees = re.search(r"\.\d{2}(\d+)", line).group(1)
        footer_fields['nb_valeurs'] = nb_valeurs
        footer_fields['montant_total'] = montant_total
        footer_fields['nb_valeurs_payees'] = nb_valeurs_payees
        return footer_fields
    else : 
        return None

def parse_line(line, fields):
    parsed_data = {}
    for field in fields:
        field_name = list(field.keys())[0]
        field_info = field[field_name]
        start = field_info["starting position"]
        length = field_info["longueur"]
        parsed_data[field_name] = line[start:start+length].strip()
    return parsed_data

def parse_file(file_path, structure_path):
    parsed_lines = {
        'file_path' : file_path,
        'header': None,
        'details': [],
        'footer': None
    }
    lines = read_asc_file(file_path)
    header_line = lines[0]
    detail_lines = lines[1:-1]
    footer_line = lines[-1]
    details_parsed_fields = []
    structure = read_yaml_file(structure_path)
    header_fields = structure['retour_sort_file_structure']['Header']['Fields'] 
    detail_fields = structure['retour_sort_file_structure']['Detail']['Fields']
    footer_fields = structure['retour_sort_file_structure']['Footer']['Fields']
    # parse header data after validation
    is_valid, errors = validate_line(header_line, 'header')
    if is_valid:
        header_parsed_fields = parse_line(header_line, header_fields)
    else:
        header_parsed_fields = extract_fields(header_line, 'header')
    # parse details data
    for line in detail_lines:
        is_valid, errors = validate_line(line, 'detail')
        if is_valid:
            details_parsed_fields.append(parse_line(line, detail_fields))
        else:
            # print(f"Detail validation errors on line: {errors}")
            details_parsed_fields.append(extract_fields(line, 'detail'))
    # parse footer data
    is_valid, errors = validate_line(footer_line, 'footer')
    if is_valid:
        footer_parsed_fields = parse_line(footer_line, footer_fields)
    else:
        # print(f"Footer validation errors: {errors}")
        footer_parsed_fields = extract_fields(footer_line, 'footer')

    # details_parsed_fields = [parse_line(line, detail_fields) for line in lines[1:-1]]
    
    #populate parsed_lines
    parsed_lines['header'] = header_parsed_fields
    parsed_lines['details'] = details_parsed_fields
    parsed_lines['footer'] = footer_parsed_fields
    return parsed_lines

# parsed_fields = parse_file('data/fee_payouts_status/AT_0825.asc' , 'config/file_structure/fee_payouts_status_structure.yaml')
# print(parsed_fields)
