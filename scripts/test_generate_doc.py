from datetime import datetime
import yaml
import pandas as pd

def generate_line(fields, data_row=None):
   
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
                        value = "*" * length
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

    with open(yaml_path, "r", encoding="utf-8") as f:
        structure = yaml.safe_load(f)["integration_file_structure"]

    lines = []
    # Header
    lines.append("\nHEADER\n")
    lines.append(generate_line(structure["Header"]["Fields"]))

    # Detail
    lines.append("\nDETAIL\n")
    for _, row in df.iterrows():
        lines.append(generate_line(structure["Detail"]["Fields"], data_row=row))

    # Footer
    lines.append("\nFOOTER\n")
    lines.append(generate_line(structure["Footer"]["Fields"]))

    # Write to txt
    with open(output_path, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")

    print(f"Fichier généré : {output_path}")


if __name__ == "__main__":

    yaml_path = "config/file_structure/fee_payouts_structure.yaml"
    df = pd.read_csv("data/donnees_fictives.csv")
    output_path = "generated_files/test_output.txt"

    generate_simt_file(yaml_path, df, output_path)