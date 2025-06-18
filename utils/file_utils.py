''' File for yaml file utilities : Read, write, etc. '''
import yaml
import os

def read_yaml_file(yaml_path):
    """Read a YAML file and return its content."""
    if not os.path.exists(yaml_path):
        raise FileNotFoundError(f"YAML file not found: {yaml_path}")
    
    with open(yaml_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)

