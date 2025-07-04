# Projet Pipeline ETL MAMDA

## Phase 1 :
Cette première phase implémente un ETL pour générer des fichiers SIMT à partir de l'extraction des informations depuis 6 bases de données, pour automatiser les échange avec un partenaire bancaire
### Structure de la première phase : 
project_root/
│
├─ data/
│   └─ fee_payouts/                   # Fichiers générés
│
├─ scripts/
│   ├─ etl_pipeline_db_to_file.py     # Pipeline principal
│   ├─ extract.py                     # Extraction depuis SQL Server
│   ├─ transform.py                   # Application des règles métier
│   └─ load.py                        # Écriture du fichier texte
│
├─ config/
│   ├─ db_config.yaml                 # Infos de connexion aux DB
│   └─ table_mapping.yaml             # Mappage fichier → table SQL
│
├─ yaml/
│   ├─ transformation_rules.yaml      # Règles métier
│   └─ file_structure/
│       └─ fee_payouts_structure.yaml # Layout du fichier fee_payouts
│
├─ utils/
│   ├─ db_utils.py                    # Connexion, requêtes
│   └─ file_utils.py                  # Lecture YAML, écriture fichier
│
├─ tests/
│   ├─ test_extract.py
│   ├─ test_transform.py
│   └─ test_load.py
│
├─ README.md
└─ requirements.txt

