# MAMDA | Fee Payouts ETL (SIMT) – End‑to‑End Guide

This repository contains a production‑ready ETL that automates SIMT “virements de masse” exchanges with the bank:
- Phase 1 (DB ➜ File): generate integration files (SIMT fixed‑width) from SQL Server data.
- Phase 2 (File ➜ DB): ingest “retour sort” status files from the bank back into the right SQL databases.
- Phase 3 (Orchestration): chain both flows monthly using an Airflow DAG.

The project is fully YAML‑driven (layout, transformation rules, DB/field mapping) and includes robust parsing with validation and regex fallback, environment‑based I/O directories, and unit tests.


## 📦 Project structure (essentials)

```
projet_test/
├─ config/
│  ├─ db_config.yaml                     # Connection info for SQL Server DBs
│  ├─ table_mapping.yaml                 # Mapping (DB→file) for Phase 1
│  ├─ retour_sort_mapping.yaml           # Mapping (file→DB) for Phase 2
│  ├─ transformation_rules.yaml          # Phase 1 field transforms
│  ├─ retour_sort_transformation_rules.yaml  # Phase 2 transforms
│  └─ file_structure/
│     ├─ fee_payouts_structure.yaml          # SIMT integration file layout
│     └─ fee_payouts_status_structure.yaml   # SIMT retour sort layout
├─ scripts/
│  ├─ etl_pipeline_db_to_file.py         # Phase 1 orchestrator
│  ├─ etl_pipeline_file_to_db.py         # Phase 2 orchestrator
│  ├─ extract.py                         # SQL query (IMP_Etat_Virement_Aux)
│  ├─ transform.py                       # Map + transform (Phase 1)
│  ├─ load.py                            # Write fixed‑width SIMT file
│  ├─ parse_.py                          # Robust parser (validation + regex)
│  ├─ transform_fields.py                # Transform parsed retour‑sort fields
│  └─ insert.py                          # Insert retour‑sort into SQL Server
├─ dags/
│  └─ expert_fee_dag.py                  # Airflow DAG (monthly)
├─ utils/
│  ├─ db_utils.py                        # ODBC connection + query builder
│  └─ file_utils.py                      # YAML/ASC file I/O helpers
├─ tests/                                # Pytest unit tests (parse, extract, load, insert, …)
├─ data/                                 # Local sample data (not used in prod)
├─ docker-compose.yaml                   # Airflow local deployment
└─ requirements.txt
```


## 🔐 Environment variables

Create a `.env` file and fill in your values.

- `DATA_DIR_FEE_PAYOUTS`: absolute path to the bank outbound drop folder where integration files will be written.
	- Example: `C:\\assurance_data\\fee_payouts`
- `DATA_DIR_FEE_STATUS`: absolute path to the bank inbound folder where retour‑sort files are deposited by the bank.
	- Example: `C:\\assurance_data\\fee_payouts_status`
- `user_name`, `password`: SQL Server credentials used by ODBC connections defined in `config/db_config.yaml`.

Notes
- The ETL stops if the external directories are missing or not accessible (no local fallback in production mode).
- Windows requires Microsoft ODBC Driver for SQL Server (v17+). Install it before running Phase 1/2.
- You can use either the local project data folders or absolute external folders:
    - Current choice: external absolute folders via `.env` for portability across environments and Airflow.
    - To switch to local folders, choose one of the following:
        - Option A : point env vars to local paths inside the repo, for example:
            - `DATA_DIR_FEE_PAYOUTS = <project_root>\data\fee_payouts`
            - `DATA_DIR_FEE_STATUS  = <project_root>\data\fee_payouts_status`
        - Option B (code change): update the ETL scripts to use the local folders directly:
            - In `scripts/load.py`, set `output_dir = os.path.join(project_root, 'data', 'fee_payouts')`.
            - In `scripts/etl_pipeline_file_to_db.py`, set `directory_path = os.path.join(project_root, '..', 'data', 'fee_payouts_status')` (or adjust relative path) and bypass the external dir check.
    - If using Airflow, ensure Docker volume mounts expose whatever folder you choose so the DAG can read/write there.


## Phase 1 — DB ➜ File (Integration SIMT)

Goal: Extract fee payout rows from SQL Server and generate SIMT fixed‑width files per SIMT layout.

Key pieces
- `scripts/extract.py`
	- Builds the query `SELECT * FROM dbo.IMP_Etat_Virement_Aux('<type_aux>')` per aux type: `A` (Avocats), `E` (Experts Auto), `M` (Médecins), `C` (Experts Comptables).
	- Uses `utils/db_utils.connect_to_dbs()` for connections defined in `config/db_config.yaml` with env `user_name` and `password`.
- `scripts/transform.py`
	- `map_tables(df, config/table_mapping.yaml)`: normalize column names to the YAML mapping expected by the SIMT layout.
	- `transform_fields(df, config/transformation_rules.yaml)`: apply business rules (e.g., date formats, amount formatting).
- `scripts/load.py`
	- `generate_simt_line(fields, row)`: writes a 500‑character fixed‑width line using `fee_payouts_structure.yaml`.
	- `generate_simt_file(...)`: builds header, one detail per row, and a footer. Computes totals if missing; writes `.asc` to `DATA_DIR_FEE_PAYOUTS`, creating subfolders by type (`Avocats`, `Experts Automobiles`, `Medecins`, `Experts Comptables`).
- `scripts/etl_pipeline_db_to_file.py`
	- Orchestrator: for each selected DB and each `type_aux` (A/E/M/C) → extract → map → transform → generate SIMT.
	- Logs progress and errors.

Data contract (extract ➜ load)
- Layout is defined in `config/file_structure/fee_payouts_structure.yaml`:
	- Header fields like `code_enregistrement='10'`, 18 zeros, `date_production`, `Heure_production`, `num_donneur_ordre`, etc.
	- Detail fields include amounts in centimes in SQL that are transformed to desired output format by rules.
	- Footer fields aggregate: `nombre_total_virements`, `montant_total_virements`.

Run it (Windows PowerShell)
```powershell
python -m venv .venv; .\.venv\Scripts\Activate
pip install -r requirements.txt
python .\scripts\etl_pipeline_db_to_file.py
```
Output
- File name pattern: `<reference_remise>_YYYYMMDD_HHMMSS.asc` under the subfolder for the `type_aux`.
- The script prints whether it used a provided `MontantTotal` or computed it.


## Phase 2 — File ➜ DB (Retour Sort ingestion)

Goal: Read SIMT status files returned by the bank (retour sort), robustly parse them, normalize fields, and insert them into the appropriate SQL Server database and table.

Key pieces
- `scripts/parse_.py`
	- `validate_line(line, header/detail/footer)`: strict checks (e.g., header starts with `10`, 18 zeros, `MAD`, production datetime; detail checks for `04`, `MAD2` amount pattern, combined RIB block containing two RIBs where one starts with `007`, `00` constant, dates `YYYYMMDD`, reference `NNN-NNNNNN`, donor name `mcma|mamda`).
	- `extract_fields(...)` with regex fallback: when a line is invalid or misaligned, critical fields are extracted via regex instead of positional parsing.
		- Header: `date_production`, `num_donneur_ordre`.
		- Detail: `date_emission`, `date_traitement`, `date_execution`, `montant`, `rib_beneficiaire`, `motif_virement`, `nom_beneficiaire`, `reference_virement`.
		- Footer: `nb_valeurs`, `montant_total`, `nb_valeurs_payees`.
	- `parse_file(path, structure)`: reads ASC lines; for each section uses validation, then positional parse if valid, otherwise regex fallback; returns a dict with `header`, `details[]`, `footer`.
- `scripts/transform_fields.py`
	- Applies `config/retour_sort_transformation_rules.yaml` to header/detail/footer fields.
	- Dates are parsed with `'%Y%m%d'` ➜ `YYYY‑MM‑DD`; amounts in centimes ➜ dirhams via `value/100` and formatted to 2 decimals.
- `scripts/insert.py`
	- Chooses the target database based on donor (`num_donneur_ordre`: MCMA 0679812 → MCMA DB; MAMDA 0679814 → MAMDA DB) and sinistre type inferred from `reference_virement` prefix (e.g., `101` ➜ AT, `202` ➜ Auto, `303` ➜ RD, etc.).
	- Uses `config/retour_sort_mapping.yaml` to map parsed fields to destination columns.
	- Inserts a file metadata row (to get `IdFile`) then inserts each detail linked to that file. Commits and closes safely; surfaces errors but leaves the process consistent.
- `scripts/etl_pipeline_file_to_db.py`
	- Scans `DATA_DIR_FEE_STATUS` for files containing the current `MMYY` pattern; for each file: parse ➜ transform ➜ insert.
	- Stops if the external folder is not accessible (no local fallback in prod).

Data contract (file ➜ insert)
- Layout is defined in `config/file_structure/fee_payouts_status_structure.yaml`.
- Required vs optional fields: optional blanks are allowed; required fields are validated. When invalid, regex fallback extracts the critical fields needed to insert.

Run it (Windows PowerShell)
```powershell
python .\scripts\etl_pipeline_file_to_db.py
```
Input expectation
- Place retour‑sort `.asc` files provided by the bank in `DATA_DIR_FEE_STATUS`.
- File names usually contain `MMYY` (e.g., `0825`) so they’re auto‑detected for the current month.


## Phase 3 — Orchestration with Airflow

The DAG `dags/expert_fee_dag.py` schedules a monthly pipeline:
1) Task run_db_to_file: executes Phase 1 script (`scripts/etl_pipeline_db_to_file.py`).
2) Task check_files: inspects `DATA_DIR_FEE_STATUS` for files containing current `MMYY`.
3) Task run_file_to_db: if found, executes Phase 2 script (`scripts/etl_pipeline_file_to_db.py`). Otherwise, a `no_new_files` branch ends the run.

Local deployment
- A `docker-compose.yaml` is provided to run Airflow locally (Webserver, Scheduler, etc.). See that file and adjust env mounts to include this repo and your `.env`.


## Tests

Pytest suites are provided to cover parsing, transformation, extract/load, and insert routing logic.

Run all tests
```powershell
pytest -q
```

Highlights
- `tests/test_parse_.py`, `tests/test_parse.py`: validation rules, regex fallback, and positional parsing.
- `tests/test_load.py`: fixed‑width line generation and footer totals.
- `tests/test_extract.py`: basic extraction path.
- `tests/test_insert.py`: database selection and error handling via mocks; ensures commits/closures even on errors.


## Troubleshooting

- “ODBC Driver not found” on Windows: install “ODBC Driver 17 for SQL Server”.
- Phase 1/2 stops immediately: verify `DATA_DIR_FEE_PAYOUTS` and `DATA_DIR_FEE_STATUS` exist and are accessible.
- Insert errors: run tests for `insert.py` to validate routing; check `num_donneur_ordre` and `reference_virement` prefixes.
- Parsing errors: ensure the bank file matches retour‑sort layout; fallback extraction still populates critical fields.


## YAML files to maintain (cheat‑sheet)

- `db_config.yaml`: 6 DB connections (driver, server, database).
- `table_mapping.yaml`: Phase 1 mapping from SQL columns ➜ SIMT field names.
- `transformation_rules.yaml`: Phase 1 business rules (dates, amounts, padding).
- `fee_payouts_structure.yaml`: Integration SIMT layout (Header/Detail/Footer, fixed widths).
- `fee_payouts_status_structure.yaml`: Retour‑sort layout (Header/Detail/Footer).
- `retour_sort_mapping.yaml`: Phase 2 mapping from parsed retour‑sort fields ➜ DB columns.
- `retour_sort_transformation_rules.yaml`: Phase 2 rules (dates `'%Y%m%d'` ➜ `YYYY‑MM‑DD`, centimes➜dirhams).


## Versions

- Python 3.12/3.13 compatible (repository contains pyc from both). Dependencies are pinned in `requirements.txt`.
- Airflow 2.7.x is included for local orchestration via Docker Compose.


