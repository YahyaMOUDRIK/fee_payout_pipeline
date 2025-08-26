# MAMDA | Fee Payouts ETL (SIMT) – End‑to‑End Guide

This repository contains a production‑ready ETL that automates SIMT “virements de masse” exchanges with the bank:
- Phase 1 (DB ➜ File): generate integration files (SIMT fixed‑width) from SQL Server data.
- Phase 2 (File ➜ DB): ingest “retour sort” status files from the bank back into the right SQL databases.
- Phase 3 (Orchestration): chain both flows monthly using an Airflow DAG.

The project is fully YAML‑driven (layout, transformation rules, DB/field mapping) and includes robust parsing with validation and regex fallback, environment‑based I/O directories, and unit tests.


## Project structure (essentials)

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


## Environment variables

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

What it does
- Automates Phase 1 (DB ➜ File) and Phase 2 (File ➜ DB) on a monthly schedule using an Airflow DAG.
- Current DAG: `dags/mamda_monthly_etl.py` (schedule: `0 2 1 * *`, no catchup).
- Tasks:
  - `run_db_to_file_etl`: runs `scripts/etl_pipeline_db_to_file.py` to generate SIMT files into `DATA_DIR_FEE_PAYOUTS` (subfolders by type: Avocats, Experts Automobiles, Medecins, Experts Comptables).
  - `check_status_files`: scans `DATA_DIR_FEE_STATUS` for files containing current `MMYY`.
  - `decide_file_to_db_execution`: branches based on presence of status files.
  - `run_file_to_db_etl`: runs `scripts/etl_pipeline_file_to_db.py` to parse/transform/insert retour‑sort files.
  - `cleanup_temp_files`: final housekeeping/logging.

How it works
- The DAG invokes the scripts as subprocesses and passes directories via environment variables:
  - `DATA_DIR_FEE_PAYOUTS` → external outbound folder (bank integration files).
  - `DATA_DIR_FEE_STATUS`  → external inbound folder (retour‑sort from bank).
- `docker-compose.yaml` bind‑mounts your Windows folders into the containers so the DAG’s processes can read/write them.
- Logs are written under `logs/` and visible in the Airflow UI per task.

Run Phase 3 (Windows)
1) Prerequisites
	- Install Docker Desktop (with WSL2 backend).
	- Ensure the Windows drive containing your external folders is shared in Docker Desktop (Settings → Resources → File Sharing).
	- Install Git.
2) Clone and configure
	- Clone this repo.
	- Create `.env` at the repo root with at least:
	  - `DATA_DIR_FEE_PAYOUTS=C:/MAMDA/data/fee_payouts`
	  - `DATA_DIR_FEE_STATUS=C:/MAMDA/data/fee_payouts_status`
	  - `user_name=<sql_user>`
	  - `password=<sql_password>`
	- Verify these Windows folders exist; optionally pre‑create type subfolders under `fee_payouts/` (Avocats, Experts Automobiles, Medecins, Experts Comptables) to avoid permission issues on first write.
3) Start Airflow
	- From the repo root, bring up the stack: `docker compose up -d`.
	- Wait ~60–90s for services (webserver, scheduler, worker) to become healthy.
4) Verify mounts inside the worker (optional)
	- `docker compose exec airflow-worker sh -lc "ls -la /mnt/mamda_data && env | grep DATA_DIR_FEE"`
	- You should see `fee_payouts/` and `fee_payouts_status/` and the environment variables set.
5) Run the pipeline
	- Open Airflow UI at http://localhost:8080.
	- Unpause/enable `mamda_monthly_etl_pipeline`.
	- Trigger a run (Play button). Watch task logs to confirm which directories are used.
6) Validate outputs
	- Generated integration files should appear under `C:\MAMDA\data\fee_payouts\<TypeAux>/`.
	- Retour‑sort ingestion reads from `C:\MAMDA\data\fee_payouts_status\` and inserts into SQL Server per mapping.

Notes & tips
- If you see “permission denied” when writing subfolders under `fee_payouts`, pre‑create those subfolders on Windows or adjust the container user in `docker-compose.yaml`.
- If tasks complain about missing Python modules (e.g., `pyodbc`), ensure the container image installs dependencies from `requirements.txt` or via `_PIP_ADDITIONAL_REQUIREMENTS` in `docker-compose.yaml`.
- If the DAG code doesn’t look updated in the UI, restart webserver/scheduler or `docker compose restart` to force a reload.
- SQL Server named instance tip: ensure SQL Browser service is running on Windows and UDP 1434 is allowed in the firewall so containers can resolve `YAHYA\\SQLEXPRESS` via the ODBC driver. The compose file already maps `host.docker.internal` and `YAHYA` to the host.

### Dockerfile and image
- This project uses `docker-compose.yaml` with `build: .` so the image is built from the repository `Dockerfile`.
- When to rebuild the image:
	- After changing the `Dockerfile` (e.g., adding OS packages like `msodbcsql17`).
	- After adding/removing Python packages used by the ETL (e.g., `python-docx`, `pyodbc`).
- How to rebuild and restart (Windows PowerShell):

```powershell
docker compose build --no-cache ; docker compose up -d --force-recreate
```

- Optional verification inside the worker:

```powershell
docker compose exec airflow-worker sh -lc "python -c 'import pyodbc, pandas; print(\"deps ok\")' && odbcinst -q -d | sed -n '/ODBC Driver 17/p'"
```

- About requirements.txt:
	- The current `Dockerfile` installs the needed Python packages inline. If you prefer managing dependencies via `requirements.txt`, you can modify the `Dockerfile` to `COPY requirements.txt` and `pip install -r requirements.txt`. Remember to rebuild the image afterward.


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


