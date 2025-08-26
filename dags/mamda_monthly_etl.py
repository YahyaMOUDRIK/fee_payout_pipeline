from __future__ import annotations

import os
import sys
import glob
import logging
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv(dotenv_path=os.path.join(Path(__file__).resolve().parents[1], ".env"))
except Exception:
    pass

# Resolve project root (repo root contains scripts/, config/, utils/)
_here = Path(__file__).resolve()
_repo_root = _here.parent.parent
project_root = os.getenv("PROJECT_ROOT", str(_repo_root))
if project_root not in sys.path:
    sys.path.append(project_root)

logger = logging.getLogger("mamda_monthly_etl")

default_args = {
    'owner': 'mamda_etl',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'mamda_monthly_etl_pipeline',
    default_args=default_args,
    description='MAMDA Monthly ETL Pipeline - DB to File and File to DB',
    schedule='0 2 1 * *',
    catchup=False,
    tags=['mamda', 'etl', 'monthly', 'simt'],
)


# def _compose_env_for_subprocess() -> dict:
#     env = os.environ.copy()
#     env['PYTHONPATH'] = project_root
#     # Prefer container-friendly paths if env points to Windows drives
#     def _safe_dir(var: str, default: str) -> str:
#         val = env.get(var) or ''
#         if not val or ':' in val or not os.path.exists(val):
#             return default
#         return val
#     env['DATA_DIR_FEE_PAYOUTS'] = _safe_dir('DATA_DIR_FEE_PAYOUTS', '/opt/airflow/data/fee_payouts')
#     # Support alternative env var name in .env
#     env['DATA_DIR_FEE_PAYOUTS_STATUS'] = _safe_dir('DATA_DIR_FEE_PAYOUTS_STATUS', _safe_dir('DATA_DIR_FEE_STATUS', '/opt/airflow/data/fee_payouts_status'))
#     # Help containers reach host SQL Server
#     env.setdefault('DB_HOST', 'host.docker.internal')
#     return env
def _compose_env_for_subprocess() -> dict:
    env = os.environ.copy()
    env['PYTHONPATH'] = project_root
    
    # Define the single, correct path for the external data inside the container
    external_payouts_path = '/mnt/mamda_data/fee_payouts'
    external_status_path = '/mnt/mamda_data/fee_payouts_status'

    # Set the environment variables for the subprocesses to use these paths
    env['DATA_DIR_FEE_PAYOUTS'] = external_payouts_path
    env['DATA_DIR_FEE_STATUS'] = external_status_path
    env['DATA_DIR_FEE_PAYOUTS_STATUS'] = external_status_path

    env.setdefault('DB_HOST', 'host.docker.internal')
    logger.info(f"Subprocess ENV: DATA_DIR_FEE_PAYOUTS -> {env['DATA_DIR_FEE_PAYOUTS']}")
    logger.info(f"Subprocess ENV: DATA_DIR_FEE_STATUS -> {env['DATA_DIR_FEE_STATUS']}")
    return env



def _fail_on_error_markers(result: subprocess.CompletedProcess, markers: list[str]):
    out = f"{result.stdout or ''}\n{result.stderr or ''}"
    for m in markers:
        if m and m in out:
            raise RuntimeError(f"Detected error in child script output: {m}")


def run_db_to_file_etl(**context):
    logger.info("=== START: DB -> FILE ETL ===")
    env = _compose_env_for_subprocess()
    script = os.path.join(project_root, 'scripts', 'etl_pipeline_db_to_file.py')
    if not os.path.exists(script):
        raise FileNotFoundError(f"Script not found: {script}")
    result = subprocess.run(
        [sys.executable, script],
        cwd=project_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=1800,
    )
    if result.stdout:
        logger.info("DB->FILE STDOUT:\n" + result.stdout)
    if result.stderr:
        logger.warning("DB->FILE STDERR:\n" + result.stderr)
    _fail_on_error_markers(result, [
        'Error connecting to database',
        'Failed to establish',
        'Traceback (most recent call last):',
    ])
    if result.returncode != 0:
        raise RuntimeError(f"DB->FILE exited with code {result.returncode}")
    logger.info("=== DONE: DB -> FILE ETL ===")


def check_for_new_status_files(**context):
    """
    This function now looks ONLY in the mounted external directory.
    """
    logger.info("=== CHECK: STATUS FILES ===")
    ti = context['ti']
    logical_dt = context.get('logical_date') or datetime.utcnow()
    pattern = logical_dt.strftime('%m%y')

    # Define the single, correct path for the external status files
    status_dir = '/mnt/mamda_data/fee_payouts_status'
    
    logger.info(f"Searching for status files in: {status_dir}")

    if not os.path.isdir(status_dir):
        logger.error(f"CRITICAL: External status directory '{status_dir}' is not mounted or not a directory.")
        # Fail the task immediately if the directory doesn't exist.
        raise FileNotFoundError(f"Directory not found inside container: {status_dir}")

    p = Path(status_dir)
    files = [f for f in p.iterdir() if f.is_file()]
    matches = [f for f in files if pattern in f.name]
    logger.info(f"Found {len(matches)} files matching MMYY={pattern} in {status_dir}")
    
    ti.xcom_push(key='has_status_files', value=bool(matches))
    return bool(matches)
# def check_for_new_status_files(**context):
#     logger.info("=== CHECK: STATUS FILES ===")
#     ti = context['ti']
#     # Use execution_date (when the DAG runs) instead of data_interval_start (previous month)
#     logical_dt = context.get('execution_date') or datetime.utcnow()
#     pattern = logical_dt.strftime('%m%y')
#     # Resolve status directory from env with safe fallbacks
#     status_dir = os.environ.get('DATA_DIR_FEE_PAYOUTS_STATUS') or os.environ.get('DATA_DIR_FEE_STATUS') or '/opt/airflow/data/fee_payouts_status'
#     if ':' in status_dir or not os.path.exists(status_dir):
#         # Try common mount location if host path was provided
#         for cand in ('/mnt/fee_status', '/mnt/fee_payouts_status', '/opt/airflow/data/fee_payouts_status'):
#             if os.path.exists(cand):
#                 status_dir = cand
#                 break
#     p = Path(status_dir)
#     if not p.exists():
#         logger.warning(f"Status directory not found: {status_dir}")
#         ti.xcom_push(key='has_status_files', value=False)
#         return False
#     files = [f for f in p.iterdir() if f.is_file()]
#     matches = [f for f in files if pattern in f.name]
#     logger.info(f"Found {len(matches)} files matching MMYY={pattern} in {status_dir}")
#     ti.xcom_push(key='has_status_files', value=bool(matches))
#     return bool(matches)


def decide_file_to_db_execution(**context):
    has_files = context['ti'].xcom_pull(task_ids='check_status_files', key='has_status_files')
    return 'run_file_to_db_etl' if has_files else 'skip_file_to_db_etl'


def run_file_to_db_etl(**context):
    logger.info("=== START: FILE -> DB ETL ===")
    env = _compose_env_for_subprocess()

    for k in ('user_name', 'username', 'DB_USER', 'password', 'DB_PASSWORD'):
        if k in os.environ:
            env[k] = os.environ[k]
    script = os.path.join(project_root, 'scripts', 'etl_pipeline_file_to_db.py')
    if not os.path.exists(script):
        raise FileNotFoundError(f"Script not found: {script}")
    result = subprocess.run(
        [sys.executable, script],
        cwd=project_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=1800,
    )
    if result.stdout:
        logger.info("FILE->DB STDOUT:\n" + result.stdout)
    if result.stderr:
        logger.warning("FILE->DB STDERR:\n" + result.stderr)
    _fail_on_error_markers(result, [
        'Error connecting to database',
        'Failed to establish',
        'Traceback (most recent call last):',
    ])
    if result.returncode != 0:
        raise RuntimeError(f"FILE->DB exited with code {result.returncode}")
    logger.info("=== DONE: FILE -> DB ETL ===")


def check_environment_and_dependencies(**context):
    logger.info("=== CHECK: ENV & DEPENDENCIES ===")
    problems: list[str] = []
    # Directories
    for var, default in (
    ('DATA_DIR_FEE_PAYOUTS', '/mnt/mamda_data/fee_payouts'),
    ('DATA_DIR_FEE_PAYOUTS_STATUS', '/mnt/mamda_data/fee_payouts_status'),
    ):
        path = os.environ.get(var, default)
        if ':' in path or not os.path.exists(path):
            # not fatal here; just warn, the file check task will decide
            problems.append(f"Check mount for {var} (current: {path})")
    # Scripts
    for rel in ('scripts/etl_pipeline_db_to_file.py', 'scripts/etl_pipeline_file_to_db.py'):
        if not os.path.exists(os.path.join(project_root, rel)):
            problems.append(f"Missing script: {rel} under {project_root}")
    # Modules (best-effort)
    try:
        import pyodbc  # noqa: F401
    except Exception:
        problems.append('pyodbc not available in image')
    try:
        import pandas  # noqa: F401
    except Exception:
        problems.append('pandas not available in image')
    if problems:
        for p in problems:
            logger.warning(p)
    else:
        logger.info('Environment looks OK')


def cleanup_temp_files(**context):
    logger.info("Cleanup finished")


# Tasks
environment_check_task = PythonOperator(
    task_id='check_environment_and_dependencies',
    python_callable=check_environment_and_dependencies,
    dag=dag,
)

check_status_files_task = PythonOperator(
    task_id='check_status_files',
    python_callable=check_for_new_status_files,
    dag=dag,
)

file_check_branch = BranchPythonOperator(
    task_id='decide_file_to_db_execution',
    python_callable=decide_file_to_db_execution,
    dag=dag,
)

db_to_file_task = PythonOperator(
    task_id='run_db_to_file_etl',
    python_callable=run_db_to_file_etl,
    dag=dag,
)

file_to_db_task = PythonOperator(
    task_id='run_file_to_db_etl',
    python_callable=run_file_to_db_etl,
    dag=dag,
)

skip_file_to_db_task = PythonOperator(
    task_id='skip_file_to_db_etl',
    python_callable=lambda **context: logger.info("No new status files found - skipping file-to-db ETL"),
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='cleanup_temp_files',
    python_callable=cleanup_temp_files,
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

# Dependencies
environment_check_task >> [check_status_files_task, db_to_file_task]
check_status_files_task >> file_check_branch
file_check_branch >> [file_to_db_task, skip_file_to_db_task]
[db_to_file_task, file_to_db_task, skip_file_to_db_task] >> cleanup_task
