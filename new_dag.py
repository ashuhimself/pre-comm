import os
import yaml
from airflow.decorators import dag, task_group
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from pendulum import datetime

from dags.ingest.loaniq.poc.scripts.donalnd_duck import MockDuckDbOperator

HERE        = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(HERE, "params", "ingest_loaniq.yaml")
SQL_DIR     = os.path.join(HERE, "sql")

# read config ONCE, hand the WHOLE thing to Jinja (no per-file loop)
with open(CONFIG_PATH) as fh:
    CFG = yaml.safe_load(fh)

INGEST_SQL = os.path.join(SQL_DIR, CFG["common"]["query_ingest"])
STD_SQL    = os.path.join(SQL_DIR, CFG["common"]["query_standardise"])


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["loaniq", "duckdb", "poc"],
    max_active_tasks=8,
    user_defined_macros={"CFG": CFG},          # entire config passed at once
)
def loaniq_poc():

    @task_group(group_id="process")            # per-file mapped boundary -> 1:1 isolation
    def process(file_cfg):

        # ============== Task-Ingest (KPO anz-duckdb) ==============
        with TaskGroup(group_id="ingest") as ingest:
            data_acquisition = MockDuckDbOperator(
                task_id="data_acquisition",                       # -> S3 Ingest
                query=INGEST_SQL,
                parameters={
                    "source_path": "{{ CFG.common.prefix_landing }}/{{ CFG.files[ti.map_index].name_prefix }}*",
                    "dest_path":   "{{ CFG.files[ti.map_index].dest_ingest }}/{{ CFG.files[ti.map_index].name_prefix }}.parquet",
                    "header":      CFG["common"]["header"],
                    "delimiter":   CFG["common"]["delimiter"],
                    "all_varchar": CFG["common"]["all_varchar"],
                    "force_fail":  "{{ CFG.files[ti.map_index].force_fail | default(False) }}",
                },
            )
            data_archive = EmptyOperator(task_id="data_archive")  # -> S3 Archive
            data_acquisition >> data_archive

        # ============== Task-Standardize (KPO anz-duckdb) ==============
        with TaskGroup(group_id="standardise") as standardise:
            source_data_extract_sql = EmptyOperator(task_id="source_data_extract_sql")
            start_quarantine_step   = EmptyOperator(task_id="start_quarantine_step")
            standardize_data = MockDuckDbOperator(
                task_id="standardize_data",
                query=STD_SQL,
                parameters={
                    "source_path":   "{{ CFG.files[ti.map_index].dest_ingest }}/{{ CFG.files[ti.map_index].name_prefix }}.parquet",
                    "dest_path":     "{{ CFG.files[ti.map_index].dest_standardise }}/{{ CFG.files[ti.map_index].name_prefix }}.parquet",
                    "header":        CFG["common"]["header"],
                    "delimiter":     CFG["common"]["delimiter"],
                    "all_varchar":   CFG["common"]["all_varchar"],
                    "ignore_errors": True,
                },
            )
            convert_to_parquet = EmptyOperator(task_id="convert_to_parquet")  # -> S3 transform
            source_data_extract_sql >> start_quarantine_step >> standardize_data >> convert_to_parquet

        # per-file: entire ingest group -> entire standardise group
        ingest >> standardise

    # map the GROUP over the whole files list — config passed directly, no loop
    process.expand(file_cfg=CFG["files"])


loaniq_poc()
