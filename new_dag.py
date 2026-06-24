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

        # ===================== INGEST stage (multi-task) =====================
        with TaskGroup(group_id="ingest") as ingest:
            start = EmptyOperator(task_id="start")
            copy_to_ingest = MockDuckDbOperator(
                task_id="copy_to_ingest",
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
            archive = EmptyOperator(task_id="archive")
            start >> copy_to_ingest >> archive

        # =============== optional standalone task(s) BETWEEN stages ===============
        bridge = EmptyOperator(task_id="post_ingest_check")

        # =================== STANDARDISE stage (multi-task) ===================
        with TaskGroup(group_id="standardise") as standardise:
            schema_validation = EmptyOperator(task_id="schema_validation")
            copy_to_standardise = MockDuckDbOperator(
                task_id="copy_to_standardise",
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
            dq_check = EmptyOperator(task_id="dq_check")
            schema_validation >> copy_to_standardise >> dq_check

        # group -> task -> group, all per-file
        ingest >> bridge >> standardise

    # map the GROUP over the whole files list — config passed directly, no loop
    process.expand(file_cfg=CFG["files"])


loaniq_poc()
