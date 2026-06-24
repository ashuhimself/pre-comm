import os

import yaml
from airflow.decorators import dag, task_group
from pendulum import datetime

from dags.ingest.loaniq.poc.scripts.donalnd_duck import MockDuckDbOperator

# new
HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(HERE, "params", "ingest_loaniq.yaml")
SQL_DIR = os.path.join(HERE, "sql")
# read config ONCE, hand the WHOLE thing to Jinja (no per-file loop)
with open(CONFIG_PATH) as fh:
    CFG = yaml.safe_load(fh)

# DAG


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["loaniq", "duckdb", "poc"],
    max_active_tasks=8,
    user_defined_macros={"CFG": CFG},  # <- ENTIRE config passed at once
)
def loaniq_poc():

    @task_group(group_id="process")
    def process(file_cfg):  # file_cfg unused; we index by map_index
        ingest = MockDuckDbOperator(
            task_id="ingest",
            query=os.path.join(SQL_DIR, CFG["common"]["query_ingest"]),
            parameters={
                # per-file -> templated, resolved per map_index at runtime
                "source_path": "{{ CFG.common.prefix_landing }}/{{ CFG.files[ti.map_index].dest_ingest }}/{{ CFG.files[ti.map_index].name_prefix }}",
                "dest_path": "{{ CFG.files[ti.map_index].dest_ingest }}/{{ CFG.files[ti.map_index].name_prefix }}.parquet",
                # common -> read once at parse, keeps real bool/str types
                "header": CFG["common"]["header"],
                "delimiter": CFG["common"]["delimiter"],
                "all_varchar": CFG["common"]["all_varchar"],
                "force_fail": "{{ CFG.files[ti.map_index].force_fail | default(False) }}",
            },
        )
        standardise = MockDuckDbOperator(
            task_id="standardise",
            query=os.path.join(SQL_DIR, CFG["common"]["query_standardise"]),
            parameters={
                "source_path": "{{ CFG.files[ti.map_index].dest_ingest }}/{{ CFG.files[ti.map_index].name_prefix }}.parquet",
                "dest_path": "{{ CFG.files[ti.map_index].dest_standardise }}/{{ CFG.files[ti.map_index].name_prefix }}.parquet",
                # Pass common parameters so standardise task has values for $header, $delimiter, etc.
                "header": CFG["common"]["header"],
                "delimiter": CFG["common"]["delimiter"],
                "all_varchar": CFG["common"]["all_varchar"],
                "ignore_errors": True,  # Assume true because standardise query requires it based on sql file, but adjust if needed
            },
        )
        ingest >> standardise  # per-file 1 -> 1 chain

    # map the GROUP over the whole files list - config passed directly, no loop
    process.expand(file_cfg=CFG["files"])


loaniq_poc()
