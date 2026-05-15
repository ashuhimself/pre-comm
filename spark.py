import socket
from typing import Any

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


class ComputeEngineSubmitOperator(SparkSubmitOperator):
    """
    Subclass of SparkSubmitOperator tailored for version 3.5.1.

    Injects S3 credentials and Spark conf required by the Compute Engine
    runtime. Credentials and endpoint are passed as Jinja-templated strings
    and resolved by Airflow's template engine at execute() time using the
    Vault-backed Airflow Connection (compute_engine_s3).

    spark.driver.host is resolved at execute() time because it depends on
    the worker host and cannot be expressed as a Jinja template.
    """

    def __init__(
        self,
        *args,
        compute_engine_conn_id: str,
        **kwargs,
    ):
        ce_conf = {
            "spark.hadoop.fs.s3a.access.key":
                f"{{{{ conn.{compute_engine_conn_id}.login }}}}",
            "spark.hadoop.fs.s3a.secret.key":
                f"{{{{ conn.{compute_engine_conn_id}.password }}}}",
            "spark.hadoop.fs.s3a.endpoint":
                f"{{{{ conn.{compute_engine_conn_id}.extra_dejson.endpoint }}}}",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
            "spark.sql.files.maxPartitionBytes": "128MB",
        }

        # Caller-provided conf takes precedence over CE defaults.
        caller_conf = kwargs.pop("conf", None) or {}
        merged_conf = {**ce_conf, **caller_conf}

        super().__init__(*args, conf=merged_conf, **kwargs)
        self._compute_engine_conn_id = compute_engine_conn_id

    def execute(self, context: Any) -> Any:
        # spark.driver.host depends on the worker host at runtime and
        # cannot be resolved via Jinja templating. Airflow has already
        # rendered the other templated values in self._conf by this point.
        self._conf = {
            **self._conf,
            "spark.driver.host": socket.gethostbyname(socket.gethostname()),
        }
        return super().execute(context)
