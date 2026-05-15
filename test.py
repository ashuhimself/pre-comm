from unittest.mock import MagicMock, patch

import pytest

from dags.DOSLibrary.common.operators.compute_engine import ComputeEngineSubmitOperator

MODULE = "dags.DOSLibrary.common.operators.compute_engine"


# ---------- Helpers ----------

def _make_operator(**kwargs):
    """Instantiate ComputeEngineSubmitOperator with minimal required args."""
    defaults = {
        "task_id": "test_spark_task",
        "application": "/dags/scripts/test.py",
        "compute_engine_conn_id": "compute_engine_s3",
        "conn_id": "dce_spark_conn",
    }
    defaults.update(kwargs)
    return ComputeEngineSubmitOperator(**defaults)


# ---------- Init tests ----------

class TestComputeEngineSubmitOperatorInit:
    """All conf injection now happens in __init__, so most assertions live here."""

    def test_stores_compute_engine_conn_id(self):
        op = _make_operator(compute_engine_conn_id="my_conn")
        assert op._compute_engine_conn_id == "my_conn"

    def test_jinja_templates_use_conn_id(self):
        """conn_id must be embedded in the Jinja template strings."""
        op = _make_operator(compute_engine_conn_id="my_conn")
        assert op._conf["spark.hadoop.fs.s3a.access.key"] == "{{ conn.my_conn.login }}"
        assert op._conf["spark.hadoop.fs.s3a.secret.key"] == "{{ conn.my_conn.password }}"
        assert op._conf["spark.hadoop.fs.s3a.endpoint"] == "{{ conn.my_conn.extra_dejson.endpoint }}"

    def test_jinja_templates_update_when_conn_id_changes(self):
        op = _make_operator(compute_engine_conn_id="other_conn")
        assert op._conf["spark.hadoop.fs.s3a.access.key"] == "{{ conn.other_conn.login }}"
        assert op._conf["spark.hadoop.fs.s3a.secret.key"] == "{{ conn.other_conn.password }}"
        assert op._conf["spark.hadoop.fs.s3a.endpoint"] == "{{ conn.other_conn.extra_dejson.endpoint }}"

    def test_static_conf_values_injected(self):
        op = _make_operator()
        assert op._conf["spark.hadoop.fs.s3a.path.style.access"] == "true"
        assert op._conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "true"
        assert op._conf["spark.sql.files.maxPartitionBytes"] == "128MB"

    def test_caller_conf_takes_precedence_over_ce_defaults(self):
        caller_conf = {
            "spark.hadoop.fs.s3a.path.style.access": "false",
            "spark.some.custom.key": "custom_value",
        }
        op = _make_operator(conf=caller_conf)
        assert op._conf["spark.hadoop.fs.s3a.path.style.access"] == "false"
        assert op._conf["spark.some.custom.key"] == "custom_value"
        assert op._conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "true"

    def test_empty_caller_conf_leaves_ce_defaults_intact(self):
        op = _make_operator(conf={})
        assert op._conf["spark.hadoop.fs.s3a.path.style.access"] == "true"
        assert op._conf["spark.hadoop.fs.s3a.access.key"] == "{{ conn.compute_engine_s3.login }}"

    def test_no_caller_conf_uses_only_ce_defaults(self):
        op = _make_operator()
        for key in [
            "spark.hadoop.fs.s3a.access.key",
            "spark.hadoop.fs.s3a.secret.key",
            "spark.hadoop.fs.s3a.endpoint",
            "spark.hadoop.fs.s3a.path.style.access",
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "spark.sql.files.maxPartitionBytes",
        ]:
            assert key in op._conf

    def test_conf_field_is_templated(self):
        """Sanity check: _conf must be in template_fields so Airflow renders it."""
        assert "_conf" in ComputeEngineSubmitOperator.template_fields


# ---------- Execute tests ----------

class TestComputeEngineSubmitOperatorExecute:
    """execute() now only injects spark.driver.host; everything else is rendered by Airflow."""

    @pytest.fixture
    def mock_execute_env(self, request):
        patches = [
            patch(
                "airflow.providers.apache.spark.operators."
                "spark_submit.SparkSubmitOperator.execute"
            ),
            patch(f"{MODULE}.socket.gethostbyname"),
            patch(f"{MODULE}.socket.gethostname"),
        ]
        mocks = [p.start() for p in patches]
        for p in patches:
            request.addfinalizer(p.stop)

        env = MagicMock()
        env.super_execute = mocks[0]
        env.gethostbyname = mocks[1]
        env.gethostname = mocks[2]
        env.gethostname.return_value = "test-host"
        env.gethostbyname.return_value = "10.0.0.5"
        return env

    def test_driver_host_injected_at_execute(self, mock_execute_env):
        op = _make_operator()
        op.execute({})
        assert op._conf["spark.driver.host"] == "10.0.0.5"

    def test_gethostbyname_called_with_gethostname_result(self, mock_execute_env):
        op = _make_operator()
        op.execute({})
        mock_execute_env.gethostbyname.assert_called_once_with("test-host")

    def test_super_execute_called_with_context(self, mock_execute_env):
        op = _make_operator()
        context = {"key": "value"}
        op.execute(context)
        mock_execute_env.super_execute.assert_called_once_with(context)

    def test_static_conf_values_preserved_after_execute(self, mock_execute_env):
        op = _make_operator()
        op.execute({})
        assert op._conf["spark.hadoop.fs.s3a.path.style.access"] == "true"
        assert op._conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "true"
        assert op._conf["spark.sql.files.maxPartitionBytes"] == "128MB"

    def test_caller_conf_preserved_after_execute(self, mock_execute_env):
        caller_conf = {"spark.some.custom.key": "custom_value"}
        op = _make_operator(conf=caller_conf)
        op.execute({})
        assert op._conf["spark.some.custom.key"] == "custom_value"
