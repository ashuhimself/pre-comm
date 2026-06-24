import json

from airflow.models import BaseOperator


class MockDuckDbOperator(BaseOperator):
    template_fields = ("env_vars",)  # <- makes the {{ }} render
    template_fields_renderers = {"env_vars": "py"}

    def __init__(self, *, query, parameters=None, service_account_name="", **kwargs):
        # mirrors your real op: parameters -> QUERY_PARAMS eagerly in __init__
        self.sql_path = query
        self.env_vars = {"QUERY_PARAMS": json.dumps(parameters or {})}
        super().__init__(**kwargs)

    def execute(self, context):
        with open(self.sql_path) as fh:
            sql = fh.read()
        params = json.loads(self.env_vars["QUERY_PARAMS"])  # already rendered
        final_sql = self._bind(sql, params)
        if str(params.get("force_fail", "")).lower() == "true":
            raise RuntimeError(
                f"Intentionally failing task {self.task_id} as requested by config."
            )
        self.log.info("=" * 60)
        self.log.info(
            "TASK        : %s  (map_index=%s)",
            self.task_id,
            context["ti"].map_index,
        )
        self.log.info("QUERY_PARAMS    :\n%s", json.dumps(params, indent=2))
        self.log.info("RENDERED SQL    :\n%s", final_sql)
        self.log.info("=" * 60)
        return final_sql

    @staticmethod
    def _bind(sql, params):
        def fmt(v):
            if isinstance(v, bool):
                return "true" if v else "false"
            if isinstance(v, (int, float)):
                return str(v)
            return "'" + str(v).replace("'", "''") + "'"

        for k in sorted(params, key=len, reverse=True):  # longest key first
            sql = sql.replace(f"${k}", fmt(params[k]))
        return sql
