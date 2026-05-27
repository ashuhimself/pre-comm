from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.hooks.base import log
from airflow.models import Connection, Variable


class DOSMongoHook(MongoHook):
    
    def __init__(
        self,
        conn_id: str,
        mtls_var_id: str | None = None,
        mtls_path: str | None = None,
    ):
        super().__init__(conn_id=conn_id)
        self.mtls_var_id = mtls_var_id
        self.mtls_path = mtls_path or f"/tmp/{conn_id}_mtls.pem"

    @classmethod
    def get_connection(cls, conn_id: str):
        """
        Overrides get_connection to inject mTLS cert.
        Falls back gracefully if no mTLS var is configured.
        """
        conn = Connection.get_connection_from_secrets(conn_id)
        log.info("Retrieved connection '%s'", conn.conn_id)

        # Derive var_id from conn_id if not explicitly set
        mtls_var_id = f"{conn_id}_tls_key"
        mtls_path = f"/tmp/{conn_id}.pem"

        try:
            mtls_key = Variable.get_variable_from_secrets(mtls_var_id)
            log.info("Retrieved mTLS variable '%s'", mtls_var_id)
            with open(mtls_path, "w") as f:
                f.write(mtls_key)
            log.info("Wrote mTLS key to '%s'", mtls_path)
            conn.extra_dejson["tlsCertificateKeyFile"] = mtls_path
        except KeyError:
            log.info("No mTLS variable found for '%s', skipping", mtls_var_id)

        return conn