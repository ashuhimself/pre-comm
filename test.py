from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.hooks.base import log
from airflow.models import Connection, Variable


class DOSMongoHook(MongoHook):
    """
    Extension of MongoHook that supports mTLS authentication.

    Expects an Airflow Variable named: {conn_id}_tls_key
    e.g. for conn_id='mongodb_open_banking', set Variable key:
         'mongodb_open_banking_tls_key' with the mTLS certificate content.
    """

    def __init__(self, conn_id: str):
        super().__init__(conn_id=conn_id)
        self.mtls_var_id = f"{conn_id}_tls_key"
        self.mtls_path = f"/tmp/{conn_id}.pem"

    @classmethod
    def get_connection(cls, conn_id: str):
        conn = Connection.get_connection_from_secrets(conn_id)
        log.info("Retrieved connection '%s'", conn.conn_id)

        # Variable naming convention: {conn_id}_tls_key
        # e.g. conn_id='mongodb_open_banking' → Variable 'mongodb_open_banking_tls_key'
        mtls_var_id = f"{conn_id}_tls_key"
        mtls_path = f"/tmp/{conn_id}.pem"

        try:
            mtls_key = Variable.get_variable_from_secrets(mtls_var_id)
        except KeyError:
            raise KeyError(
                f"mTLS variable '{mtls_var_id}' not found. "
                f"Please create an Airflow Variable named '{mtls_var_id}' "
                f"with the mTLS certificate content."
            )

        log.info("Retrieved mTLS variable '%s'", mtls_var_id)
        with open(mtls_path, "w") as f:
            f.write(mtls_key)
        log.info("Wrote mTLS key to '%s'", mtls_path)
        conn.extra_dejson["tlsCertificateKeyFile"] = mtls_path

        return conn