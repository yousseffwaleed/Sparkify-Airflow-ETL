from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    Loads data into a fact table in Redshift using a provided SQL query.

    Fact tables use append-only inserts — existing data is never deleted,
    because fact tables accumulate historical events over time.

    :param redshift_conn_id: Airflow connection ID for Redshift
    :param table: Target fact table name (used for row count logging)
    :param sql: SQL INSERT statement to execute
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="redshift",
        table="",
        sql="",
        *args,
        **kwargs
    ):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        if not self.sql:
            raise ValueError("No SQL statement provided to LoadFactOperator")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Loading fact table: {self.table}")
        redshift.run(self.sql)

        row_count = redshift.get_records(f"SELECT COUNT(*) FROM {self.table}")[0][0]
        self.log.info(f"Fact table load complete: {self.table} now has {row_count} rows")