from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Loads data into a dimension table in Redshift.

    Supports two modes via truncate_insert:
      - True  (default): TRUNCATE the table first, then INSERT (full refresh)
      - False: Append-only INSERT (no deletion of existing rows)

    :param redshift_conn_id: Airflow connection ID for Redshift
    :param table: Target dimension table name
    :param sql: SQL INSERT statement to execute
    :param truncate_insert: If True, truncate table before inserting
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="redshift",
        table="",
        sql="",
        truncate_insert=True,
        *args,
        **kwargs
    ):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.truncate_insert = truncate_insert

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_insert:
            self.log.info(f"Truncating dimension table: {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table}")

        self.log.info(f"Loading dimension table: {self.table}")
        redshift.run(self.sql)

        row_count = redshift.get_records(f"SELECT COUNT(*) FROM {self.table}")[0][0]
        self.log.info(f"Dimension load complete: {self.table} now has {row_count} rows")