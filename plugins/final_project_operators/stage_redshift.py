from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Loads JSON-formatted files from S3 into Amazon Redshift staging tables.

    Uses the Redshift COPY command for high-performance bulk loading.
    Clears the target table before each load to ensure idempotent runs.

    :param redshift_conn_id: Airflow connection ID for Redshift
    :param aws_credentials_id: Airflow connection ID for AWS (IAM)
    :param table: Target staging table in Redshift
    :param s3_bucket: Source S3 bucket name
    :param s3_key: S3 key prefix (supports Airflow templating)
    :param json_path: JSONPaths file URI or 'auto' for automatic mapping
    :param region: AWS region where the S3 bucket resides
    """

    ui_color = '#358140'
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="",
        s3_bucket="",
        s3_key="",
        json_path="auto",
        region="us-west-2",
        *args,
        **kwargs
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.region = region

    def execute(self, context):
        self.log.info(f"Starting staging for table: {self.table}")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_hook = S3Hook(aws_conn_id=self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"

        self.log.info(f"Clearing existing data from {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        copy_sql = f"""
            COPY {self.table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            REGION '{self.region}'
            FORMAT AS JSON '{self.json_path}'
        """

        self.log.info(f"Running COPY: {s3_path} → {self.table}")
        redshift.run(copy_sql)

        row_count = redshift.get_records(f"SELECT COUNT(*) FROM {self.table}")[0][0]
        self.log.info(f"Staging complete: {row_count} rows loaded into {self.table}")





