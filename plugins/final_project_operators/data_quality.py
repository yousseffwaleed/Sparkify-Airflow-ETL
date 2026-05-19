from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Runs configurable data quality checks against Redshift tables.

    Each check is a dict with:
      - 'sql': a SQL query that returns a single numeric value
      - 'expected_result': the value the query should return

    If any check fails (actual != expected), the operator raises a
    ValueError, causing Airflow to retry and eventually fail the task.

    :param redshift_conn_id: Airflow connection ID for Redshift
    :param dq_checks: List of dicts, each with 'sql' and 'expected_result'
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 dq_checks=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks or []

    def execute(self, context):
        if not self.dq_checks:
            raise ValueError("No data quality checks provided")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        failed_checks = []

        self.log.info(f"Running {len(self.dq_checks)} data quality check(s)")

        for i, check in enumerate(self.dq_checks, 1):
            sql = check.get("sql")
            expected_result = check.get("expected_result")

            self.log.info(f"Check {i}/{len(self.dq_checks)}: {sql}")
            records = redshift.get_records(sql)

            if records is None or len(records) < 1:
                failed_checks.append(f"Check {i}: No results returned for: {sql}")
                continue

            actual_result = records[0][0]

            if actual_result != expected_result:
                failed_checks.append(
                    f"Check {i}: FAILED | Expected {expected_result}, got {actual_result} | {sql}"
                )
            else:
                self.log.info(f"Check {i}: PASSED (result = {actual_result})")

        if failed_checks:
            for failure in failed_checks:
                self.log.error(failure)
            raise ValueError(
                f"{len(failed_checks)} data quality check(s) failed:\n"
                + "\n".join(failed_checks)
            )

        self.log.info(f"All {len(self.dq_checks)} data quality checks passed")

        self.log.info("All data quality checks passed successfully")