from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, redshift_conn_id='', checks=[], *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.checks = checks
            
    def execute(self, context):
        redshift_hook = PostgresHook(self.conn_id)
        for check in self.checks:
            result = int(redshift_hook.get_first(sql=check['sql'])[0])
            # check if equal
            if check['op'] == 'eq':
                if result != check['val']:
                    raise AssertionError(f"Check failed: {result} {check['op']} {check['val']}")
            # check if not equal
            elif check['op'] == 'ne':
                if result == check['val']:
                    raise AssertionError(f"Check failed: {result} {check['op']} {check['val']}")
            # check if greater than
            elif check['op'] == 'gt':
                if result <= check['val']:
                    raise AssertionError(f"Check failed: {result} {check['op']} {check['val']}")
            self.log.info(f"Passed check: {result} {check['op']} {check['val']}")
