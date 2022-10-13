from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Defining operators params (with defaults) here
                 postgres_conn_id = '',
                 sql = '',
                 table = '',
                 truncate = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Mapping params here
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql
        self.table = table
        self.truncate = truncate

    def execute(self, context):
        self.log.info('LoadFactOperator is implemented!')
        
        if self.truncate:
            self.log.info(f'Truncate table {self.table}')
            postgres.run(f'TRUNCATE {self.table}')

        self.log.info(f'Fact table {self.table} is loaded.')
        postgres.run(f'INSERT INTO {self.table} {self.sql}')
