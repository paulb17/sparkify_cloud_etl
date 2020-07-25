import psycopg2
from etl_config import redshift_config
from db_manager.db_queries.sql_queries import *
import logging

logger = logging.getLogger(__name__)


class QueryExecutor:
    """
    Class for connecting to redshift and executing queries
    """
    def __init__(
            self,
            db_host=None,
            db_name=redshift_config['db_name'],
            db_user=redshift_config['db_user'],
            db_password=redshift_config['db_password'],
            db_port=redshift_config['db_port']
    ):

        conn_string = "postgresql://{}:{}@{}:{}/{}".format(
            db_user, db_password, db_host, db_port, db_name
        )

        self.conn = psycopg2.connect(conn_string)
        self.cur = self.conn.cursor()

    def execute_query(self, query):
        try:
            self.cur.execute(query)
            self.conn.commit()
        except Exception as e:
            logger.info(f"{e}", exc_info=True)
            self.conn.commit()

    def copy_table_queries(self, staging_tables, role_arn):
        for staging_table, table_config in staging_tables.items():

            logger.info(f"Copying data from S3 into {staging_table}")
            query = (
                copy_table_queries.format(
                    staging_table,
                    table_config['path'],
                    role_arn,
                    table_config['json_path_file'],
                    table_config['region']
                )
            )

            self.execute_query(query)
