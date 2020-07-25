import psycopg2
from etl_config import staging_table_data_config
from db_manager.db_query_executor import QueryExecutor
from db_manager.db_queries.sql_queries import create_table_queries, drop_table_queries, insert_table_queries


class SparkifyQueries(QueryExecutor):

    def __init__(
            self,
            db_endpoint=None,
            role_arn=None,
            staging_table_paths=staging_table_data_config
    ):

        super().__init__(db_host=db_endpoint)

        self.role_arn = role_arn
        self.staging_table_paths = staging_table_paths

    def drop_tables(self):
        for query in drop_table_queries:
            self.execute_query(query)

    def create_tables(self):
        for query in create_table_queries:
            self.execute_query(query)

    def loading_star_schema(self):
        for query in insert_table_queries:
            self.execute_query(query)

    def loading_staging_tables(self):
        self.copy_table_queries(
            self.staging_table_paths, self.role_arn
        )


if __name__ == "__main__":
    sparkify_queries = SparkifyQueries()
    sparkify_queries.drop_tables()
    sparkify_queries.create_tables()
