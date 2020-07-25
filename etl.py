from db_manager.aws_infrastructure import CloudServiceInitializer
from create_tables import SparkifyQueries
import argparse
import etl_config
import logging

logger = logging.getLogger(__name__)


class SparkifyETL:
    """
    Class for Initializing AWS services, Creating tables on Redshift and Running the ETL
    """
    def __init__(self):

        # setting up IAM roles and redshift cluster
        self.cloud_service = CloudServiceInitializer()
        self.cloud_service.set_up_cloud_services()

        # instantiating class for querying
        self.sparkify_queries = SparkifyQueries(
            db_endpoint=self.cloud_service.cluster_endpoint,
            role_arn=self.cloud_service.cluster_role_arn
        )

    def run_etl(self):
        logger.info("Dropping and re-creating existing tables")
        self.sparkify_queries.drop_tables()
        self.sparkify_queries.create_tables()

        self.sparkify_queries.loading_staging_tables()

        logger.info("Inserting data into star schema")
        self.sparkify_queries.loading_star_schema()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-d',  '--delete_cluster', help='delete AWS Redshift cluster', action='store_true'
    )

    args = parser.parse_args()

    sparkify_etl = SparkifyETL()

    if args.delete_cluster:
        sparkify_etl.cloud_service.delete_aws_services()
    else:
        sparkify_etl.run_etl()

