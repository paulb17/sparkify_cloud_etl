from db_manager.db_cluster.cluster_manager import ClusterManager
from db_manager.db_role.role_manager import RoleManager
import logging

logger = logging.getLogger(__name__)


class CloudServiceInitializer:
    """
    Class which sets up the various Amazon Web services infrastructure for the ETL pipeline
    """
    def __init__(self):
        self.role_manager = RoleManager()
        self.cluster_manager = ClusterManager()

        self.cluster_endpoint = None
        self.cluster_role_arn = None

    def set_up_cloud_services(self):
        role_arn = self.set_up_iam_role()
        self.set_up_redshift_cluster(role_arn=role_arn)

    def set_up_iam_role(self):
        logger.info("Setting up IAM role for Redshift Cluster")
        self.role_manager.create_role()
        self.role_manager.attach_role()

        return self.role_manager.get_role()

    def set_up_redshift_cluster(self, role_arn=None):

        logger.info("Setting up Redshift Cluster")
        self.cluster_manager.role_arn = role_arn
        self.cluster_manager.create_cluster()
        cluster_properties = self.cluster_manager.get_available_cluster_properties()

        self.cluster_endpoint = self.cluster_manager.get_cluster_endpoints(
            cluster_properties
        )

        self.cluster_role_arn = self.cluster_manager.get_cluster_role_arn(
            cluster_properties
        )

        logger.info("Opening TCP Port for accessing Redshift Cluster enpoint")
        self.cluster_manager.open_port(cluster_properties)

    def delete_aws_services(self):
        self.cluster_manager.delete_cluster()
        self.role_manager.delete_role()
