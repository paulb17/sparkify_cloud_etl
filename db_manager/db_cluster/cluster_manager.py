import boto3
from etl_config import redshift_config
import time
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)


class ClusterManager:
    """
    Class responsible for creating Redshift Clusters
    """
    def __init__(
            self,
            cluster_type=redshift_config['cluster_type'],
            node_type=redshift_config['node_type'],
            num_nodes=redshift_config['num_nodes'],
            db_name=redshift_config['db_name'],
            db_user=redshift_config['db_user'],
            db_password=redshift_config['db_password'],
            cluster_identifier=redshift_config['cluster_identifier'],
            iam_role_name=redshift_config['iam_role_name'],
            db_port=redshift_config['db_port'],
            ipv4_cidr_range=redshift_config['ipv4_cidr_range'],
            ip_protocol=redshift_config['ip_protocol'],
            role_arn=None
    ):

        self.cluster_type = cluster_type
        self.node_type = node_type
        self.num_nodes = num_nodes
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_port = db_port
        self.cluster_identifier = cluster_identifier
        self.iam_role_name = iam_role_name
        self.ipv4_cidr_range=ipv4_cidr_range
        self.role_arn = role_arn
        self.ip_protocol = ip_protocol

        # instantiating clients
        self.redshift = boto3.client('redshift')
        self.ec2 = boto3.resource('ec2')

    def create_cluster(self):
        try:
            self.redshift.create_cluster(
                # HW
                ClusterType=self.cluster_type,
                NodeType=self.node_type,
                NumberOfNodes=self.num_nodes,

                # Identifiers & Credentials
                DBName=self.db_name,
                ClusterIdentifier=self.cluster_identifier,
                MasterUsername=self.db_user,
                MasterUserPassword=self.db_password,

                # Roles (for s3 access)
                IamRoles=[self.role_arn]
            )

        except ClientError as e:
            error_message = str(e)
            if 'AlreadyExists' in error_message:
                pass
            else:
                raise

    def get_available_cluster_properties(self):

        logger.info('Checking for cluster availability')
        cluster_properties = self.get_cluster_properties()

        while cluster_properties['ClusterStatus'] != 'available':

            logger.info("Cluster unavailable, will check again for availability in 30s")
            time.sleep(30)
            cluster_properties = self.get_cluster_properties()

        logger.info('Cluster is available')

        return cluster_properties

    def get_cluster_properties(self):
        return self.redshift.describe_clusters(
            ClusterIdentifier=self.cluster_identifier
        )['Clusters'][0]

    @staticmethod
    def get_cluster_endpoints(cluster_properties):
        return cluster_properties['Endpoint']['Address']

    @staticmethod
    def get_cluster_role_arn(cluster_properties):
        return cluster_properties['IamRoles'][0]['IamRoleArn']

    @staticmethod
    def get_vpc_security_group_id(cluster_properties):
        return cluster_properties['VpcSecurityGroups'][0]['VpcSecurityGroupId']

    def get_security_group_object(self, vpc_id, vpc_security_group_id):

        vpc = self.ec2.Vpc(id=vpc_id)
        security_group = list(
            vpc.security_groups.filter(GroupIds=[vpc_security_group_id])
        )[0]

        return security_group

    def open_port(self, cluster_properties):
        """
        Opens the incoming port to access the cluster endpoint
        """

        vpc_security_group_id = self.get_vpc_security_group_id(cluster_properties)

        cluster_security_group = self.get_security_group_object(
            cluster_properties['VpcId'], vpc_security_group_id
        )

        try:
            cluster_security_group.authorize_ingress(
                GroupName=cluster_security_group.group_name,
                CidrIp=self.ipv4_cidr_range,
                IpProtocol=self.ip_protocol,
                FromPort=self.db_port,
                ToPort=self.db_port
            )
        except ClientError as e:
            error_message = str(e)
            if 'already exists' in error_message:
                pass
            else:
                raise

    def delete_cluster(self):
        self.redshift.delete_cluster(
            ClusterIdentifier=self.cluster_identifier,  SkipFinalClusterSnapshot=True
        )
