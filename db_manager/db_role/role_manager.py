import boto3
from etl_config import iam_config
from botocore.exceptions import ClientError


class RoleManager:
    """
    Class responsible for creating IAM roles
    """
    def __init__(
            self,
            role_path=iam_config['role_path'],
            role_name=iam_config['role_name'],
            role_description=iam_config['role_description'],
            policy_document=iam_config['policy_document'],
            policy_arns=iam_config['policyARNs']
    ):

        self.role_path = role_path
        self.role_name = role_name
        self.role_description = role_description
        self.policy_document = policy_document
        self.policy_arns = policy_arns
        self.role = None

        # instantiating client
        self.iam = boto3.client('iam')

    def create_role(self):
        try:
            self.role = self.iam.create_role(
                Path=self.role_path,
                RoleName=self.role_name,
                Description=self.role_description,
                AssumeRolePolicyDocument=self.policy_document
            )
        except ClientError as e:
            error_message = str(e)
            if 'AlreadyExists' in error_message:
                pass
            else:
                raise

    def attach_role(self):

        for arn_policy in self.policy_arns:
            self.iam.attach_role_policy(
                RoleName=self.role_name,
                PolicyArn=arn_policy
            )

    def get_role(self):
        return self.iam.get_role(RoleName=self.role_name)['Role']['Arn']

    def delete_role(self):
        for arn_policy in self.policy_arns:
            self.iam.detach_role_policy(
                RoleName=self.role_name,
                PolicyArn=arn_policy
            )

            self.iam.delete_role(RoleName=self.role_name)



