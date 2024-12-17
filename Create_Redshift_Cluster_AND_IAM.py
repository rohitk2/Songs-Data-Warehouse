import pandas as pd
import boto3
import json
import configparser
from botocore.exceptions import ClientError


def load_config(config_file):
    """Load configuration from a file."""
    config = configparser.ConfigParser()
    config.read_file(open(config_file))
    return config


def create_iam_role(iam_client, role_name):
    """Create an IAM role for Redshift."""
    try:
        print("1.1 Creating a new IAM Role")
        dwh_role = iam_client.create_role(
            Path='/',
            RoleName=role_name,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps({
                'Statement': [{
                    'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}
                }],
                'Version': '2012-10-17'
            })
        )
        print("1.2 Attaching Policy")
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )
        print("1.3 Getting the IAM role ARN")
        role_arn = iam_client.get_role(RoleName=role_name)['Role']['Arn']
        print(role_arn)
        return role_arn
    except ClientError as e:
        print(f"Error creating IAM role: {e}")
        raise


def create_redshift_cluster(redshift_client, cluster_config, role_arn):
    """Create a Redshift cluster."""
    try:
        print("Creating Redshift Cluster")
        response = redshift_client.create_cluster(
            ClusterType=cluster_config["DWH_CLUSTER_TYPE"],
            NodeType=cluster_config["DWH_NODE_TYPE"],
            NumberOfNodes=int(cluster_config["DWH_NUM_NODES"]),
            DBName=cluster_config["DWH_DB"],
            ClusterIdentifier=cluster_config["DWH_CLUSTER_IDENTIFIER"],
            MasterUsername=cluster_config["DWH_DB_USER"],
            MasterUserPassword=cluster_config["DWH_DB_PASSWORD"],
            IamRoles=[role_arn]
        )
        return response
    except ClientError as e:
        print(f"Error creating Redshift cluster: {e}")
        raise


def describe_cluster(redshift_client, cluster_identifier):
    """Describe the Redshift cluster and return its properties."""
    try:
        cluster_props = redshift_client.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
        return cluster_props
    except ClientError as e:
        print(f"Error describing cluster: {e}")
        raise


def pretty_redshift_props(props):
    """Format Redshift cluster properties into a DataFrame."""
    keys_to_show = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername",
                    "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    data = [(k, v) for k, v in props.items() if k in keys_to_show]
    return pd.DataFrame(data=data, columns=["Key", "Value"])


def main():
    # Load configuration
    config = load_config('dwh2.cfg')

    # Extract AWS and cluster configuration
    aws_config = {
        "KEY": config.get('AWS', 'KEY'),
        "SECRET": config.get('AWS', 'SECRET')
    }

    cluster_config = {
        "DWH_CLUSTER_TYPE": config.get("DWH", "DWH_CLUSTER_TYPE"),
        "DWH_NUM_NODES": config.get("DWH", "DWH_NUM_NODES"),
        "DWH_NODE_TYPE": config.get("DWH", "DWH_NODE_TYPE"),
        "DWH_CLUSTER_IDENTIFIER": config.get("DWH", "DWH_CLUSTER_IDENTIFIER"),
        "DWH_DB": config.get("DWH", "DWH_DB"),
        "DWH_DB_USER": config.get("DWH", "DWH_DB_USER"),
        "DWH_DB_PASSWORD": config.get("DWH", "DWH_DB_PASSWORD"),
        "DWH_PORT": config.get("DWH", "DWH_PORT"),
        "DWH_IAM_ROLE_NAME": config.get("DWH", "DWH_IAM_ROLE_NAME")
    }

    # Initialize AWS clients
    redshift = boto3.client(
        'redshift',
        region_name="us-west-2",
        aws_access_key_id=aws_config["KEY"],
        aws_secret_access_key=aws_config["SECRET"]
    )
    iam = boto3.client(
        'iam',
        aws_access_key_id=aws_config["KEY"],
        aws_secret_access_key=aws_config["SECRET"],
        region_name='us-west-2'
    )

    # Create IAM role and get ARN
    role_arn = create_iam_role(iam, cluster_config["DWH_IAM_ROLE_NAME"])

    # Create Redshift cluster
    create_redshift_cluster(redshift, cluster_config, role_arn)

    # Describe Redshift cluster
    cluster_props = describe_cluster(redshift, cluster_config["DWH_CLUSTER_IDENTIFIER"])
    print(pretty_redshift_props(cluster_props))

    print(
        redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['Endpoint']['Address']
    )

if __name__ == "__main__":
    main()
