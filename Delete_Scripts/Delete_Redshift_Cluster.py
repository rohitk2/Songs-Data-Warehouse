import boto3
from botocore.exceptions import ClientError
import configparser

def delete_redshift_cluster(redshift_client, cluster_identifier, skip_snapshot=True):
    """Delete a Redshift cluster."""
    try:
        print("Deleting Redshift Cluster")
        response = redshift_client.delete_cluster(
            ClusterIdentifier=cluster_identifier,
            SkipFinalClusterSnapshot=skip_snapshot
        )
        print("Cluster deletion initiated.")
        return response
    except ClientError as e:
        print(f"Error deleting Redshift cluster: {e}")
        raise

def main():
    # Load configuration
    config = configparser.ConfigParser()
    config.read_file(open('dwh2.cfg'))

    # Extract AWS configuration
    aws_config = {
        "KEY": config.get('AWS', 'KEY'),
        "SECRET": config.get('AWS', 'SECRET')
    }

    cluster_identifier = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")

    # Initialize Redshift client
    redshift = boto3.client(
        'redshift',
        region_name="us-west-2",
        aws_access_key_id=aws_config["KEY"],
        aws_secret_access_key=aws_config["SECRET"]
    )

    # Delete Redshift cluster
    delete_redshift_cluster(redshift, cluster_identifier)

if __name__ == "__main__":
    main()
