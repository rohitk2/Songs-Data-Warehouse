import boto3
from botocore.exceptions import ClientError
import configparser

def delete_iam_role(iam_client, role_name):
    """Delete an IAM role."""
    try:
        print("Detaching Policy from IAM Role")
        iam_client.detach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )
        print("Deleting IAM Role")
        iam_client.delete_role(RoleName=role_name)
        print("IAM role deleted successfully.")
    except ClientError as e:
        print(f"Error deleting IAM role: {e}")
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

    role_name = config.get("DWH", "DWH_IAM_ROLE_NAME")

    # Initialize IAM client
    iam = boto3.client(
        'iam',
        aws_access_key_id=aws_config["KEY"],
        aws_secret_access_key=aws_config["SECRET"],
        region_name='us-west-2'
    )

    # Delete IAM role
    delete_iam_role(iam, role_name)

if __name__ == "__main__":
    main()
