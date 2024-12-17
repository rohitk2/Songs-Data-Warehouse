import boto3
import configparser
from botocore.exceptions import ClientError


def load_config(config_file):
    """Load configuration from a file."""
    config = configparser.ConfigParser()
    config.read_file(open(config_file))
    return config


def delete_objects_from_s3_bucket(s3_client, bucket_name):
    """Delete all objects from an S3 bucket."""
    try:
        print(f"Deleting all objects from the bucket: {bucket_name}")
        objects = s3_client.list_objects_v2(Bucket=bucket_name)
        if "Contents" in objects:
            for obj in objects["Contents"]:
                print(f"Deleting object: {obj['Key']}")
                s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
            print("All objects deleted successfully.")
        else:
            print("No objects to delete.")
    except ClientError as e:
        print(f"Error deleting objects from bucket: {e}")
        raise


def delete_s3_bucket(s3_client, bucket_name):
    """Delete an S3 bucket."""
    try:
        print(f"Deleting S3 bucket: {bucket_name}")
        s3_client.delete_bucket(Bucket=bucket_name)
        print(f"S3 bucket '{bucket_name}' deleted successfully.")
    except ClientError as e:
        print(f"Error deleting S3 bucket: {e}")
        raise


def main():
    # Load configuration
    config = load_config('dwh2.cfg')
    
    # Extract AWS credentials
    aws_config = {
        "KEY": config.get('AWS', 'KEY'),
        "SECRET": config.get('AWS', 'SECRET'),
        "REGION": "us-west-2"
    }
    
    # Define S3 bucket name
    S3_BUCKET_NAME = "udacity-data-engineering-rohit1998"

    # Initialize S3 client
    s3_client = boto3.client(
        's3',
        region_name=aws_config["REGION"],
        aws_access_key_id=aws_config["KEY"],
        aws_secret_access_key=aws_config["SECRET"]
    )

    # Delete all objects from the bucket
    delete_objects_from_s3_bucket(s3_client, S3_BUCKET_NAME)

    # Delete the S3 bucket
    delete_s3_bucket(s3_client, S3_BUCKET_NAME)


if __name__ == "__main__":
    main()
