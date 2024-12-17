import boto3
import configparser
import os
from botocore.exceptions import ClientError

def load_config(config_file):
    """Load configuration from a file."""
    config = configparser.ConfigParser()
    config.read_file(open(config_file))
    return config


def create_s3_bucket(s3_client, bucket_name, region):
    """Create an S3 bucket."""
    try:
        print("Creating a new S3 bucket...")
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': region
            }
        )
        print(f"S3 bucket '{bucket_name}' created successfully.")
    except ClientError as e:
        print(f"Error creating S3 bucket: {e}")
        raise


def upload_file_to_s3(s3_client, bucket_name, file_path, s3_key):
    """Upload a file to a specific S3 key."""
    try:
        print(f"Uploading {os.path.basename(file_path)} to {s3_key}...")
        s3_client.upload_file(file_path, bucket_name, s3_key)
        print(f"Uploaded {os.path.basename(file_path)} to {s3_key} in {bucket_name}.")
    except ClientError as e:
        print(f"Error uploading {os.path.basename(file_path)}: {e}")
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
    
    # Define S3 bucket and file paths
    S3_BUCKET_NAME = "udacity-data-engineering-rohit1998"
    DATA_DIR = "./data"  # Local directory containing events.csv and songs.csv
    files_to_upload = {
        "events_data/events.csv": os.path.join(DATA_DIR, "events.csv"),
        "songs_data/songs.csv": os.path.join(DATA_DIR, "songs.csv")
    }

    # Initialize S3 client
    s3_client = boto3.client(
        's3',
        region_name=aws_config["REGION"],
        aws_access_key_id=aws_config["KEY"],
        aws_secret_access_key=aws_config["SECRET"]
    )

    # Create S3 bucket
    create_s3_bucket(s3_client, S3_BUCKET_NAME, aws_config["REGION"])

    # Upload files to S3
    for s3_key, file_path in files_to_upload.items():
        upload_file_to_s3(s3_client, S3_BUCKET_NAME, file_path, s3_key)


if __name__ == "__main__":
    main()
