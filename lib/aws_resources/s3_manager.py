import os
import logging
from botocore.exceptions import ClientError


def create_bucket(s3_client, bucket_name):
    """
    Create an S3 bucket if it doesn't already exist.
    If the bucket exists, return the URL of the existing bucket.

    Parameters
    :param s3_client: Boto3 S3 client
    :param bucket_name: Name of the bucket to create

    Return
    :return: The URL of the bucket if it exists or is created, else None
    """
    try:
        # Check if bucket exist
        existing_buckets = s3_client.list_buckets()

        for bucket in existing_buckets['Buckets']:
            if bucket['Name'] == bucket_name:
                print(f"Bucket {bucket_name} already exists")
                return

        # Create bucket
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} created successfully")
        return

    except ClientError as e:
        logging.error(f"Error creating or checking bucket: {e}")
        return None


def upload_file(s3_client, file_name, bucket_name, object_name=None):
    """
    Upload a file to an S3 bucket if it doesn't already exist.

    Parameters
    :param s3_client: Boto3 S3 client
    :param file_name: File to upload
    :param bucket_name: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used

    Return
    :return: True if file was uploaded, False if skipped or error occurred
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    try:
        # Check if the file already exists in the bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=object_name)
        if 'Contents' in response:
            print(f"File {object_name} already exists in bucket {bucket_name} - skipping upload")
            return False

        # Upload the file
        s3_client.upload_file(file_name, bucket_name, object_name)
        print(f"Successfully uploaded {file_name} into {bucket_name} as {object_name}")
        return True

    except ClientError as e:
        logging.error(f"Error uploading {file_name} to {bucket_name}: {e}")
        return False


def delete_from_prefix(s3_client, bucket_name, chunk_prefix):
    """
    Upload a file to an S3 bucket if it doesn't already exist.

    Parameters
    :param s3_client: Boto3 S3 client
    :param bucket_name: Bucket to upload to
    :param chunk_prefix: S3 object prefix, prefix for deleting object
    """
    print(f"Deleting all old chunks in prefix: {chunk_prefix}")
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=chunk_prefix)
    if 'Contents' in response:
        delete_objects = {'Objects': [{'Key': obj['Key']} for obj in response['Contents']]}
        s3_client.delete_objects(Bucket=bucket_name, Delete=delete_objects)
        print(f"All old chunks deleted under prefix: {chunk_prefix}")
