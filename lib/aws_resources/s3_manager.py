import logging
import os

import boto3
from botocore.exceptions import ClientError


def create_bucket(s3_client, bucket_name):
    """
    :param s3_client:
    :param bucket_name: Bucket to create
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_file(s3_client, file_name, bucket_name, object_name=None):
    """Upload a file to an S3 bucket
    :param file_name: File to upload
    :param bucket_name: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    try:
        response = s3_client.upload_file(file_name, bucket_name, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def download_files(s3_client, object_name, bucket_name, file_name=None):

    """Download a file from an S3 bucket
        :param object_name: S3 object to download
        :param bucket_name: Bucket to download from
        :param file_name: Name under which the file is download. If not specified then object_name is used
        :return: True if file was downloaded, else False
        """

    # If file_name was not specified, use object_name
    if file_name is None:
        object_name = object_name

    # Download the file
    try:
        response = s3_client.download_file(bucket_name, object_name, file_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def delete_file(object_name, bucket_name):
    """Deleting a file to an S3 bucket
    :param object_name: File to delete
    :param bucket_name: Bucket to delete from
    :return: True if file was deleted, else False
    """

    # Delete the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.delete_object(Bucket=bucket_name, Key=object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True
