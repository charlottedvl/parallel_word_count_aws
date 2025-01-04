import json
import logging

from botocore.exceptions import ClientError


def create_queue(sqs_client, queue_name):
    try:
        queue = sqs_client.create_queue(QueueName=queue_name, Attributes={'DelaySeconds': '0'})
        print(f"Queue {queue_name} created with url: {queue['QueueUrl']}.")
    except ClientError as e:
        logging.error(e)
        return
    return queue['QueueUrl']


def send_to_queue(sqs_client, queue_url, chunk_files, bucket_name):
    for chunk_file in chunk_files:
        message_body = json.dumps({
            'bucket': bucket_name,
            'key': chunk_file
        })
        sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body
        )
        print(f"Message sent to queue for chunk: {chunk_file}")