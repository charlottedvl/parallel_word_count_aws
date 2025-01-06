import logging
from botocore.exceptions import ClientError


def create_queue(sqs_client, queue_name):
    """
    Create a standard queue if it doesn't already exist

    Parameters
    :param sqs_client: Boto3 SQS client
    :param queue_name: Queue name to create the queue

    Return
    :return: string, the queue URL
    """
    try:
        # Check if the queue already exists
        existing_queues = sqs_client.list_queues(QueueNamePrefix=queue_name)

        if 'QueueUrls' in existing_queues and existing_queues['QueueUrls']:
            print(f"Queue {queue_name} already exists")
            return existing_queues['QueueUrls'][0]

        # Create the queue
        queue = sqs_client.create_queue(QueueName=queue_name, Attributes={'DelaySeconds': '0'})
        print(f"Queue {queue_name} created with url: {queue['QueueUrl']}.")
        return queue['QueueUrl']

    except ClientError as e:
        logging.error(f"Error creating or checking queue: {e}")
        return None
