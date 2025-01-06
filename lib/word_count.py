import boto3
import json

# AWS Clients
s3_client = boto3.client('s3', region_name='us-east-1')
sqs_client = boto3.client('sqs', region_name='us-east-1')

QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/166914594921/cloud-computing-wordcount-queue'
BUCKET_NAME = 'cloud-computing-wordcount-bucket'
RESULT_DIRECTORY = "datalake/results/"


def process_chunk(bucket_name, chunk_key):
    try:
        # Download the chunk from S3
        file_obj = s3_client.get_object(Bucket=bucket_name, Key=chunk_key)
        file_content = file_obj['Body'].read().decode('utf-8')

        # Word count logic
        word_count = len(file_content.split())
        print(f"Processed {chunk_key}: Word count = {word_count}")

        # Save the result to S3
        result_key = f"{RESULT_DIRECTORY}{chunk_key.split('/')[-1].replace('.txt', '_result.txt')}"
        result_data = {
            "chunk_key": chunk_key,
            "word_count": word_count
        }
        s3_client.put_object(Bucket=bucket_name, Key=result_key, Body=json.dumps(result_data))
        print(f"Result saved to {result_key}")

    except Exception as e:
        print(f"Error processing chunk {chunk_key}: {e}")


def poll_queue_and_process():
    try:
        isMessage = True
        while isMessage:
            response = sqs_client.receive_message(
                QueueUrl=QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10,
            )

            if 'Messages' not in response:
                print("No messages in queue, stopping jobs")
                isMessage = False

            for message in response['Messages']:
                receipt_handle = message['ReceiptHandle']
                body = json.loads(message['Body'])

                bucket_name = body['bucket_name']
                chunk_key = body['chunk_key']

                process_chunk(bucket_name, chunk_key)

                sqs_client.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)
                print(f"Message processed and deleted: {chunk_key}")

    except Exception as e:
        print(f"Error polling SQS queue: {e}")

if __name__ == "__main__":
    poll_queue_and_process()
