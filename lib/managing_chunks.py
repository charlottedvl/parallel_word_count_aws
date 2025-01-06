import json

from lib.aws_resources.s3_manager import delete_from_prefix


def split_file_to_chunks(sqs_client, s3_client, bucket_name, queue_url, object_key, chunk_size):
    """
    Split a file into smaller chunks of known size, uploaded in S3 and send message to SQS

    Parameters
    :param sqs_client: Boto3 SQS client
    :param s3_client: Boto3 S3 client
    :param bucket_name: Bucket to upload to
    :param queue_url: string, Queue URL
    :param object_key: string, file to divide into chunks
    :param chunk_size: int, number of lines per chunks

    """
    try:
        # Get object from S3
        file_obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        file_content = file_obj['Body'].read().decode('utf-8')

        # Split
        lines = file_content.split('\n')
        chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]
        print(f"Splitting {object_key} into {len(chunks)} chunks")

        # Suppressing previous chunks and chunks results so that it doesn't interfere with the new ones
        chunk_prefix = f"datalake/results/" + object_key.split('/')[-1].replace('.txt', '')
        delete_from_prefix(s3_client, bucket_name, chunk_prefix)
        chunk_prefix = f"datalake/chunks/{chunk_size}/" + object_key.split('/')[-1].replace('.txt', '')
        delete_from_prefix(s3_client, bucket_name, chunk_prefix)

        #Put chunks into the bucket
        for idx, chunk in enumerate(chunks):
            chunk_key = f"{chunk_prefix}_chunk_{idx}.txt"
            s3_client.put_object(Bucket=bucket_name, Key=chunk_key, Body='\n'.join(chunk))

            # Send message to SQS
            message = {
                'bucket_name': bucket_name,
                'chunk_key': chunk_key
            }
            sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))

        print(f"File {object_key} split into chunks and messages sent to SQS")

    except Exception as e:
        print(f"Error splitting file into chunks: {e}")


def aggregate_results(s3_client, bucket_name, result_directory, chunk_size, instances_count):
    """
    Combine results file for each chunks

    Parameters
    :param s3_client: Boto3 S3 client
    :param bucket_name: Bucket to upload to
    :param result_directory: Directory in which the results are
    :param chunk_size: int, number of lines per chunks
    :param instances_count: number of instances running

    Return total_word_count: int, number total of word in raw file

    """
    try:
        total_word_count = 0
        # Get results objects
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=result_directory)

        if 'Contents' in response:
            for obj in response['Contents']:
                # Extract the word_count from the JSON object and add it to the total
                file_data = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
                content = file_data['Body'].read().decode('utf-8')
                result_data = json.loads(content)
                word_count = result_data.get("word_count", 0)
                total_word_count += word_count

        # Save final result to S3
        final_result_key = f"datalake/final_results/{instances_count}-instances-{chunk_size}-size.txt"
        s3_client.put_object(Bucket=bucket_name, Key=final_result_key, Body=str(total_word_count))
        print(f"Final Word Count: {total_word_count} saved to {final_result_key}")
        return total_word_count
    except Exception as e:
        print(f"Error aggregating results: {e}")
