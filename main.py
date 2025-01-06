# General import
from datetime import datetime
import csv


# Local import
from lib.aws_resources.ec2_manager import *
from lib.aws_resources.s3_manager import *
from lib.aws_resources.sqs_manager import *
from lib.connect_to_instances import *
from lib.managing_chunks import *

# Load environment variables
load_dotenv()

# Create AWS clients
s3_client = boto3.client('s3')
ec2_client = boto3.client('ec2')
sqs_client = boto3.client('sqs')
lambda_client = boto3.client('lambda')
cloudwatch_client = boto3.client('cloudwatch')
sns_client = boto3.client('sns')

# Configuration
BUCKET_NAME = 'cloud-computing-wordcount-bucket'
INPUT_FILE = 'datalake/original_document/Shakespeare'
CHUNK_DIRECTORY = "datalake/chunks/"
RESULT_DIRECTORY = "datalake/results/"
QUEUE_NAME = 'cloud-computing-wordcount-queue'
INSTANCE_NAME = "cloud-computing-word-count-ec2"
RESULT_FILE = "media/execution_and_error_data.csv"
RESULT_FILE_BUCKET = "final_results/" + RESULT_FILE
INPUT_THEORICAL_COUNT = 967889  # Value got from Word counter
INSTANCE_COUNT = [2, 3, 4]
FILE_COUNT = 5
CHUNK_SIZES = [2000, 10000, 100000]


# Creation of AWS resources
create_bucket(s3_client, BUCKET_NAME)
queue_url = create_queue(sqs_client, QUEUE_NAME)

# /!\ Do not uncomment those lines to avoid blocking the AWS Academy account by having more than 4 EC2 instances
"""
# Create and launch EC2 instances
instance_ids = create_ec2_instances(ec2_client, 4)
"""

#Get IPs
instance_ips = get_running_instance_ips()


# Definition of useful functions
def simulation(chunk_size, instances_count):
    """
    Run a simulation for a chunk size and a number of instances

    Parameters
    :param chunk_size: int, number of lines per chunks
    :param instances_count: int, number of instances running

    Return
    :return execution_time: time to run the app
    :return results: number of words in the input file
    """
    start_time = datetime.now()  # Start timer
    object_names = []
    for i in range(FILE_COUNT):
        # Input, split file and send chunks to SQS
        input_file = f"{INPUT_FILE}.txt"
        object_name = f"{INPUT_FILE}-{i}.txt"
        upload_file(s3_client, input_file, BUCKET_NAME, object_name)
        object_names.append(object_name)
        split_file_to_chunks(sqs_client, s3_client, BUCKET_NAME, queue_url, object_name, chunk_size)

    #Run instances in parallel to process chunks
    run_parallel_connections(instance_ips[:instances_count])

    results = aggregate_results(s3_client, BUCKET_NAME, RESULT_DIRECTORY, chunk_size, instances_count)
    end_time = datetime.now()  # Stop timer

    execution_time = end_time - start_time
    execution_time_seconds = execution_time.total_seconds()

    print(f"Total execution time: {execution_time_seconds:.2f} seconds")
    return results, execution_time_seconds


def compute_error(counted_value):
    """
    Compute error from a theorical value

    Parameters
    :param counted_value: value computed from the app

    Return
    :return error of the counted value
    """
    return abs(counted_value - INPUT_THEORICAL_COUNT * FILE_COUNT) / (INPUT_THEORICAL_COUNT * FILE_COUNT)


def log_data(ec2_count, chunk_size, execution_time, error_rate):
    """
    Log the data into a separate file

    Parameters
    :param ec2_count: int, number of instances runnning
    :param chunk_size: int, number of lines per chunks
    :param execution_time: time for executing the code
    :param error_rate: Error for the computed value
    """
    with open(RESULT_FILE, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([ec2_count, chunk_size, execution_time, error_rate])


def run_experiment():
    """
    Run experiment for several configurations
    """
    for ec2_count in INSTANCE_COUNT:
        for chunk_size in CHUNK_SIZES:

            results, execution_time = simulation(chunk_size, ec2_count)

            error = compute_error(results)
            log_data(ec2_count, chunk_size, execution_time, error)

            print(f"Experiment with EC2 count: {ec2_count}, chunk size: {chunk_size} completed.")


run_experiment()
upload_file(s3_client, RESULT_FILE, BUCKET_NAME, RESULT_FILE_BUCKET)
