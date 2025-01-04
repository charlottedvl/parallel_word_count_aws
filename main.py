# General import
from dotenv import load_dotenv

# Local import
from lib.aws_resources.apigateway_manager import *
from lib.aws_resources.ec2_manager import *
from lib.aws_resources.s3_manager import *
from lib.aws_resources.sqs_manager import *
from lib.aws_resources.lambda_manager import *

# Load environment variables
load_dotenv()

# AWS clients
s3_client = boto3.client('s3')
ec2_client = boto3.client('ec2')
sqs_client = boto3.client('sqs')
lambda_client = boto3.client('lambda')
cloudwatch_client = boto3.client('cloudwatch')
sns_client = boto3.client('sns')

# Configuration
BUCKET_NAME = 'cloud-computing-wordcount-bucket'
INPUT_FILE = 'datalake/original_document/Shakespeare.txt'
CHUNK_DIRECTORY = "datalake/chunks/"
RESULT_DIRECTORY = "datalake/results/"
CHUNK_SIZE = 500
QUEUE_NAME = 'cloud-computing-wordcount-queue'
INSTANCE_NAME = "cloud-computing-word-count-ec2"


# Main Process
create_bucket(s3_client, BUCKET_NAME)

