import os
import boto3
from botocore.exceptions import ClientError


def start_stop_instance(ec2_client, instance_id, action):
    """
    Start or stop EC2 instance thanks to instance id

    Parameters
    :param ec2_client: Boto3 EC2 client
    :param instance_id: string, id of the instance that is start/stop
    :param action: string, either ON or OFF, it defines the action performed on the instance

    Return
    :return: The string response of the api call
    """
    if action == 'ON':
        # Verify permissions
        try:
            ec2_client.start_instances(InstanceIds=[instance_id], DryRun=True)
        except ClientError as e:
            if 'DryRunOperation' not in str(e):
                raise
        try:
            # Launch instance
            response = ec2_client.start_instances(InstanceIds=[instance_id], DryRun=False)
            print(response)
        except ClientError as e:
            print(e)
    else:
        # Verify permissions
        try:
            ec2_client.stop_instances(InstanceIds=[instance_id], DryRun=True)
        except ClientError as e:
            if 'DryRunOperation' not in str(e):
                raise
        # Stop instance
        try:
            response = ec2_client.stop_instances(InstanceIds=[instance_id], DryRun=False)
            print(response)
        except ClientError as e:
            print(e)


def create_ec2_instances(ec2_client, count):
    """
    Create EC2 instances

    Parameters
    :param ec2_client: Boto3 EC2 client
    :param count: int, Number of instances to be launched

    Return
    :return: The string response of the api call
    """
    try:
        ami_id = "ami-01816d07b1128cd2d"  # Amazon Linux
        instance_type = "t2.micro"

        instances = ec2_client.run_instances(
            ImageId=ami_id,
            InstanceType=instance_type,
            KeyName=os.environ.get("KEY_PAIR_NAME"),
            MinCount=count,
            MaxCount=count,
            SecurityGroupIds=[os.environ.get("SECURITY_GROUP_ID")],
        )
        # Retrieve instances ids
        instance_ids = [instance['InstanceId'] for instance in instances['Instances']]
        print(f"EC2 Instances launched: {instance_ids}")
        return instance_ids
    except Exception as e:
        print(f"Error creating EC2 instances: {e}")
        return []


def get_running_instance_ips(region_name='us-east-1'):
    """
    Retrieve the public IP addresses of all running EC2 instances in a specified region.

    :param region_name: AWS region to query (default: 'us-east-1')
    :return: List of public IP addresses
    """
    try:
        ec2_client = boto3.client('ec2', region_name=region_name)

        # Describe EC2 instances and filter for running state
        response = ec2_client.describe_instances(
            Filters=[{'Name': 'instance-state-name', 'Values': ['running']}]
        )

        # Extract public IPs from response
        ips = []
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                if 'PublicIpAddress' in instance:
                    ips.append(instance['PublicIpAddress'])

        return ips

    except Exception as e:
        print(f"Error fetching running EC2 instance IPs: {e}")
        return []
