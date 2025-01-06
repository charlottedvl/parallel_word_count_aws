import os

import boto3
from botocore.exceptions import ClientError


def start_stop_instance(ec2_client, instance_id, action):
    """
    :param instance_id: String, id of the instance that is start/stop
    :param action: String, either ON or OFF, it defines the action performed on the instance
    :return: The String response of the api call
    """
    if action == 'ON':
        # Do a dryrun first to verify permissions
        try:
            ec2_client.start_instances(InstanceIds=[instance_id], DryRun=True)
        except ClientError as e:
            if 'DryRunOperation' not in str(e):
                raise
                # Dry run succeeded, run start_instances without dryrun
        try:
            response = ec2_client.start_instances(InstanceIds=[instance_id], DryRun=False)
            print(response)
        except ClientError as e:
            print(e)
    else:
        # Do a dryrun first to verify permissions
        try:
            ec2_client.stop_instances(InstanceIds=[instance_id], DryRun=True)
        except ClientError as e:
            if 'DryRunOperation' not in str(e):
                raise
        # Dry run succeeded, call stop_instances without dryrun
        try:
            response = ec2_client.stop_instances(InstanceIds=[instance_id], DryRun=False)
            print(response)
        except ClientError as e:
            print(e)


def create_ec2_instances(ec2_client, count):
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

        # Extract public IPs
        ips = []
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                if 'PublicIpAddress' in instance:
                    ips.append(instance['PublicIpAddress'])

        return ips

    except Exception as e:
        print(f"Error fetching running EC2 instance IPs: {e}")
        return []
