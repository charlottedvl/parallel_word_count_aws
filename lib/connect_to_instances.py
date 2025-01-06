import os
import subprocess
import threading

from dotenv import load_dotenv

load_dotenv()


def connect_with_ssh(instance_ip):
    """
    Connect to the instance through a ssh connexion

    Parameters
    :param instance_ip: Instance IP
    """
    # Retrieve private key
    private_key = os.environ.get('PRIVATE_KEY')

    try:
        # Connect the instance
        print(f"Connecting to EC2 instance at {instance_ip} and running the script...")

        ssh_command = [
            "ssh",
            "-i", private_key,
            f"ec2-user@{instance_ip}",
            "pip3 install boto3; python3 /home/ec2-user/word_count.py"
        ]

        # Adding a timeout to prevent hanging
        subprocess.run(ssh_command, check=True, timeout=60)
        print(f"Script executed successfully on the EC2 instance at {instance_ip}.")

    except subprocess.TimeoutExpired:
        print(f"SSH command to {instance_ip} timed out.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred with instance {instance_ip}: {e}")


def run_parallel_connections(instance_ips):
    """
    Run instances in parallel trhough ssh connexion

    Parameters
    :param instance_ips: list of instances IP to run
    """
    threads = []

    # Create a thread for each SSH connection
    for instance_ip in instance_ips:
        thread = threading.Thread(target=connect_with_ssh, args=(instance_ip,))
        threads.append(thread)
        thread.start()

    # Monitor threads with debug logs
    for i, thread in enumerate(threads):
        print(f"Waiting for thread {i + 1} to finish...")
        thread.join()
        print(f"Thread {i + 1} finished.")
