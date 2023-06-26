# This file contains functions for setting up any aws infrastructure

# Create EC2 instance using Python boto3 ---
import boto3

# define function that creates your resource


def create_ec2_instance():
    try:
        print("Creating EC2 Instance")
        resource_ec2 = boto3.client('ec2')
        resource_ec2.run_instances(
            # look up AMI ID of a EC2 resource and provide here (entry must be type string)
            ImageId='<ami ID>',
            # e.g. t2.micro (entry must be type string)
            InstanceType='<instance type>',
            MaxCount=1,
            MinCount=1,
            # separately create key pair and enter name here (entry must be type string)
            KeyName='<key_pair>'
        )
    except Exception as e:
        print(e)
