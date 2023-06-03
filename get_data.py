"""
This file contains functions used to get data from various sources for the 
DS4A Data Engineering, Data Swan project
"""

# Imports

# Load in necessary config variables
import requests
import pandas as pd
import boto3 as b3
import yaml
with open('./config.yaml', "r") as fl:
    config = yaml.safe_load(fl)
s3_user = config['cloud_acct']['aws_user']
s3_key = config['cloud_acct']['aws_key']
s3_bucket_name = config['cloud_acct']['bucket_name']
s3_bucket_path = config['cloud_acct']['bucket_path']


######Getter functions########
def write_to_s3(to_write, bucket_location, write_key):
    # Using client object instead
    s3_client = b3.client('s3',  aws_access_key_id=s3_user,
                          aws_secret_access_key=s3_key)
    s3_client.put_object(Body=to_write, Bucket=bucket_location, Key=write_key)
    return print("Successfully wrote to S3 Bucket")


# Data Links
cms_links = {'gen_2020': 'https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_GNRL_PGYR2020_P01202023.csv',
             'gen_2021': 'https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_GNRL_PGYR2021_P01202023.csv',
             'research_2020': 'https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_RSRCH_PGYR2020_P01202023.csv',
             'research_2021': 'https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_RSRCH_PGYR2021_P01202023.csv',
             'ownership_2020': 'https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_OWNRSHP_PGYR2020_P01202023.csv',
             'ownership_2021': 'https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_OWNRSHP_PGYR2021_P01202023.csv', }


cms_links_test = {
    'research_2020': 'https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_RSRCH_PGYR2020_P01202023.csv'}

# Gets the data and writes to S3 bucket
for item, link in cms_links_test.items():
    response = requests.get(link)
    item_content = response.content
    print("successfully pulled data and generated content")
    print("writing to S3")
    write_to_s3(item_content, s3_bucket_name,
                s3_bucket_path + f'{item}_.csv')
