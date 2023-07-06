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
import io
from extraction_processing import update_general_payments, update_general_payments_sample

with open('./config.yaml', "r") as fl:
    config = yaml.safe_load(fl)
s3_user = config['cloud_acct']['aws_user']
s3_key = config['cloud_acct']['aws_key']
s3_bucket_name = config['cloud_acct']['bucket_name']
s3_bucket_path = config['cloud_acct']['bucket_path']
s3_mips_path = config['cloud_acct']['mips_path']


####Define script constants#######
# ALL DATA
cms_links = {'gen_2020': ['general_payment/', 'https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_GNRL_PGYR2020_P01202023.csv'],
             'gen_2021': ['general_payment/', 'https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_GNRL_PGYR2021_P01202023.csv'],
             'research_2020': ['research_payment/', 'https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_RSRCH_PGYR2020_P01202023.csv'],
             'research_2021': ['research_payment/', 'https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_RSRCH_PGYR2021_P01202023.csv'],
             'ownership_2020': ['ownership_payment/', 'https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_OWNRSHP_PGYR2020_P01202023.csv'],
             'ownership_2021': ['ownership_payment/', 'https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_OWNRSHP_PGYR2021_P01202023.csv']}

# GENERAL PAYMENTS
cms_gen_links = {'gen_2020': ['general_payment/', 'https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_GNRL_PGYR2020_P01202023.csv'],
                 'gen_2021': ['general_payment/', 'https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_GNRL_PGYR2021_P01202023.csv']}


######Helper functions########
def write_to_s3(to_write, bucket_location, write_key, aws_user=s3_user, aws_secret=s3_key):
    # AWS with client object
    s3_client = b3.client('s3',  aws_access_key_id=aws_user,
                          aws_secret_access_key=aws_secret)
    s3_client.put_object(Body=to_write, Bucket=bucket_location, Key=write_key)
    return print("Successfully wrote to S3 Bucket")


def get_from_s3(to_get, bucket_location):
    # AWS with client object
    s3_client = b3.client('s3',  aws_access_key_id=s3_user,
                          aws_secret_access_key=s3_key)
    response = s3_client.get_object(Bucket=bucket_location, Key=to_get)['Body']
    df = pd.read_csv(io.BytesIO(response.read()))
    return df


def get_and_write_MIPS(bucket_location, write_key="mips_data.csv", aws_user=s3_user, aws_secret=s3_key, data_path="./mips_data/ec_score_file.csv"):
    full_key = s3_mips_path + write_key
    s3_client = b3.client('s3',  aws_access_key_id=aws_user,
                          aws_secret_access_key=aws_secret)
    with open(data_path, "rb") as file:
        s3_client.upload_fileobj(file, bucket_location, full_key)
    return print("MIPS written successfully")


for item, link in cms_gen_links.items():
    df = pd.read_csv(link[1], usecols=config['cms_columns'])
    df = update_general_payments(df)
    print("successfully pulled data and generated content")
    # ONLY WRITE FIRST 100000 rows as a sample (AWS Free tier)
    df = df.head(100000)
    print("writing to S3")
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer)
    write_to_s3(csv_buffer.getvalue(), s3_bucket_name,
                s3_bucket_path + link[0] + f'{item}_.csv')
    csv_buffer.close()
print("Getting and writing MIPS")
get_and_write_MIPS(s3_bucket_name)


###API implementation###
# Define CMS dataset IDs
cms_datasets = {'gen_2020': ['general_payment/', 'a08c4b30-5cf3-4948-ad40-36f404619019'],
                'gen_2021': ['general_payment/', '0380bbeb-aea1-58b6-b708-829f92a48202']}

for item, cms_id in cms_datasets.items():
    request_url = f'https://openpaymentsdata.cms.gov/api/1/datastore/query/{cms_id[1]}/0?limit=500&offset=0&count=true&results=true&schema=true&keys=true&format=json&rowIds=false'
    results = requests.get(request_url)
    content = results.json()
    df = pd.DataFrame.from_dict(content['results'])
    df = df[[x.lower() for x in config['cms_columns']]]
    df = update_general_payments_sample(df)
    print("writing to S3")
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer)
    write_to_s3(csv_buffer.getvalue(), s3_bucket_name,
                s3_bucket_path + cms_id[0] + f'{item}_.csv')
    csv_buffer.close()
