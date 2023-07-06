from extraction_funcs.get_data import write_to_s3, get_and_write_MIPS
from extraction_funcs.extraction_processing import update_general_payments_sample
import io
import yaml
import boto3 as b3
import requests
import pandas as pd
print("imported pandas")

print("Imports Complete")

with open('./config.yaml', "r") as fl:
    config = yaml.safe_load(fl)
s3_user = config['cloud_acct']['aws_user']
s3_key = config['cloud_acct']['aws_key']
s3_bucket_name = config['cloud_acct']['bucket_name']
s3_bucket_path = config['cloud_acct']['bucket_path']
s3_mips_path = config['cloud_acct']['mips_path']

print("Loaded Config")

###API implementation###
# Define CMS dataset IDs
cms_datasets = {'gen_2020': ['general_payment/', 'a08c4b30-5cf3-4948-ad40-36f404619019'],
                'gen_2021': ['general_payment/', '0380bbeb-aea1-58b6-b708-829f92a48202']}

if __name__ == "__main__":
    for item, cms_id in cms_datasets.items():
        print(f"Extracting {item}.")
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
    get_and_write_MIPS(bucket_location=s3_bucket_name)
