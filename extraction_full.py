"""
Pure script that pulls data from CMS sources, and write to S3 using functions defined in get_data.py file
"""

import requests
import pandas as pd
import io
import yaml
from extraction_funcs.get_data import write_to_s3, get_and_write_MIPS
from extraction_funcs.extraction_processing import update_general_payments
print("Imports successful")

###Handle AWS constants#####
with open('./config.yaml', "r") as fl:
    config = yaml.safe_load(fl)
s3_user = config['cloud_acct']['aws_user']
s3_key = config['cloud_acct']['aws_key']
s3_bucket_name = config['cloud_acct']['bucket_name']
s3_raw_path = config['cloud_acct']['raw_path']


####Define script constants####
cms_gen_links = {'gen_2020': ['general_payment/', 'https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_GNRL_PGYR2020_P01202023.csv'],
                 'gen_2021': ['general_payment/', 'https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_GNRL_PGYR2021_P01202023.csv']}

print("Variables loaded")
if __name__ == "__main__":
    # Gets the data and writes to S3 bucket
    for item, link in cms_gen_links.items():
        print(f"Reading {item} from source.")
        df = pd.read_csv(
            link[1], usecols=config['cms_columns'], engine='pyarrow')
        df = update_general_payments(df)
        # ONLY WRITE FIRST 50000 rows as a sample (AWS Free tier)
        df = df.head(250000)
        print("successfully pulled data and generated content")
        print("writing to S3")
        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer)
        write_to_s3(csv_buffer.getvalue(), s3_bucket_name,
                    s3_raw_path + link[0] + f'{item}_.csv', s3_user, s3_key)
        csv_buffer.close()
    get_and_write_MIPS(s3_bucket_name)
