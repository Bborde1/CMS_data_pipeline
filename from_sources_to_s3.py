"""
Pure script that pulls data from CMS sources, and write to S3 using functions defined in get_data.py file
"""

import requests
import os
import yaml
from .get_data import write_to_s3

###Handle AWS constants#####
with open('./config.yaml', "r") as fl:
    config = yaml.safe_load(fl)
s3_user = config['cloud_acct']['aws_user']
s3_key = config['cloud_acct']['aws_key']
s3_bucket_name = config['cloud_acct']['bucket_name']
s3_bucket_path = config['cloud_acct']['bucket_path']

####Define script constants#######
# Data Links
cms_links = {'gen_2020': ['general_payment', 'https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_GNRL_PGYR2020_P01202023.csv'],
             'gen_2021': ['general_payment/', 'https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_GNRL_PGYR2021_P01202023.csv'],
             'research_2020': ['research_payment/', 'https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_RSRCH_PGYR2020_P01202023.csv'],
             'research_2021': ['research_payment/', 'https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_RSRCH_PGYR2021_P01202023.csv'],
             'ownership_2020': ['ownership_payment/', 'https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_OWNRSHP_PGYR2020_P01202023.csv'],
             'ownership_2021': ['ownership_payment/', 'https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_OWNRSHP_PGYR2021_P01202023.csv']}

gen_cms_links = {'gen_2020': ['general_payment', 'https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_GNRL_PGYR2020_P01202023.csv'],
                 'gen_2021': ['general_payment/', 'https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_GNRL_PGYR2021_P01202023.csv']}

cms_links_test = {
    'research_2020': ['research_payment', 'https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_RSRCH_PGYR2020_P01202023.csv']}

if __name__ == "__main__":
    # Gets the data and writes to S3 bucket
    for item, link in cms_links_test.items():
        response = requests.get(link)
        item_content = response.content
        print("successfully pulled data and generated content")
        print("writing to S3")
        write_to_s3(item_content, s3_bucket_name,
                    s3_bucket_path + f'{item}_.csv')  # Write loop to delete previous object if exists
