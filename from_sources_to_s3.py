"""
Pure script that pulls data from CMS sources, and write to S3 using functions defined in get_data.py file
"""

import requests
import pandas as pd
import io
import yaml
from .get_data import write_to_s3, get_and_write_MIPS

###Handle AWS constants#####
with open('./config.yaml', "r") as fl:
    config = yaml.safe_load(fl)
s3_user = config['cloud_acct']['aws_user']
s3_key = config['cloud_acct']['aws_key']
s3_bucket_name = config['cloud_acct']['bucket_name']
s3_bucket_path = config['cloud_acct']['bucket_path']

####Define script constants#######
# ALL DATA
cms_links = {'gen_2020': ['general_payment', 'https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_GNRL_PGYR2020_P01202023.csv'],
             'gen_2021': ['general_payment/', 'https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_GNRL_PGYR2021_P01202023.csv'],
             'research_2020': ['research_payment/', 'https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_RSRCH_PGYR2020_P01202023.csv'],
             'research_2021': ['research_payment/', 'https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_RSRCH_PGYR2021_P01202023.csv'],
             'ownership_2020': ['ownership_payment/', 'https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_OWNRSHP_PGYR2020_P01202023.csv'],
             'ownership_2021': ['ownership_payment/', 'https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_OWNRSHP_PGYR2021_P01202023.csv']}

cms_gen_links = {'gen_2020': ['general_payment', 'https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_GNRL_PGYR2020_P01202023.csv'],
                 'gen_2021': ['general_payment/', 'https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_GNRL_PGYR2021_P01202023.csv']}

if __name__ == "__main__":
    # Gets the data and writes to S3 bucket
    for item, link in cms_gen_links.items():
        try:
            del df
        except:
            pass
        df = pd.read_csv(link[1], usecols=config['cms_columns'])
        print("successfully pulled data and generated content")
        print("writing to S3")
        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer)
        write_to_s3(csv_buffer.getvalue(), s3_bucket_name,
                    s3_bucket_path + link[0] + f'{item}_.csv')
        csv_buffer.close()
