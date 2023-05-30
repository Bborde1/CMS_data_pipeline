#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 12 14:50:38 2023

@author: brandonborde
"""

"""
This file contains functions used to get data from various sources for the 
DS4A Data Engineering, Data Swan project
"""

import requests
import urllib3
import pandas as pd
import boto3 as b3
import yaml
with open('./config.yaml', "r") as fl:
    config = yaml.safe_load(fl)

s3_user = config.aws_user
s3_key = config.aws_key

######Getter functions########


def get_cms_data_to_df(csv_links):
    # Takes a list of csv links and returns a dictionary of
    for item in csv_links:
        cms_csv = pd.read_csv(item)
    return cms_csv


def write_to_s3(to_write, bucket_location):
    ds4a_s3 = b3.resource('s3')
    bucket_object = ds4a_s3.Object(bucket_location, s3_key)
    bucket_object.put(to_write)
    return print("Successfully wrote to S3 Bucket")


# May not need this if we are recieving a one off pull
def fetch_API_data(base_url, *params):
    reviews_json = requests.get(base_url)
    return reviews_json


# Data Links
cms_links = {'gen_2020': 'https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_GNRL_PGYR2020_P01202023.csv',
             'gen_2021': 'https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_GNRL_PGYR2021_P01202023.csv',
             'research_2020': 'https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_RSRCH_PGYR2020_P01202023.csv',
             'research_2021': 'https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_RSRCH_PGYR2021_P01202023.csv',
             'ownership_2020': 'https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_OWNRSHP_PGYR2020_P01202023.csv',
             'ownership_2021': 'https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_OWNRSHP_PGYR2021_P01202023.csv', }


cms_links_test = {
    'research_2020': 'https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_RSRCH_PGYR2020_P01202023.csv'}

for item, link in cms_links_test:
    response = requests.get(link)
    item_content = response.content
    with open(f"{item}_.csv", "wb") as file:
        file.write(item_content)
        write_to_s3(file, 'ds4a_bucket')
