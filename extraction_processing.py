"""

This file contains any functions that are used to do any data processing PRIOR to writing to S3.
The goal is to maintain 

"""
import requests
import pandas as pd
import boto3 as b3
import yaml


# Pull CMS data into dataframe for manipulation
def get_cms_data_to_df(csv_links):
    # Takes a list of csv links and returns a dictionary of
    for item in csv_links:
        cms_csv = pd.read_csv(item)
    return cms_csv


def update_general_payments(columns_list, general_df):
    # Get only columns that matter
    updated_df = data_df[[columns_list]]
    return updated_df


def update_research_payments(columns_list, research_df):
    # Get only columns that matter
    updated_df = research_df[[columns_list]]
    return updated_df


def update_ownership_payments(columns_list, ownership_df):
    # Get only columns that matter
    updated_df = ownership_df[[columns_list]]
    return updated_df
