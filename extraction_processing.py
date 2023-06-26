"""

This file contains any functions that are used to do any data processing PRIOR to writing to S3.
The goal is to maintain a lower storage footprint, and reduce extraction and load times.

"""
import pandas as pd

# import geo_id generation function
from get_geo_id import generate_geo_id


def update_general_payments(general_df):
    # filter out observations with missing first name and last name
    general_df = general_df[general_df['Covered_Recipient_First_Name'].notnull()]
    general_df = general_df[general_df['Covered_Recipient_Last_Name'].notnull()]
    general_df = general_df[general_df['Recipient_Primary_Business_Street_Address_Line1'].notnull()]

    # concatenate addresses into one address
    # note: corrects for any recipients at the same address, but in different suites
    general_df['Recipient_Primary_Business_Street_Address'] = general_df['Recipient_Primary_Business_Street_Address_Line1'] + \
        general_df['Recipient_Primary_Business_Street_Address_Line1']

    # generate unique geographic id
    general_df['geo_id'] = general_df['Recipient_Primary_Business_Street_Address'].map(
        generate_geo_id)

    return general_df


def update_research_payments(columns_list, research_df):
    # Get only columns that matter
    updated_df = research_df[[columns_list]]
    return updated_df


def update_ownership_payments(columns_list, ownership_df):
    # Get only columns that matter
    updated_df = ownership_df[[columns_list]]
    return updated_df
