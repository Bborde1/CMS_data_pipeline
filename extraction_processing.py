"""

This file contains any functions that are used to do any data processing PRIOR to writing to S3.
The goal is to maintain a lower storage footprint, and reduce extraction and load times.

"""
import pandas as pd


def update_general_payments(columns_list, general_df):
    # Get only columns that matter
    updated_df = general_df[[columns_list]]

    # filter out observations with missing first name and last name
    updated_df = general_df[general_df['Covered_Recipient_First_Name'].notnull()]
    updated_df = general_df[general_df['Covered_Recipient_Last_Name'].notnull()]   

    # concatenate addresses into one address
    #note: corrects for any recipients at the same address, but in different suites
    updated_df['Recipient_Primary_Business_Street_Address'] = updated_df['Recipient_Primary_Business_Street_Address_Line1'] + df['Recipient_Primary_Business_Street_Address_Line1']
 
    
    return updated_df


def update_research_payments(columns_list, research_df):
    # Get only columns that matter
    updated_df = research_df[[columns_list]]
    return updated_df


def update_ownership_payments(columns_list, ownership_df):
    # Get only columns that matter
    updated_df = ownership_df[[columns_list]]
    return updated_df
