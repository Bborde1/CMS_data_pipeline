"""

This file contains any functions that are used to do any data processing PRIOR to writing to S3.
The goal is to maintain 

"""
import pandas as pd


def update_general_payments(columns_list, general_df):
    # Get only columns that matter
    updated_df = general_df[[columns_list]]
    return updated_df


def update_research_payments(columns_list, research_df):
    # Get only columns that matter
    updated_df = research_df[[columns_list]]
    return updated_df


def update_ownership_payments(columns_list, ownership_df):
    # Get only columns that matter
    updated_df = ownership_df[[columns_list]]
    return updated_df
