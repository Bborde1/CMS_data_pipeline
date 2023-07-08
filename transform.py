import pandas as pd
import numpy as np
import uuid
import hashlib
import io
import yaml
from extraction_funcs.get_data import write_to_s3, get_from_s3


with open('./config.yaml', "r") as fl:
    config = yaml.safe_load(fl)
s3_user = config['cloud_acct']['aws_user']
s3_key = config['cloud_acct']['aws_key']
s3_bucket_name = config['cloud_acct']['bucket_name']
s3_raw_path = config['cloud_acct']['raw_path']
s3_processed_path = config['cloud_acct']['processed_path']
s3_mips_path = config['cloud_acct']['mips_path']


def generate_geo_id(address):
    """
    Input: an address consists of street address and zipcode
    Output: Geo id using hash function
    """
    geoid = hashlib.sha256(address.encode()).hexdigest()
    return geoid


time_id_map = {}


def generate_time_id(date):
    """
    Input: a date
    Output: time id
    """
    if date in time_id_map:
        return time_id_map[date]
    else:
        time_id = len(time_id_map) + 1  # Generate a new ID
        time_id_map[date] = time_id  # Store the mapping
        return time_id


def tf_cms(df, dropdup=False):
    """
    Input: the raw CMS data
    Output: transformed data ready to be split into fact and dimension tables
    """
    # for simplicity we will limit the scope to recipients with US addresses
    df = df[df.Recipient_Country == "United States"]

    df["Recipient_Zip_Code"] = df["Recipient_Zip_Code"].str[:5]
    PROPERNAMES = [
        "Recipient_Primary_Business_Street_Address_Line1",
        "Recipient_City",
        "Teaching_Hospital_Name",
        "Covered_Recipient_First_Name",
        "Covered_Recipient_Last_Name",
        "Product_Category_or_Therapeutic_Area_1",
    ]
    for col in PROPERNAMES:
        df[col].fillna("", inplace=True)
        df[col] = df[col].str.title()

    df["Time_ID"] = df["Date_of_Payment"].map(generate_time_id)
    df["Recipient_Address"] = (
        df["Recipient_Primary_Business_Street_Address_Line1"]
        + " "
        + df["Recipient_Primary_Business_Street_Address_Line2"]
        + " "
        + df["Recipient_Zip_Code"]
    )
    df["Geo_ID"] = df["Recipient_Address"].apply(
        lambda x: generate_geo_id(str(x)))

    if dropdup:
        df.drop_duplicates(inplace=True)

    return df


def tf_dimPaymentTime(df, cols=["Time_ID", "Date_of_Payment"]):
    """
    Input: transformed data as the output of the tf_cms function
    Output: dimension table dimPaymentTime
    """
    dimPaymentTime = df.loc[df.Date_of_Payment.notnull(),
                            cols].drop_duplicates()
    dimPaymentTime["Date_of_Payment"] = pd.to_datetime(df["Date_of_Payment"])
    dimPaymentTime["Payment_Day"] = dimPaymentTime["Date_of_Payment"].dt.day
    dimPaymentTime["Payment_Month"] = dimPaymentTime["Date_of_Payment"].dt.month
    dimPaymentTime["Payment_Year"] = dimPaymentTime["Date_of_Payment"].dt.year

    return dimPaymentTime


def tf_dimPhysician(
    df,
    cols=[
        "Covered_Recipient_NPI",
        "Covered_Recipient_First_Name",
        "Covered_Recipient_Last_Name",
        "Covered_Recipient_Specialty_1",
    ],
    specialty_onehot=False,
):
    """
    Input: transformed data as the output of the tf_cms function
    Output: dimension table dimPhysician
    """
    dimPhysician = df.loc[df.Covered_Recipient_NPI.notnull(),
                          cols].drop_duplicates()
    dimPhysician.Covered_Recipient_NPI = dimPhysician.Covered_Recipient_NPI.astype(
        "int"
    )
    dimPhysician.columns = ["NPI", "FirstName", "LastName", "Specialty"]

    if specialty_onehot:
        specialty = dimPhysician.Covered_Recipient_Specialty_1.str.get_dummies()
        dimPhysician = dimPhysician.join(specialty.add_prefix("Specialty_"))

    return dimPhysician


def tf_dimService(
    df,
    cols=[
        "Record_ID",
        "Covered_Recipient_Type",
        "Product_Category_or_Therapeutic_Area_1",
    ],
):
    """
    Input: transformed data as the output of the tf_cms function
    Output: dimension table dimService
    """
    dimService = df.loc[df.Record_ID.notnull(), cols].drop_duplicates()
    return dimService


def tf_dimTeachingHospital(
    df, cols=["Teaching_Hospital_ID",
              "Teaching_Hospital_CCN", "Teaching_Hospital_Name"]
):
    """
    Input: transformed data as the output of the tf_cms function
    Output: dimension table dimTeachingHospital
    """
    dimTeachingHospital = df.loc[
        df.Teaching_Hospital_ID.notnull(), cols
    ].drop_duplicates()
    dimTeachingHospital.Teaching_Hospital_ID = dimTeachingHospital.Teaching_Hospital_ID.astype(
        "int"
    )
    dimTeachingHospital.Teaching_Hospital_CCN = dimTeachingHospital.Teaching_Hospital_CCN.astype(
        "int"
    )

    return dimTeachingHospital


def tf_dimGeography(
    df,
    cols=[
        "Geo_ID",
        "Recipient_Primary_Business_Street_Address_Line1",
        "Recipient_City",
        "Recipient_State",
        "Recipient_Zip_Code",
    ],
):
    """
    Input: transformed data as the output of the tf_cms function
    Output: dimension table dimGeography
    """
    dimGeography = df[cols].drop_duplicates()
    renamed = {
        "Recipient_Primary_Business_Street_Address_Line1": "Recipient_Street_Address_Line1",
        "Recipient_Primary_Business_Street_Address_Line2": "Recipient_Street_Address_Line2",
    }

    factPayment.rename(columns=renamed, inplace=True)

    return dimGeography


def tf_factPayment(
    df,
    cols=[
        "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID",
        "Time_ID",
        "Covered_Recipient_NPI",
        "Record_ID",
        "Geo_ID",
        "Teaching_Hospital_ID",
        "Total_Amount_of_Payment_USDollars",
        "Number_of_Payments_Included_in_Total_Amount",
    ],
):
    """
    Input: transformed data as the output of the tf_cms function
    Output: fact table factPayment
    """
    factPayment = df[cols]
    renamed = {
        "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID": "Payment_ID",
        "Covered_Recipient_NPI": "Recipient_NPI",
        "Total_Amount_of_Payment_USDollars": "Total_Amount",
        "Number_of_Payments_Included_in_Total_Amount": "Number_of_Payments",
    }

    factPayment.rename(columns=renamed, inplace=True)

    return factPayment


def tf_mips(df):
    """
    Input: the raw MIPS data
    Output: transformed data ready to be split into fact and dimension tables
    """
    df.rename(str.strip, axis="columns", inplace=True)
    return df


def tf_factRating(df, cols=["NPI", "Org_PAC_ID", "source", "final_MIPS_score"]):
    factRating = df[cols].drop_duplicates()
    factRating["Rating_ID"] = factRating["final_MIPS_score"].map(
        lambda x: str(uuid.uuid4())
    )
    fullcols = ["Rating_ID"] + cols
    return factRating[fullcols]


if __name__ == "__main__":
    rawcms = pd.read_csv("gen_2020_.csv", nrows=100000)
    rawmips = pd.read_csv("./mips_data/ec_score_file.csv")
    cms = tf_cms(rawcms)
    processed_dataframes = {}
    try:
        factPayment = tf_factPayment(cms)
        processed_dataframes['factPayment'] = factPayment
    except:
        pass

    try:
        dimGeography = tf_dimGeography(cms)
        processed_dataframes['dimGeography'] = dimGeography
    except:
        pass
    try:
        dimTeachingHospital = tf_dimTeachingHospital(cms)
        processed_dataframes['dimTeachingHospital'] = dimTeachingHospital
    except:
        pass
    try:
        dimService = tf_dimService(cms)
        processed_dataframes['dimService'] = dimService
    except:
        pass
    try:
        dimPhysician = tf_dimPhysician(cms)
        processed_dataframes['dimPhysician'] = dimPhysician
    except:
        pass
    try:
        dimPaymentTime = tf_dimPaymentTime(cms)
        processed_dataframes['dimPaymentTime'] = dimPaymentTime
    except:
        pass
    for table, data in processed_dataframes.items():
        write_buffer = io.BytesIO()
        data.to_csv(write_buffer)
        write_to_s3(write_buffer.getvalue(), s3_bucket_name,
                    s3_processed_path + f'{table}.csv')
        write_buffer.close()
