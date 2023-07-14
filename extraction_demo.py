from extraction_funcs.get_data import write_to_s3, get_and_write_MIPS
from extraction_funcs.extraction_processing import update_general_payments_sample
import io
import yaml
import boto3 as b3
import requests
import pandas as pd
print("Imports Complete")

with open('./config.yaml', "r") as fl:
    config = yaml.safe_load(fl)
s3_user = config['cloud_acct']['aws_user']
s3_key = config['cloud_acct']['aws_key']
s3_bucket_name = config['cloud_acct']['bucket_name']
s3_raw_path = config['cloud_acct']['raw_path']
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
        df.columns = [x.title() for x in df.columns]
        df.rename(columns={'Teaching_Hospital_Ccn': 'Teaching_Hospital_CCN', 'Teaching_Hospital_Id':
                  'Teaching_Hospital_ID', 'Covered_Recipient_Npi': 'Covered_Recipient_NPI',
                           'Total_Amount_Of_Payment_Usdollars': 'Total_Amount_of_Payment_USDollars',
                           'Date_Of_Payment': 'Date_of_Payment', 'Number_Of_Payments_Included_In_Total_Amount': 'Number_of_Payments_Included_in_Total_Amount',
                           'Record_Id': 'Record_ID', 'Product_Category_Or_Therapeutic_Area_1': 'Product_Category_or_Therapeutic_Area_1',
                           'Applicable_Manufacturer_Or_Applicable_Gpo_Making_Payment_Id': 'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID'}, inplace=True)
        df = df[config['cms_columns']]
        print("writing to S3")
        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer)
        write_to_s3(csv_buffer.getvalue(), s3_bucket_name,
                    s3_raw_path + cms_id[0] + f'{item}_.csv', s3_user, s3_key)
        csv_buffer.close()
    get_and_write_MIPS(bucket_location=s3_bucket_name)
