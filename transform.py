import pandas as pd


def transform_cms(df):
    df["Date_of_Payment"] = pd.to_datetime(df["Date_of_Payment"])
    df["Payment_Day"] = df["Date_of_Payment"].dt.day
    df["Payment_Month"] = df["Date_of_Payment"].dt.month
    df["Payment_Year"] = df["Date_of_Payment"].dt.year

    specialty = df.Covered_Recipient_Specialty_1.str.get_dummies()
    df = df.join(specialty.add_prefix("Specialty_"))

    df["Recipient_Zip_Code"] = df["Recipient_Zip_Code"].str[:5]
    df["Recipient_City"] = df["Recipient_City"].str.capitalize()

    df.drop_duplicates(inplace=True)

    return df


def transform_mips(df):
    df.rename(str.strip, axis="columns", inplace=True)
    return (
        df.groupby(["NPI", "lst_nm", "frst_nm"])["final_MIPS_score"]
        .mean()
        .reset_index()
    )

