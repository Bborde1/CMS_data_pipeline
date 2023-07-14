-- Create schema
CREATE SCHEMA IF NOT EXISTS dataSwan;

-- Create dimension tables
CREATE TABLE IF NOT EXISTS dataSwan.dimService (
    Record_ID INTEGER NOT NULL PRIMARY KEY,
    Covered_Recipient_Type VARCHAR(40),
    Product_Category_Therapeutic_Area VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS dataSwan.dimPhysician (
    Physician_NPI INTEGER NOT NULL PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    Specialty VARCHAR(300)
);

CREATE TABLE IF NOT EXISTS dataSwan.dimTeachingHospital (
    Teaching_Hospital_ID INTEGER NOT NULL PRIMARY KEY,
    Teaching_Hospital_CNN INTEGER,
    Teaching_Hospital_Name VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dataSwan.dimPaymentTime (
    Time_ID INTEGER NOT NULL PRIMARY KEY,
    Date_of_Payment DATE,
    Day INTEGER,
    Month INTEGER,
    Year INTEGER
);

CREATE TABLE IF NOT EXISTS dataSwan.dimGeography (
    Geo_ID VARCHAR(100) NOT NULL PRIMARY KEY,
    Recipient_Street_Address_Line1 VARCHAR(200),
    Recipient_Street_Address_Line2 VARCHAR(50),
    Recipient_City VARCHAR(100),
    Recipient_State VARCHAR(40),
    Recipient_Zip_Code VARCHAR(10)

);

-- Create fact tables
CREATE TABLE IF NOT EXISTS dataSwan.factPayment (
    Payment_ID INTEGER NOT NULL PRIMARY KEY,
    Time_ID INTEGER,
    Physician_NPI INTEGER,
    Record_ID INTEGER,
    Geo_ID INTEGER,
    Teaching_Hospital_ID INTEGER,
    Total_Amount DOUBLE PRECISION,
    Number_of_Payments INTEGER,
    CONSTRAINT fk_factPayment_teaching_hospital_id FOREIGN KEY (Teaching_Hospital_ID)
        REFERENCES dataSwan.dimTeachingHospital (Teaching_Hospital_ID),
    CONSTRAINT fk_factPayment_physician_npi FOREIGN KEY (Physician_NPI)
        REFERENCES dataSwan.dimPhysician (Physician_NPI),
    CONSTRAINT fk_factPayment_record_id FOREIGN KEY (Record_ID)
        REFERENCES dataSwan.dimService (Record_ID),
    CONSTRAINT fk_factPayment_geo_id FOREIGN KEY (Geo_ID)
        REFERENCES dataSwan.dimGeography (Geo_ID),
    CONSTRAINT fk_factPayment_time_id FOREIGN KEY (Time_ID)
        REFERENCES dataSwan.dimPaymentTime (Time_ID)
);

CREATE TABLE IF NOT EXISTS dataSwan.factRating (
    Rating_ID VARCHAR(40) NOT NULL PRIMARY KEY,
    Physician_NPI INTEGER,
    Org_PAC_ID INTEGER,
    Source VARCHAR(20),
    Final_MIPS_Score DOUBLE PRECISION,
    CONSTRAINT fk_factRating_physician_npi FOREIGN KEY (Physician_NPI)
        REFERENCES dataSwan.dimPhysician (Physician_NPI)
);
