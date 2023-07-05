-- Create schema
CREATE SCHEMA IF NOT EXISTS dataSwan;

-- Create dimension tables
CREATE TABLE IF NOT EXISTS dataSwan.dimService (
    Record_ID INTEGER NOT NULL PRIMARY KEY,
    Covered_Recipient_Type VARCHAR(40) NOT NULL,
    Product_Category_Therapeutic_Area VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS dataSwan.dimPhysician (
    Physician_NPI INTEGER NOT NULL PRIMARY KEY,
    FirstName VARCHAR(50) NOT NULL,
    LastName VARCHAR(50) NOT NULL,
    Specialty VARCHAR(300) NOT NULL
);

CREATE TABLE IF NOT EXISTS dataSwan.dimTeachingHospital (
    Teaching_Hospital_ID INTEGER NOT NULL PRIMARY KEY,
    Teaching_Hospital_CNN INTEGER NOT NULL,
    Teaching_Hospital_Name VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS dataSwan.dimPaymentTime (
    Time_ID INTEGER NOT NULL PRIMARY KEY,
    Date_of_Payment DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS dataSwan.dimGeography (
    Geo_ID INTEGER NOT NULL PRIMARY KEY,
    City VARCHAR(50) NOT NULL,
    State VARCHAR(50) NOT NULL,
    Street_Address_Line1 VARCHAR(100) NOT NULL,
    Street_Address_Line2 VARCHAR(50) NOT NULL
);

-- Create fact tables
CREATE TABLE IF NOT EXISTS dataSwan.factPayment (
    Payment_ID INTEGER NOT NULL PRIMARY KEY,
    Time_ID INTEGER NOT NULL,
    Physician_NPI INTEGER NOT NULL,
    Record_ID INTEGER NOT NULL,
    Geo_ID INTEGER NOT NULL,
    Teaching_Hospital_ID INTEGER NOT NULL,
    Total_Amount_of_Payment_USDollars DOUBLE PRECISION NOT NULL,
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
    RatingID INTEGER NOT NULL PRIMARY KEY,
    Physician_NPI INTEGER NOT NULL,
    NumericalRating DOUBLE PRECISION NOT NULL,
    CONSTRAINT fk_factRating_physician_npi FOREIGN KEY (Physician_NPI)
        REFERENCES dataSwan.dimPhysician (Physician_NPI)
);
