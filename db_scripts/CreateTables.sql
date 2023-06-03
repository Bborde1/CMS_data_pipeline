--Assumes a schema name of dataSwan
--Create Fact Tables
CREATE IF NOT EXISTS dataSwan.factPaymentPhysician(
    PaymentID INT UNIQUE PRIMARY KEY,
    PaymentType VARCHAR(40),
    PaymentAmount FLOAT,
    PaymentDate DATE,
    ProductCategory VARCHAR(20),
    PhysicianNPI INT FOREIGN KEY REFERENCES dimPhysician(PhysicianNPI)
);

CREATE IF NOT EXISTS dataSwan.factRatingPhysician(
    RatingID INT UNIQUE PRIMARY KEY,
    NumericalRating FLOAT,
    RatingDate DATE,
    Platform varchar(10),
    PhysicianNPI INT FOREIGN KEY REFERENCES dimPhysician(PhysicianNPI)
);

CREATE IF NOT EXISTS dataSwan.factPaymentHospital(
    PaymentID INT UNIQUE PRIMARY KEY,
    PaymentType VARCHAR(40),
    PaymentAmount FLOAT,
    PaymentDate DATE,
    ProductCategory VARCHAR(20),
    HospitalID INT FOREIGN KEY REFERENCES dimHospital(HospitalID)
);

--Create Dimension Tables
CREATE IF NOT EXISTS dataSwan.dimPhysician(
    PhysicianNPI INT UNIQUE PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    City VARCHAR(50),
    State VARCHAR(40),
    Specialty VARCHAR(300),
    APM_affl_1 VARCHAR(50),
    APM_affl_1_id INT FOREIGN KEY REFERENCES dimHospital(HospitalID),
    APM_affl_2 VARCHAR(50),
    APM_affl_2_id INT FOREIGN KEY REFERENCES dimHospital(HospitalID),
    APM_affl_3 VARCHAR(50),
    APM_affl_3_id INT FOREIGN KEY REFERENCES dimHospital(HospitalID),
);

CREATE IF NOT EXISTS dataSwan.dimHospital(
    HospitalID INT UNIQUE PRIMARY KEY,
    HospitalCCN INT,
    Name VARCHAR(50),
    City VARCHAR(50),
    State VARCHAR(40)
);