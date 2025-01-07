USE cab_db;

CREATE TABLE cab_data (
    id INTEGER IDENTITY(1,1) PRIMARY KEY,
    tpep_pickup_datetime DATETIME NOT NULL,
    tpep_dropoff_datetime DATETIME NOT NULL,
    passenger_count INTEGER NOT NULL CHECK(passenger_count > 0),
    trip_distance FLOAT NOT NULL CHECK(trip_distance > 0),
    store_and_fwd_flag VARCHAR(3),
    PULocationID INTEGER NOT NULL CHECK(PULocationID > 0),
    DOLocationID INTEGER NOT NULL CHECK(DOLocationID > 0),
    fare_amount FLOAT NOT NULL CHECK(fare_amount > 0),
    tip_amount FLOAT NOT NULL CHECK(tip_amount > 0)
)