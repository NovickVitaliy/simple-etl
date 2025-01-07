USE cab_db;

CREATE TABLE cab_data (
    id INTEGER IDENTITY(1,1) PRIMARY KEY,
    tpep_pickup_datetime DATETIME,
    tpep_dropoff_datetime DATETIME,
    passenger_count INTEGER CHECK(passenger_count > 0),
    trip_distance FLOAT CHECK(trip_distance > 0),
    store_and_fwd_flag VARCHAR(3),
    PULocationID INTEGER CHECK(PULocationID > 0),
    DOLocationID INTEGER CHECK(DOLocationID > 0),
    fare_amount FLOAT CHECK(fare_amount > 0),
    tip_amount FLOAT CHECK(tip_amount > 0)
)