# BigQuery Continuous Query Real-Time Taxi Rides
An end-to-end demo of BigQuery stateful continuous queries with a taxi ride use case.


Create the two tables:
```
CREATE OR REPLACE TABLE `real_time_taxi_streaming.taxirides` (
  timestamp         TIMESTAMP NOT NULL,
  taxi_id           STRING,
  ride_id           STRING,
  latitude          FLOAT64,
  longitude         FLOAT64,
  ride_status       STRING,
  meter_reading     FLOAT64,
  passenger_count   INTEGER
)
PARTITION BY
  DATE(timestamp)
CLUSTER BY
  timestamp
OPTIONS (
  description = 'A real-time stream of taxi supply events, including location and ride status.'
);
```

and
```
CREATE OR REPLACE TABLE `real_time_taxi_streaming.ride_requests` (
  timestamp             TIMESTAMP NOT NULL,
  request_id            STRING,
  request_status        STRING,
  latitude              FLOAT64,
  longitude             FLOAT64,
  destination_address   STRING
)
PARTITION BY
  DATE(timestamp)
CLUSTER BY
  timestamp
OPTIONS (
  description = 'A real-time stream of passenger ride requests, representing market demand.'
);
```

