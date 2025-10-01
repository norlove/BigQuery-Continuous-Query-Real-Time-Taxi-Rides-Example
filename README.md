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
and

```
CREATE OR REPLACE TABLE `real_time_taxi_streaming.matched_rides` (
  request_time          TIMESTAMP NOT NULL OPTIONS(description="The timestamp when the ride request was made."),
  request_id            STRING OPTIONS(description="The unique ID for the ride request."),
  taxi_id               STRING OPTIONS(description="The ID of the closest available taxi matched to the request."),
  distance_in_meters    FLOAT64 OPTIONS(description="The distance between the taxi and the ride request at the time of matching."),
  taxi_available_time   TIMESTAMP OPTIONS(description="The timestamp when the matched taxi last reported its 'available' status.")
)
PARTITION BY
  DATE(request_time)
CLUSTER BY
  taxi_id
OPTIONS (
  description = 'Stores the real-time matches between ride requests and the closest available taxis.'
);
```
