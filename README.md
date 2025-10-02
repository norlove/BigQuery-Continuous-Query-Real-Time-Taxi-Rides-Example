# BigQuery Continuous Query Real-Time Taxi Rides

This creates some sample data to use in conjunction with [BigQuery continuous queries](https://cloud.google.com/bigquery/docs/continuous-queries-introduction).

## Within your Google Cloud BigQuery project we'll create 5 tables using the following script: ##

```
CREATE OR REPLACE TABLE `real_time_taxi_streaming.taxirides` (
  timestamp         TIMESTAMP NOT NULL,
  taxi_id           STRING,
  ride_id           STRING,
  latitude          FLOAT64,
  longitude         FLOAT64,
  geohash           STRING,
  ride_status       STRING,
  meter_reading     FLOAT64,
  passenger_count   INTEGER
)
PARTITION BY
  DATE(timestamp)
CLUSTER BY
  geohash, timestamp
OPTIONS (
  description = 'A real-time stream of taxi supply events, including location and ride status.'
);


CREATE OR REPLACE TABLE `real_time_taxi_streaming.ride_requests` (
  timestamp             TIMESTAMP NOT NULL,
  request_id            STRING,
  latitude              FLOAT64,
  longitude             FLOAT64,
  geohash               STRING,
  destination_address   STRING
)
PARTITION BY
  DATE(timestamp)
CLUSTER BY
  geohash, timestamp
OPTIONS (
  description = 'A real-time stream of passenger ride requests, representing market demand.'
);


CREATE OR REPLACE TABLE `real_time_taxi_streaming.matched_rides` (
  request_time            TIMESTAMP NOT NULL,
  request_id              STRING,
  taxi_id                 STRING,
  distance_in_meters      FLOAT64,
  taxi_available_time     TIMESTAMP
)
PARTITION BY
  DATE(request_time)
CLUSTER BY
  taxi_id
OPTIONS (
  description = 'Stores the real-time matches between ride requests and the closest available taxis.'
);


CREATE OR REPLACE TABLE `real_time_taxi_streaming.hourly_driver_stats` (
  ride_date                   DATE NOT NULL,
  window_end                  TIMESTAMP,
  taxi_id                     STRING,
  total_rides_per_hour        INT64,
  avg_fare_per_hour           FLOAT64,
  total_passengers_per_hour   INT64
)
PARTITION BY ride_date
CLUSTER BY
  taxi_id
OPTIONS (
  description = 'Stores the hourly statistics for each driver in terms of their total rides per hour, average fare, and total passengers per hour'
);


CREATE OR REPLACE TABLE `real_time_taxi_streaming.neighborhood_taxi_health` (
  window_end TIMESTAMP,
  geohash STRING,
  avg_latitude FLOAT64,
  avg_longitude FLOAT64,
  taxi_demand_volume INT64,
  avg_proximity_meters FLOAT64,
  min_proximity_meters FLOAT64,
  max_proximity_meters FLOAT64,
  proximity_stddev FLOAT64
)
PARTITION BY
  DATE(window_end)
CLUSTER BY
  geohash
OPTIONS (
  description = 'Stores the statistics for where each neighborhoods taxi volume is.'
);
```

## Within BigQuery create a Colab Notebook: ##
