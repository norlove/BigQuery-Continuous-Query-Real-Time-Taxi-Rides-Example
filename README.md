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

## Generate synthetic Taxi and Rider data 

1. Let's generate some taxi and ride request data. We're going to do this using a Colab Enterprise notebook in BigQuery [[ref](https://cloud.google.com/bigquery/docs/notebooks-introduction)].

2. Go into BigQuery, click Create new Notebook. If required click the button to enable the API (there may be other APIs the wizard will have you enable).

    ![Screenshot 2025-04-24 at 2 14 32 PM](https://github.com/user-attachments/assets/8916fe51-7621-4e9f-830f-c5bfa47be719)


3. Click to add a new Code block. Paste in the following Python code and run the cell.
   
  ```
  pip install google-cloud-bigquery google-cloud-bigquery-storage pygeohash
  ```
  
5. Click to add a new Code block. Paste in the following Python code, being sure to change the project ID below from "your-gcp-project-id" to your own project. Then run the cell which will start streaming data into your taxirides and ride_requests tables. You can now proceed with running the stateful continuous queries.

```
"""
BigQuery Real-Time Taxi Data Generator

This script generates synthetic data for a fleet of taxis and streams it directly
into two BigQuery tables: `taxirides` (supply) and `ride_requests` (demand).

**Key Features:**
- Uses the BigQuery Storage Write API for high-throughput ingestion.
- Simulates a fleet with statuses (`available`, `occupied`, `enroute_to_pickup`).
- Includes a **geohash** for each event's location to enable efficient geospatial
  equijoins in BigQuery continuous queries.
- Models realistic behavior like post-trip cooldowns and periodic "heartbeat"
  updates for available taxis.

**Prerequisites:**
1.  Install the required Python libraries:
    `pip install google-cloud-bigquery google-cloud-bigquery-storage pygeohash`
2.  Authenticate with Google Cloud in your environment:
    `gcloud auth application-default login`
"""

import datetime
import time
import random
import uuid
import logging
import math
from google.cloud import bigquery
import google.auth
import pygeohash as pgh  # Library for geohashing

# --- Your Google Cloud Project and BigQuery Details ---
try:
    credentials, project_id = google.auth.default()
    PROJECT_ID = project_id
except google.auth.exceptions.DefaultCredentialsError:
    PROJECT_ID = "your-gcp-project-id" # <-- SET YOUR PROJECT ID HERE

DATASET_ID = "real_time_taxi_streaming"
TAXIRIDES_TABLE_ID = "taxirides"
RIDEREQUESTS_TABLE_ID = "ride_requests"

# --- Simulation Parameters ---
NUM_TAXIS = 100
NYC_CENTER_LAT = 40.7128
NYC_CENTER_LON = -74.0060
MAX_KM_FROM_CENTER = 15.0
MIN_TRIP_DURATION_SECS = 60 * 2
MAX_TRIP_DURATION_SECS = 60 * 15
REPORTING_INTERVAL_SECONDS = 10
GEOHASH_PRECISION = 6  # Precision for the geohash key (6 is ~0.6km x 0.6km)
NYC_STREET_NAMES = [
    "Broadway", "Park Ave", "Madison Ave", "Fifth Ave", "Lexington Ave",
    "Wall St", "Canal St", "Houston St", "Columbus Ave", "Amsterdam Ave",
    "Bleecker St", "Delancey St", "Bowery", "St Marks Pl", "W 4th St",
    "E 14th St", "W 42nd St", "E 57th St", "W 86th St", "E 125th St"
]

class Taxi:
    """Manages the state of a single taxi."""
    def __init__(self, taxi_id):
        self.id = taxi_id
        self.ride_id = None
        self.status = 'available'
        self.lat, self.lon = self.get_random_nyc_coords()
        self.passenger_count = 0
        self.meter_reading = 0.0
        self.trip_end_time = None
        self.available_again_at = time.time()
        self.last_heartbeat_time = 0

    def get_random_nyc_coords(self):
        """Generates random coordinates within a radius of NYC center."""
        radius_in_deg = MAX_KM_FROM_CENTER / 111.32
        u, v = random.uniform(0, 1), random.uniform(0, 1)
        w = radius_in_deg * (u ** 0.5)
        t = 2 * math.pi * v
        lat = w * math.sin(t) + NYC_CENTER_LAT
        lon = w * math.cos(t) + NYC_CENTER_LON
        return round(lat, 6), round(lon, 6)

    def start_trip(self):
        """Starts a new trip, returns the 'enroute_to_pickup' event."""
        self.status = 'occupied'
        self.ride_id = str(uuid.uuid4())
        self.available_again_at = None
        self.passenger_count = 0
        self.meter_reading = 0.0
        enroute_event = self.create_event('enroute_to_pickup')
        self.passenger_count = random.randint(1, 4)
        self.meter_reading = 2.50
        trip_duration = random.randint(MIN_TRIP_DURATION_SECS, MAX_TRIP_DURATION_SECS)
        self.trip_end_time = time.time() + trip_duration
        return [enroute_event]

    def end_trip(self):
        """Ends the trip, returns 'dropoff' and 'available' events."""
        self.lat, self.lon = self.get_random_nyc_coords()
        dropoff_event = self.create_event('dropoff')
        self.status = 'available'
        self.ride_id = None
        self.passenger_count = 0
        self.meter_reading = 0.0
        self.trip_end_time = None
        self.available_again_at = time.time() + (2 * 60)
        available_event = self.create_event('available')
        return [dropoff_event, available_event]

    def update_position(self):
        """Simulates movement for an occupied taxi."""
        self.meter_reading += random.uniform(0.5, 2.0)
        self.lat += random.uniform(-0.0005, 0.0005)
        self.lon += random.uniform(-0.0005, 0.0005)
        return [self.create_event('occupied')]

    def heartbeat(self):
        """Generates a heartbeat event for an available taxi."""
        self.last_heartbeat_time = time.time()
        return [self.create_event('available')]

    def create_event(self, status_override=None):
        """Creates a dictionary representing a taxiride event."""
        event_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
        return {
            'timestamp': event_time,
            'taxi_id': self.id,
            'ride_id': self.ride_id,
            'latitude': self.lat,
            'longitude': self.lon,
            'geohash': pgh.encode(self.lat, self.lon, precision=GEOHASH_PRECISION),
            'ride_status': status_override if status_override else self.status,
            'meter_reading': round(self.meter_reading, 2),
            'passenger_count': self.passenger_count
        }

def generate_ride_request_event():
    """Generates a single synthetic ride request event."""
    temp_taxi = Taxi('temp')
    lat, lon = temp_taxi.get_random_nyc_coords()
    event_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
    random_street = random.choice(NYC_STREET_NAMES)
    return {
        'timestamp': event_time,
        'request_id': str(uuid.uuid4()),
        'latitude': lat,
        'longitude': lon,
        'geohash': pgh.encode(lat, lon, precision=GEOHASH_PRECISION),
        'destination_address': f"{random.randint(100, 9999)} {random_street}, New York, NY"
    }

def stream_synthetic_data_to_bigquery(taxi_fleet):
    """Generates and streams synthetic taxi data to BigQuery tables."""
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    taxirides_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TAXIRIDES_TABLE_ID}"
    riderequests_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{RIDEREQUESTS_TABLE_ID}"

    logging.info("--- Starting Data Stream ---")
    logging.info(f"Streaming to tables: \n\t- {taxirides_table_ref}\n\t- {riderequests_table_ref}")
    logging.info(">>> IMPORTANT: To stop, press CTRL+C <<<")

    start_time_global = time.time()
    last_report_time = time.time()
    last_request_time = time.time()
    total_rows_inserted = 0

    try:
        while True:
            try:
                batch_start_time = time.time()
                taxirides_batch = []
                riderequests_batch = []

                if time.time() - last_request_time >= 10:
                    riderequests_batch.append(generate_ride_request_event())
                    last_request_time = time.time()

                for taxi in taxi_fleet:
                    events = []
                    now = time.time()
                    is_ready_for_new_trip = taxi.available_again_at is not None and now >= taxi.available_again_at
                    
                    if taxi.status == 'available' and is_ready_for_new_trip:
                        if random.random() < 0.03:
                            events.extend(taxi.start_trip())
                        elif now - taxi.last_heartbeat_time > 60:
                            events.extend(taxi.heartbeat())
                    elif taxi.status == 'occupied':
                        if taxi.trip_end_time and now >= taxi.trip_end_time:
                            events.extend(taxi.end_trip())
                        elif random.random() < 0.15:
                            events.extend(taxi.update_position())
                    
                    if events:
                        taxirides_batch.extend(events)

                errors = []
                if taxirides_batch:
                    errors.extend(client.insert_rows_json(taxirides_table_ref, taxirides_batch))
                if riderequests_batch:
                    errors.extend(client.insert_rows_json(riderequests_table_ref, riderequests_batch))

                if not errors:
                    total_rows_inserted += len(taxirides_batch) + len(riderequests_batch)
                else:
                    logging.error(f"Encountered errors while inserting rows: {errors}")

                batch_end_time = time.time()
                elapsed_time = batch_end_time - batch_start_time
                sleep_time = 1.0 - elapsed_time
                if sleep_time > 0:
                    time.sleep(sleep_time)

                current_time = time.time()
                if current_time - last_report_time >= REPORTING_INTERVAL_SECONDS:
                    elapsed_total = current_time - start_time_global
                    avg_rate = total_rows_inserted / elapsed_total if elapsed_total > 0 else 0
                    logging.info(f"Inserted: {total_rows_inserted} rows, Avg Rate: {avg_rate:.2f} rows/sec")
                    last_report_time = current_time
            except Exception as e:
                logging.error(f"An error occurred; will retry. Error: {e}", exc_info=False)
                time.sleep(5)

    except KeyboardInterrupt:
        logging.info("Shutdown signal received. Stopping publisher...")
    finally:
        elapsed_total = time.time() - start_time_global
        avg_rate = total_rows_inserted / elapsed_total if elapsed_total > 0 else 0
        logging.info("--------------------")
        logging.info("Final Run Statistics:")
        logging.info(f"Total Time Elapsed: {elapsed_total:.2f} seconds")
        logging.info(f"Total Rows Inserted: {total_rows_inserted}")
        logging.info(f"Overall Average Insert Rate: {avg_rate:.2f} rows/sec")
        logging.info("Publisher finished.")

# --- Main Execution Block ---
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    if PROJECT_ID == "your-gcp-project-id":
        logging.error("FATAL: Please update the PROJECT_ID variable before running.")
    else:
        taxi_fleet = [Taxi(taxi_id=f"taxi_{i:03d}") for i in range(NUM_TAXIS)]
        logging.info(f"Initialized a fleet of {len(taxi_fleet)} taxis.")
        stream_synthetic_data_to_bigquery(taxi_fleet)
```  
