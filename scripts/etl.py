import pandas as pd
import requests
import os
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, trim, udf, row_number, monotonically_increasing_id, to_date, from_unixtime, substring
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql.window import Window

from dotenv import load_dotenv
from datetime import datetime, UTC

from geopy.distance import geodesic

import psycopg2
from sqlalchemy import create_engine, text

import sys

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.udfs import geo_distance_in_km


# Spark Session
print("Initializing spark session.......")
spark = SparkSession.builder.appName("SkyTrack DE") \
                            .config("spark.jars.packages", "org.postgresql:postgresql:42.2.27") \
                            .getOrCreate()


# Load Env vars
load_dotenv(override=True)

# Collect all raw folders from today
print("Extracting flights data")
raw_data_path = "data/raw"
today = datetime.now(UTC).strftime("%Y-%m-%d")

folders = [f"{raw_data_path}/{f}" for f in os.listdir(raw_data_path) if today in f]
   
flights_df_from_file = spark.read.json(folders)
flights_df_from_file.show()



# Extraction Layer
# Collect all raw folders from today

raw_data_path = "data/raw"
today = datetime.now(UTC).strftime("%Y-%m-%d")

folders = [f"{raw_data_path}/{f}" for f in os.listdir(raw_data_path) if today in f]

flights_df_from_file = spark.read.json(folders)


#  Transformation (Silver/Layer)
flights_df_exploded = flights_df_from_file.select(
    col("time").alias("snapshot_time"),
    explode(col("states")).alias("states_array")
)
flights_df = flights_df_exploded.select(
    col("snapshot_time"),
    col("states_array")[0].alias("icao24"),
    col("states_array")[1].alias("callsign"),
    col("states_array")[2].alias("origin_country"),
    col("states_array")[3].alias("time_position"),
    col("states_array")[4].alias("last_contact"),
    col("states_array")[5].alias("longitude").cast(DoubleType()),
    col("states_array")[6].alias("latitude").cast(DoubleType()),
    col("states_array")[7].alias("baro_altitude"),
    col("states_array")[8].alias("on_ground"),
    col("states_array")[9].alias("velocity"),
    col("states_array")[10].alias("true_track"),
    col("states_array")[11].alias("vertical_rate"),
    col("states_array")[12].alias("sensors"),
    col("states_array")[13].alias("geo_altitude"),
    col("states_array")[14].alias("squawk"),
    col("states_array")[15].alias("spi"),
    col("states_array")[16].alias("position_source"),
    col("states_array")[17].alias("category")
)

# Remove trailing spaces from callsign column (Trimming) 
flights_df = flights_df.withColumn("callsign", trim(col("callsign")))
print("Flight data extracted successfully.")




# Deriving dim_airports
print("Extracting dimension airports data")
dim_airports = spark.read.csv("data/airports.csv", header= True, inferSchema=True)
dim_airports = dim_airports.select(
                            col("id"),
                            col("name"),
                            col("type"), 
                            col("latitude_deg").alias("latitude"), 
                            col("longitude_deg").alias("longitude"),
                            col("iso_country").alias("country"),
                            col("icao_code").alias("icao"),
                            col("iata_code").alias("iata"),
                            col("gps_code"),
                        ).dropDuplicates()

# Leaving the list of airports to only big, medium and small. Filtering out closed, 
# heliports, balloonport, large_airport, seaplane_base.. e.t.c
dim_airports = dim_airports.filter(dim_airports["type"].isin(['large_airport', 'medium_airport', 'small_airport']))

# Fill up of unknown values
dim_airports = dim_airports.fillna({
    "icao": "Unknown",
    "iata": "Unknown",
    "gps_code": "Unknown"
})
print("Airport dimension created successful")



#  Creating a bounding box for each flight's long and lat
print("Calculating closest airport...")
delta = 1.5
flights_boundings = flights_df.withColumn("lat_min", col("latitude") - delta) \
                            .withColumn("lat_max", col("latitude") + delta) \
                            .withColumn("long_min", col("longitude") - delta) \
                            .withColumn("long_max", col("longitude") + delta)

# Join for flights and dim_airports (Only cross joining 
# flights with airports whose locations are within the flight's bounding box)
flights_x_airs = flights_boundings.alias("flight") \
                     .join(dim_airports.alias("airport"), \
                                (col("airport.latitude") >= col("flight.lat_min")) & (col("airport.latitude") <= col("flight.lat_max")) & \
                                (col("airport.longitude") >= col("flight.long_min")) & (col("airport.longitude") <= col("flight.long_max")) \
                                ) \
                     .select(
                         "flight.*",
                         col("airport.name").alias("airport_name"),
                         col("airport.id").alias("airport_id"),                
                         col("airport.name").alias("airport_name"),          
                         col("airport.type").alias("airport_type"),      
                         col("airport.latitude").alias("airport_lat"),
                         col("airport.longitude").alias("airport_long"),
                         col("airport.country").alias("airport_country"),
                         col("airport.icao").alias("airport_icao"),
                         col("airport.iata").alias("airport_iata"),
                         col("airport.gps_code").alias("airport_gps_code")
                     )

# Calculate column distance_km between flights and airports to the joined df
flights_x_airs = flights_x_airs.withColumn("distance_km", 
                                            geo_distance_in_km(col("latitude"), col("longitude"), 
                                                               col("airport_lat"), col("airport_long") 
                                                            )
                                          )

windowSpec = Window.partitionBy("snapshot_time", "icao24").orderBy("distance_km")

flights_x_airs_closests = flights_x_airs.withColumn("rank",
                                                    row_number().over(windowSpec)
                                                    )

flights_x_airs_closests = flights_x_airs_closests.filter(col("rank") == 1).drop("rank")

flights_x_airs_closests.columns

fact_flights_df = flights_x_airs_closests.select(
    monotonically_increasing_id().alias("flight_id"),
    "snapshot_time",
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "longitude",
    "latitude",
    col("geo_altitude").alias("altitude"),
    "on_ground",
    "velocity",
    col("true_track").alias("heading"),
    col("airport_id").alias("closest_airport_id"),
    col("distance_km").alias("airport_distance_km"),
    to_date(from_unixtime(col("snapshot_time"))).alias("flight_date")
)

fact_flights_df = fact_flights_df.withColumn("airline_code", substring(col("callsign"), 1, 3))
print("Closest aiport calculation completed, with fact_flights and dim_airport ready")



# Creating the Airlines Dimension
print("Creating dimension airlines...")
dim_airline = spark.read.csv("data/silver/airlines.dat", header=False, inferSchema=True)
dim_airline = dim_airline.selectExpr(
    "_c0 as airline_id",
    "_c1 as airline_name",
    "_c2 as airline_alias",
    "_c4 as airline_icao",
    "_c5 as airline_callsign",
    "_c6 as country",
    "_c7 as active"
).filter((col("_c4") != "\\N") & (col("_c4").isNotNull()) & (col("_c4") != "N/A")) \
.drop("airline_id")

# Add montonically_increading_id for airline_id
dim_airline = dim_airline.withColumn("airline_id", monotonically_increasing_id() + 1)\
.select("airline_id", "airline_name", "airline_icao", "airline_callsign", "country", "active")
dim_airline.show()

print("dimension airlines created successfully")




# Creating the Aircrafts Dimension
print("Creating the Aircrafts Dimension...")
dim_aircraft_base = fact_flights_df.select("icao24").dropna().dropDuplicates()
dim_aircraft = spark.read.csv("data/silver/aircrafts.csv", header= True, inferSchema=True)

# Clean column names
for old_column in dim_aircraft.columns:
    new_column = old_column.strip().strip("'")
    if old_column != new_column:
        dim_aircraft = dim_aircraft.withColumnRenamed(old_column, new_column)

dim_aircraft = dim_aircraft.select([
    "icao24",
    "model",
    "manufacturerName",
    "manufacturerIcao",
    "typecode",
    "icaoAircraftClass",
    "categoryDescription",
    "country",
    "engines",
    "built",
    "serialNumber",
    "registration",
    "status"
])

# Final dim_aircraft, filtered to only those present in the fact table
dim_aircraft = dim_aircraft_base.join( dim_aircraft, ["icao24"], how="left")
dim_aircraft = dim_aircraft.withColumnRenamed("icao24", "aircraft_id")
print("Aircrafts dimension created successfully")



print("Join dimensions aircraft and airline to facts_flights...")
fact_flights_df = fact_flights_df.join(dim_airline, fact_flights_df.airline_code == dim_airline.airline_icao, how="left") \
                                .join(dim_aircraft, fact_flights_df.icao24 ==  dim_aircraft.aircraft_id, how="left")\
                                .select(["flight_id", "snapshot_time", "icao24", "callsign", "origin_country", "time_position",
                                         "longitude", "latitude", "altitude", "on_ground", "velocity", "heading", "closest_airport_id", 
                                         "airport_distance_km", "flight_date", "airline_code", "airline_id", "aircraft_id"]) \
                                .filter(col("airline_id").isNotNull())
print("Joining to fact_flights completed")



#  Save dataframes to CSV
print("Saving dataframes to local...")
fact_flights_df.write.mode("append").parquet("data/gold/fact")
dim_aircraft.write.mode("overwrite").parquet("data/gold/aircrafts")
dim_airline.write.mode("overwrite").parquet("data/gold/airlines")
dim_airports.write.mode("overwrite").parquet("data/gold/airports")
print("All dataframes successfully saved.")




# Loading
print("Loading Environment Variables...")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME").lower()

# Load Env vars
load_dotenv(override=True)

print("Environment Variables loaded successfully")



# Loading data into the database
print("Loading data into the database...")
db_properties = {
    "user": DB_USER,
    "password": DB_PASS,
    "driver": "org.postgresql.Driver"
}

jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

fact_flights_df.write.jdbc(url=jdbc_url, table="skytracker.facts", mode="append", properties=db_properties)
dim_aircraft.write.jdbc(url=jdbc_url, table="skytracker.aircrafts", mode="overwrite", properties=db_properties)
dim_airline.write.jdbc(url=jdbc_url, table="skytracker.airlines", mode="overwrite", properties=db_properties)
dim_airports.write.jdbc(url=jdbc_url, table="skytracker.airports", mode="overwrite", properties=db_properties)

print("Loading data into the database successfully.")
