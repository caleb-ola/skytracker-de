from pyspark.sql.functions import udf
from geopy.distance import geodesic
from pyspark.sql.types import DoubleType


# Defining a udf to calculate accurate distance

@udf(returnType=DoubleType())
def geo_distance_in_km(lat1,lon1, lat2, lon2):
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None

    return geodesic((lat1,lon1), (lat2,lon2)).km


