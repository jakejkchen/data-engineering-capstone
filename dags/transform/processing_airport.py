import numpy as np
import pandas as pd
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from us_states_abbrev import us_states_abbrev

S3_HEADER = 's3://'
S3_BUCKET = '/data-eng-jun'
AIRPORT_FILE_PATH = '/raw_data/airport-codes_csv.csv'

df_air = spark.read.format('csv').load(S3_HEADER+S3_BUCKET+AIRPORT_FILE_PATH, header=True, inferSchema=True)

df_air.createOrReplaceTempView("airport_table")


# aggregate airport data by counting the number of differnt types of airports in each city
df_air_transformed = spark.sql("""
SELECT iso_country, RIGHT(iso_region, 2) as state, municipality as city, 
        SUM(CASE WHEN type='small_airport' THEN 1 ELSE 0 END) AS small_airport,
        SUM(CASE WHEN type='medium_airport' THEN 1 ELSE 0 END) AS medium_airport,
        SUM(CASE WHEN type='large_airport' THEN 1 ELSE 0 END) AS large_airport
FROM airport_table
WHERE iso_country = 'US' AND iata_code IS NOT NULL
GROUP BY iso_country, iso_region, municipality
""")



df_air_transformed.write.csv(S3_HEADER+S3_BUCKET+'/transformed_data/us_city_airport_summary.csv')