import numpy as np
import pandas as pd
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from us_states_abbrev import us_states_abbrev

S3_HEADER = 's3://'
S3_BUCKET = '/data_eng_jun'
DEMO_FILE_PATH = '/raw_data/us-cities-demographics.csv'

df_demo = spark.read.format('csv').load(S3_HEADER+S3_BUCKET+DEMO_FILE_PATH, header=True, inferSchema=True, sep=';')

df_demo.createOrReplaceTempView("demo_table")

df_demo_transformed = spark.sql("""
    SELECT DISTINCT City AS city, State As state, `Median Age` AS median_age, `Male Population`/`Total Population` AS male_pct,
        `Female Population`/`Total Population` AS female_pct, `Number of Veterans`/`Total Population` AS veteran_pct,
        `Foreign-born`/`Total Population` AS foreigner_pct
    FROM demo_table
""")

abbreviate_state = udf(lambda x: us_state_abbrev[x], StringType())

# convert full state name into abbreviation
df_demo_transformed = df_demo_transformed.withColumn('state', abbreviate_state('state'))

df_demo_combo.write.csv(S3+HEADER+S3_BUCKET+'/transformed_data/us_city_demographics.csv')