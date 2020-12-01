import numpy as np
import pandas as pd
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

S3_HEADER = 's3://'
S3_BUCKET = '/data_eng_jun'
TEMP_FILE_PATH = '/raw_data/GlobalLandTemperaturesByCity.csv'

df_temp = spark.read.format('csv').load(S3_HEADER+S3_BUCKET+TEMP_FILE_PATH, header=True, inferSchema=True)

df_temp_transformed = df_temp.filter("Country = 'United States'").\
    selectExpr('dt as date', 'AverageTemperature as avg_temp', 
                'AverageTemperatureUncertainty as temp_std', 'city as city',
                'Country as country')


df_temp_transformed.write.csv(S3_HEADER+S3_BUCKET+'/transformed_data/us_city_temp.csv')