import numpy as np
import pandas as pd
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

S3_HEADER = 's3://'
S3_BUCKET = 'data-eng-jun'
IMMIGRATION_FILE_PATH = '/raw_data/I94_DATA/i94_apr16_sub.sas7bdat'

df_immigration = spark.read.format("com.github.saurfang.sas.spark")\
    .load(S3_HEADER + S3_BUCKET + IMMIGRATION_FILE_PATH)

df_immigration_transformed = df_immigration.selectExpr('cast(cicid as int)', 'cast(i94yr as int) as entry_year',
             'cast(i94mon as int) as entry_month', 'cast(i94res as int) as origin_country_code', 
             'i94port as destination_port_code', 'cast(i94mode as int)', 'i94bir as age', 'gender', 'visatype')


df_immigration_transformed.write.partitionBy('entry_year', 'entry_month').parquet(S3_HEADER+S3_BUCKET+'/transformed_data/immigration')