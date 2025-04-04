import findspark
from pyspark.sql import SparkSession
import pandas as pd
findspark.init()

spark = SparkSession.builder.getOrCreate()

df_ax = pd.read_excel("Films_2.xlsx", sheet_name="film", index_col="film_id")

df = spark.createDataFrame(df_ax)

df.show(5)