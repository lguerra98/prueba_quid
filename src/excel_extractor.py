from abc import ABC, abstractmethod
import pandas as pd
import findspark
from pyspark.sql import SparkSession

class DataExtractor(ABC):
    @abstractmethod
    def extract(self):
        pass

class ExcelExtractor(DataExtractor):

    def __init__(self, path):
        self.path = path
    

    def extract(self):

        findspark.init()

        spark = SparkSession.builder\
            .getOrCreate()
        
        for x in ["film", "inventory", "rental", "customer", "store"]:

            df_aux = pd.read_excel(self.path, sheet_name=x)

            df = spark.createDataFrame(df_aux)

            for c in df.columns:

                df = df.withColumnRenamed(c, c.strip())

            globals().update({f"df_{x}":df})

        return df_film, df_inventory, df_rental, df_customer, df_store  # type: ignore

        
