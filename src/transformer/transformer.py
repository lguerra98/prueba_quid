from pyspark.sql.functions import regexp_extract, col, to_timestamp, trim, substring, regexp_replace
from abc import ABC, abstractmethod
import findspark

findspark.init()

class DataTransformer(ABC):
    @abstractmethod
    def transform(self, *dfs):
        
        df_film, df_inventory, df_rental, df_customer, df_store = dfs

        df_film = df_film.drop('original_language_id')

        return df_film, df_inventory, df_rental, df_customer, df_store



class Transformer(DataTransformer):

    def transform(self, *dfs):

        df_film, df_inventory, df_rental, df_customer, df_store = super().transform(dfs)

        cols_n = ["film_id",
        "release_year",
        "language_id",
        "rental_duration",
        "rental_rate",
        "length",
        "replacement_cost",
        "num_voted_users"]

        cols_s = ['title',
        'description',
        'rating',
        'special_features']


        for c in cols_n:

            if c in ["release_year", "language_id", "rental_duration", "length", "num_voted_users", "film_id"]:

                df_film = df_film.withColumn(c, regexp_extract(c, "\d{1,}\.{0,}\d{0,}", 0)).withColumn(c, col(c).cast("int"))

            else:

                df_film = df_film.withColumn(c, regexp_extract(c, "\d{1,}\.{0,}\d{0,}", 0)).withColumn(c, col(c).cast("double"))

            for c in cols_s:
                
                df_film = df_film.withColumn(c, trim(c))
                


        df_film = df_film.drop_duplicates()

        df_film = df_film.withColumn("last_update", trim("last_update")).withColumn("last_update", to_timestamp("last_update", "yyyy-MM-dd HH:mm:ss"))
        df_film = df_film.withColumn("description", substring("description", 3, 1000))
        
        df_inventory = df_inventory.withColumn("store_id", regexp_extract("store_id", "\d{0,}", 0)).withColumn("store_id", col("store_id").cast("int"))

        df_inventory = df_inventory.withColumn("last_update", trim("last_update")).withColumn("last_update", to_timestamp("last_update", "yyyy-MM-dd HH:mm:ss"))

        cols_s = ["inventory_id", "film_id"]

        for c in cols_s:
            df_inventory = df_inventory.withColumn(c, col(c).cast("int"))
        
        cols_t = ['rental_date', 'return_date', 'last_update']

        cols_n = ["rental_id", "inventory_id", "customer_id", "staff_id"]

        for c in cols_t:
            df_rental = df_rental.withColumn(c, trim(c)).withColumn(c, to_timestamp(c, "yyyy-MM-dd HH:mm:ss"))

        for c in cols_n:
            df_rental = df_rental.withColumn(c, col(c).cast("int"))


        cols_s = ['first_name',
        'last_name',
        'email',
        'customer_id_old',
        'segment']

        cols_n = ["customer_id",
        "store_id",
        "address_id",
        "active"]

        for c in cols_n:
            df_customer = df_customer.withColumn(c, col(c).cast("int"))

        for c in cols_s:  
                df_customer = df_customer.withColumn(c, trim(c))

        for c in ["create_date", "last_update"]:

            df_customer = df_customer.withColumn(c, trim(c)).withColumn(c, to_timestamp(c, "yyyy-MM-dd HH:mm:ss"))

        val = val = df_customer.groupBy("segment").count().orderBy(col("count").desc()).filter("segment != 'NULL'").orderBy(col("count").desc()).limit(1).collect()[0]["segment"]

        df_customer = df_customer.withColumn("segment", regexp_replace("segment", "NULL", val))
        df_customer = df_customer.withColumn("customer_id_old", regexp_replace("customer_id_old", "NULL", "Not Aplicable"))


        df_store = df_store.withColumn("last_update", trim("last_update")).withColumn("last_update", to_timestamp("last_update", "yyyy-MM-dd HH:mm:ss"))

        return df_film, df_inventory, df_rental, df_customer, df_store

