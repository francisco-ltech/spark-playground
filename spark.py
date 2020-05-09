from pyspark.sql import SparkSession
from functools import lru_cache

@lru_cache(maxsize = None)
def get_spark(appName = "local"):
    return (SparkSession
            .builder
            .appName(appName)
            .master("local[*]")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate())