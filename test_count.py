from pyspark.sql import SparkSession
from spark import get_spark

spark = get_spark()

print("Testing simple count: " + str(spark.range(100).count()))

spark.stop()