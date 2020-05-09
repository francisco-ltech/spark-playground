from pyspark.sql import SparkSession
from spark import get_spark

if __name__ == "__main__":
 
    spark =  get_spark()

    l = [('Alice', 30)]
    ar = spark.createDataFrame(l, ['name', 'age'])

    ar.show()

    spark.stop()


