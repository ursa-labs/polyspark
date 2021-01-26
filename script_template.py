from pyspark.sql import SparkSession
# add additional imports here if needed

spark = SparkSession.builder.getOrCreate()
#
# add your code here
#
spark.stop()
