import sys, json
if len(sys.argv) > 1:
    args = json.loads(sys.argv[1].replace('\\"', '"'))

from pyspark.sql import SparkSession
# add additional imports here if needed

spark = SparkSession.builder.getOrCreate()
#
# add your code here
# **kwargs passed to run_on_spark() are in args
#
spark.stop()
