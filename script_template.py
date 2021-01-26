from pyspark.sql import SparkSession
# add additional imports here if needed

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    # add your code here

    spark.stop()
