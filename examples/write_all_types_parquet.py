from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#TODO(ianmcook): capture args and use to set output filename?

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

schema = StructType([
    StructField("ByteType_nullok", ByteType(), True),
    StructField("ByteType_nonull", ByteType(), False),
    StructField("ShortType_nullok", ShortType(), True),
    StructField("ShortType_nonull", ShortType(), False),
    StructField("IntegerType_nullok", IntegerType(), True),
    StructField("IntegerType_nonull", IntegerType(), False),
    StructField("LongType_nullok", LongType(), True),
    StructField("LongType_nonull", LongType(), False),
    StructField("FloatType_nullok", FloatType(), True),
    StructField("FloatType_nonull", FloatType(), False),
    #TODO(ianmcook): continue adding types here
])

#rows contain:
# 0. bottom of range
# 1. top of range
# 2. zero
# 3. null

#TODO(ianmcook): consider adding additional rows with above-range and below-
#range values for some types (like the floating point types) but not for the
#types for which this causes JSON parsing errors

#TODO(ianmcook): continue adding rows here
json = """
[
    {
      "ByteType_nullok": -128,
      "ByteType_nonull": -128,
      "ShortType_nullok": -32768,
      "ShortType_nonull": -32768,
      "IntegerType_nullok": -2147483648,
      "IntegerType_nonull": -2147483648,
      "LongType_nullok": -9223372036854775808,
      "LongType_nonull": -9223372036854775808,
      "FloatType_nullok": -3.40282346638528860e+38,
      "FloatType_nonull": -1.40129846432481707e-45
    },
    {
      "ByteType_nullok": 127,
      "ByteType_nonull": 127,
      "ShortType_nullok": 32767,
      "ShortType_nonull": 32767,
      "IntegerType_nullok": 2147483647,
      "IntegerType_nonull": 2147483647,
      "LongType_nullok": 9223372036854775807,
      "LongType_nonull": 9223372036854775807,
      "FloatType_nullok": 3.40282346638528860e+38,
      "FloatType_nonull": 1.40129846432481707e-45
    },
    {
      "ByteType_nullok": 0,
      "ByteType_nonull": 0,
      "ShortType_nullok": 0,
      "ShortType_nonull": 0,
      "IntegerType_nullok": 0,
      "IntegerType_nonull": 0,
      "LongType_nullok": 0,
      "LongType_nonull": 0,
      "FloatType_nullok": 0.0,
      "FloatType_nonull": 0.0
    },
    {
      "ByteType_nullok": null,
      "ByteType_nonull": null,
      "ShortType_nullok": null,
      "ShortType_nonull": null,
      "IntegerType_nullok": null,
      "IntegerType_nonull": null,
      "LongType_nullok": null,
      "LongType_nonull": null,
      "FloatType_nullok": null,
      "FloatType_nonull": null
    }
]
"""

data = spark.read.schema(schema).json(sc.parallelize([json]))

data.show() # for debugging

#TODO(ianmcook): write data to a Parquet file
#data.write.parquet(...)

spark.stop()
