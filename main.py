from pyspark.sql import SparkSession
import sys


# TODO: Parse arguments

ss = SparkSession.builder.appName("hsplit").master("local[*]").getOrCreate()

dataframe = ss.read.option("maxColumns", "30000").csv(sys.argv[1])
input = dataframe.rdd

numParts = 8
br_numParts = ss.sparkContext.broadcast(numParts)

partitioned = input.map(lambda row: (row[len(row) - 1], row)).groupByKey()\
    .flatMap(lambda key_iterable: enumerate(key_iterable[1]))\
    .map(lambda index_row: (index_row[0] % br_numParts.value, index_row[1]))

print(partitioned.countByKey())
