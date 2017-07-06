from pyspark.sql import SparkSession
import sklearn.feature_selection as fs
import numpy as np
import sys

# TODO: Parse arguments

ss = SparkSession.builder.appName("hsplit").master("local[*]").getOrCreate()

dataframe = ss.read.option("maxColumns", "30000").csv(sys.argv[1])
dataframe2 = dataframe.select([c for c in dataframe.columns[22200:]])
input = dataframe2.rdd
print(len(dataframe2.columns))
numParts = 8
br_numParts = ss.sparkContext.broadcast(numParts)

# First we flatMap and get index for each key, then we assign every index to a partition
partitioned = input.map(lambda row: (row[len(row) - 1], row)).groupByKey() \
    .flatMap(lambda key_iterable: enumerate(key_iterable[1])) \
    .map(lambda index_row: (index_row[0] % br_numParts.value, index_row[1]))


def funcion(s):
    iterable = s[1]
    classes = []
    features = []
    for x in iterable:
        classes.append(x[len(x) - 1])
        features.append(x[:len(x) - 1])
    classes = np.array(classes).astype(np.float)
    features = np.array(features).astype(np.float)
    sel = fs.SelectKBest(fs.chi2).fit(features, classes)
    return sel.get_support(True)


print(partitioned.groupByKey().map(funcion).take(10))
