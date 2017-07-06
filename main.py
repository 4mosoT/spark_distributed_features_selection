from pyspark.sql import SparkSession
import sklearn.feature_selection as fs
import sklearn.preprocessing as pre
import numpy as np
import sys



# TODO: Parse arguments

ss = SparkSession.builder.appName("hsplit").master("local[*]").getOrCreate()

dataframe = ss.read.option("maxColumns", "30000").csv(sys.argv[1])
for column in dataframe.columns[:22200]:
    dataframe = dataframe.drop(column)
input = dataframe.rdd
print(len(dataframe.columns))
numParts = 8
br_numParts = ss.sparkContext.broadcast(numParts)

# First we flatMap and get index for each key, then we assign every index to a partition
partitioned = input.map(lambda row: (row[len(row) - 1], row)).groupByKey()\
    .flatMap(lambda key_iterable: enumerate(key_iterable[1]))\
    .map(lambda index_row: (index_row[0] % br_numParts.value, index_row[1]))

def funcion(s):
    iterable = s[1]
    classes = []
    features = []
    for x in iterable:
        classes.append(x[len(x)-1])
        features.append(x[:len(x)-1])
    classes = np.asarray(classes)
    features = np.asarray(features)
    #sel = fs.SelectKBest(fs.chi2).fit(features, classes)
    return (classes.shape, features.shape)

print(partitioned.groupByKey().map(funcion).take(10))

