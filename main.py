from pyspark.sql import SparkSession
import sklearn.feature_selection as fs
import numpy as np
import sys
import partitions.partitioner as hr

# TODO: Parse arguments

ss = SparkSession.builder.appName("hsplit").master("local[*]").getOrCreate()

dataframe = ss.read.option("maxColumns", "30000").csv(sys.argv[1])

#Selecting last rows of Gli85 dataset to speed up for testing purposes
dataframe = dataframe.select([c for c in dataframe.columns[22200:]])

input = dataframe.rdd
numParts = 8
br_numParts = ss.sparkContext.broadcast(numParts)

hpartitioner = hr.Partitioner()
partitioned = hpartitioner.horizontal_partition(input, br_numParts)


def funcion(s):
    iterable = s[1]
    classes = []
    features = []
    #Splitting each row in its class and features
    for x in iterable:
        classes.append(x[len(x) - 1])
        features.append(x[:len(x) - 1])
    classes = np.array(classes).astype(np.float)
    features = np.array(features).astype(np.float)
    #Apply selection feature algorithm
    sel = fs.SelectKBest(fs.chi2).fit(features, classes)
    #It returns an array with the index of selected features
    return sel.get_support(True)


print(partitioned.groupByKey().map(funcion).take(10))
