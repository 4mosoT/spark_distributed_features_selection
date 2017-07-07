

class Partitioner:

    def horizontal_partition(self, rdd, numParts):
        #Return a horizontally partitioned RDD -> RDD[(partitionNumber, row)]

        #GroupingByKey to apply an index for each row belonging to a key, then assign each index
        # to a particular partition
        return rdd.map(lambda row: (row[len(row) - 1], row)).groupByKey() \
            .flatMap(lambda key_iterable: enumerate(key_iterable[1])) \
            .map(lambda index_row: (index_row[0] % numParts.value, index_row[1]))