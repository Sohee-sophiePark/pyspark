# Memo from the Course

## RDD - key/value Pair 
    - key / aggregated values
        e.g. (age, # of friends)
    - map pairs of data into RDD
        e.g. totalByAge = rdd.map(lambda x: (x, 1)) 
        - set the original value as a key and 1 as single occurence to be used for aggregation (count / sum)
    

## key/value Actions
    - reduceByKey() - combine values with the same key using function
        e.g. rdd.reduceByKey(lambda x, y: x + y) 
    - groupByKey() - Group values with the same key
    - sortByKey() - sort RDD by key values
    - keys() - create an RDD of just for keys
    - values() - or just for values


## SQL-style JOINS on KEY/VALUEs
    - join / rightOuterJoin / leftOuterJoin / cogroup / subtractByKey
    - TWO Key/value Pairs


## Value only for RDD Transformatoin: Efficient
    - mapValues()
    - flatMapValues()


## Parsing (Mapping) Input Data
    def parseLine(line): # only target age & # of friends
        fields = line.split(',')
        age = int(fields[2])
        numFriends = int(fields[3])
        return (age, numFriends)

    lines = sc.textFile("filePath/friends.csv")
    rdd = lines.map(parseLine)


## Count up Sum of Friends and Numbers of Entries per Age
    totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

    rdd.mapValues(lambda x: (x, 1))
    (33, 385) -> (33, (385, 1))
    (33, 2) -> (33, (2, 1))
    (55, 22) -> (55, (22, 1))

    reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

    (33, (385, 1))
    (33, (2, 1))
    (33, (96, 1))

    -> (33, (387, 2)) 
    -> (33, (483, 3))

    Accumulately adds up all values for unique key: AGGREGATION


## Compute Average
    averageByAge = totalByAge.mapValues(lambda x: x[0]/x[1])
    (33, (483, 3))
    -> (33, (483/3))
    -> (33, 161)


## Collect and Display Results
    results = averagesByAge.collect()
    for result in results:
        print(result)