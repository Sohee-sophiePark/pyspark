from pyspark import SparkConf, SparkContext # mostly initialized in python script
import collections # order

# set config - fundamental start point
# running a local machine only as a master node - there are extensions to local to split it up among multiple cores 
# or running a job on a real cluster using Elastic MapReduce
conf = SparkConf().setMaster("local").setAppName("RatingHistogram")
sc = SparkContext(conf = conf)

# open a textfile and grab it - breaks up lines as a string
path = "C:\\Users\\SophiePark\\Documents\\Git\\project2021\\project2021\\spark-hadoop\\movie-rating-histogram\\data-100k"
lines = sc.textFile(path) # RDD

# extract (map) data - userID | moiveID | ratings | timestamps
ratings = lines.map(lambda x: x.split()[2]) 
print(ratings)


# mapping every sigle row with the function split() and take the third column value
result = ratings.countByValue() # count every occurences for the values in the ratings
print(result)

# print("test3")
# print(sorted(result.items()))

# # sort the values and display the results
# sortedResults = collections.OrderedDict(sorted(result.items()))
# print("test1")
# print(sortedResults)

# for key, value in sortedResults.items():
#     print("%s %i" % (key, value))
 


