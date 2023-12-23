from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import json
import time as tm

#-------------- Initialize Spark ---------------
conf = SparkConf().setAppName("Twitter-Top20HashTags")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession(sc)

print("_______________________________________")
# ----------------------------------------------

# df = sc.textFile("/user/fzanonboito/CISD/tiny_twitter.json")
df = sc.textFile("/user/fzanonboito/CISD/smaller_twitter.json")
# df = sc.textFile("/user/auber/data_ple/tweets/")

# my_line is a string in jsonnl format, my_dict will be a dictionary
filtered = df.map(lambda x: json.loads(x)) \
    		 .filter(lambda x: 'entities' in x and 'hashtags' in x['entities'] and len(x["entities"]["hashtags"]) != 0)

def get_hashtags(tweet):
    tags = []
    assert "entities" in tweet
    assert "hashtags" in tweet["entities"]
    for tag in tweet["entities"]["hashtags"]:
        tags.append("#"+tag["text"])
    return tags
    

top20hashtags = filtered.map(lambda x: get_hashtags(x)) \
    		 			.flatMap(lambda x: x)			\
			 			.map(lambda x: (x, 1))			\
             			.reduceByKey(lambda a, b: a+b)	\
             			.map(lambda x: (x[1], x[0]))	\
             		 	# .top(20)

start = tm.perf_counter()
top = top20hashtags.top(20)
end = tm.perf_counter()

print(end-start)