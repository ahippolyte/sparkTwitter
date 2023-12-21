from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import json
from collections import Counter

#-------------- Initialize Spark ---------------
conf = SparkConf().setAppName("Twitter-HastagFriends")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession(sc)

print("_______________________________________")
# ----------------------------------------------

df = sc.textFile("../data/tiny_twitter.json")

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

def toCounter(x):
	count_dict = Counter(x)
	del count_dict[x[0]]
	return count_dict

def SplitHashtags(x):
	combination = []
	for hashtag in x:
		tags = x.copy()
		tags.remove(hashtag)
		combination.append((hashtag, list(dict(Counter(tags)).items())))
	return combination

hashtagFriends = filtered.map(lambda x: get_hashtags(x))	\
						 .map(lambda x: SplitHashtags(x))	\
						 .flatMap(lambda x: x)				\
						 .reduceByKey(lambda a, b: a + b)	\

num = hashtagFriends.count()

print(hashtagFriends.top(num))

# associatedHashtags.coalesce(1).saveAsTextFile("output_directory")

# x = ["One", "Two", "Three", "Four"]
# print(SplitHashtags(x))