'''
Program to remove "stop words" from a given text
Author: Mantha Sai Gopal
Reg.no: 23358
'''

from pyspark.sql import SparkSession

def stopwords_mapper(word):
    if word not in stopwords:
        return (word,1)
    else:
        return (word, 0)

def stopwords_reducer(a,b):
    return (a+b)

def splitter(line):
    return line.split(" ")


logFile = "log.txt"

spark = SparkSession.builder.appName("WordCount").master("local").getOrCreate()
spark.sparkContext.setLogLevel("OFF")

read_file = spark.sparkContext.textFile(logFile)
words = read_file.flatMap(splitter)

stopwords = []
with open('stopwords.txt','r') as file:
    for line in file:
        stopwords.append(line.split()[0])

word_count_mapped = words.map(stopwords_mapper)
word_count_reduced = word_count_mapped.reduceByKey(stopwords_reducer)
temp = word_count_reduced.collect()


for x in temp:
    if x[1] != 0:
        print(x)

spark.stop()
