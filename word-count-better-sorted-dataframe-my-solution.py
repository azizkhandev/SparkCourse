from pyspark.sql import SparkSession
from pyspark.sql import functions as func
import re

spark = SparkSession.builder.appName("WordCountSorted").getOrCreate()

inputDF = spark.read.text("file:///SparkCourse/book")

words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
words.filter(words.word != "")

lowercaseWords = words.select(func.lower(words.word).alias("word"))

wordCounts = lowercaseWords.groupBy("word").count()

wordCountsSorted = wordCounts.sort("count")

wordCountsSorted.show(wordCountsSorted.count())

spark.stop()