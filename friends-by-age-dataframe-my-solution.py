from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

lines = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///SparkCourse/fakefriends-header.csv")

friendsByAge = lines.select("age", "friends")

friendsByAge.createOrReplaceTempView("friends")

friends_by_age = spark.sql("SELECT age, AVG(friends) FROM friends GROUP BY age ORDER BY age")

for age in friends_by_age.collect():
    print(age)

#An alternative way to using SQL statements
friendsByAge.groupBy("age").avg("friends").sort("age").show(n=100)

friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show(n=100)

friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("friends_avg")).sort("age").show(n=100)