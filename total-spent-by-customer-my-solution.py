import re
from pyspark import SparkConf, SparkContext

def extractCustomerPricePairs(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

conf = SparkConf().setMaster("local").setAppName("CustomerSpend")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///SparkCourse/customer-orders.csv")
mappedInput = input.map(extractCustomerPricePairs)
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)

results = totalByCustomer.collect()
for result in results:
    print(result)