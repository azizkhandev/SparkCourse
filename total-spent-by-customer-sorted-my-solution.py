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
totalByCustomerSorted = totalByCustomer.map(lambda x: (x[1], x[0])).sortByKey()

results = totalByCustomerSorted.collect()
for result in results:
    customerId = result[1]
    totalOrdersPrice = result[0]
    print("(", customerId, "{:.2f}".format(totalOrdersPrice), ")")