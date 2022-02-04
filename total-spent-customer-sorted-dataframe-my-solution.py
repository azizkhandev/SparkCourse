from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

spark = SparkSession.builder.appName("TotalSpendByCustomer").getOrCreate()

customerOrderSchema = StructType([ \
                    StructField("customerID", IntegerType(), True), \
                    StructField("productID", IntegerType(), True), \
                    StructField("order_price", FloatType(), True)])

customerDF = spark.read.schema(customerOrderSchema).csv("file:///SparkCourse/customer-orders.csv")
customerDF.printSchema()

#df_modified = df.select("customerID", "order_price")

totalSpendByCustomer = customerDF.groupBy("customerID").sum("order_price").sort("customerID")
totalSpendByCustomer.show()

#round off the results
totalSpendByCustomer = customerDF.groupBy("customerID").agg(func.round(func.sum("order_price"), 2) \
                                                            .alias("total_spent"))
totalSpendByCustomer.sort(totalSpendByCustomer.total_spent.desc()).show()

totalSpendByCustomer.sort(func.col("total_spent").desc()).show()

spark.stop()
