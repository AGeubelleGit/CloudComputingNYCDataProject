from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import argparse
import csv

# Get input/output files from user
parser = argparse.ArgumentParser()
parser.add_argument('bigone', help='Centerline fixed file')
parser.add_argument('centerline', help='Centerline fixed file')
args = parser.parse_args()

# Setup Spark
conf = SparkConf().setAppName("getLatitudes")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

bigone = sqlContext.read.csv(args.bigone,header=True,inferSchema=True)
#fixAddress(bigone)

lats = sqlContext.read.csv(args.centerline,header=True,inferSchema=True)
table.printSchema()
#fixAddress(lats)
comb = sqlContext.sql("SELECT bigone.starttime, bigone.endtime, bigone.routes, lats.latitude, lats.longtitude FROM bigone INNER JOIN lats ON bigone.address=review.address")

#I already fixed the address formats using a VBA script in my initial local stuff, trust me you dont want to see that VBA is nasty
#but i might include it
comb.printSchema()
filter1 = comb.filter(comb.latitude < 50)
filter2 = filter1.filter(filter1.latitude < 50)

bigone = filter2
bigone.write.csv("/data/bigone")



#arbitrary location values they are sometimes less than the shuold be 
