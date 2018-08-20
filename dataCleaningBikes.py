from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import argparse
import csv

# Get input/output files from user
parser = argparse.ArgumentParser()
parser.add_argument('bigone', help='Centerline fixed file')
args = parser.parse_args()

# Setup Spark
conf = SparkConf().setAppName("makeSeperateFiles")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

bigone = sqlContext.read.csv(args.bigone,header=True,inferSchema=True)
#fixAddress(bigone)
bigone = bigone.withColumn("month", bigone.startime[0:2]
bigone = bigone.withColumn("year", bigone.startime[3:7]
bigone = bigone.withColumn("startHour", bigone.startime[12:14]
                           
bigone = bigone.filter(bigone.duration < 1)
bigone = bigone.filter(bigone.startTime < 1)
                           
bigone = bigone.filter(bigone.stopTime < 1)
bigone = bigone.filter(bigone.startId < 1)
                           
bigone = bigone.filter(bigone.endId < 1)
bigone = bigone.filter(bigone.endName < 1)
                           
                           
bigone = bigone.drop('gender').collect()
bigone = bigone.drop('birth year').collect()
bigone = bigone.drop('usertype').collect()
               
                           
#not sure where the final part went
                           
bigone.write.csv("/data/bigone")
                           
                           