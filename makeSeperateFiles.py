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


morning = bigone.filter(bigone.startHour < 10)
evening = bigone.filter(bigone.startHour > 17)

winter = bigone.filter(bigone.month = 2 | bigone.month = 12 | bigone.month = 1) 
spring = bigone.filter(bigone.month = 3 | bigone.month = 4 | bigone.month = 5) 
summer = bigone.filter(bigone.month = 6 | bigone.month = 7 | bigone.month = 8) 
fall = bigone.filter(bigone.month = 9 | bigone.month = 10 | bigone.month = 11) 

winter.printSchema()
spring.printSchema()
summer.printSchema()
fall.printSchema()


with open('winter.csv, 'wb') as csvfile:
    csv = winter.to_csv()
    
with open('spring.csv, 'wb') as csvfile:
    csv = spring.to_csv()
    
with open('summer.csv, 'wb') as csvfile:
    csv = summer.to_csv()
    
    
with open('fall.csv, 'wb') as csvfile:
    csv = fall.to_csv()
    
    #donze

#arbitrary location values they are sometimes less than the shuold be 