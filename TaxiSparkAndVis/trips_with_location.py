from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import dayofmonth, month, year, mean, hour
import argparse
import sys

def setup_table(sc, sqlContext, filename):
    df = sqlContext.read.csv(filename, header=True, inferSchema=True)
    sqlContext.registerDataFrameAsTable(df, 'trips')

def daily_trips_query(sc, sqlContext):
    trip_data = sqlContext.sql("SELECT tpep_pickup_datetime, pickup_longitude, pickup_latitude FROM trips")
    return trip_data.filter(hour(trip_data.tpep_pickup_datetime) < 1)

if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='File to load Yelp review data from')
    parser.add_argument('output', help='File to save RDD to')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("taxi_data_aggregates")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    setup_table(sc, sqlContext, args.input)
    results = daily_trips_query(sc, sqlContext)
    results.write.csv('results')
