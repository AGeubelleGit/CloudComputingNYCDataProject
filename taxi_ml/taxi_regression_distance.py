from pyspark import SparkContext, SparkConf
import argparse

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from pyspark.ml.feature import VectorAssembler

from numpy import array
import numpy as np

from pyspark.ml.feature import QuantileDiscretizer

from pyspark.sql import Column

def pythagorean(row):
        return ((float(row['pickup_latitude']) - float(row['dropoff_latitude']))**2 + (float(row['pickup_longitude']) - float(row['dropoff_longitude']))**2)**0.5

def get_labeled_point(row):
    # row['prediction_dropoff']]
    return (float(row['fare_amount']), [pythagorean(row)])


def print_func(row):
    print(row)


def taxi_regression(sc, filename):
    '''
    Args:
        sc: The Spark Context
        filename: Filename of the Amazon reviews file to use, where each line represents a review    
    '''

    sqlContext = SQLContext(sc)
    df = sqlContext.read.load(filename, 
                          format='com.databricks.spark.csv', 
                          header='true', 
                          inferSchema='true').sample(False, 0.001)

    # Limit Data: longitude > 40.6 and longitude < 40.9 and latitude < -73.75 and latitude > -74.05
    df = df.filter((df.pickup_longitude < -73.75) & (df.pickup_longitude > -74.05) & (df.dropoff_longitude < -73.75) & (df.dropoff_longitude > -74.05))
    df = df.filter((df.pickup_latitude < 40.9) & (df.pickup_latitude > 40.6) &(df.dropoff_latitude < 40.9) & (df.dropoff_latitude > 40.6))

    labeled_rdd = df.rdd.map(lambda x: get_labeled_point(x))
    labeled_rdd = labeled_rdd.filter(lambda row: row[0] > 0.0 and row[0] < 60 and row[1][0] < 0.21 and row[1][0] > 0.0)

    # For Graphing:
    # df = labeled_rdd.map(lambda row: (row[0], row[1][0])).toDF()
    # df.write.format("com.databricks.spark.csv").option("header", "true").save("distance_fare.csv")



    labeled_rdd = labeled_rdd.map(lambda row: LabeledPoint(row[0], row[1]))

    # Build model
    training_data, test_data = labeled_rdd.randomSplit([0.8, 0.2])
    model = LinearRegressionWithSGD.train(training_data, intercept=True)

    valuesAndPredsTraining = training_data.map(lambda p: (float(model.predict(p.features)), p.label))
    valuesAndPreds = test_data.map(lambda p: (float(model.predict(p.features)), p.label))

    trainingMetrics = RegressionMetrics(valuesAndPredsTraining)
    metrics = RegressionMetrics(valuesAndPreds)

    # for row in labeled_rdd.collect():
    #     print("distance: " + str(row.features) + " actual fare: " + str(row.label) + " predcted fare: " + str(model.predict(row.features)))
    print("RMSE = ", metrics.rootMeanSquaredError," Explained Variance = ", metrics.explainedVariance, " RMSE Training = ", trainingMetrics.rootMeanSquaredError)

def toCSVLine(data):
  return ','.join(str(d) for d in data)

if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='File to load taxi data from')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("taxi_regression")
    sc = SparkContext(conf=conf)

    taxi_regression(sc, args.input)