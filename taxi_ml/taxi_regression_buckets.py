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

def get_labeled_point(row):
    feature_list = row['features'].toArray()
    return LabeledPoint(float(row['fare_amount']), feature_list)


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

    df = df.filter((df.pickup_longitude < -73.75) & (df.pickup_longitude > -74.05) & (df.dropoff_longitude < -73.75) & (df.dropoff_longitude > -74.05))
    df = df.filter((df.pickup_latitude < 40.9) & (df.pickup_latitude > 40.6) &(df.dropoff_latitude < 40.9) & (df.dropoff_latitude > 40.6))

    discretizer1 = QuantileDiscretizer(numBuckets=100, inputCol="pickup_latitude", outputCol="pickup_latitude_bucket")
    discretizer2 = QuantileDiscretizer(numBuckets=100, inputCol="pickup_longitude", outputCol="pickup_longitude_bucket")
    discretizer3 = QuantileDiscretizer(numBuckets=100, inputCol="dropoff_latitude", outputCol="dropoff_latitude_bucket")
    discretizer4 = QuantileDiscretizer(numBuckets=100, inputCol="dropoff_longitude", outputCol="dropoff_longitude_bucket")
    result = discretizer1.fit(df).transform(df)
    result = discretizer2.fit(result).transform(result)
    result = discretizer3.fit(result).transform(result)
    result = discretizer4.fit(result).transform(result)


    vecAssembler3 = VectorAssembler(inputCols=["pickup_latitude_bucket", "pickup_longitude_bucket", "dropoff_latitude_bucket", "dropoff_longitude_bucket"], outputCol="features")
    transformed = vecAssembler3.transform(result)
    # cluster_df = transformed.select("pickup_latitude","pickup_longitude","predction_pickup")
    # cluster_df.write.format("com.databricks.spark.csv").option("header", "true").save("file.csv")
    transformed = transformed.select("features", "fare_amount")



    labeled_rdd = transformed.rdd.map(lambda x: get_labeled_point(x))

    for row in labeled_rdd.collect():
        print(row)
    training_data, test_data = labeled_rdd.randomSplit([0.8, 0.2])
    model = LinearRegressionWithSGD.train(training_data, iterations=100, step=0.2)

    valuesAndPredsTraining = training_data.map(lambda p: (float(model.predict(p.features)), p.label))
    valuesAndPreds = test_data.map(lambda p: (float(model.predict(p.features)), p.label))

    trainingMetrics = RegressionMetrics(valuesAndPredsTraining)
    metrics = RegressionMetrics(valuesAndPreds)


    print("RMSE = ", metrics.rootMeanSquaredError," Explained Variance = ", metrics.explainedVariance, " RMSE Training = ", trainingMetrics.rootMeanSquaredError)

if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='File to load taxi data from')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("taxi_regression")
    sc = SparkContext(conf=conf)

    taxi_regression(sc, args.input)
