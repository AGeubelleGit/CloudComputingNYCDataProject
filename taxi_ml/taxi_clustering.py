from pyspark import SparkContext, SparkConf
import argparse

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.sql import functions as F

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint

from pyspark.mllib.evaluation import RegressionMetrics

from numpy import array
import numpy as np


from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans


def get_labeled_point(row):
    feature_list = row['features'].toArray()
    # row['prediction_dropoff']]
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
                          inferSchema='true').sample(False, 0.033)

    df = df.filter(df.pickup_latitude != 0.0)
    df = df.filter(df.pickup_longitude != 0.0)
    df = df.filter(df.dropoff_latitude != 0.0)
    df = df.filter(df.dropoff_longitude != 0.0)

    vecAssembler1 = VectorAssembler(inputCols=["pickup_latitude", "pickup_longitude"], outputCol="features1")
    # vecAssembler2 = VectorAssembler(inputCols=["dropoff_latitude","dropoff_longitude"], outputCol="features2")

    df = vecAssembler1.transform(df)
    df = vecAssembler2.transform(df)

    kmeans_pickup = KMeans(k= 20, seed=1, predictionCol= "predction_pickup", featuresCol="features1")
    # kmeans_dropoff = KMeans(k=5, seed=1, predictionCol="prediction_dropoff", featuresCol="features2")

    model1 = kmeans_pickup.fit(df.select('features1'))

    # Double clustering for regression:
    # model2 = kmeans_dropoff.fit(df.select('features2'))

    transformed = model1.transform(df)
    # transformed = model2.transform(transformed)

    transformed.show()
    vecAssembler3 = VectorAssembler(inputCols=["predction_pickup", "prediction_dropoff"], outputCol="features")
    transformed = vecAssembler3.transform(transformed)
    cluster_df = transformed.select("pickup_latitude","pickup_longitude","predction_pickup")
    cluster_df.write.format("com.databricks.spark.csv").option("header", "true").save("file.csv")
    transformed = transformed.select("features", "fare_amount")


    labeled_rdd = transformed.rdd.map(lambda x: get_labeled_point(x))

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
    conf = SparkConf().setAppName("taxi_clustering")
    sc = SparkContext(conf=conf)

    taxi_regression(sc, args.input)

