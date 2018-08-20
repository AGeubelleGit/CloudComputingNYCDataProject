from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import argparse
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import datetime


"""
Arguments:
    sc - the spark context
    sqlContext - the sql Context (for creating dataframes)
    file_path - the file path for the csv file containing the data.
Returns: 
    A dataframe of parking data with columns:
    PlateId, IssueDate, StreetName, HouseNumber, ViolationTime, IssuerCode, MeterNo
"""
def load_parking_dataset(sc, sqlContext, file_path=None):
    default_path = "hdfs:///projects/group12/ParkingData/Parking_Violations_Issued_-_Fiscal_Year_2014__August_2013___June_2014_.csv"
    if file_path is None:
        file_path = default_path

    parking_dataframe = sqlContext.read.csv(file_path, inferSchema="true", header=True)
    # We are selecting the useful cols that might be needed and naimg them using camel case rather than spaces.
    parking_dataframe = parking_dataframe.select(col("Plate Id").alias("PlateId"), col("Issue Date").alias("IssueDate"), 
                                 col("Street Name").alias("StreetName"), col("House Number").alias("HouseNumber"), 
                                 col("Violation Time").alias("ViolationTime"), col("Issuer Code").alias("IssuerCode"),
                                 col("Meter Number").alias("MeterNo"))
    return parking_dataframe


"""
Arguments:
    sc - the spark context
    sqlContext - the sql Context (for creating dataframes)
    file_path - the file path for the csv file containing the data.
Returns: 
    A dataframe of centerline (street) data with columns:
    L_HIGH_HN (HN = house number), L_LOW_HN, R_HIGH_HN, R_LOW_HN, ST_LABEL, PHYSICALID, 
    the_geom (gemetry of the street lat and longitude)
"""
def load_centerline_dataset(sc, sqlContext, file_path=None):
    default_path = "hdfs:///projects/group12/StreetData/Centerline.csv"
    if file_path is None:
        file_path = default_path

    centerline_dataframe = sqlContext.read.csv(file_path, inferSchema="true", header=True)
    # Select the usefule columns
    centerline_dataframe = centerline_dataframe.select("L_HIGH_HN", "L_LOW_HN", "R_HIGH_HN", "R_LOW_HN", 
                                "ST_LABEL", "PHYSICALID", "the_geom")
    return centerline_dataframe


"""
Arguments:
    sc - the spark context
    sqlContext - the sql Context (for creating dataframes)
    file_path - the file path for the csv file containing the data.
Returns: 
    A dataframe contianing the data for parking meters in NYC.
"""
def load_meters_dataset(sc, sqlContext, file_path=None):
    default_path = "hdfs:///projects/group12/ParkingData/NYCMetersDataset.csv"
    if file_path is None:
        file_path = default_path

    meters_dataframe = sqlContext.read.csv(file_path, inferSchema="true", header=True)
    return meters_dataframe


"""
Arguments:
    sc - the spark context
    sqlContext - the sql Context (for creating dataframes)
    ticket_file_path - the file path for the csv file containing the parking ticket data.
    centerline_file_path - the file path for the centerline(street data) csv file.
    output_directory - the file path for the directory where the spark will output the csv parts.
Returns: 
    In a provided directory, spark will output a set of csv file parts with two columns:
    1. The geometry of the street
    2. The number of tickets given out on this street
Performance:
    This function joins two datasets, one of size ~3GB and another of size ~50MB
    Because we do an inner join as well as other operations, this function takes some time
    It took around 73.287493 seconds which is ~40MB/Second
"""
def map_parking_tickets_to_centerline_locations(sc, sqlContext, 
            ticket_file_path=None, centerline_file_path=None, output_directory=None):
    default_ticket_path = "hdfs:///projects/group12/ParkingData/Parking_Violations_Issued_-_Fiscal_Year_2014__August_2013___June_2014_.csv"
    if ticket_file_path is None:
        ticket_file_path = default_ticket_path

    default_centerline_path = "hdfs:///projects/group12/StreetData/Centerline.csv"
    if centerline_file_path is None:
        centerline_file_path = default_centerline_path

    default_output_path = "hdfs:///projects/group12/tickets_to_streets"
    if output_directory is None:
        output_directory = default_output_path


    centerline_df=load_centerline_dataset(sc, sqlContext, centerline_file_path)
    tickets_df = load_parking_dataset(sc, sqlContext, ticket_file_path)
    # Cast the house number boundary columns to be integers
    centerline_df=centerline_df.withColumn("L_LOW_HN", centerline_df["L_LOW_HN"].cast(IntegerType()))
    centerline_df=centerline_df.withColumn("L_HIGH_HN", centerline_df["L_HIGH_HN"].cast(IntegerType()))
    centerline_df=centerline_df.withColumn("R_LOW_HN", centerline_df["R_LOW_HN"].cast(IntegerType()))
    centerline_df=centerline_df.withColumn("R_HIGH_HN", centerline_df["R_HIGH_HN"].cast(IntegerType()))
    # Join the two datasets with an inner join such that each ticket matches up to a street such that:
    # The street names match and the tickt's house number is within the range of the street house numbers.
    joined_df = tickets_df.join(centerline_df, (centerline_df.ST_LABEL == tickets_df['StreetName'])
                             & (((tickets_df['HouseNumber'] >= centerline_df.R_LOW_HN) & (tickets_df['HouseNumber'] <= centerline_df.R_HIGH_HN))
                                | ((tickets_df['HouseNumber'] >= centerline_df.L_LOW_HN) & (tickets_df['HouseNumber'] <= centerline_df.L_HIGH_HN))), "inner")

    # Group our new dataframe and count so that we have one point for each street and a value which is
    # in the column "count" and is equal to the number of tickets on the street.
    grouped = joined_df.groupBy("PHYSICALID", "L_LOW_HN", "L_HIGH_HN", "R_LOW_HN", "R_HIGH_HN", "ST_LABEL", "the_geom").count()
    # Select the columns used for latitude/longitude and the count
    grouped = grouped.select("the_geom", "count")
    grouped.write.csv(output_directory)
    # If we are in hdfs, you can group the directory of csv files into one csv file by:
    # hdfs dfs -getmerge /user/ageubell/yelp_clustering/kmeans.csv kmeans.csv


if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()
    parser.add_argument('--parking_file', help='File path to load parking ticket data from')
    parser.add_argument('--centerline_file', help='File path to load cetnerline data from')
    parser.add_argument('--output_directory', help='File path for folder that will contain the multiple parts of output csv')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("Parking Data Mapping to Location")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    map_parking_tickets_to_centerline_locations(sc, sqlContext, args.parking_file, args.centerline_file, args.output_directory)



# END OF FILE