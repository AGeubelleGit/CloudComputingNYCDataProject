from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import argparse
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import datetime
from pyspark.sql.functions import split
from pyspark.sql.functions import lit
#Prepare data from output to csv
#https://stackoverflow.com/questions/40426106/spark-2-0-x-dump-a-csv-file-from-a-dataframe-containing-one-array-of-type-string
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


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
Used to turn our arrays into strings so that they can be printed to csv.
"""
def array_to_string(my_list):
    return ','.join([str(elem) for elem in my_list])


"""
Arguments:
    sc - the spark context
    sqlContext - the sql Context (for creating dataframes)
    latitude - center latitude where we searh from 
    longitude - center longitude where we searh from 
    min_range - minimum range for which parking meters must be inside.
    ticket_file_path - the file path for the csv file containing the parking ticket data.
    centerline_file_path - the file path for the centerline(street data) csv file.
    output_directory - the file path for the directory where the spark will output the csv parts.
Returns: 
    In a provided directory, spark will output a set of csv file parts with 4 columns:
    1. The number of tickets given to cars at this specific parking meter
    2. String version of parking meter's latitude and longitude in format "<lat>,<long>"
    3. Latitude of the center(user)
    4. Longitude of the center(user)
Performance:
    
"""
def get_parking_meters_near_me(sc, sqlContext, latitude, longitude, min_range=.1,
            ticket_file_path=None, meters_file_path=None, output_directory=None):
    default_ticket_path = "hdfs:///projects/group12/ParkingData/Parking_Violations_Issued_-_Fiscal_Year_2014__August_2013___June_2014_.csv"
    if ticket_file_path is None:
        ticket_file_path = default_ticket_path

    default_meters_path = "hdfs:///projects/group12/ParkingData/NYCMetersDataset.csv"
    if meters_file_path is None:
        meters_file_path = default_meters_path

    default_output_path = "hdfs:///projects/group12/meters_near_me_output"
    if output_directory is None:
        output_directory = default_output_path

    default_min_range = 0.1
    if min_range is None:
        min_range = default_min_range

    tickets_df = load_parking_dataset(sc, sqlContext, ticket_file_path)
    meters_df = load_meters_dataset(sc, sqlContext, meters_file_path)

    # We need to replace the "-" character in the meterNo. and remove all rows without a meter number.
    tickets_df = tickets_df.withColumn("MeterNo", regexp_replace('MeterNo', '-', ''))
    tickets_df = tickets_df[tickets_df["MeterNo"] != ""]

    # Do some transformations on the "the_geom" column to access the actual latitude and longitude vals.
    curr_meters_df = meters_df.withColumn("LatLon", regexp_replace('the_geom', 'POINT \(', ''))
    curr_meters_df = curr_meters_df.withColumn("LatLon", regexp_replace('LatLon', '\)', ''))
    curr_meters_df = curr_meters_df.withColumn("LatLon", split("LatLon", " "))
    # Filter the dataframe to only contain meters withing .1 latitude and longitude value of our center
    curr_meters_df = curr_meters_df.filter( ( abs(curr_meters_df["LatLon"][0] - latitude) <= min_range ) & 
                      ( abs(curr_meters_df["LatLon"][1] - longitude) <= min_range ) )
    
    # Join the curr_meters_dataframe with the tickets dataframe to get tickets at each specific parking meter.
    joined_df = curr_meters_df.join(tickets_df, "MeterNo", "inner")
    # By doing this join, we access only the "MeterNo" and "LatLon" columns of our dataframe.
    selection = joined_df.join(curr_meters_df, ["MeterNo", "LatLon"], "right")
    # Now we group by these two columns so that we can aggregate and count the number of tickets at each meter.
    selection = selection.groupBy("MeterNo", "LatLon").count()
    # Add two constant columns which are the latitude and longitude of the center.
    selection = selection.withColumn("Center_Lat", lit(latitude))
    selection = selection.withColumn("Center_Long", lit(longitude))

    # We are not allowed to output a column that is made up of arrays, so we need to change the LatLon Column.
    array_to_string_udf = udf(array_to_string,StringType())
    output_df = selection.withColumn('StringLatLon',array_to_string_udf(selection["LatLon"]))
    # Now that we have a string version of the "LatLon" column, remove that column
    output_df = output_df.drop("LatLon")

    # Print output dataframe to csv.
    output_df.write.csv(output_directory)


if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()
    
    parser.add_argument('latitude', help='Latitude in NYC for center of search (suggest between -74.016685 and -73.915438)', type=float)
    parser.add_argument('longitude', help='Longitude in NYC for cetner of search (suggest between 40.703957 and 40.798414)', type=float)
    parser.add_argument('--minimum_range', help='The range in which the meters must be from the cetner (in terms of lat and lon)', type=float)
    parser.add_argument('--parking_file', help='File path to load parking ticket data from')
    parser.add_argument('--parking_meters_file', help='File path to load parking meters data from')
    parser.add_argument('--output_directory', help='File path for folder that will contain the multiple parts of output csv')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("Locate Parking Meters Near Users")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    get_parking_meters_near_me(sc, sqlContext, latitude=args.latitude, longitude=args.longitude, min_range=args.minimum_range, ticket_file_path=args.parking_file, meters_file_path=args.parking_meters_file, output_directory=args.output_directory)


# END OF FILE