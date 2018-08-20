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
    In 7 seperate provided directories, spark will output a set of csv file parts with two columns:
    1. The geometry of the street
    2. The number of tickets given out on this street
    The 7 directories will only contain the data for tickets that occured on a specific day of the week.
"""
def map_parking_tickets_to_centerline_locations_by_day(sc, sqlContext, 
            ticket_file_path=None, centerline_file_path=None, output_directory=None):
    default_ticket_path = "hdfs:///projects/group12/ParkingData/Parking_Violations_Issued_-_Fiscal_Year_2014__August_2013___June_2014_.csv"
    if ticket_file_path is None:
        ticket_file_path = default_ticket_path

    default_centerline_path = "hdfs:///projects/group12/StreetData/Centerline.csv"
    if centerline_file_path is None:
        centerline_file_path = default_centerline_path

    default_output_path = "hdfs:///projects/group12/TicketsOnDay"
    if output_directory is None:
        output_directory = default_output_path

    # Load in Datasets
    centerline_df=load_centerline_dataset(sc, sqlContext, centerline_file_path)
    tickets_df = load_parking_dataset(sc, sqlContext, ticket_file_path)

    # Create columns for information about day of week.
    tickets_df = tickets_df.withColumn('date_str', from_unixtime(unix_timestamp('IssueDate', 'MM/dd/yyy')))
    tickets_df = tickets_df.withColumn('day_of_week', date_format('date_str', 'u'))
    tickets_df = tickets_df.where(tickets_df.day_of_week.isNotNull())

    # Cast the house number boundary columns to be integers
    centerline_df=centerline_df.withColumn("L_LOW_HN", centerline_df["L_LOW_HN"].cast(IntegerType()))
    centerline_df=centerline_df.withColumn("L_HIGH_HN", centerline_df["L_HIGH_HN"].cast(IntegerType()))
    centerline_df=centerline_df.withColumn("R_LOW_HN", centerline_df["R_LOW_HN"].cast(IntegerType()))
    centerline_df=centerline_df.withColumn("R_HIGH_HN", centerline_df["R_HIGH_HN"].cast(IntegerType()))

    for day_int in range(1, 8):
        # Filter to the current day of the week we are looking at.
        day_df = dataframe[dataframe["day_of_week"] == day_int]
        # Join the two datasets with an inner join such that each ticket matches up to a street such that:
        # The street names match and the tickt's house number is within the range of the street house numbers.
        joined_df = day_df.join(centerline_df, (centerline_df.ST_LABEL == day_df['StreetName'])
                             & (((day_df['HouseNumber'] >= centerline_df.R_LOW_HN) & (day_df['HouseNumber'] <= centerline_df.R_HIGH_HN))
                                | ((day_df['HouseNumber'] >= centerline_df.L_LOW_HN) & (day_df['HouseNumber'] <= centerline_df.L_HIGH_HN))), "inner")
        grouped = joined_df.groupBy("PHYSICALID", "L_LOW_HN", "L_HIGH_HN", "R_LOW_HN", "R_HIGH_HN", "ST_LABEL", "the_geom", "day_of_week").count()
        grouped = grouped.select("the_geom", "count", "day_of_week")
        current_output_dir = output_directory + str(day_int)
        grouped.write.csv(current_output_dir)



"""
Arguments:
    sc - the spark context
    sqlContext - the sql Context (for creating dataframes)
    ticket_file_path - the file path for the csv file containing the parking ticket data.
    centerline_file_path - the file path for the centerline(street data) csv file.
    output_directory - the file path for the directory where the spark will output the csv parts.
Returns: 
    In a single directory of csv parts, this code will write a series of 4 columns:
    1. The geometry of the road segement
    2. How many tickets were given during the current hour of the day
    3. The hour of the day (1-12)
    4. Whether or not it is AM or PM.
"""
def map_parking_tickets_to_centerline_locations_by_hour(sc, sqlContext, 
            ticket_file_path=None, centerline_file_path=None, output_directory=None):
    default_ticket_path = "hdfs:///projects/group12/ParkingData/Parking_Violations_Issued_-_Fiscal_Year_2014__August_2013___June_2014_.csv"
    if ticket_file_path is None:
        ticket_file_path = default_ticket_path

    default_centerline_path = "hdfs:///projects/group12/StreetData/Centerline.csv"
    if centerline_file_path is None:
        centerline_file_path = default_centerline_path

    default_output_path = "hdfs:///projects/group12/TicketsByHour"
    if output_directory is None:
        output_directory = default_output_path

    # Load in Datasets
    centerline_df=load_centerline_dataset(sc, sqlContext, centerline_file_path)
    tickets_df = load_parking_dataset(sc, sqlContext, ticket_file_path)

    # Filter to 2 new colums named Hour and AMPM
    tickets_df = tickets_df.withColumn("AMPM", regexp_replace('ViolationTime', '(\\d+)', ''))
    tickets_df = tickets_df.withColumn("Time", regexp_replace('ViolationTime', '([a-zA-Z]+)', ''))
    tickets_df = tickets_df.withColumn('Hour', tickets_df.Time / 100)
    tickets_df = tickets_df.withColumn('Hour', tickets_df.Hour.cast('int'))
    tickets_df = tickets_df.where(tickets_df.AMPM.isNotNull() & tickets_df.Hour.isNotNull())
    tickets_df = tickets_df[(tickets_df['Hour'] >= 1) & (tickets_df['Hour'] <= 12)]
    tickets_df = tickets_df[(tickets_df['AMPM'] == 'A') | (tickets_df['AMPM'] == 'P')]

    # Cast the house number boundary columns to be integers
    centerline_df=centerline_df.withColumn("L_LOW_HN", centerline_df["L_LOW_HN"].cast(IntegerType()))
    centerline_df=centerline_df.withColumn("L_HIGH_HN", centerline_df["L_HIGH_HN"].cast(IntegerType()))
    centerline_df=centerline_df.withColumn("R_LOW_HN", centerline_df["R_LOW_HN"].cast(IntegerType()))
    centerline_df=centerline_df.withColumn("R_HIGH_HN", centerline_df["R_HIGH_HN"].cast(IntegerType()))

    # Join by street such that the parking ticket was writting within the street segment's range.
    joined_df = tickets_df.join(centerline_df, (centerline_df.ST_LABEL == tickets_df['StreetName'])
                             & (((tickets_df['HouseNumber'] >= centerline_df.R_LOW_HN) & (tickets_df['HouseNumber'] <= centerline_df.R_HIGH_HN))
                                | ((tickets_df['HouseNumber'] >= centerline_df.L_LOW_HN) & (tickets_df['HouseNumber'] <= centerline_df.L_HIGH_HN))), "inner")

    # Join by street segment as well as the hour of the day, 
    # this means we should have about 24 times the number of rows as when we just map tickets to street segments.
    grouped = joined_df.groupBy("PHYSICALID", "L_LOW_HN", "L_HIGH_HN", "R_LOW_HN", "R_HIGH_HN", "ST_LABEL", "the_geom", "Hour", "AMPM").count()
    # Select only the relevant columns which include:
    # 1. the geometry of the road segment, the # of tickets, the hour of the day, and whether it is AM or PM.
    grouped = grouped.select("the_geom", "count", "Hour", "AMPM")
    # Write to .csv file.
    grouped.write.csv(output_directory)


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

    map_parking_tickets_to_centerline_locations_by_day(sc, sqlContext, args.parking_file, args.centerline_file, args.output_directory)
    map_parking_tickets_to_centerline_locations_by_hour(sc, sqlContext, args.parking_file, args.centerline_file, args.output_directory)


# END OF FILE