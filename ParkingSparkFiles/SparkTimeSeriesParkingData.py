from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import argparse
from pyspark.sql.functions import *


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
	output_file_path - the file path for where we should output our data
Returns: 
	Prints out two arrays of size 24 to a specified .txt file. 
	The first array will be hours of the day and the second will be the # of tickets in that hour.
Performance:
	This function runs in 15.3452 seconds on a dataset of about 3GB (~195 MB/Second)
"""
def group_parking_tickets_by_time_period(sc, sqlContext, ticket_file_path=None, output_file_path=None):
	default_path = "hdfs:///projects/group12/ParkingData/Parking_Violations_Issued_-_Fiscal_Year_2014__August_2013___June_2014_.csv"
	if ticket_file_path is None:
		ticket_file_path = default_path

	default_output_path = "by_hour.txt"
	if output_file_path is None:
		output_file_path = default_output_path

	dataframe =load_parking_dataset(sc, sqlContext, ticket_file_path)
	# Split up the Violation Time column into two columns: 
	# "AMPM" has either 'A' or 'P' and "Time" has the time in format '0623'
	dataframe = dataframe.withColumn("AMPM", regexp_replace('ViolationTime', '(\\d+)', ''))
	dataframe = dataframe.withColumn("Time", regexp_replace('ViolationTime', '([a-zA-Z]+)', ''))
	# Turn our time column into an integer hour of the day column
	dataframe = dataframe.withColumn('Hour', dataframe.Time / 100)
	dataframe = dataframe.withColumn('Hour', dataframe.Hour.cast('int'))
	# Remove bad entries (AMPM or Hour column is null) or (Hours are not withing 1-12)
	dataframe = dataframe.where(dataframe.AMPM.isNotNull() & dataframe.Hour.isNotNull())
	dataframe = dataframe[(dataframe['Hour'] >= 1) & (dataframe['Hour'] <= 12)]
	dataframe = dataframe[(dataframe['AMPM'] == 'A') | (dataframe['AMPM'] == 'P')]
	# Group tickets by the hour of the day (now have 24 rows "A" + 1-12 andd "P" + 1-12)
	grouped = dataframe.groupBy("AMPM", "Hour").count()
	rows = grouped.take(24)
	# The time and val lists are what we are outputting
	time = list()
	val = list()
	for row in rows:
	    time_milit = 0
	    if row['AMPM'] == 'P':
	        time_milit += 12
	    time_milit += row['Hour'] % 12
	    time.append(time_milit)
	    val.append(row['count'])

	output_file = open(output_file_path, "w")
	output_file.write(str(time))
	output_file.write(str(val))
	output_file.close()


"""
Arguments:
	sc - the spark context
	sqlContext - the sql Context (for creating dataframes)
	ticket_file_path - the file path for the csv file containing the parking ticket data.
	output_file_path - the file path for where we should output our data
Returns: 
	Prints out two arrays of size 7 to a specified .txt file. 
	The first array will be days of the week as an integer (1(monday) - 7(sunday))
	and the second will be the # of tickets in that day.
Performance:
	This function runs in 13.1431 seconds on a dataset of about 3GB (~215 MB/Second)
"""
def group_parking_tickets_by_day_of_week(sc, sqlContext, ticket_file_path=None, output_file_path=None):
	default_path = "hdfs:///projects/group12/ParkingData/Parking_Violations_Issued_-_Fiscal_Year_2014__August_2013___June_2014_.csv"
	if ticket_file_path is None:
		ticket_file_path = default_path

	default_output_path = "by_day.txt"
	if output_file_path is None:
		output_file_path = default_output_path

	dataframe =load_parking_dataset(sc, sqlContext, ticket_file_path)
	# Create new column with datetime object of given date in column IssueDate
	dataframe = dataframe.withColumn('date_str', from_unixtime(unix_timestamp('IssueDate', 'MM/dd/yyy')))
	# New column to say what day of the week the ticket occured on (integer 1-7)
	dataframe = dataframe.withColumn('day_of_week', date_format('date_str', 'u'))
	# Clean data by removing null values.
	dataframe = dataframe.where(dataframe.day_of_week.isNotNull())
	# Group tickets by the day of the week.
	grouped = dataframe.groupBy("day_of_week").count()

	rows = grouped.take(7)
	val = list()
	day = list()
	for row in rows:
	    val.append(row['count'])
	    day.append(int(row['day_of_week']))

	output_file = open(output_file_path, "w")
	output_file.write(str(day))
	output_file.write(str(val))
	output_file.close()


if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()
    parser.add_argument('parking_file', help='File path to load parking ticket data from')
    parser.add_argument('--week_output_file', help='File path for where you want the by day of week data to be stored (.txt)')
    parser.add_argument('--hour_output_file', help='File path for where you want the by hour of day data to be stored (.txt)')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("Parking Data")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    group_parking_tickets_by_day_of_week(sc, sqlContext, args.parking_file, args.week_output_file)
    group_parking_tickets_by_time_period(sc, sqlContext, args.parking_file, args.hour_output_file)



 # END OF FILE