import configparser
from datetime import datetime
import os
import boto3
import pandas as pd
import time

# # Import pyspark in python shell or Jupyter Notebok, if need.
# import findspark
# findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, StringType, IntegerType, DateType, LongType, TimestampType

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

def create_spark_session():
    """
    Description: Create spark session with hadoop-aws package.
    Arguments: None
    Returns: pyspark.sql.session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: Read song_data from S3, transform them to create song table and artists table,
    and write them to partitioned parquet files in table directories on S3.
    Arguments:
        spark: pyspark.sql.session
        input_data: S3 input data folder path
        output_data: S3 output data folder path
    Returns: None
    """
    # get filepath to song data file
    song_data = input_data

    RawSongSchema = StructType([
    StructField('artist_id', StringType()),
    StructField('artist_latitude', DoubleType()),
    StructField('artist_location', StringType()),
    StructField('artist_longitude', DoubleType()),
    StructField('artist_name', StringType()),
    StructField('duration', DoubleType()),
    StructField('num_songs', IntegerType()),
    StructField('song_id', StringType()),
    StructField('title', StringType()),
    StructField('year', IntegerType())])

    # read song data file
    df = spark.read.json(song_data, schema=RawSongSchema)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")\
    .where("song_id is not null").dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + '/song', mode="overwrite", partitionBy=['year','artist_id'])

    # extract columns to create artists table
    artists_table = df.select("artist_id", col("artist_name").alias("name"), \
    col("artist_location").alias("location"), col("artist_latitude").alias("latitude"), \
    col("artist_longitude").alias("longitude")).where("artist_id is not null").dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(output_data + '/artists', mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Description: Read log_data from S3, transform them to create users, time and songplays tables,
    and write them to partitioned parquet files in table directories on S3.
    Arguments:
        spark: pyspark.sql.session
        input_data: S3 input data folder path
        output_data: S3 output data folder path
    Returns: None
    """
    # get filepath to log data file
    log_data = input_data

    RawLogSchema = StructType([
    StructField('artist', StringType()),
    StructField('auth', StringType()),
    StructField('firstName', StringType()),
    StructField('gender', StringType()),
    StructField('itemInSession', IntegerType()),
    StructField('lastName', StringType()),
    StructField('length', DoubleType()),
    StructField('level', StringType()),
    StructField('location', StringType()),
    StructField('method', StringType()),
    StructField('page', StringType()),
    StructField('registration', DoubleType()),
    StructField('sessionId', IntegerType()),
    StructField('song', StringType()),
    StructField('status', IntegerType()),
    StructField('ts', LongType()),
    StructField('userAgent', StringType()),
    StructField('userId', StringType())])

    # read log data file
    df = spark.read.json(log_data, schema=RawLogSchema)

    # filter by actions for song plays
    df = df.select("*").where("page = 'NextSong'")

    # extract columns for users table
    users_table = df.select("userId", "firstName", "lastName", "gender", "level", "ts", \
                            row_number().over(Window.partitionBy("userId").orderBy(col("ts").desc()))\
                            .alias("ts_rank")).where("userId is not null")

    users_table = users_table.selectExpr("userId as user_id", "firstName as first_name", \
                                         "lastName as last_name", "gender", "level").where("ts_rank = 1")

    # write users table to parquet files
    users_table.write.parquet(output_data + '/users', mode="overwrite")

    # create timestamp column from original timestamp column
    # get_timestamp = udf()
    @udf(TimestampType())
    def get_timestamp(x):
        if x:
            return pd.to_datetime(x,unit='ms')
        else:
            return None
    df = df.withColumn("start_time", get_timestamp("ts"))
    df.createOrReplaceTempView("log_raw")

    # extract columns to create time table
    time_table = spark.sql("""
    SELECT start_time, hour(start_time) as hour, dayofmonth(start_time) as day,
    weekofyear(start_time) as week, month(start_time) as month,
    year(start_time) as year, date_format(start_time,'E') as weekday
    FROM log_raw
    WHERE start_time is not null
    """).dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + '/time', mode="overwrite", partitionBy=['year','month'])

    # read in song data and artists data to use for songplays table
    song_df = spark.read.parquet(output_data + '/song')
    artists_df = spark.read.parquet(output_data + '/artists')

    song_df.createOrReplaceTempView("songs")
    artists_df.createOrReplaceTempView("artists")

    # add songplay_id
    df = df.select("*").withColumn("songplay_id", monotonically_increasing_id())
    df.createOrReplaceTempView("log_raw")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql("""
    SELECT songplay_id, start_time, b.song_id as song_id, c.artist_id as artist_id,
    userId as user_id,
    sessionId as session_id,
    a.location,
    level,
    userAgent as user_agent
    FROM log_raw a
    left join songs b
    on a.song = b.title
    left join artists c
    on a.artist = c.name
    """).dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + '/songplays', mode="overwrite")


def main():
    """
    Description: This is the main function, which performs ETL on log and song data.
    Arguments: None
    Returns: None
    """
    start_time = time.time()

    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']

    spark = create_spark_session()

    log_input_data = config.get('S3','log_data')
    song_input_data = config.get('S3','song_data')
    output_data = config.get('S3','output_path')


    process_song_data(spark, song_input_data, output_data)
    process_log_data(spark, log_input_data, output_data)

    run_time = (time.time() - start_time)//60

    print('The ETL processes successfully ran. You can find the output here:')
    print(str(output_data))
    print('\t')
    print("Run Time: %s min" % (run_time))

if __name__ == "__main__":
    main()
