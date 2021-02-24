import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, col, row_number, date_format
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Establish spark connection
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Read in song data files with Schema-on-read
    Extract columns needed to create songs and artists tables
    Write data into songs and artists tables

    INPUTS
    * spark - spark connection
    * input_data - s3 bucket path
    * output_data - path for tables to be written to
    '''
    # get filepath to song data file (sub set is initiated for the reviewer)
    song_data = input_data + "song_data/A/A/A/*.json"
    # to get all song data files
#     song_data = input_data + "song_data/*/*/*/*.json"

    print("Reading in song data")
    # read song data file
    df = spark.read.json(song_data)

    # review schema
    df.printSchema()

    print("Extracing columns to create song table")
    # extract columns to create songs table
    # limited records to reduce wait times
    songs_table = df.select("song_id",
                            "title",
                            "artist_id",
                            col("year").cast("int"),
                            "duration").distinct().limit(100)

    # review schema
    songs_table.printSchema()

    # view sample set
    songs_table.limit(10).show()

    print("Writing song table to a parquet file")
    # write songs table to parquet files partitioned by year and artist
    # Source: https://sparkbyexamples.com/pyspark/pyspark-read-and-write-parquet-file/
    songs_table = songs_table.write \
                             .partitionBy("year", "artist_id") \
                             .mode("overwrite") \
                             .parquet(output_data + "songs.parquet")

    print("Extracing columns to create artist table")
    # extract columns to create artists table
    # limited records to reduce wait times
    artists_table = df.select("artist_id",
                              col("artist_name").alias("name"),
                              col("artist_location").alias("location"),
                              col("artist_latitude").alias("latitude"),
                              col("artist_longitude").alias("longitude")) \
                      .distinct().limit(100)

    # review schema
    artists_table.printSchema()

    # view sample set
    artists_table.limit(10).show()

    print("Writing artist table to a parquet file")
    # write artists table to parquet files
    artists_table = artists_table.write \
                                 .mode("overwrite") \
                                 .parquet(output_data + "artists.parquet")


def process_log_data(spark, input_data, output_data):
    '''
    Read in log data files with Schema-on-read
    Filter data frame to song plays only
    Extract columns needed to create users table
    Write data into users table
    Create timestamp and datetime columns from original timestamp column
    Parse date and time information for time table
    Write data into time table
    Read in song table to use for songplays table
    Extract columns from joined song and log datasets to create songplays table
    Write songplays table to parquet files partitioned by year and month

    INPUTS
    * spark - spark connection
    * input_data - s3 bucket path
    * output_data - path for tables to be written to
    '''
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    print("Reading in log data")
    # read log data file
    df = spark.read.json(log_data)

    # review schema
    df.printSchema()

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    print("Extracing columns to create user table")
    # extract columns for users table
    # limited records to reduce wait times
    users_table = df.select(col("userid").cast("int").alias("user_id"),
                            col("firstname").alias("first_name"),
                            col("lastname").alias("last_name"),
                            "gender",
                            "level").distinct().limit(100)

    # review schema
    users_table.printSchema()

    # view sample set
    users_table.limit(10).show()

    print("Writing user table to a parquet file")
    # write users table to parquet files
    users_table = users_table.write \
                             .mode("overwrite") \
                             .parquet(output_data + "users.parquet")

    print("Creating timestamp and datetime columns")
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:  datetime.fromtimestamp(x/1000)
                                           .strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000)
                                         .strftime('%Y-%m-%d'))
    df = df.withColumn("datetime", get_datetime(df.ts))

    print("Extracting columns to time table")
    # extract columns to create time table
    # limited records to reduce wait times
    time_table = df.select(
                        (col('ts') / 1000).cast('double').alias('start_time'),
                        hour('timestamp').alias('hour'),
                        dayofmonth('timestamp').alias('day'),
                        weekofyear('timestamp').alias('week'),
                        month('timestamp').alias('month'),
                        year('timestamp').alias('year'),
                        date_format('timestamp', 'E').alias('weekday')) \
                   .distinct().limit(100)

    # review schema
    time_table.printSchema()

    # view sample set
    time_table.limit(10).show()

    print("Writing time table to parquet file")
    # write time table to parquet files partitioned by year and month
    time_table = time_table.write \
                           .partitionBy("year", "month") \
                           .mode("overwrite") \
                           .parquet(output_data + "time.parquet")

    print("Reading in song data for songplays table")
    # read in song data to use for songplays table
    # (sub set is initiated for the reviewer)
    song_df = spark.read.json(input_data + "song_data/A/A/A/*.json")
    # to get all song data files
#     song_df = spark.read.json(input_data + "song_data/*/*/*/*.json")

    # create temp tables for songplays join
    df.createOrReplaceTempView("log_table")
    song_df.createOrReplaceTempView("song_table")

    print("Extracing columns for songplays table")
    # extract columns from joined song and log tables to create songplays table
    # limited records to reduce wait times
    songplays_table = spark.sql('''
                          select distinct
                              log_table.ts / 1000 as ts,
                              cast(log_table.userid as int) as user_id,
                              log_table.level,
                              song_table.song_id,
                              song_table.artist_id,
                              cast(log_table.sessionid as int) as session_id,
                              log_table.location,
                              log_table.useragent,
                              year(log_table.timestamp) as year,
                              month(log_table.timestamp) as month
                          from log_table
                          left join song_table
                              on song_table.title = log_table.song
                                  and song_table.duration = log_table.length
                                  and song_table.artist_name = log_table.artist
                          limit 100
                                ''')

    # add in songplays_id
    # Source: https://towardsdatascience.com/adding-sequential-ids-to-a-spark-dataframe-fa0df5566ff6
    window = Window.orderBy(col("ts"))
    songplays_table = songplays_table.withColumn("songplays_id",
                                                 row_number().over(window))

    # review schema
    songplays_table.printSchema()

    # view sample set
    songplays_table.limit(10).show()

    print("Writing songplays table to parquet file")
    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write \
                                     .partitionBy("year", "month") \
                                     .mode("overwrite") \
                                     .parquet(output_data + "songplays.parquet")


def main():
    '''
    Establish spark session
    Define input and output paths
    Process song and log data
    '''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-lake-project-don/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
