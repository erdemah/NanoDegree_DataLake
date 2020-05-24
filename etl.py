from datetime import datetime
import os
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, from_unixtime
from pyspark.sql.types import DateType, IntegerType, TimestampType
from pyspark.sql.functions import monotonically_increasing_id


def create_spark_session():
    """
    Creating spark session
    :return: spark
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def read_all_files(root):
    """
    Reads all the paths of files under root directory
    :param root: root path where the files will be read from the top (root) to inner subfolders
    :return: list of all the paths of files for a given path
    """
    all_files = []
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('udacity-sparkify-emr')
    #iterates through prefix to find all the file paths
    prefix_objs = bucket.objects.filter(Prefix=root)
    for obj in prefix_objs:
        key = obj.key
        all_files.append('s3://udacity-sparkify-emr/'+key)
    return all_files


def process_song_data(spark, input_data, output_data):
    """
    Reads the data from input address (s3://<path> in this case),
    does transformation to create song and artist tables,
    Finally writes the files as parquet to s3 bucket based on the specified path as output_data
    :param spark: spark session
    :param input_data: the location of the source data
    :param output_data: the location where the new tables are inserted into
    :return: None
    """
    # get filepath to song data file
    song_data = read_all_files("song_data")

    # read song data file
    df = spark.read.format("json").load(song_data)

    # extract columns to create songs table
    songs_table = ['song_id', 'title', 'artist_id', 'year', 'duration']

    # extracting the song table
    df_song_extracted = df.select(songs_table).dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    df_song_extracted.coalesce(1).write.mode("overwrite").partitionBy('year', 'artist_id').parquet(output_data + 'song_table/')

    # extract columns to create artists table
    artists_table = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']

    # extracting the artist table
    df_artist_extracted = df.select(artists_table).dropDuplicates()

    # write artists table to parquet files
    df_artist_extracted.coalesce(1).write.mode("overwrite").parquet(output_data + 'artist_table/')


def process_log_data(spark, input_data, output_data):
    """
    Reads the data from input address (s3://<path> in this case),
    does transformation to create user and time and songplay tables,
    Finally writes the files as parquet to s3 bucket based on the specified path as output_data
    :param spark: spark session
    :param input_data: the location of the source data
    :param output_data: the location where the new tables are inserted into
    :return: None
    """

    # get filepath to log data file
    log_data = read_all_files("log-data")

    # read log data file
    df = spark.read.format("json").load(log_data)

    # filter by actions for song plays
    df_log = df.where(df.page == "NextSong")

    # extract columns to create users table
    users_table = ['userId', 'firstName', 'lastName', 'gender', 'level']

    # extracting the song table
    df_users_extracted = df_log.select(users_table).dropDuplicates()

    # write users table to parquet files
    df_users_extracted.coalesce(1).write.mode("overwrite").parquet(output_data + 'user_table/')

    # converting timestamp from ms to seconds
    get_timestamp = udf(lambda x: x / 1000)

    # create datetime column from original timestamp column
    df2 = df_log.withColumn("ts2", df_log["ts"].cast(IntegerType()))
    df2 = df2.withColumn('ts3', get_timestamp('ts2'))
    df2 = df2.withColumn("ts4", df2["ts3"].cast(IntegerType()))
    df2 = df2.withColumn('date_time', from_unixtime('ts4'))
    df2 = df2.withColumn('start_time', df2['date_time'].cast(TimestampType()))
    df2 = df2.withColumn('hour', hour('start_time'))
    df2 = df2.withColumn('day', dayofmonth('start_time'))
    df2 = df2.withColumn('week', weekofyear('start_time'))
    df2 = df2.withColumn('month', month('start_time'))
    df2 = df2.withColumn('year', year('start_time'))
    df2 = df2.withColumn('weekday', dayofweek('start_time'))

    # extract columns to create time table
    time_table = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']

    # dropping duplicates
    df_time_table = df2.select(time_table).dropDuplicates()

    # write time table to parquet files partitioned by year and month
    df_time_table.coalesce(1).write.mode("overwrite").partitionBy('year', 'month').parquet(output_data + 'time_table/')

    # get filepath to song data file
    song_data = read_all_files("song_data")
    # read in song data to use for songplays table
    song_df = spark.read.format("json").load(song_data)

    # extract columns from joined song and log datasets to create songplays table
    songplays_columns = ["userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent", "ts"]

    song_data = read_all_files("song_data")
    # read song data file
    df_song = spark.read.format("json").load(song_data)

    # joining condition to create songplay table
    songplay_df = df_log.join(df_song,(df_song["artist_name"] == df_log["artist"]) & (df_song["title"] == df_log["song"]), "inner")

    # selecting songplay dataframe columns
    songplay_filtered_df = songplay_df.select(songplays_columns)

    # converting timestamp to date time
    songplay_table = songplay_filtered_df.withColumn("ts2", songplay_filtered_df["ts"].cast(IntegerType()))
    songplay_table = songplay_table.withColumn('ts3', get_timestamp('ts2'))
    songplay_table = songplay_table.withColumn("ts4", songplay_table["ts3"].cast(IntegerType()))
    songplay_table = songplay_table.withColumn('date_time', from_unixtime('ts4'))
    songplay_table = songplay_table.withColumn('start_time', songplay_table['date_time'].cast(TimestampType()))
    songplay_table = songplay_table.withColumn('year', year('start_time'))
    songplay_table = songplay_table.withColumn('month', month('start_time'))
    songplay_table = songplay_table.drop("ts", "ts2", "ts3", "ts4", "date_time")

    # drop duplicates
    songplay_table = songplay_table.dropDuplicates()
    # songplay_id incremental
    songplay_table = songplay_table.withColumn("songplay_id", monotonically_increasing_id() + 1)
    # write songplays table to parquet files partitioned by year and month
    songplay_table.coalesce(1).write.mode("overwrite").partitionBy('year', 'month').parquet(output_data + 'songplay_table/')


def main():
    spark = create_spark_session()
    input_data = "s3://udacity-sparkify-emr/"
    output_data = "s3://udacity-sparkify-emr/"


    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
