import configparser
from datetime import datetime
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id, year, month, dayofmonth, dayofweek, hour, weekofyear, date_format, to_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data,'song_data/*/*/*/*.json') #'song_data/A/B/C/TRABCEI128F424C983.json')
    
    schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", IntegerType()),
        StructField("title", StringType()),
        StructField("year", IntegerType()),
    ])
    
    # read song data file
    df = spark.read.json(song_data, schema=schema)

    # extract columns to create songs table
    songs_table = df.select(['title', 'artist_id', 'year', 'duration'])\
                    .dropDuplicates()\
                    .withColumn('song_id', monotonically_increasing_id()
    )
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite')\
        .partitionBy('year', 'artist_id')\
        .parquet(output_data+'songs')

    # extract columns to create artists table
    artists_table = df.selectExpr(\
        ["artist_id", "artist_name as name", "artist_location as location",\
         "artist_latitude as latitude", "artist_longitude as longitude"])\
        .dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json') #'log_data/2018/11/2018-11-12-events.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page=='NextSong')

    # extract columns for users table    
    users_table = df.selectExpr(
        ['userId as user_id', 'firstName as first_name', 'lastName as last_name', 'gender', 'level']
    ).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    df = df.withColumn('start_time', to_timestamp(df.timestamp))#F.get_datetime(df.timestamp))
    
    # extract columns to create time table
    df = df.withColumn("hour", hour("start_time")) \
        .withColumn("day", dayofmonth("start_time")) \
        .withColumn("week", weekofyear("start_time")) \
        .withColumn("month", month("start_time")) \
        .withColumn("year", year("start_time")) \
        .withColumn("weekday", dayofweek("start_time"))
    
    time_table = df.select('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite')\
        .partitionBy('year', 'month')\
        .parquet(output_data+'time')

    # read in song data to use for songplays table
    songs_df = spark.read.parquet(os.path.join(output_data, 'songs/*/*/*'))

    # extract columns from joined song and log datasets to create songplays table 
    songs_log = df.join(songs_df, (df.song==songs_df.title))
    songs_log = songs_log.drop(songs_log.location).drop(songs_log.month)
    
    artists_df = spark.read.parquet(os.path.join(output_data, 'artists'))
    
    artists_songs_log = songs_log.join(artists_df, (songs_log.artist==artists_df.name))
    artists_songs_log.ts = to_timestamp(artists_songs_log.ts)
    
    songplays = artists_songs_log.join(
        time_table,
        artists_songs_log.ts==time_table.start_time,
        'left'
    ).drop(artists_songs_log.year)
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays.select(
        col('ts').alias('start_time'),
        col('userId').alias('user_id'),
        col('level'),
        col('song_id'),
        col('artist_id'),
        col('sessionId').alias('session_id'),
        col('location'),
        col('userAgent').alias('user_agent'),
        col('year'),
        col('month')
    ).repartition("year", "month")
    
    songplays_table.write.mode('overwrite')\
        .partitionBy('year', 'month')\
        .parquet(output_data+'songplays')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-data-lake-emr/data-lake-emr/"
    
    #process_song_data(spark, input_data, output_data)
    print('Song data processed.')
    process_log_data(spark, input_data, output_data)
    print('Log data processed.')


if __name__ == "__main__":
    main()