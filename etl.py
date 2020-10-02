import configparser
from datetime import datetime
import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import (year, month, dayofweek, dayofmonth, hour,
                                   weekofyear, monotonically_increasing_id)


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    filename='datalake_udacity.log',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
LOGGER.addHandler(logging.StreamHandler(sys.stdout))


def create_spark_session() -> SparkSession:
    """Create a Spark session locally"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark: SparkSession, input_data: str, output_data: str) -> None:
    """Read data from input_data, extract columns for songs and artist tables
    and write data back to parquet files, loaded to the location output_data on
    S3"""

    LOGGER.info("-------------------------------")
    song_data = input_data + 'song_data/*/*/*/*.json'
    LOGGER.info(f"Reading from S3:\n{song_data}")
    df = spark.read.json(song_data)

    # Songs table
    LOGGER.info("Building SONGS table")
    songs_table = df.select('song_id', 'title', 'artist_id',
                            'year', 'duration') \
                    .dropDuplicates()

    songs_table.createOrReplaceTempView('songs')
    songs_table.write.partitionBy('year', 'artist_id').parquet(
        os.path.join(output_data, 'songs/songs.parquet'), 'overwrite'
        )

    # Artists table
    LOGGER.info("Building ARTISTS table")
    artists_table = df.select('artist_id', 'artist_name', 'artist_location',
                              'artist_latitude', 'artist_longitude') \
                      .withColumnRenamed('artist_name', 'name') \
                      .withColumnRenamed('artist_location', 'location') \
                      .withColumnRenamed('artist_latitude', 'latitude') \
                      .withColumnRenamed('artist_longitude', 'longitude') \
                      .dropDuplicates()

    artists_table.write.parquet(
        os.path.join(output_data, 'artists/artists.parquet'), 'overwrite'
        )


def process_log_data(spark: SparkSession, input_data: str, output_data: str) -> None:
    """Process data loaded from input_data location in S3, combine with tables creates from
    song_data (artists and songs tables) and populate users, time and songplays tables,
    which are then saved to output_data location in S3"""

    # Read and filter data
    log_data = input_data + 'log_data/*.json'
    LOGGER.info(f"Reading from S3:\n{log_data}")
    df = spark.read.json(log_data)
    df = df.filter(df.page == 'NextSong')

    # USERS table
    LOGGER.info("Building USERS table")
    users_table = df.select('userId', 'firstName', 'lastName',
                            'gender', 'level').dropDuplicates()
    users_table.createOrReplaceTempView('users')
    users_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')

    # TIME table
    LOGGER.info("Building TIME table")
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn('datetime', get_datetime(df.ts))
    time_table = df.select('datetime') \
                   .withColumn('start_time', df.datetime) \
                   .withColumn('hour', hour('datetime')) \
                   .withColumn('day', dayofmonth('datetime')) \
                   .withColumn('week', weekofyear('datetime')) \
                   .withColumn('month', month('datetime')) \
                   .withColumn('year', year('datetime')) \
                   .withColumn('weekday', dayofweek('datetime')) \
                   .dropDuplicates()
    time_table.write.partitionBy('year', 'month') \
                    .parquet(os.path.join(output_data,
                                          'time/time.parquet'), 'overwrite')

    # SONGPLAYS table
    LOGGER.info("Building SONGPLAYS table")
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    joined_df = df.join(song_df, col('df.artist') == col(
        'song_df.artist_name'), 'inner')

    songplays_table = joined_df.select(
        col('df.datetime').alias('start_time'),
        col('df.userId').alias('user_id'),
        col('df.level').alias('level'),
        col('song_df.song_id').alias('song_id'),
        col('song_df.artist_id').alias('artist_id'),
        col('df.sessionId').alias('session_id'),
        col('df.location').alias('location'),
        col('df.userAgent').alias('user_agent'),
        year('df.datetime').alias('year'),
        month('df.datetime').alias('month')) \
        .withColumn('songplay_id', monotonically_increasing_id())

    songplays_table.createOrReplaceTempView('songplays')
    songplays_table.write.partitionBy(
        'year', 'month').parquet(os.path.join(output_data,
                                 'songplays/songplays.parquet'),
                                 'overwrite')

    LOGGER.info("Done writing parquet files!")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-nanodegree-bucket-taniavas/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
