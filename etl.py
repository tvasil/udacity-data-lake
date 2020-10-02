import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import (year, month, dayofmonth, hour,
                                   weekofyear, date_format)


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


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

    song_data = input_data + 'song_data/*/*/*/*.json'
    df = spark.read.json(song_data)

    # Songs table
    songs_table = df.select('song_id', 'title', 'artist_id',
                            'year', 'duration') \
                    .dropDuplicates()

    songs_table.createOrReplaceTempView('songs')
    songs_table.write.partitionBy('year', 'artist_id').parquet(
        os.path.join(output_data, 'songs/songs.parquet'), 'overwrite'
        )

    # Artists table
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


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =

    # read log data file
    df =

    # filter by actions for song plays
    df =

    # extract columns for users table
    artists_table =

    # write users table to parquet files
    artists_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df =

    # create datetime column from original timestamp column
    get_datetime = udf()
    df =

    # extract columns to create time table
    time_table =

    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df =

    # extract columns from joined song and log datasets to create songplays table
    songplays_table =

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
