import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id, row_number
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
)


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
os.environ["output_data"] = config["S3"]["output_bucket"]


def create_spark_session():
    """Init spark instance."""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Load and preprocess song dataset into song and artist table. Write back to S3."""
    # get filepath to song data file
    song_data = input_data + 'song_data'
    
    # define song data schema
    song_data_schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", IntegerType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("year", IntegerType()),
    ])
    # read song data file
    df = spark.read.json(song_data + "/*/*/*/*.json", schema=song_data_schema)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite') \
    .partitionBy('year', 'artist_id') \
    .parquet(output_data + 'songs.parquet')

    # extract columns to create artists table
    artists_table = df.select(
        'artist_id', 
        col('artist_name').alias('name'),
        col('artist_location').alias('location'),
        col('artist_latitude').alias('latitude'),
        col('artist_longitude').alias('longitude')
    ).distinct()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite') \
    .parquet(output_data + 'artists.parquet')


def process_log_data(spark, input_data, output_data):
    """Load and preprocess log dataset into time, user and songplays table. Write back to S3."""
    # get filepath to log data file
    log_data = input_data + 'log_data'
    # define log data schema
    log_data_schema = StructType([
        StructField("artist", StringType()),
        StructField("auth", StringType()),
        StructField("firstName", StringType()),
        StructField("gender", StringType()),
        StructField("itemInSession", IntegerType()),
        StructField("lastName", StringType()),
        StructField("length", DoubleType()),
        StructField("level", StringType()),
        StructField("location", StringType()),
        StructField("method", StringType()),
        StructField("page", StringType()),
        StructField("registration", DoubleType()),
        StructField("sessionId", IntegerType()),
        StructField("song", StringType()),
        StructField("status", IntegerType()),
        StructField("ts", DoubleType()),
        StructField("userAgent", StringType()),
        StructField("userId", StringType())
    ])
    # read log data file
    df = spark.read.json(log_data + "/*/*/*.json", schema=log_data_schema)
    
    # filter by actions for song plays
    df = df.filter(col('page') == 'NextSong')

    # extract columns for users table    
    users_table = df.select(
        col('userId').alias('user_id'),
        col('firstName').alias('first_name'),
        col('lastName').alias('last_name'),
        'gender', 'level'
    ).distinct()    
    
    # write users table to parquet files
    users_table.write.mode('overwrite') \
    .parquet(output_data + 'users.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(x/1000)))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(
        col('datetime').alias('start_time'),
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day'),
        weekofyear('datetime').alias('week'),
        month('datetime').alias('month'),
        year('datetime').alias('year'),
        date_format('datetime', 'EEEE').alias('weekday'),
    ).distinct() 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite') \
    .partitionBy('year', 'month').parquet(output_data + 'time.parquet')

    # read in song data to use for songplays table
    song_data_schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", IntegerType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("year", IntegerType()),
    ])
    song_data = input_data + 'song_data'
    song_df = spark.read.json(song_data + "/*/*/*/*.json", schema=song_data_schema)

    # extract columns from joined song and log datasets to create songplays table 
    t1 = df.alias('t1')
    t2 = song_df.alias('t2')

    songplays_table = t1.join(
        t2, [
            t1.song == t2.title,
            t1.artist == t2.artist_name,
            t1.length == t2.duration
        ]
    ).select(
        col('t1.datetime').alias('start_time'),
        col('t1.userId').alias('user_id'),
        col('t1.level').alias('level'),
        col('t2.song_id').alias('song_id'),
        col('t2.artist_id').alias('artist_id'),
        col('t1.sessionId').alias('session_id'),
        col('t1.location').alias('location'),
        col('t1.userAgent').alias('user_agent')
    )
    # join with time_table
    t1 = songplays_table.alias('t1')
    t2 = time_table.alias('t2')

    songplays_table = t1.join(
      t2, [t1.start_time == t2.start_time]
    ).select(
        col('t1.start_time').alias('start_time'),
        col('t1.user_id').alias('user_id'),
        col('t1.level').alias('level'),
        col('t1.song_id').alias('song_id'),
        col('t1.artist_id').alias('artist_id'),
        col('t1.session_id').alias('session_id'),
        col('t1.location').alias('location'),
        col('t1.user_agent').alias('user_agent'),
        col('t2.month').alias('month'),
        col('t2.year').alias('year')
    )
    # using monotonically_increasing_id
    songplays_table = songplays_table.withColumn('idx', monotonically_increasing_id())
    # monotonically_increasing_id is not consecutive, so we need idx to be sorted
    songplays_table.createOrReplaceTempView('songplays_table')
    songplays_table = spark.sql(
        'select row_number() over (order by "idx") as songplay_id, * from songplays_table'
    ).drop('idx') 

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite') \
    .partitionBy('year', 'month').parquet(output_data + 'songplays.parquet')


def main():
    """Main."""
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = os.environ["output_data"]
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
