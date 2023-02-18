import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, DoubleType, StringType , IntegerType
from pyspark.sql.functions import col,struct,when
from pyspark.sql.types import * #for datetime <https://sparkbyexamples.com/pyspark/pyspark-sql-date-and-timestamp-functions/>
from pyspark.sql.functions import dayofweek #<https://stackoverflow.com/questions/38928919/how-to-get-the-weekday-from-day-of-month-using-pyspark>

config = configparser.ConfigParser()
config.read('dl.cfg')
# To use from dl.cfg to log in AWS 
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data, run_start_time):
    """Load JSON input data (song_data) from input_data path,
        process the data to extract song_table and artists_table, and
        store the queried data to parquet files.
    
    # spark         -- reference to Spark session.
    # input_data    -- path to input_data to be processed (song_data)
    # output_data   -- path to location to store the output (parquet files).
    Output::
    * songs_table   -- directory with parquet files
                       stored in output_data path.
    * artists_table -- directory with parquet files
                       stored in output_data path.
    """
    # Load song_data 
    start_songd = datetime.now()
    start_songdl = datetime.now()
    print("Start processing song_data JSON files...")
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/A/A/A/*.json')
    
    
    # read song data file
    """ # data types as ***Reviwer noticed*** that me have not set data types for the fields in my final             tables.This is not a critical requirement for this project, but it is beneficial to do this to             avoid some errors during data collection. You may set data types in PySpark using StructType and           StructField. Here is an example of using these classes to build a schema that you may use in the           read.json() function:
    """
    
    #use HInt <https://www.projectpro.io/recipes/explain-structtype-and-structfield-pyspark-databricks>
    song_schema = StructType([
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("artist_id", StringType()),
        StructField("year", DoubleType()),
        StructField("duration", StringType())       
    ])
    
    print("Reading song_data files from {}...".format(song_data))
    df_song_data = spark.read.json(song_data)
    stop_songdl = datetime.now()
    total_sdl = stop_songdl - start_songdl
    print("...finished reading song_data in {}.".format(total_sdl))
    print("Song_data schema:")
    df_song_data.printSchema()
    # extract columns to create songs table
    start_st = datetime.now()
    
    songs_table = df_song_data.select('song_id', 'title', 'artist_id',
                            'year', 'duration') \
                    .dropDuplicates(subset=['song_id'])
    
    songs_table.createOrReplaceTempView('songs')
    print("Songs_table schema:")
    songs_table.printSchema()
    print("Songs_table examples:")
    songs_table.show(5, truncate=False)
    
    # write songs table to parquet files partitioned by year and artist
    print("Writing songs_table parquet files to {}..."\
        .format(songs_table))
    
    #Thanking Udacity Reviewer in <https://review.udacity.com/#!/reviews/3929359> ; fellow repartition \
    #<https://mungingdata.com/apache-spark/partitionby/> and fellow (ToREAD to Enhance) section in file (README.md)
    
    #For Multiple columns use list as compined/united (year_artist_id_cols)one column structure 
    #year_artist_id_cols = ["year", "artist_id"]
    songs_table.repartition(4, col('year')) \
               .write.partitionBy('year', 'artist_id') \
               .parquet(os.path.join(output_data, 'songs/songs.parquet'), 'overwrite')
    
    stop_st = datetime.now()
    total_st = stop_st - start_st
    print("...finished writing songs_table in {}.".format(total_st))
    # Create and write artists_table 
    # extract columns to create artists table
    start_at = datetime.now()
    artists_table = df_song_data.select('artist_id','artist_name',  'artist_location',
                              'artist_latitude', 'artist_longitude') \
                      .withColumnRenamed('artist_name', 'name') \
                      .withColumnRenamed('artist_location', 'location') \
                      .withColumnRenamed('artist_latitude', 'latitude') \
                      .withColumnRenamed('artist_longitude', 'longitude') \
                      .dropDuplicates(subset=['artist_id'])
    artists_table.createOrReplaceTempView('artists')
    
    artists_table.printSchema()
    artists_table.show(5, truncate=False)

    # write artists table to parquet files
    print("Writing artists_table parquet files to {}..."\
        .format(artists_table))
    artists_table.write.parquet(os.path.join(output_data, 'artists/artists.parquet'), 'overwrite')
    stop_at = datetime.now()
    total_at = stop_at - start_at
    print("...finished writing artists_table in {}.".format(total_at))
    stop_songd = datetime.now()
    total_sd = stop_songd - start_songd
    print("Finished processing song_data in {}.\n".format(total_sd))

    return songs_table, artists_table


#process_log_data(spark, input_data, output_data)
def  process_log_data(spark, input_data, output_data,run_start_time):
    
    """Load JSON input data (log_data) from input_data path,
        process the data to extract users_table, time_table,
        songplays_table, and store the queried data to parquet files.
    
    # spark            -- reference to Spark session.
    # input_data       -- path to input_data to be processed (log_data)
    # output_data      -- path to location to store the output
                          (parquet files).
    Output::
    # users_table      -- directory with users_table parquet files
                          stored in output_data path.
    # time_table       -- directory with time_table parquet files
                          stored in output_data path.
    # songplays_table -- directory with songplays_table parquet files
                          stored in output_data path.
    """
    #Load log_data 
    start_logd = datetime.now()
    start_logdl = datetime.now()
    print("Start processing log_data JSON files...")
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*.json')

    # read log data file
       
    log_schema = StructType([
        StructField("ts", DoubleType()),
        StructField("userId", StringType()),
        StructField("level", StringType()),
        StructField("song_id", StringType()),
        StructField("artist_id", StringType()),
        StructField("sessionId", StringType()),
        StructField("location", StringType()),
        StructField("userAgent", StringType()), 
        StructField("page", StringType()),
        StructField("firstName", StringType()), 
        StructField("lastName", StringType()), 
        StructField("gender", StringType()),
        StructField("song", StringType()),
        StructField("length", StringType()),
        StructField("artist", StringType())
    ])
    
    print("Reading log_data files from {}...".format(log_data))
    df_log_data = spark.read.json(log_data, schema = log_schema)
    stop_logdl = datetime.now()
    total_logdl = stop_logdl - start_logdl
    print("...finished reading log_data in {}.".format(total_logdl))
    
    # Create and write users_table 
    # filter by actions for song plays
    start_user_t = datetime.now()
    
    filter_by_actions_df = df_log_data.filter(df_log_data.page == 'NextSong') \
                   .select('ts', 'userId', 'level', 'song_id', 'artist_id',
                           'sessionId', 'location', 'userAgent','song','length','artist')

    # extract columns for users table    
    users_table = df_log_data.select('userId', 'firstName', 'lastName','gender', 'level')\
                                      .withColumnRenamed('userId', 'user_id') \
                                      .withColumnRenamed('firstName', 'first_name') \
                                      .withColumnRenamed('lastName', 'last_name') \
                                      .dropDuplicates(subset=['user_id'])
    
    df_log_data.select(col("userId").alias("user_id"),col("firstName").alias("first_name"),col("lastName").alias("last_name"))
    
    users_table.createOrReplaceTempView('users')
    
    print("Users_table schema:")
    users_table.printSchema()
    print("Users_table examples:")
    users_table.show(5)
    
    # write users table to parquet files
    print("Writing users_table parquet files to {}..."\
            .format(users_table))
    
    users_table.repartition(4).write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')
    
    stop_user_t = datetime.now()
    total_user_t = stop_user_t - start_user_t
    print("...finished writing users_table in {}.".format(total_user_t))

    # Create and write time_table 
    # create timestamp column from original timestamp column
    start_timestamp_t = datetime.now()
    print("Creating timestamp column...")
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    filter_by_actions_df = filter_by_actions_df.withColumn('timestamp', get_timestamp(filter_by_actions_df.ts))
    filter_by_actions_df.printSchema()
    filter_by_actions_df.show(5)
    
    # create datetime column from original timestamp column
    print("Creating datetime column...")
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    filter_by_actions_df = filter_by_actions_df.withColumn('datetime', get_datetime(filter_by_actions_df.ts))
    #dayofweek = udf(lambda x : str ( x.weekday()))
    filter_by_actions_df = filter_by_actions_df.withColumn('dayofweek', dayofweek(filter_by_actions_df.datetime))
    print("Log_data + timestamp + datetime + dayofweek columns schema:")
    filter_by_actions_df.printSchema()
    print("Log_data + timestamp + datetime + dayofweek columns examples:")
    filter_by_actions_df.show(5)
    
    # extract columns to create time table  
    time_table = filter_by_actions_df.select('datetime') \
                           .withColumn('start_time', filter_by_actions_df.datetime) \
                           .withColumn('hour', hour('datetime')) \
                           .withColumn('day', dayofmonth('datetime')) \
                           .withColumn('week', weekofyear('datetime')) \
                           .withColumn('month', month('datetime')) \
                           .withColumn('year', year('datetime')) \
                           .withColumn('weekday', dayofweek('datetime')) \
                           .dropDuplicates() 
    print("Time_table schema:")
    time_table.printSchema()
    print("Time_table examples:")
    time_table.show(5)
    
    # write time table to parquet files partitioned by year and month
    print("Writing time_table parquet files to {}..."\
            .format(time_table))
    
    #Appreciation to Reviewer ; fellow repartition [https://mungingdata.com/apache-spark/partitionby/]
    
    time_table.repartition(4, col('year')) \
              .write.partitionBy('year', 'month')         \
              .parquet(os.path.join(output_data,'time/time.parquet'), 'overwrite')
    
    stop_timestamp_t = datetime.now()
    total_timestamp_t = stop_timestamp_t - start_timestamp_t
    print("...finished writing time_table in {}.".format(total_timestamp_t))
    
    # read in song data to use for songplays table
    
    song_schema = StructType([
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("artist_id", StringType()),
        StructField("artist_name", StringType()),
        StructField("year", DoubleType()),
        StructField("duration", DoubleType())       
    ])
    
    start_songplays_t = datetime.now()
    song_data = os.path.join(input_data, 'song_data/A/A/A/*.json')
    print("Reading song_data files from {}...".format(song_data))
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    filter_by_actions_df = filter_by_actions_df.alias('log_df')
    song_df = song_df.alias('song_df')
    print("Joining log_data and song_data df s...")
    # JOIN in pyspark see <https://sparkbyexamples.com/pyspark/pyspark-join-multiple-columns/>
    # also <https://www.geeksforgeeks.org/removing-duplicate-columns-after-dataframe-join-in-pyspark/>
    # For the songplays table, JOIN the songs based on song [ song , length and artist_name ] to [ title , duration and artist ] 
    # <https://knowledge.udacity.com/questions/890877 Thank You Mentor/Survesh>
    joined_df = filter_by_actions_df.join( song_df , col('log_df.song') == col('song_df.title'))
    joined_df = filter_by_actions_df.join( song_df , col('log_df.length') ==  col('song_df.duration'))
    joined_df = filter_by_actions_df.join( song_df , col('log_df.artist') == col('song_df.artist_name') , 'inner')
    print("...finished joining song_data and log_data dfs.")
    
    print("Joined song_data + log_data schema:")
    joined_df.printSchema()
    print("Joined song_data + log_data examples:")
    joined_df.show(5)
    
    print("Extracting columns from joined df...")
    # songplays_table citation1 to <https://github.com/jonathankamau/udacity-data-lake-project/blob/master/etl.py> Thank You 
    songplays_table = joined_df.select(
        col('log_df.datetime').alias('start_time'),
        col('log_df.userId').alias('user_id'),
        col('log_df.level').alias('level'),
        col('song_df.song_id').alias('song_id'),
        col('song_df.artist_id').alias('artist_id'),
        col('log_df.sessionId').alias('session_id'),
        col('log_df.location').alias('location'), 
        col('log_df.userAgent').alias('user_agent'),
        year('log_df.datetime').alias('year'),
        month('log_df.datetime').alias('month')) \
        .withColumn('songplay_id', monotonically_increasing_id())
    #end citation1

    songplays_table.createOrReplaceTempView('songplays')
    
    songplays_schema = StructType([
        StructField("start_time", DoubleType()),
        StructField("user_id", StringType()),
        StructField("level", StringType()),
        StructField("song_id", StringType()),
        StructField("artist_id", StringType()),
        StructField("session_id", StringType()),
        StructField("location", StringType()),
        StructField("user_agent", StringType()),
        StructField("year", DoubleType()),
        StructField("month", StringType()),
        StructField("songplay_id", StringType()) 
    ])
    
    
    print("Songplays_table schema:")
    songplays_table.printSchema()
    print("Songplays_table examples:")
    songplays_table.show(5, truncate=False)
    
    # write songplays table to parquet files partitioned by year and month
    print("Writing songplays_table parquet files to {}..."\
            .format(songplays_table))
    
    time_table = time_table.alias('timetable')
    
    #Appreciation to Udacity Reviewer ; fellow repartition <https://mungingdata.com/apache-spark/partitionby/>
    
    songplays_table.repartition(4,col('year')) \
                   .write.partitionBy('year', 'month')         \
                   .parquet(os.path.join(output_data,'songplays/songplays.parquet'),'overwrite')
    
    stop_songplays_t = datetime.now()
    total_songplays_t = stop_songplays_t - start_songplays_t
    print("...finished writing songplays_table in {}.".format(total_songplays_t))

    return users_table, time_table, songplays_table


def query_table_count(spark, table):
    """Query example returning row count of the given table.
    
    # spark            -- spark session
    # table            -- table to count
    Output::
    # count            -- count of rows in given table
    """
    
    return table.count()

def query_songplays_table( spark, songs_table, artists_table, users_table, time_table, songplays_table):
    
    """Query example using all the created tables.
        Provides example set of songplays and who listened them.
    Input::
    # spark            -- spark session
    # songs_table      -- songs_table dataframe
    # artists_table    -- artists_table dataframe
    # users_table      -- users_table dataframe
    # time_table       -- time_table dataframe
    
    # songplays_table  -- songplays_table dataframe
    
    Output::
    
    # schema           -- schema of the created dataframe
    # songplays        -- songplays by user 
    """
    df_all_tables_joined = songplays_table.alias('splays')\
        .join(users_table.alias('u'), col('u.user_id') == col('splays.user_id'))\
        .join(songs_table.alias('s'), col('s.song_id') == col('splays.song_id'))\
        .join(artists_table.alias('a'), col('a.artist_id') == col('splays.artist_id'))\
        .join(time_table.alias('t'), col('t.start_time')  == col('splays.start_time'))\
        .select('splays.songplay_id','splays.start_time', 'u.user_id', 's.song_id',
                'a.artist_id', 'splays.session_id' , 's.title' , 'splays.location',
                'splays.user_agent')\
        .sort('splays.start_time')\
        .limit(50)

    print("\nJoined dataframe schema:")
    df_all_tables_joined.printSchema()
    
    print("Sample Songplays by users:")
    df_all_tables_joined.show(20)
    
    
    return
      
# def query_examples citation2 to <https://github.com/DrakeData/data-engineer-nanodegree/blob/master/data-lakes-spark/Projects/etl.py>
def query_examples( spark, songs_table, artists_table, users_table, time_table, songplays_table):
    
    """Query example using all the created tables.
    
    Input::
    # spark            -- spark session
    # songs_table      -- songs_table dataframe
    # artists_table    -- artists_table dataframe
    # users_table      -- users_table dataframe
    # time_table       -- time_table dataframe
    # songplays_table  -- songplays_table dataframe
    
    Output::
    
    * schema           -- schema of the created dataframe
    * songplays        -- songplays by user (if any)
    """
    # Query count of rows in the table
    print("Songs_table count: " \
            + str(query_table_count(spark, songs_table)))
    print("Artists_table count: " \
            + str(query_table_count(spark, artists_table)))
    print("Users_table count: " \
            + str(query_table_count(spark, users_table)))
    print("Time_table count: " + \
            str(query_table_count(spark, time_table)))
    print("Songplays_table count: " \
            + str(query_table_count(spark, songplays_table)))
    
    query_songplays_table(  spark, \
                            songs_table, \
                            artists_table, \
                            users_table, \
                            time_table, \
                            songplays_table)
#end citation2 Tank You
    
def main():
    """Load JSON input data (song_data and log_data) from input_data path,
        process the data to extract songs_table, artists_table,
        users_table, time_table, songplays_table,
        and store the queried data to parquet files to output_data path.
    
    Output:
    * songs_table      -- directory with songs_table parquet files
                          stored in output_data path.
    * artists_table    -- directory with artists_table parquet files
                          stored in output_data path.
    * users_table      -- directory with users_table parquet files
                          stored in output_data path.
    * time_table       -- directory with time_table parquet files
                          stored in output_data path.
    * songplayes_table -- directory with songplays_table parquet files
                          stored in output_data path.
    """
    start = datetime.now()
    run_start_time = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
    print("\nSTARTING ETL pipeline (to process song_data and log_data) \
            at {}\n".format(start))
    # Get or create a spark session
    spark = create_spark_session()
    # Read the song and log data from s3
    # Take the data and transform them to tables which will then be written to parquet files
    
    #input_data = "http://s3-us-west-2.amazonaws.com/udacity-dend/"
    #output_data = ""
    
    # Variables to be used in when data is processed from/to S3.
   
    #input_data = config['AWS']['INPUT_DATA']
    #output_data = config['AWS']['OUTPUT_DATA']
    
    # Use LOCAL input_data + output_data paths.
    input_data = config['LOCAL']['INPUT_DATA_LOCAL']
    output_data   = config['LOCAL']['OUTPUT_DATA_LOCAL']

    # Load the parquet files on s3
    # Use AWS input_data + output_data paths.
    songs_table, artists_table = process_song_data(spark, input_data, output_data, run_start_time)    
    
    users_table, time_table, songplays_table = process_log_data(spark, input_data, output_data, run_start_time)
   
    print("Finished the ETL pipeline processing.")
    print("ALL DONE.")

    stop = datetime.now()
    print("FINISHED ETL pipeline (to process song_data and log_data) at {}"\
            .format(stop))
    print("TIME: {}".format(stop-start))

    print("Running example queries...")
    query_examples( spark, \
                    songs_table, \
                    artists_table, \
                    users_table, \
                    time_table, \
                    songplays_table)
    
    print(''' "Fine" ''')

if __name__ == "__main__":
    main()
