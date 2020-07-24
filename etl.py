import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, TimestampType
from pyspark.sql.functions import udf, col, from_unixtime
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id 


'''Establishes AWS authentication credentials'''
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_CREDS']['AWS_SECRET_ACCESS_KEY']

'''Creates a Spark session to be used for generating a Spark instance'''
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

'''Reads from the songs data file to create the songs and artists data tables
   These tables will be converted into parquet files, stored in an AWS S3 storage
'''

def process_song_data(spark, input_data, output_data):
    song_data = os.path.join(input_data,  "song_data/*/*/*/*.json")
    
    song_df = spark.read.json(song_data)

    song_df.createOrReplaceTempView("songs")
    songs_table = spark.sql("""SELECT DISTINCT song_id as songId,
                                               title as title,
                                               artist_id as artistId,
                                               year as year,
                                               duration as duration
                                FROM songs          
                                ORDER BY songId""")
    
    try:
        songs_table.write.mode("overwrite").partitionBy("year","artistId").parquet(os.path.join(output_data, "songs.parquet"))
        print("Successfully wrote parquet files for songs table")
    
    except:
        print("There was an error writing parquet files for songs table")
    
    song_df.createOrReplaceTempView("artists")
    artists_table = spark.sql("""SELECT DISTINCT artist_id as artistId,
                                                 artist_name as name,
                                                 artist_location as location,
                                                 artist_latitude as latitude,
                                                 artist_longitude as longitude
                                                 
                                 FROM artists
                                 ORDER BY artistId""")
    
    try:
        artists_table.write.mode("overwrite").partitionBy("artistId").parquet(os.path.join(output_data,"artists.parquet"))
        print("Successfully wrote parquet files for artists table")
    
    except:
        print("There was an error writing parquet files for artists table")

'''Reads from the log files to create data tables for users, time and songplays tables
   These tables will then be converted into parquet files, stored in AWS S3'''
def process_log_data(spark, input_data, output_data):
    log_data = os.path.join(input_data,"log_data/*/*/*.json")

    log_df = spark.read.json(log_data)
    

'''Log files will be filtered to include the start of a new song, which uses the criteria
page = NextSong'''
    log_df_filtered = log_df.filter(log_df.page == "NextSong") 
    log_df_filtered.createOrReplaceTempView("events")

    # extract columns for users table    
    users_table = spark.sql("""SELECT DISTINCT userId as user_id,
                                               firstName as firstName,
                                               lastName as lastName,
                                               gender as gender,
                                               level as level
                                               
                                FROM events
                                ORDER BY user_id""")
    

    try:
        users_table.write.mode("overwrite").partitionBy("user_id").parquet(os.path.join(output_data, "users.parquet"))
        print("Successfully wrote parquet files for users")
    
    except:
        print("There was an error writing parquet file to specified bucket")
        
    
    '''Since the timestamp values from the original log table (ts) are in milliseconds, the first function
    will convert the ts values into seconds, which will be a column named timestamp'''
    get_timestamp = udf(lambda x: x/1000, FloatType())
    log_df_filtered = log_df_filtered.withColumn('timestamp',get_timestamp('ts'))
    
    '''Using timestamp, the datetime format will be extracted using the from_unixtime function'''
    get_datetime = udf(lambda x: from_unixtime(x), TimestampType())
    log_df_filtered = log_df_filtered.withColumn('datetime', get_datetime('timestamp'))
    
    log_df_filtered.createOrReplaceTempView("events")
    

    time_table = spark.sql("""SELECT DISTINCT datetime as start_time,
                                              hour(datetime) as hour,
                                              day(datetime) as day,
                                              weekofyear(datetime) as week,
                                              month(datetime) as month,
                                              year(datetime) as year,
                                              dayofmonth(datetime) as weekday
                                              
                               FROM events
                               ORDER BY start_time""")
    
    try:
        time_table.write.mode("overwrite").partitionBy("year","month").parquet(os.path.join(output_data,"time.parquet"))
        print("Sucessfully wrote parquet files for time")
    except:
        print("There was an error writing parquet files for time table")
    
    song_data = os.path.join(input_data,  "song_data/*/*/*/*.json") 
    song_df = spark.read.json(song_data)


    log_df_filtered = log_df_filtered.withColumn("songplay_id", monotonically_increasing_id())
    
    song_log_joined_df = song_df.join(log_df_filtered, (log_df_filtered.artist == song_df.artist_name) & (log_df_filtered.song == song_df.title)) 
    
    song_log_joined_df.createOrReplaceTempView("merged_tables")
    songplays_table = spark.sql("""SELECT songplay_id as songplay_id,
                                          datetime as start_time,
                                          month(datetime) as month,
                                          year(datetime) as year,
                                          userId as user_id,
                                          level as level,
                                          song_id as songId,
                                          artist_id as artistId,
                                          sessionId as sessionId,
                                          location as location,
                                          userAgent as userAgent
                                          
                                    FROM merged_tables
                                    ORDER BY songplay_id""")
    
    try:
        songplays_table.write.mode("overwrite").partitionBy("year","month").parquet(os.path.join(output_data, "songplays.parquet"))
        print("Successfully printed parquet files for songplays table")
    except:
        print("There was an error writing parquet files for songplays table.")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    
if __name__ == "__main__":
    main()
