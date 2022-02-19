# Sparkify Data Lake

The enclosed code configures AWS IAM credentials to read data from a public AWS S3 bucket, udacity-dend. Two files, i.e., log and songs, will be read using a Spark instance and used to create a star schema using the SQL functions in the Pyspark.sql module. These new tables will then be written to parquet files in a user-supplied AWS S3 URL. The following are user-required parameters that should be populated in the **dl.cfg** file:

## AWS Credentials
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=

## AWS Datasets
The two datasets, **song_data*** and **log_data** are hierarchically organized json files located in the AWS S3 bucket **udacity/dend**.


## Star Schema
The AWS files are used to generate the following tables: **songplays**, **users**, **songs**, **artists**, and **time**.

## FACT TABLE
**songplays**
This fact table is composed of the following fields: songplay_id, start_time, year, month, user_id, level, song_id, artist_id, session_id, location, user_agent. 

## DIMENSION TABLES
**users**
This dimension table is composed of user_id, first_name, last_name, gender, level

**songs**
This dimension table is composed of song_id, title, artist_id, year, duration

**artists**
This dimension table is composed of artist_id, name, location, latitutde, longitude

**time**
This dimension table is composed of timestamp, hour, day, week, month, year, weekday

## ETL Pipeline
The dimension tables are constructed using the S3 data files, song_data and log_data. Specifically, the songs and artists table is created from the song_data files. The users and time are constructed from the log_data. The songplays table is constructed from the joining of both the songs and logs data files. 

## Output
Once the files are read, they are converted into SQL tables, which are then written to an AWS S3 bucket as parquet files. The **songplays** and **time** parquet files is partiotioned by year, followed by month. The **users** parquet files are partitioned by user_id, the **artists** parquet files are partitioned by year and artistId, and the **songs** parquet files are partitioned by song_id. 
