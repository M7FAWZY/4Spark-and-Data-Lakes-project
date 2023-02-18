Project Description
====================

In this project, you will apply what you have learned on Spark 
and data lakes to build an ETL pipeline for a data lake hosted on S3.
To complete the project, you will need to load data from S3,
process the data into analytics tables using Spark, and load them back into S3.
You will deploy this Spark process on a cluster using AWS.

------------------------------------------------------------

## ETL (Extract, Transform, Load) Pipeline
1.Load AWS credentials  **(dl.cfg)**

2.Read Sparkify data from S3

- song_data: s3://udacity-dend/song_data 
- log_data: s3://udacity-dend/log_data

> ` http://s3-us-west-2.amazonaws.com/udacity-dend/ `    

3.Process the data using  pySpark.
> Transform the data and create five tables (see **' Tables '** below)

4.Load data back into S3

Schema for Song Play Analysis
==============================
Using the song and log datasets,
you will need to create a star schema optimized for queries on song play analysis. 
This includes the following tables.

## Tables
### Fact Table

#### songplays -
***records in log data associated with song plays i.e. records with page NextSong***

songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
#### users - 
***users in the app***

user_id, first_name, last_name, gender, level

#### songs - 
***songs in music database***

song_id, title, artist_id, year, duration

#### artists - 
***artists in music database***

artist_id, name, location, lattitude, longitude

#### time - 
***timestamps of records in songplays broken down into specific units***

start_time, hour, day, week, month, year, weekday

----------------------------------------------------------

# FILES included
**etl.py** :: reads data from S3, processes that data using Spark, and writes them back to S3
**dl.cfg** :: contains your AWS credentials
**README.md** :: provides discussion on your process and decisions

Run etl.py
===========
using command line ` python3 etl.py `

## To execute ETL of Data Lake project on Spark Cluster in AWS(
to run and test etl.py on actual EMR Spark cluster on AWS)
Follow
FROM <https://knowledge.udacity.com/questions/46619#552992>
a set of steps:


*Start Up EMR Cluster:*
========================

1.Log into the AWS console for us-west2 and navigate to EMR

2.Click "Create Cluster"

3.Select "Go to advanced options"

4.Under "Software Configuration", select Hadoop, Hive, and Spark
> Optional: Select Hue (to view HDFS) and Livy (for running a notebook)

5.Under "Edit software settings", enter the following configuration:

` [{"classification":"spark", "properties":{"maximizeResourceAllocation":"true"}, "configurations":[]}] ` 

6.Click "Next" at the bottom of the page to go to the "Hardware" page

7.I found some EC2 subnets do not work in the Oregon region (where the Udacity S3 data is)

> For example, us-west-2b works fine. us-west-2d does not work (so don't select that)

8.You should only need a couple of worker instances (in addition to the master)

> m3.xlarge was sufficient for me when running against the larger song dataset

9.Click "Next" at the bottom of the page to go to the "General Options" page

10.Give your cluster a name and click "Next" at the bottom of the page

11.Pick your EC2 key pair in the drop-down.

> This is essential if you want to log onto the master node and set up a tunnel to it, etc.

12.Click "Create Cluster"

-------------------
-------------------

My Appreciation
================
 
I appreciate Reviewer/s in <https://review.udacity.com/#!/reviews/3929359>
<https://review.udacity.com/#!/reviews/3929600>  <https://review.udacity.com/#!/reviews/3929329>
also Mentor / Survesh in <https://knowledge.udacity.com/questions/890877>

-------------------
-------------------

*ToREAD to Enhance*
===================

<https://knowledge.udacity.com/>
<https://knowledge.udacity.com/questions/890877>

Partitioning helps queries to run much faster. See this article on detailed info on partition, 
<https://mungingdata.com/apache-spark/partitionby/>
<https://sparkbyexamples.com/pyspark/pyspark-repartition-usage/>
<https://sparkbyexamples.com/pyspark/pyspark-repartition-vs-partitionby/>
<https://sparkbyexamples.com/pyspark/pyspark-partitionby-example/>
Use as helper for Multiple Columns <https://sparkbyexamples.com/pyspark/pyspark-groupby-on-multiple-columns/>

Use for dayofweek <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.dayofweek.html>
<https://stackoverflow.com/questions/38928919/how-to-get-the-weekday-from-day-of-month-using-pyspark>
<https://spark.apache.org/docs/3.0.0-preview/sql-ref-datatypes.html>
<https://mungingdata.com/pyspark/column-to-list-collect-tolocaliterator/>

### README 
<https://www.markdownguide.org/basic-syntax/#paragraphs-1>
<https://www.lucidchart.com/>

<https://github.com/matiassingers/awesome-readme>

<https://bulldogjob.com/news/449-how-to-write-a-good-readme-for-your-github-project>

<https://medium.com/@meakaakka/a-beginners-guide-to-writing-a-kickass-readme-7ac01da88ab3>

<https://www.pythonforbeginners.com/basics/python-docstrings>

<https://realpython.com/python-pep8/>


### for JOIN

<https://sparkbyexamples.com/pyspark/pyspark-join-multiple-columns/>
<https://www.geeksforgeeks.org/how-to-avoid-duplicate-columns-after-join-in-pyspark/>

FOR **README.md** BY Markdown use simple guide in *Udacity Workspace* From menu use Help then Markdown Reference then  [TRY OUR 10 MINUTE MARKDOWN TUTORIAL](<https://commonmark.org/help/tutorial/>) 

<https://commonmark.org/help/tutorial/>

## test_run.ipynb
To test code on sample data 

1.download log_data.zip and song_data.zip 

2.unzip folders

3.create new folders log_data , song-data and output_data into workspace in folder data

4.upload some files into log_data , song_data with respective to order internal folders and files 

5.use instrcution to run test
 
*Good luck.*
