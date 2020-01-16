# udacity project 4

## summary
This project is for ETL pipeline demo on aws S3 spark data lake. first, load song and log dataset from S3. Second, using spark to analyze and preprocess Song and Log Dataset to match project's requirement.The results are 5 spark dataframes.(song, artist, user, time, songplay) Finally, writes them to partitioned parquet files in table directories on S3.  
 
## requirement
This project need aws ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and output data S3 bucket uri. you should write your own dl.cfg file for aws connection infomation yourself. 
 
## file definition
1. etl.py
python script for all ETL logic, including load raw data from S3, data preprocess, and write back to S3. execute this file in terminal.

2. dl.cfg
Provide your aws connection infomation yourself.

## execute command
```
$ python etl.py
```
