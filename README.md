### Purpose
The purpose is this database helps in keeping the keeping the files in a systematic manner and managing large ampunt of info in small time. From this the Analytics team can run reports and perform analysis for growth of the company.

### Datasets
1. Song Dataset - Complete details and metadata about the song.
2. Log Dataset - User activity log.

### Database Schema 
we are using Star Schema to model dividing them into one fact table (songplays) and dimensions(users, songs, artists etc) so that it is structured.

### Fact Table:
songplays table which contains the metadata of the complete information about each user activity. Dimension table are connected to this fact table.

### Dimension Tables:
users,songs,artists,time are dimension tables. These tables will be having detailed info of fact table data.

### Staging Tables:
stagingevents, stagingsongs are used to load data from S3 before inserting into redshift.

### Steps to run the project 

1) Fill dwh.cfg with information to start a redshift cluster.
2) Run Redshift_Cluster_Create - Jupyter notebook to create redshift cluster.
3) Run create_tables.py - python file to drop and create tables.
4) Run etl.py - python file to create the etl pipeline to insert data into tables.
5) Run test_analytics.py - python file to run some basic analytical queries.

### Analytical queries and results

select COUNT(*) AS total FROM artists
10025

select COUNT(*) AS total FROM songs
14896

select COUNT(*) AS total FROM time
6813

select COUNT(*) AS total FROM users
104

select COUNT(*) AS total FROM songplays
39828

#### Don't forgot to delete cluster by running the last steps.
