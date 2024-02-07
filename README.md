# Project: Data modelling with Postgres
## Author: Jakub Pitera

## Description
The purpose of the project was to build an ETL pipeline that extracts data stored in json files and loads it into Postgres SQL database.
The data are songs and logs from a music app of fictional company called Sparkify.

## Database schema
Database is built for analytical purposes. It consists of a fact table ('songplays') and 4 dimension tables (users, songs, artists, time) organized in a star schema.

## Analytical goals
This database not only is recording all activity on the app. It would allow for analytical queries like:
* What users that frequently listen on the app are still not subscribed to premium? 
* What operating system/browser/mobile are the users connecting from? Low activity of specific medium - look for expansion possibilities.
* What is the age distribution of the users? Optimize the marketing campaign.
* In what area is Sparkify app popular? What is the current market? Into which states, countries should it expand?
* Do users listen to band albums in their entiriety or prefer playlists of different artists?
* Broad analsys of app songplays. Popular bands, music genres, songplays during one session. 

## Running the scripts
Use terminal to run create_tables.py and etl.py.

## Files description
* create_tables.py - It restores sparkifydb database to default (empty) state. It drops all tables and the sparkifydb and recreates them.
* etl.py - a pipeline where the json data is processed and inserted into tables according to db schema.
* sql_queries.py - all SQL queries are stored here
* etl.ipynb - Notebook where the data was examined and the correct etl code was developed
* test.ipynb - Queries that allow to inspect and QC the database
* data - directory containing song_data and log_data json files
