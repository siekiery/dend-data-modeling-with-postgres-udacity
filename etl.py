import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import numpy as np

# To adapt np.int64 format in psycopg2 (https://stackoverflow.com/questions/50626058/psycopg2-cant-adapt-type-numpy-int64)
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


def process_song_file(cur, filepath):
    """Procedure for processing the song data. 
    Reads json file and extracts artist data and stores it in the artists table.
    Then it extracts song data and stores it in the songs table. 
    
    Args:
        cur (psycopg2.extensions.cursor): Cursor for querying the database
        filepath (string): Filepath to song data in json format.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # insert artist record
    artist_data = df.loc[0, ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].tolist()
    cur.execute(artist_table_insert, artist_data)

    # insert song record
    song_data = df.loc[0, ['song_id', 'title', 'artist_id', 'year', 'duration']].tolist()
    cur.execute(song_table_insert, song_data)



def process_log_file(cur, filepath):
    """Procedure for processing the log data. 
    Reads json file and extracts log data filtered by NextSong action. 
    Then converts timestamp to time data records and stores it in the time table. 
    Extracts user data and stores in the users table.
    Extracts songplay data and stores along song_id and artist_id foreign keys in the songplays table.
    
    Args:
        cur (psycopg2.extensions.cursor): Cursor for querying the database.
        filepath (string): Filepath to log json file.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = np.transpose((df['ts'], t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday))
    column_labels = ('ts', 'hour', 'day', 'week', 'month',' year',' weeekday')
    time_df = pd.DataFrame(time_data, columns=column_labels).astype(int)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.filter(['userId', 'firstName', 'lastName', 'gender', 'level'])

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, round(row.length, 3)))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Procedure for processing the files.
    It creates a list of json files. Then walks over files and applies the processing function. 
    Notification about the files found and progress is printed.
    
    Args:
        cur (psycopg2.extensions.cursor): Psycopg2 cursor for querying the database.
        conn (psycopg2.extensions.connection): Psycopg2 connection to the database.
        filepath (string): Filepath to json.
        func (function): Appropriate processing function to call. Either process_song_file or process_log_file.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """Main of ETL pipeline. Connects to database and processes data files."""
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()