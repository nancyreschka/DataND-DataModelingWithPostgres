import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Reads one song file
    - Inserts song record into sparkifydb
    - Inserts artist record into sparkifydb
    """

    # open song file
    df = pd.DataFrame([pd.read_json(filepath,  typ='series')])

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = [df.values[0][1], df.values[0][5], df.values[0][4], df.values[0][2], df.values[0][3]]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Reads one log file
    - Filters data by NextSong action
    - Converts timestamp to datetime
    - Inserts time data record into sparkifydb
    - Loads user table data
    - Inserts user record into sparkifydb
    - Gets songid and artistid from song and artist tables
    - Inserts songplay record into sparkifydb
    """
    
    # open log file
    df = pd.DataFrame(pd.read_json(filepath,  lines=True))

    # filter by NextSong action
    df = df[(df["page"] == "NextSong")]
    
    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')
    
    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.week , t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ('timestamp', 'hour', 'day', 'week of year', 'month', 'year', 'weekday')
    time_df = pd.DataFrame({c: d for c,d in zip (column_labels, time_data)}).dropna()
    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
        
    # load user table
    user_data = [df.userId, df.firstName, df.lastName, df.gender, df.level]
    column_labels_user = ('userId', 'firstName', 'lastName', 'gender', 'level')
    user_df = pd.DataFrame(user_data, column_labels_user)
    user_df = user_df.transpose()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    
    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - Gets all json files from directory
    - States the total number of found files
    - Processes each found file with the given function in "func"
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
    """
    - Connects to sparkifydb
    - Processes all files in 'data/song_data'
    - Processes all files in 'data/log_data'
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()