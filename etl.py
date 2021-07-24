import glob
import os
import psycopg2
import pandas as pd
from sql_queries import *

def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))
            
    return all_files


def process_song_data(conn, cur, filepath):
    """
    Description: This function is responsible for reading the songs metadata JSON files
    and populating The artists and songs tables with the right data according to their
    columns
    Keyword arguments:
        cur      -- cursor
        filepath -- folder where data is stored   
    Returns:
        None
    """
    
    all_song_files = get_files(filepath)
    dfs = []
    for song_file_path in all_song_files:
        df_aux = pd.read_json(song_file_path, lines=True)
        dfs.append(df_aux)
    df = pd.concat(dfs, ignore_index=True)
    
    for index, row in df.iterrows():
        artist_data = (row.artist_id, row.artist_name, row.artist_location, row.artist_latitude, row.artist_longitude)
        cur.execute(artist_table_insert,artist_data)
        conn.commit()
        song_data = (row.song_id, row.title, row.artist_id, row.year, row.duration)
        cur.execute(song_table_insert, song_data)
        conn.commit()


def process_log_data(conn, cur, filepath):
    
    """
    Description: This function is responsible for reading the user activity logs JSON files
    and populating The time, users and song_plays tables with the right data according to their
    columns
    Keyword arguments:
        cur      -- cursor
        filepath -- folder where data is stored   
    Returns:
        None
    """

    all_logs_files = get_files(filepath)
    dfs = []
    for log_file_path in all_logs_files:
        df_aux = pd.read_json(log_file_path, lines=True)
        dfs.append(df_aux)
    df = pd.concat(dfs, ignore_index=True)
    df = df[df['page'] == 'NextSong']
    
    for index, row in df.iterrows():
        
        user_data = (row.userId, row.firstName, row.lastName, row.gender, row.level)
        cur.execute(user_table_insert, user_data)
        conn.commit()
        
        dt_obj = pd.to_datetime(row.ts, unit='ms')
        time_data = (dt_obj, dt_obj.hour, dt_obj.day, dt_obj.week, dt_obj.month, dt_obj.year, dt_obj.day_name())
        cur.execute(time_table_insert, time_data)
        
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None
            
        song_play_data = (pd.to_datetime(row.ts,unit='ms'), int(row.userId), row.level, song_id, artist_id, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, song_play_data)
        conn.commit()

def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    process_song_data(conn, cur, 'data/song_data')
    process_log_data(conn, cur, 'data/log_data')
    
if __name__ == '__main__':
    main()