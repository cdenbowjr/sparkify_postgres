import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):

    # open song file
    df = pd.read_json(filepath,lines=True)
    df2 = pd.read_csv('data/SongCSV.csv')

    # insert song record

    song_df = df2.copy()[['SongID', 'Title', 'ArtistID', 'Year', 'Duration']]
    song_df = song_df.applymap(lambda x: x[1:].strip("'").strip('"""') if type(x) == str else x)

    song_data = df.copy()[['song_id','title','artist_id','year','duration']]
    for i,row in song_data.iterrows():

        try:
            cur.execute(song_table_insert, list(row))

        except psycopg2.Error as e:
            print("Error: data not inserted")
            print(e)

    for i,row in song_df.iterrows():

        try:
            cur.execute(song_table_insert, list(row))

        except psycopg2.Error as e:
            print("Error: data not inserted")
            print(e)



    # # insert artist record
    artist_data =df.copy()[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']]
    artist_data.columns = ['artist_id', 'artist_name', 'location', 'latitude', 'longitude']

    artist_df = df2.copy()[['ArtistID', 'ArtistName', 'ArtistLocation', 'ArtistLatitude', 'ArtistLongitude']]
    artist_df = artist_df.applymap(lambda x: x[1:].strip("'").strip('"""') if type(x) == str else x)

    for i, row in artist_data.iterrows():

        try:
            cur.execute(artist_table_insert, list(row))

        except psycopg2.Error as e:
            print("Error: data not inserted")
            print(e)

    for i, row in artist_df.iterrows():

        try:
            cur.execute(artist_table_insert, list(row))

        except psycopg2.Error as e:
            print("Error: data not inserted")
            print(e)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    time_stamp = pd.to_datetime(df.ts,unit='ms')
    time_stamp.name = 'start_time'
    time_df = pd.DataFrame(time_stamp.dt.time)
    
    # insert time data records
    time_df['hour'] = time_stamp.dt.hour
    time_df['day'] = time_stamp.dt.day
    time_df['week'] = time_stamp.dt.week
    time_df['month'] = time_stamp.dt.month
    time_df['year'] = time_stamp.dt.year
    time_df['weekday'] = time_stamp.dt.day_name()

    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))

        except psycopg2.Error as e:
            print("Error: data not inserted")
            print(e)


    # load user table

    user_list = ['userId', 'firstName', 'lastName', 'gender', 'level']

    user_df = df.copy()[user_list]

    try:
        user_df.userId = user_df.userId.apply(lambda x: int(x))

    except psycopg2.Error as e:
        print("integer conversion failed")
        print(e)

    # insert user records
    for i, row in user_df.iterrows():

        try:
            cur.execute(user_table_insert, list(row))

        except psycopg2.Error as e:
            print("user record insertion failed")
            print(e)

    # insert songplay records
    songplay_df = df.copy()[['ts', 'userId', 'level', 'song', 'artist', 'sessionId', 'location', 'userAgent']]

    songplay_df['ts'] = pd.to_datetime(songplay_df['ts'], unit='ms')

    for i, row in songplay_df.iterrows():

        try:
            # get songid and artistid from song and artist tables
            cur.execute(song_select, (row.artist, row.song,))
            results = cur.fetchone()

            if results:
                row.artist, row.song = results

            else:
                row.artist, row.song = None, None

            # insert songplay record
            cur.execute(songplay_table_insert, list(row))

        except:
            print("songplay record insertion failed")


def process_data(cur, conn, filepath, func):
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=1234")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()