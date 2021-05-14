import os
import io
import glob
import psycopg2
import pandas as pd
from sql_queries import *


#Global variable containing data of Songs and Artists for bulk insert
bulk_song_df_dict = {
    'songs': pd.DataFrame(columns=['song_id', 'song_title', 'song_artist_id', 'song_year', 'song_duration']),
    'artist': pd.DataFrame(columns=['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])
}

#Global variable containing data of time, users and songs plays for bulk insert
bulk_log_df_dict = {
    'time': pd.DataFrame(columns=['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']),
    'users': pd.DataFrame(columns=['userId', 'firstName', 'lastName', 'gender', 'level']),
    'songplays': pd.DataFrame(columns=['start_time', 'userId', 'level', 'song_id', 'artist_id', \
                                       'session_id', 'location', 'user_agent']),
}

def copy_dataframes_to_db(conn, cur, df, table):
    """Does the buld copy of data to the database
    Args:
        conn: psycopg2 connection
        cur: psycopg2 cursor
        df: dataframe containing data for bulk insert
        filepath: Song's file location
    Returns:
        None
    """
    sio = io.StringIO()
    sio.write(df.to_csv(index=False, header=False, na_rep='NULL', sep='|'))
    # reset the position to the start of the stream
    sio.seek(0)
    cur.copy_from(sio, table, columns=df.columns, sep='|', null='NULL')
    conn.commit()

def process_song_file(conn, cur, filepath):
    """Process and load data for song file
    Args:
        cur: psycopg2 cursor
        filepath: Song's file location
    Returns:
        None
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    df.head()
    
    global bulk_song_df_dict

    # insert song records to global variable
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data.columns = ['song_id', 'song_title', 'song_artist_id', 'song_year', 'song_duration']
    bulk_song_df_dict['songs'] = bulk_song_df_dict['songs'].append(song_data, sort=False)
   
    # insert artist records to global variable
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    bulk_song_df_dict['artist'] = bulk_song_df_dict['artist'].append(artist_data, sort=False)
    #print(artist_data)



def process_log_file(conn, cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df =  df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data =  [(dt, dt.hour, dt.day, dt.week, dt.month, dt.year, dt.weekday()) for dt in t]
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(time_data, columns=column_labels)
    bulk_log_df_dict['time'] = bulk_log_df_dict['time'].append(time_df, sort = False)

    # load user table
    user_df = df[['userId','firstName', 'lastName', 'gender', 'level']]
    user_df.columns = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_df = user_df[user_df['userId'].astype(bool)]
    user_df['userId'] = df['userId'].astype(str)
    user_df = user_df.drop_duplicates(subset='userId')
    bulk_log_df_dict['users'] = bulk_log_df_dict['users'].append(user_df, sort = False)
    
    # insert songplay records
    rows_list = []
    for index, row in df.iterrows():        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()        
       
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            continue
        
        rows_list.append({
            'start_time': pd.to_datetime(round(row.ts / 1000.0)),
            'userId': row.userId,
            'level': row.level,
            'song_id': songid,
            'artist_id': artistid,
            'session_id': row.sessionId,
            'location': row.location,
            'user_agent': row.userAgent
        })

    songplay_df = pd.DataFrame(rows_list)
    bulk_log_df_dict['songplays'] = bulk_log_df_dict['songplays'].append(songplay_df)
    print("Song plays", bulk_log_df_dict['songplays'])

def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []

    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    #print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(conn, cur, datafile)
        conn.commit()
        #print('{}/{} files processed.'.format(i, num_files))

        
def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    #Create Song's and Artists data frame from file
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    
    #Bulk copy of the songs data from above data frame
    bulk_song_df_dict['songs'].drop_duplicates(subset='song_id', keep="first", inplace = True)
    copy_dataframes_to_db(conn, cur,  bulk_song_df_dict['songs'], "songs")
    
    #Bulk copy of the artists data from above created data frame
    bulk_song_df_dict['artist'].drop_duplicates(subset='artist_id', keep="first", inplace = True)
    copy_dataframes_to_db(conn, cur,  bulk_song_df_dict['artist'], "artist")
        
    #Create Time, users and songplays data from file
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    
    #Bulk copy of the time, users and songplay data from above created data frame
    bulk_log_df_dict['time'].drop_duplicates(subset='start_time', keep="first", inplace = True)
    copy_dataframes_to_db(conn, cur,  bulk_log_df_dict['time'], "time")
    
    bulk_log_df_dict['users'].drop_duplicates(subset='userId', keep="first", inplace = True)
    copy_dataframes_to_db(conn, cur,  bulk_log_df_dict['users'], "users")
    
    copy_dataframes_to_db(conn, cur,  bulk_log_df_dict['songplays'], "songplays")

    conn.close()


if __name__ == "__main__":
    main()