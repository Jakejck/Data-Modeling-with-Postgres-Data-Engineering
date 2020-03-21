import os
import glob
import psycopg2
import pandas as pd
from datetime import datetime
from sql_queries import *


def process_song_file(cur, filepath):
    
    """
    Description: This function can be used to read the file in the filepath (data/song_data)
    to get the user and time info and used to populate the users and time dim tables.
    
    Arguments:
        cur: the cursor object. 
        filepath: log data file path. 
        
    Returns:
        None
    """ 
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    
    song_data =[]
    for i in df.loc[:,['song_id','title','artist_id','year','duration']].values.tolist():
        x = 0 
        while x < len(i):
            song_data.append(i[x])
            x += 1 
            
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data =[]
    for i in df.loc[:,['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values.tolist():
        x = 0
        while x < len(i):
            artist_data.append(i[x])
            x +=1
    cur.execute(artist_table_insert, artist_data)



def process_log_file(cur, filepath):
    
    """
    Description: This function can be used to read the file in the filepath (data/log_data)
    to get the user and time info and used to populate the users and time dim tables.
    
    Arguments:
        cur: the cursor object. 
        filepath: log data file path. 
        
    Returns:
        None
    """ 

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    df_ts = df['ts'].apply (lambda x: x/1000)
    t = df_ts.apply(lambda x: datetime.fromtimestamp(x))
    
    # insert time data records
    
    timestamp = []
    hour = []
    day = []
    week_of_year = []
    month = []
    year = []
    weekday = []
    
    for i in t:
        timestamp.append(datetime.timestamp(i))
    
    for i in t:
        hour.append(i.strftime("%H"))
        
    for i in t:
        day.append(i.strftime("%d"))
    
    for i in t:
        week_of_year.append(i.strftime("%W"))
    
    for i in t:
        month.append(i.strftime("%m"))    
    
    for i in t:
        year.append(i.strftime("%Y"))
    
    for i in t:
        weekday.append(i.strftime("%A"))
    
    time_data = [timestamp,hour,day,week_of_year,month,year,weekday]
    column_labels = ['timestamp','hour','day','weeek_of_year','month','year','weekday']
    
    dict_trans = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame(dict_trans)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        results = cur.execute(song_select, (row.song, row.artist, row.length))
        songid, artistid = results if results else None, None

        # insert songplay record
        songplay_data = [row.ts,row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    """
    Description: This function can be used to insert the file data into tables in the Database
    
    Arguments:
        cur: the cursor object. 
        conn: the connection with database
        filepath: log data or song data file path.
        func : file process function(process_song_file,process_log_file)
        
    Returns:
        None
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
    Description: This function is the main function for the etl process
    
    Arguments:
        None
        
    Returns:
        None
    """ 
    
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()