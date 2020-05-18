# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

artist_table_create = ("""CREATE TABLE artists (artist_id varchar PRIMARY KEY,artist_name varchar,location varchar\
,latitude numeric,longitude numeric); """)

user_table_create = ("""CREATE TABLE users (user_id int PRIMARY KEY, first_name varchar, last_name varchar, gender varchar(1), level varchar(4));
""")

song_table_create = ("""CREATE TABLE songs (song_id varchar PRIMARY KEY,title varchar,artist_id varchar,year smallint,duration numeric);
""")

time_table_create = ("""CREATE TABLE time (start_time time PRIMARY KEY,hour int,day int,week int,month smallint,year smallint, weekday varchar(10));
""")

songplay_table_create = ("""CREATE TABLE songplay (songplay_id serial PRIMARY KEY, start_time time, user_id int, level varchar(4)\
,song_id varchar, artist_id varchar,session_id int, location varchar, user_agent varchar\
,FOREIGN KEY (user_id) REFERENCES users (user_id)\
,FOREIGN KEY (artist_id) REFERENCES artists (artist_id)\
,FOREIGN KEY (song_id) REFERENCES songs (song_id)\
,FOREIGN KEY (start_time) REFERENCES time (start_time));
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplay (start_time ,user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES(%s,%s,%s,%s,%s) ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;
""")

song_table_insert = ("""INSERT INTO songs (song_id,title,artist_id,year,duration) VALUES(%s,%s,%s,%s,%s) ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id,artist_name,location,latitude,longitude) VALUES(%s,%s,%s,%s,%s) ON CONFLICT (artist_id) DO NOTHING;
""")


time_table_insert = ("""INSERT INTO time (start_time,hour,day,week,month,year,weekday) VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS

song_select = ("""SELECT artists.artist_id,songs.song_id FROM artists \
            JOIN songs ON artists.artist_id = songs.artist_id \
            WHERE artists.artist_name = %s AND songs.title = %s""")

# QUERY LISTS
create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]