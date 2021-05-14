# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artist CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                            songplay_id SERIAL PRIMARY KEY,
                            start_time TIMESTAMP NOT NULL,
                            userId BIGINT NOT NULL,
                            level VARCHAR,
                            song_id VARCHAR,
                            artist_id VARCHAR,
                            session_id INT,
                            location VARCHAR,
                            user_agent VARCHAR                       
                                               
                            );
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users ( 
                              userId BIGINT PRIMARY KEY,
                              firstName VARCHAR NOT NULL,
                              lastName VARCHAR NOT NULL,
                              gender VARCHAR,
                              level VARCHAR NOT NULL);
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs (
                         song_id VARCHAR PRIMARY KEY,
                         song_title VARCHAR NOT NULL,
                         song_artist_id VARCHAR,
                         song_year INT,
                         song_duration FLOAT NOT NULL           

                        )
""")



artist_table_create = (""" CREATE TABLE IF NOT EXISTS artist (
                            artist_id VARCHAR PRIMARY KEY,
                            artist_name VARCHAR NOT NULL,
                            artist_location VARCHAR,
                            artist_latitude FLOAT,
                            artist_longitude FLOAT
                            )
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (
                         start_time TIMESTAMP PRIMARY KEY,
                         hour INT NOT NULL,
                         day INT NOT NULL,
                         week INT NOT NULL,
                         month INT NOT NULL,
                         year INT NOT NULL,
                         weekday VARCHAR NOT NULL
                        )
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (start_time, userId, level, song_id, artist_id, session_id, location, user_agent)
                                    VALUES (to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""INSERT INTO users(userId,firstName,lastName,gender,level)
                        VALUES(%s,%s,%s,%s,%s)
                        ON CONFLICT (userId) DO UPDATE
                        SET level = EXCLUDED.level
;
""")

song_table_insert = ("""INSERT INTO songs (song_id, song_title, song_artist_id, song_year, song_duration)
                                    VALUES (%s, %s, %s, %s, %s)
                                    ON CONFLICT(song_id) DO NOTHING
""")

artist_table_insert = ("""INSERT INTO artist (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
                                    VALUES (%s, %s, %s, %s, %s)
                                    ON CONFLICT (artist_id)
                                    DO UPDATE SET artist_location = EXCLUDED.artist_location, 
                                    artist_latitude = EXCLUDED.artist_latitude, artist_longitude = EXCLUDED.artist_longitude
""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                                    VALUES (to_timestamp(%s), %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT(start_time) DO NOTHING
""")

# FIND SONGS


song_select = (""" SELECT songs.song_id, songs.song_artist_id FROM songs
                   INNER JOIN artist ON  songs.song_artist_id = artist.artist_id 
                                         WHERE songs.song_title = %s
                                         AND artist.artist_name = %s 
                                         AND songs.song_duration = %s
                   
""")


# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]