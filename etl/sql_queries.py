import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# STAGING TABLES

staging_events_copy = ("""
    SET search_path TO sparkify;
    COPY staging_events FROM 's3://udacity-dend/log_data'
        credentials 'aws_iam_role={}'
        region 'us-west-2'
        format as JSON 's3://udacity-dend/log_json_path.json'
""")

staging_songs_copy = ("""
    SET search_path TO sparkify;
    COPY staging_songs FROM 's3://udacity-dend/song_data'
        credentials 'aws_iam_role={}'
        region 'us-west-2'
        json 'auto'
""")

# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays
        (start_time,
         user_id,
         level,
         song_id,
         artist_id,
         session_id,
         location,
         user_agent)
        SELECT events.ts,
               events.userid,
               events.level,
               songs.song_id,
               songs.artist_id,
               events.sessionId,
               events.location,
               events.userAgent
        FROM (SELECT ts,
                     userId,
                     level,
                     song,
                     sessionId,
                     location,
                     userAgent,
                     length
              FROM staging_events
              WHERE page = 'NextSong'
                AND song IS NOT NULL) events
                 LEFT JOIN songs songs
                           ON (events.song = songs.title
                               AND events.length = songs.duration);
    
""")

user_table_insert = ("""
    INSERT INTO users(
        SELECT 
        DISTINCT (userId) AS userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page = 'NextSong'
        AND userId is not null
        GROUP BY userId, firstname, lastname, gender, level
);

""")

song_table_insert = ("""
    INSERT INTO songs (
    SELECT st.song_id,
           st.title,
           st.artist_id,
           st.year,
           st.duration
    FROM staging_songs st
);
""")

artist_table_insert = ("""
    INSERT INTO artists (
 	  SELECT 
    	   DISTINCT(s.artist_id), s.artist_name, s.artist_location, s.artist_latitude, s.artist_longitude
        FROM staging_songs s
        WHERE s.artist_id IS NOT NULL
        GROUP BY s.artist_id, s.artist_name, s.artist_location, s.artist_latitude, s.artist_longitude
);
""")

time_table_insert = ("""
    INSERT INTO time (
        SELECT ts,
               extract(hour from timestamp 'epoch' + ts / 1000 * interval '1 second') as hour,
               extract(day from timestamp 'epoch' + ts / 1000 * interval '1 second')  as day,
               extract(week from timestamp 'epoch' + ts / 1000 * interval '1 second') as week,
               to_char(timestamp 'epoch' + ts / 1000 * interval '1 second', 'MONTH')  as month,
               extract(year from timestamp 'epoch' + ts / 1000 * interval '1 second') as year,
               to_char(timestamp 'epoch' + ts / 1000 * interval '1 second', 'DAY')    as weekday
        FROM staging_events
        WHERE page = 'NextSong'
);
""")

# QUERY LISTS
copy_table_queries = [staging_songs_copy, staging_events_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert, songplay_table_insert]
