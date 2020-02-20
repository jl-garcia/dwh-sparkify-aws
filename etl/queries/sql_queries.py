import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# CREATE SCHEMA
create_and_set_schema = """
    CREATE SCHEMA IF NOT EXISTS sparkify;
    SET search_path TO sparkify;
"""

# DELETE SCHEMA
delete_schema = """
    DROP SCHEMA IF EXISTS sparkify CASCADE;
"""

# DROP TABLES
staging_events_table_drop = 'DROP TABLE IF EXISTS staging_events;'
staging_songs_table_drop = 'DROP TABLE IF EXISTS staging_songs;'

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
        artist        VARCHAR(250),
        auth          VARCHAR(250),
        firstName     VARCHAR(250),
        gender        CHAR(1),
        itemInSession INTEGER,
        lastName      VARCHAR(250),
        length        DECIMAL(9,5),
        level         VARCHAR(10),
        location      VARCHAR(250),
        method        VARCHAR(5),
        page          VARCHAR(20),
        registration  DOUBLE PRECISION,
        sessionId     INTEGER,
        song          VARCHAR(250),
        status        INTEGER,
        ts            REAL,
        userAgent     VARCHAR(250),
        userId        INTEGER
    );    
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        artist_id        VARCHAR(18),
        artist_latitude  REAL,
        artist_location  VARCHAR(250),
        artist_longitude REAL,
        artist_name      VARCHAR(250),
        duration         DECIMAL(9,5),
        num_songs        INTEGER,
        song_id          VARCHAR(18),
        title            VARCHAR(250),
        year             INTEGER
    );
""")

# Dimension tables
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id INTEGER IDENTITY (1,1) PRIMARY KEY UNIQUE distkey,
        start_time  REAL         NOT NULL sortkey,
        user_id     VARCHAR(18)  NOT NULL,
        level       VARCHAR(10)  NOT NULL,
        song_id     VARCHAR(18),
        artist_id   VARCHAR(18),
        session_id  VARCHAR(18)  NOT NULL,
        location    VARCHAR(250) NOT NULL,
        user_agent  VARCHAR(250) NOT NULL
    );        
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        user_id    INTEGER      NOT NULL PRIMARY KEY UNIQUE sortkey,
        first_name VARCHAR(250) NOT NULL,
        last_name  VARCHAR(250) NOT NULL,
        gender     CHAR(1)      NOT NULL,
        level      VARCHAR(10)  NOT NULL
    ) diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id   VARCHAR(18)   NOT NULL PRIMARY KEY UNIQUE distkey,
        title     VARCHAR(250)  NOT NULL,
        artist_id VARCHAR(18)   NOT NULL,
        year      INTEGER       NOT NULL sortkey,
        duration  DECIMAL(9, 5) NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
        artist_id VARCHAR(18)  NOT NULL PRIMARY KEY UNIQUE sortkey,
        name      VARCHAR(250) NOT NULL,
        location  VARCHAR(250),
        latitude  REAL,
        longitude REAL
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time REAL NOT NULL PRIMARY KEY UNIQUE sortkey,
        hour       INTEGER     NOT NULL,
        day        INTEGER     NOT NULL,
        week       INTEGER     NOT NULL,
        month      VARCHAR(10) NOT NULL,
        year       INTEGER     NOT NULL,
        weekday    VARCHAR(10) NOT NULL
    ) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM 's3://udacity-dend/log_data'
        credentials 'aws_iam_role={}'
        region 'us-west-2'
        format as JSON 's3://udacity-dend/log_json_path.json'
""")

staging_songs_copy = ("""
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

create_staging_tables = [staging_events_table_create, staging_songs_table_create]
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create,
                        time_table_create]

drop_staging_tables = [staging_events_table_drop, staging_songs_table_drop]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_songs_copy, staging_events_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert, songplay_table_insert]
