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

# Fact table
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

# Dimension table
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

# Dimension table
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

# Dimension table
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

# Dimension table
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

# QUERY LISTS
create_staging_tables = [staging_events_table_create, staging_songs_table_create]
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create,
                        time_table_create]

drop_staging_tables = [staging_events_table_drop, staging_songs_table_drop]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

