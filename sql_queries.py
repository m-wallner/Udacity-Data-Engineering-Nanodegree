import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')
S3_LOG_DATA = config.get('S3', 'LOG_DATA')
S3_LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
S3_SONG_DATA = config.get('S3', 'SONG_DATA')
DWH_REGION = config.get('DWH', 'DWH_REGION')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
        artist                  VARCHAR,
        auth                    VARCHAR,
        firstName               VARCHAR,
        gender                  VARCHAR,
        itemInSession           VARCHAR,
        lastName	            VARCHAR,
        length	                FLOAT,
        level	                VARCHAR,
        location	            VARCHAR,
        method	                VARCHAR,
        page	                VARCHAR,
        registration	        VARCHAR,
        sessionId	            INT,
        song	                VARCHAR,
        status	                INT,
        ts	                    BIGINT,
        userAgent	            VARCHAR,
        userId                  INT
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        song_id                 VARCHAR,
        artist_id	            VARCHAR,
        artist_latitude         FLOAT,
        artist_location	        VARCHAR,
        artist_longitude        FLOAT,
        artist_name             VARCHAR,
        duration                FLOAT,
        num_songs               INT,
        title	                VARCHAR,
        year                    INT
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id             INT IDENTITY(0,1)   NOT NULL    SORTKEY,
        start_time              TIMESTAMP           NOT NULL,
        user_id                 INTEGER             NOT NULL,
        level                   VARCHAR,
        song_id                 VARCHAR,
        artist_id               VARCHAR,
        session_id              INT,
        location                VARCHAR,
        user_agent              VARCHAR
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        user_id                 INT             NOT NULL    PRIMARY KEY,
        first_name              VARCHAR,
        last_name               VARCHAR,
        gender                  VARCHAR,
        level                   VARCHAR
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id                 VARCHAR         NOT NULL    PRIMARY KEY,
        title                   VARCHAR,
        artist_id               VARCHAR,
        year                    INT,
        duration                FLOAT
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
        artist_id	            VARCHAR         NOT NULL    PRIMARY KEY,
        name                    VARCHAR,
        location	            VARCHAR,
        latitude                FLOAT,
        longitude               FLOAT
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time              TIMESTAMP       NOT NULL    PRIMARY KEY,
        hour                    INT,
        day                     INT,
        week                    INT,
        month                   INT,
        year                    INT,
        weekday                 VARCHAR
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON {}
    REGION '{}'
    COMPUPDATE OFF
    TIMEFORMAT AS 'epochmillisecs'
""").format(S3_LOG_DATA, ARN, S3_LOG_JSONPATH, DWH_REGION)

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON 'auto'
    REGION '{}'
    COMPUPDATE OFF
""").format(S3_SONG_DATA, ARN, DWH_REGION)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
            se.userId AS user_id,
            se.level AS level,
            ss.song_id AS song_id,
            ss.artist_id AS artist_id,
            se.sessionId AS session_id,
            se.location AS location,
            se.userAgent AS user_agent
    FROM staging_events AS se
    JOIN staging_songs AS ss
        ON se.artist = ss.artist_name
        AND se.song = ss.title
    WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  userId,
            firstName,
            lastName,
            gender,
            level
    FROM staging_events AS se
    WHERE page = 'NextSong' AND userId IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  song_id,
            title,
            artist_id,
            year,
            duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT  artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  start_time,
            EXTRACT(hour from start_time),
            EXTRACT(day from start_time),
            EXTRACT(week from start_time),
            EXTRACT(month from start_time),
            EXTRACT(year from start_time),
            EXTRACT(weekday from start_time)
    FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
