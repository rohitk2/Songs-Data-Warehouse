import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events cascade;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs cascade;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay cascade;"
user_table_drop = "DROP TABLE IF EXISTS users cascade;"
song_table_drop = "DROP TABLE IF EXISTS songs cascade;"
artist_table_drop = "DROP TABLE IF EXISTS artists cascade;"
time_table_drop = "DROP TABLE IF EXISTS time cascade;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist text,
        auth text,
        firstName text,
        gender text,
        itemInSession bigint,
        lastName text,
        length float,
        level text,
        location text,
        method text,
        page text,
        registration numeric,
        sessionId bigint,
        song text,
        status bigint,
        ts bigint,
        userAgent text,
        userId bigint
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id text,
        artist_latitude float,
        artist_longitude float,
        artist_location text,
        artist_name text,
        song_id text,
        num_songs int,
        title text,
        duration float,
        year int
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
        songplay_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        start_time bigint NOT NULL,
        user_id bigint NOT NULL,
        level text NOT NULL,
        song_id text NOT NULL,
        artist_id text NOT NULL,
        session_id bigint NOT NULL,
        location text NOT NULL,
        user_agent text NOT NULL
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id bigint NOT NULL PRIMARY KEY,
        firstName text NOT NULL,
        lastName text NOT NULL,
        gender text,
        level text
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id text NOT NULL PRIMARY KEY,
        title text NOT NULL,
        artist_id text NOT NULL,
        year int,
        duration float
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id text NOT NULL PRIMARY KEY,
        name text,
        location text,
        latitude float,
        longitude float
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        starttime bigint NOT NULL PRIMARY KEY,
        hour int NOT NULL,
        day int NOT NULL,
        week int NOT NULL,
        month int NOT NULL,
        year int NOT NULL,
        weekday text NOT NULL
    );
""")

# STAGING TABLES

#staging_events_copy = ("""
#    COPY staging_events
#    FROM 'https://udacity-project2-rkumar1998.s3.us-west-2.amazonaws.com/events/events.csv'
#    IAM_ROLE '{}';
#""").format(DWH_ROLE_ARN)

#staging_songs_copy = ("""
#    COPY staging_songs
#    FROM 'https://udacity-project2-rkumar1998.s3.us-west-2.amazonaws.com/songs/songs.csv'
#    IAM_ROLE '{}';
#""").format(DWH_ROLE_ARN)



# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT 
            se.ts AS start_time,
            se.userId AS user_id,
            se.level,
            ss.song_id,
            ss.artist_id,
            se.sessionId AS session_id,
            se.location,
            se.userAgent AS user_agent
        FROM staging_events se
        JOIN staging_songs ss
            ON se.song = ss.title AND se.artist = ss.artist_name
        WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, firstName, lastName, gender, level)
        SELECT 
            se.userId AS user_id,
            se.firstName AS first_name,
            se.lastName AS last_name,
            se.gender,
            se.level
        FROM staging_events se
        WHERE se.userId IS NOT NULL
            AND se.page = 'NextSong'
            AND NOT EXISTS (
                SELECT 1 FROM users u WHERE u.user_id = se.userId
            );
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT DISTINCT 
            ss.song_id,
            ss.title,
            ss.artist_id,
            ss.year,
            ss.duration
        FROM staging_songs ss;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT 
            ss.artist_id,
            ss.artist_name AS name,
            ss.artist_location AS location,
            ss.artist_latitude AS latitude,
            ss.artist_longitude AS longitude
        FROM staging_songs ss;
""")

time_table_insertOLD = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
           TIMESTAMP 'epoch' + start_time / 1000 * INTERVAL '1 second' AS start_time,
           EXTRACT(HOUR FROM TIMESTAMP 'epoch' + start_time / 1000 * INTERVAL '1 second') AS hour,
           EXTRACT(DAY FROM TIMESTAMP 'epoch' + start_time / 1000 * INTERVAL '1 second') AS day,
           EXTRACT(WEEK FROM TIMESTAMP 'epoch' + start_time / 1000 * INTERVAL '1 second') AS week,
           EXTRACT(MONTH FROM TIMESTAMP 'epoch' + start_time / 1000 * INTERVAL '1 second') AS month,
           EXTRACT(YEAR FROM TIMESTAMP 'epoch' + start_time / 1000 * INTERVAL '1 second') AS year,
           EXTRACT(DOW FROM TIMESTAMP 'epoch' + start_time / 1000 * INTERVAL '1 second') AS weekday
    FROM songplay;
""")


time_table_insert = ("""
INSERT INTO time (starttime, hour, day, week, month, year, weekday)
SELECT DISTINCT
       se.ts AS starttime,
       EXTRACT(HOUR FROM TIMESTAMP 'epoch' + se.ts / 1000 * INTERVAL '1 second') AS hour,
       EXTRACT(DAY FROM TIMESTAMP 'epoch' + se.ts / 1000 * INTERVAL '1 second') AS day,
       EXTRACT(WEEK FROM TIMESTAMP 'epoch' + se.ts / 1000 * INTERVAL '1 second') AS week,
       EXTRACT(MONTH FROM TIMESTAMP 'epoch' + se.ts / 1000 * INTERVAL '1 second') AS month,
       EXTRACT(YEAR FROM TIMESTAMP 'epoch' + se.ts / 1000 * INTERVAL '1 second') AS year,
       EXTRACT(DOW FROM TIMESTAMP 'epoch' + se.ts / 1000 * INTERVAL '1 second') AS weekday
FROM staging_events se;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
#copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]