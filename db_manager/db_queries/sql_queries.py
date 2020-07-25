# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table;"
user_table_drop = "DROP TABLE IF EXISTS user_table;"
song_table_drop = "DROP TABLE IF EXISTS song_table;"
artist_table_drop = "DROP TABLE IF EXISTS artist_table;"
time_table_drop = "DROP TABLE IF EXISTS time_table;"

# CREATE TABLES

staging_songs_table_create = (
    """
    CREATE TABLE staging_songs_table (
        song_id VARCHAR NOT NULL DISTKEY SORTKEY,
        artist_id VARCHAR NOT NULL,
        artist_latitude VARCHAR,
        artist_location VARCHAR,
        artist_longitude VARCHAR,
        artist_name VARCHAR NOT NULL,
        duration DECIMAL(10, 5)  NOT NULL,
        num_songs INTEGER NOT NULL,
        title VARCHAR NOT NULL,
        year INTEGER NOT NULL 
    );
    """
)

staging_events_table_create = (
    """
    CREATE TABLE staging_events_table (
        artist VARCHAR,
        auth VARCHAR NOT NULL,
        first_name VARCHAR,
        gender VARCHAR,
        item_in_session INTEGER NOT NULL,
        last_name VARCHAR,
        length DECIMAL(10, 5),
        level VARCHAR NOT NULL,
        location VARCHAR, 
        method VARCHAR NOT NULL,
        page VARCHAR NOT NULL,
        registration VARCHAR,
        session_id INTEGER NOT NULL, 
        song VARCHAR,
        status VARCHAR,
        ts BIGINT NOT NULL DISTKEY SORTKEY,
        user_agent VARCHAR,
        user_id INTEGER
    );
    """
)

songplay_table_create = (
    """
    CREATE TABLE songplay_table(
        start_time DATETIME NOT NULL  DISTKEY SORTKEY,
        songplay_id INTEGER IDENTITY(1,1), 
        user_id INTEGER NOT NULL,
        song_id VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        session_id INTEGER NOT NULL,
        level VARCHAR NOT NULL,
        location VARCHAR NOT NULL,
        user_agent VARCHAR NOT NULL
    );
    """
)

user_table_create = (
    """
    CREATE TABLE user_table(
        user_id INTEGER NOT NULL SORTKEY, 
        first_name VARCHAR NOT NULL, 
        last_name VARCHAR NOT NULL, 
        gender VARCHAR NOT NULL, 
        level VARCHAR NOT NULL
    )
    DISTSTYLE ALL;
    """
)

song_table_create = (
    """
    CREATE TABLE song_table (
        song_id VARCHAR NOT NULL SORTKEY,
        artist_id VARCHAR NOT NULL,
        title VARCHAR NOT NULL,
        year INTEGER NOT NULL,
        duration DECIMAL(10, 5) NOT NULL
    )
    DISTSTYLE ALL;
    """
)

artist_table_create = (
    """
    CREATE TABLE artist_table(
        artist_id VARCHAR NOT NULL SORTKEY,
        artist_name VARCHAR NOT NULL,
        artist_location VARCHAR,
        artist_latitude VARCHAR,
        artist_longitude VARCHAR
    )
    DISTSTYLE ALL;
    """
)

time_table_create = (
    """
    CREATE TABLE time_table(
        start_time DATETIME NOT NULL DISTKEY SORTKEY,
        hour INTEGER NOT NULL,
        day INTEGER NOT NULL,
        week INTEGER NOT NULL,
        month INTEGER NOT NULL,
        year INTEGER NOT NULL,
        weekday INTEGER NOT NULL
    );
    """
)

# STAGING TABLES

staging_table_copy = (
    """
    COPY {}
    FROM '{}'
    IAM_ROLE '{}'
    JSON '{}'
    REGION '{}'
    """
)

# FINAL TABLES

songplay_table_insert = (
    """
    INSERT INTO songplay_table
        (user_id, song_id, artist_id, start_time, session_id, level, location, user_agent)
    SELECT 
        et.user_id,
        st.song_id,
        st.artist_id,
        TIMESTAMP 'epoch' + et.ts/1000 * interval '1 second' AS start_time,
        et.session_id,
        et.level,
        et.location,
        et.user_agent
    FROM staging_events_table et
    JOIN staging_songs_table st
        ON  st.duration = et.length
        AND st.title = et.song
        AND st.artist_name = et.artist 
    WHERE et.page = 'NextSong';
    """
)

user_table_insert = (
    """
    INSERT INTO user_table
    SELECT DISTINCT
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level
    FROM staging_events_table
    WHERE page = 'NextSong';
    """
)

song_table_insert = (
    """
    INSERT INTO song_table
    SELECT DISTINCT
        song_id,
        artist_id,
        title,
        year,
        duration
    FROM staging_songs_table;
    """
)

artist_table_insert = (
    """
    INSERT INTO artist_table
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs_table;
    """
)

time_table_insert = (
    """
    INSERT INTO time_table
    SELECT DISTINCT
        TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
        DATEPART('hour', TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS hour,
        DATEPART('day', TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS day,
        DATEPART('week', TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS week,
        DATEPART('month', TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS month,
        DATEPART('year', TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS year,
        DATEPART('dow', TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS weekday
    FROM staging_events_table
    WHERE page = 'NextSong';
    """
)

# QUERY LISTS
copy_table_queries = staging_table_copy

create_table_queries = [
    staging_events_table_create, staging_songs_table_create, songplay_table_create,
    user_table_create, song_table_create, artist_table_create, time_table_create
]

drop_table_queries = [
    staging_events_table_drop, staging_songs_table_drop, songplay_table_drop,
    user_table_drop, song_table_drop, artist_table_drop, time_table_drop
]

insert_table_queries = [
    songplay_table_insert, user_table_insert, song_table_insert,
    artist_table_insert, time_table_insert
]
