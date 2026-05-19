class SqlQueries:
    """
    SQL statements for the Sparkify ETL pipeline.

    Contains CREATE TABLE definitions for staging + star schema tables,
    and INSERT...SELECT transformations for loading the star schema
    from staging data.
    """

    # ── TABLE CREATION ────────────────────────────────────────

    staging_events_table_create = """
        CREATE TABLE IF NOT EXISTS staging_events (
            artist        VARCHAR,
            auth          VARCHAR,
            firstname     VARCHAR,
            gender        CHAR(1),
            iteminsession INTEGER,
            lastname      VARCHAR,
            length        FLOAT,
            level         VARCHAR,
            location      VARCHAR,
            method        VARCHAR,
            page          VARCHAR,
            registration  FLOAT,
            sessionid     INTEGER,
            song          VARCHAR,
            status        INTEGER,
            ts            BIGINT,
            useragent     VARCHAR,
            userid        INTEGER
        );
    """

    staging_songs_table_create = """
        CREATE TABLE IF NOT EXISTS staging_songs (
            num_songs        INTEGER,
            artist_id        VARCHAR,
            artist_latitude  FLOAT,
            artist_longitude FLOAT,
            artist_location  VARCHAR,
            artist_name      VARCHAR,
            song_id          VARCHAR,
            title            VARCHAR,
            duration         FLOAT,
            year             INTEGER
        );
    """

    songplay_table_create = """
        CREATE TABLE IF NOT EXISTS songplays (
            songplay_id VARCHAR PRIMARY KEY,
            start_time  TIMESTAMP NOT NULL,
            userid      INTEGER   NOT NULL,
            level       VARCHAR,
            song_id     VARCHAR,
            artist_id   VARCHAR,
            sessionid   INTEGER,
            location    VARCHAR,
            useragent   VARCHAR
        );
    """

    user_table_create = """
        CREATE TABLE IF NOT EXISTS users (
            userid    INTEGER PRIMARY KEY,
            firstname VARCHAR,
            lastname  VARCHAR,
            gender    CHAR(1),
            level     VARCHAR
        );
    """

    song_table_create = """
        CREATE TABLE IF NOT EXISTS songs (
            song_id   VARCHAR PRIMARY KEY,
            title     VARCHAR,
            artist_id VARCHAR,
            year      INTEGER,
            duration  FLOAT
        );
    """

    artist_table_create = """
        CREATE TABLE IF NOT EXISTS artists (
            artist_id        VARCHAR PRIMARY KEY,
            artist_name      VARCHAR,
            artist_location  VARCHAR,
            artist_latitude  FLOAT,
            artist_longitude FLOAT
        );
    """

    time_table_create = """
        CREATE TABLE IF NOT EXISTS time (
            start_time TIMESTAMP PRIMARY KEY,
            hour       INTEGER,
            day        INTEGER,
            week       INTEGER,
            month      INTEGER,
            year       INTEGER,
            weekday    INTEGER
        );
    """

    # ── INSERT TRANSFORMATIONS ────────────────────────────────

    songplay_table_insert = ("""
        INSERT INTO songplays (
            songplay_id,
            start_time,
            userid,
            level,
            song_id,
            artist_id,
            sessionid,
            location,
            useragent
        )
        SELECT
            md5(events.sessionid || events.start_time) AS songplay_id,
            events.start_time,
            events.userid,
            events.level,
            songs.song_id,
            songs.artist_id,
            events.sessionid,
            events.location,
            events.useragent
        FROM (
            SELECT
                TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
                *
            FROM staging_events
            WHERE page = 'NextSong'
        ) events
        LEFT JOIN staging_songs songs
        ON events.song = songs.title
        AND events.artist = songs.artist_name
        AND events.length = songs.duration;
    """)

    user_table_insert = ("""
        INSERT INTO users (
            userid,
            firstname,
            lastname,
            gender,
            level
        )
        SELECT DISTINCT
            userid,
            firstname,
            lastname,
            gender,
            level
        FROM staging_events
        WHERE page = 'NextSong';
    """)

    song_table_insert = ("""
        INSERT INTO songs (
            song_id,
            title,
            artist_id,
            year,
            duration
        )
        SELECT DISTINCT
            song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs;
    """)

    artist_table_insert = ("""
        INSERT INTO artists (
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
        )
        SELECT DISTINCT
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
        FROM staging_songs;
    """)

    time_table_insert = ("""
        INSERT INTO time (
            start_time,
            hour,
            day,
            week,
            month,
            year,
            weekday
        )
        SELECT DISTINCT
            start_time,
            EXTRACT(hour FROM start_time),
            EXTRACT(day FROM start_time),
            EXTRACT(week FROM start_time),
            EXTRACT(month FROM start_time),
            EXTRACT(year FROM start_time),
            EXTRACT(dayofweek FROM start_time)
        FROM songplays;
    """)