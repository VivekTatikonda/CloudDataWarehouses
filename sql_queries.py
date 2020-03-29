import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE if exists stagingevents;"
staging_songs_table_drop = "DROP TABLE if exists stagingsongs;"
songplay_table_drop = "DROP TABLE if exists songsplays;"
user_table_drop = "DROP TABLE if exists users;"
song_table_drop = "DROP TABLE if exists songs;"
artist_table_drop = "DROP TABLE if exists artists;"
time_table_drop = "DROP TABLE if exists time;"

# CREATE TABLES

staging_events_table_create = ("""create table if not exists stagingevents(
                                    event_id    BIGINT IDENTITY(0,1) NOT NULL,
                                    artist        VARCHAR,
                                    auth          VARCHAR,
                                    firstName     VARCHAR,
                                    gender        VARCHAR,
                                    itemInSession VARCHAR,
                                    lastName      VARCHAR,
                                    length        VARCHAR,
                                    level         VARCHAR,
                                    location      VARCHAR,
                                    method        VARCHAR,
                                    page          VARCHAR,
                                    registration  VARCHAR,
                                    sessionId     INTEGER SORTKEY DISTKEY,
                                    song          VARCHAR,
                                    status        INTEGER,
                                    ts            BIGINT,
                                    userAgent     VARCHAR,
                                    userId        INTEGER);
""")

staging_songs_table_create = ("""create table if not exists stagingsongs(
                                num_songs           INTEGER,
                                artist_id           VARCHAR        SORTKEY DISTKEY,
                                artist_latitude     VARCHAR,
                                artist_longitude    VARCHAR,
                                artist_location     VARCHAR(500),
                                artist_name         VARCHAR(500),
                                song_id             VARCHAR,
                                title               VARCHAR(500),
                                duration            DECIMAL(9),
                                year                INTEGER);
""")

songplay_table_create = ("""CREATE TABLE if not exists songplays(
                            songplay_id BIGINT    IDENTITY(0,1) NOT NULL SORTKEY, 
                            start_time  timestamp               NOT NULL,
                            user_id     varchar(50)             NOT NULL DISTKEY,  
                            level       varchar(6), 
                            song_id     varchar(50)             NOT NULL, 
                            artist_id   varchar(50)             NOT NULL, 
                            session_id  integer                 NOT NULL, 
                            location    varchar(100), 
                            user_agent  varchar(500)
                        );
""")

user_table_create = ("""CREATE TABLE if not exists users(
                        user_id    varchar(50) NOT NULL SORTKEY DISTKEY, 
                        first_name varchar(70), 
                        last_name  varchar(70), 
                        gender     varchar(7), 
                        level      varchar(6)
                    );
""")

song_table_create = ("""CREATE TABLE if not exists songs(
                        song_id   varchar(50) NOT NULL SORTKEY,
                        title     varchar(500), 
                        artist_id varchar(50) NOT NULL,
                        year      integer,
                        duration  DECIMAL(9)
                    )diststyle all;
""")

artist_table_create = ("""CREATE TABLE if not exists artists(
                          artist_id   varchar(50) NOT NULL SORTKEY,
                          name        varchar(500) NOT NULL,
                          location    varchar(500),
                          latitude    DECIMAL(9),
                          longitude   DECIMAL(9)
                      )diststyle all;
""")

time_table_create = ("""CREATE TABLE if not exists time(
                        start_time timestamp NOT NULL SORTKEY,
                        hour       SMALLINT, 
                        day        SMALLINT,
                        week       SMALLINT, 
                        month      SMALLINT,
                        year       SMALLINT,
                        weekday    SMALLINT
                    )diststyle all;
""")

# STAGING TABLES
LogData=config.get('S3','LOG_DATA')
songData=config.get('S3','SONG_DATA')
roleArn=config.get('IAM_ROLE','ARN')
logfilepath=config.get('S3','LOG_JSONPATH')

staging_events_copy = ("""copy stagingevents from {} 
                          credentials 'aws_iam_role={}'
                          format as json {}
                          STATUPDATE ON
                          region 'us-west-2';
                      """).format(LogData,roleArn,logfilepath)

staging_songs_copy = ("""copy stagingsongs from {}
                         credentials 'aws_iam_role={}'
                         format as json 'auto'
                         ACCEPTINVCHARS AS '^'
                         STATUPDATE ON
                         region 'us-west-2';
                      """).format(songData,roleArn)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time,
                                        user_id,
                                        level,
                                        song_id,
                                        artist_id,
                                        session_id,
                                        location,
                                        user_agent) SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                * INTERVAL '1 second'   AS start_time,
            se.userId                   AS user_id,
            se.level                    AS level,
            ss.song_id                  AS song_id,
            ss.artist_id                AS artist_id,
            se.sessionId                AS session_id,
            se.location                 AS location,
            se.userAgent                AS user_agent
    FROM stagingevents AS se
    JOIN stagingsongs AS ss
        ON (se.artist = ss.artist_name)
    WHERE se.page = 'NextSong';""")

user_table_insert = ("""INSERT INTO users (user_id,
                                        first_name,
                                        last_name,
                                        gender,
                                        level)
    SELECT  DISTINCT se.userId          AS user_id,
            se.firstName                AS first_name,
            se.lastName                 AS last_name,
            se.gender                   AS gender,
            se.level                    AS level
    FROM stagingevents AS se
    WHERE se.page = 'NextSong';""")

song_table_insert = ("""INSERT INTO songs(song_id,
                                        title,
                                        artist_id,
                                        year,
                                        duration)
    SELECT  DISTINCT ss.song_id         AS song_id,
            ss.title                    AS title,
            ss.artist_id                AS artist_id,
            ss.year                     AS year,
            ss.duration                 AS duration
    FROM stagingsongs AS ss;""")

artist_table_insert = ("""INSERT INTO artists (artist_id,
                                        name,
                                        location,
                                        latitude,
                                        longitude)
    SELECT  DISTINCT ss.artist_id       AS artist_id,
            ss.artist_name              AS name,
            ss.artist_location          AS location,
            ss.artist_latitude          AS latitude,
            ss.artist_longitude         AS longitude
    FROM stagingsongs AS ss;
""")

time_table_insert = ("""INSERT INTO time (start_time,
                                        hour,
                                        day,
                                        week,
                                        month,
                                        year,
                                        weekday)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                * INTERVAL '1 second'        AS start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
    FROM    stagingevents                   AS se
    WHERE se.page = 'NextSong';""")

# QUERY LISTS

analytics_queries = ['select COUNT(*) AS total FROM artists','select COUNT(*) AS total FROM songs','select COUNT(*) AS total FROM time','select COUNT(*) AS total FROM users','select COUNT(*) AS total FROM songplays']
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]