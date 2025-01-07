-------------------------
-- CREATE Database
-------------------------
CREATE OR REPLACE DATABASE spotify_db;

-------------------------------------
-- CREATE Tables 
-------------------------------------

CREATE OR REPLACE TABLE songs (
    song_id VARCHAR,
    song_name VARCHAR,
    duration_ms INT,
    url VARCHAR,
    popularity INT,
    song_added TIMESTAMP_NTZ,
    album_id VARCHAR,
    artist_id VARCHAR
);

CREATE OR REPLACE TABLE artist (
    artist_id VARCHAR,
    artist_name VARCHAR,
    external_url VARCHAR
);

CREATE OR REPLACE TABLE album (
    album_id VARCHAR,
    name VARCHAR,      
    releASe_date DATE,
    total_tracks INT,
    url VARCHAR
);

-------------------------------------
-- CREATE FILE FORMAT
-------------------------------------

CREATE OR REPLACE FILE FORMAT csv_file_format
TYPE = 'csv'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY='"'
NULL_IF=('NULL','null')
EMPTY_FIELD_AS_NULL=TRUE;

-------------------------------------
-- CREATE INTEGRATION
-------------------------------------

USE ROLE ACCOUNTADMIN;

GRANT CREATE INTEGRATION ON ACCOUNT TO SYSADMIN;

USE ROLE SYSADMIN;

CREATE OR REPLACE STORAGE INTEGRATION spotify_s3_int
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::590183673587:role/SNOWFLAKE-S3-INTEGRATION-ROLE'
    STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-snowflake-data/transformed_data/')
     COMMENT = 'creating connection to s3';

DESC INTEGRATION spotify_s3_int;

-------------------------------------
-- CREATE STAGE (3 Stages)
-------------------------------------

CREATE OR REPLACE STAGE album_csv_folder
    URL='s3://spotify-snowflake-data/transformed_data/album_data/'
    STORAGE_INTEGRATION=spotify_s3_int
    file_format=csv_file_format;

DESC STAGE album_csv_folder;
LIST @album_csv_folder;

CREATE OR REPLACE STAGE artist_csv_folder
    URL='s3://spotify-snowflake-data/transformed_data/artist_data/'
    STORAGE_INTEGRATION=spotify_s3_int
    file_format=csv_file_format;

LIST @artist_csv_folder;

CREATE OR REPLACE STAGE songs_csv_folder
    URL='s3://spotify-snowflake-data/transformed_data/song_data/'
    STORAGE_INTEGRATION=spotify_s3_int
    file_format=csv_file_format;

LIST @songs_csv_folder;
    
-----------------------------------------
-- CREATE PIPE  (3 PIPES FOR THREE TABLES)
-----------------------------------------
-- album, artist, songs

CREATE OR REPLACE pipe album_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO album
  FROM @album_csv_folder;

DESC pipe album_pipe;

CREATE OR REPLACE pipe artist_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO artist
  FROM @artist_csv_folder;

DESC pipe artist_pipe;

CREATE OR REPLACE pipe songs_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO songs
  FROM @songs_csv_folder;

DESC pipe songs_pipe;
  

SELECT * FROM album;
SELECT * FROM artist;
SELECT * FROM songs;