CREATE KEYSPACE IF NOT EXISTS spotify_db
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE spotify_db;

CREATE TABLE IF NOT EXISTS songs_by_track_id (
    track_id text PRIMARY KEY,
    track_name text,
    album_name text,
    release_date text,
    duration_ms int,
    popularity int,
    explicit boolean,
    tempo double,
    energy double,
    artists list<text>,
    genres list<text>
);

CREATE TABLE IF NOT EXISTS songs_by_popularity (
    popularity_bucket int,
    popularity int,
    track_id text,
    track_name text,
    album_name text,
    PRIMARY KEY ((popularity_bucket), popularity, track_id)
) WITH CLUSTERING ORDER BY (popularity DESC, track_id ASC);

CREATE TABLE IF NOT EXISTS songs_by_artist (
    artist_id text,
    track_id text,
    artist_name text,
    track_name text,
    album_name text,
    release_date text,
    popularity int,
    PRIMARY KEY ((artist_id), track_id)
);

CREATE TABLE IF NOT EXISTS songs_by_genre (
    genre text,
    popularity int,
    track_id text,
    track_name text,
    album_name text,
    PRIMARY KEY ((genre), popularity, track_id)
) WITH CLUSTERING ORDER BY (popularity DESC, track_id ASC);
