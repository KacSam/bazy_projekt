CREATE TABLE IF NOT EXISTS artists (
    artist_id BIGSERIAL PRIMARY KEY,
    artist_name TEXT NOT NULL,
    spotify_artist_id TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS albums (
    album_id BIGSERIAL PRIMARY KEY,
    album_name TEXT NOT NULL,
    release_date DATE,
    total_tracks INT,
    album_type TEXT
);

CREATE TABLE IF NOT EXISTS tracks (
    track_id BIGSERIAL PRIMARY KEY,
    spotify_track_id TEXT UNIQUE,
    track_name TEXT NOT NULL,
    duration_ms INT,
    explicit BOOLEAN,
    popularity INT,
    album_id BIGINT REFERENCES albums(album_id)
);

CREATE TABLE IF NOT EXISTS genres (
    genre_id BIGSERIAL PRIMARY KEY,
    genre_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS track_audio_features (
    track_id BIGINT PRIMARY KEY REFERENCES tracks(track_id) ON DELETE CASCADE,
    danceability DOUBLE PRECISION,
    energy DOUBLE PRECISION,
    key INT,
    loudness DOUBLE PRECISION,
    mode INT,
    speechiness DOUBLE PRECISION,
    acousticness DOUBLE PRECISION,
    instrumentalness DOUBLE PRECISION,
    liveness DOUBLE PRECISION,
    valence DOUBLE PRECISION,
    tempo DOUBLE PRECISION,
    time_signature INT
);

CREATE TABLE IF NOT EXISTS track_artists (
    track_id BIGINT NOT NULL REFERENCES tracks(track_id) ON DELETE CASCADE,
    artist_id BIGINT NOT NULL REFERENCES artists(artist_id) ON DELETE CASCADE,
    PRIMARY KEY (track_id, artist_id)
);

CREATE TABLE IF NOT EXISTS album_artists (
    album_id BIGINT NOT NULL REFERENCES albums(album_id) ON DELETE CASCADE,
    artist_id BIGINT NOT NULL REFERENCES artists(artist_id) ON DELETE CASCADE,
    PRIMARY KEY (album_id, artist_id)
);

CREATE TABLE IF NOT EXISTS track_genres (
    track_id BIGINT NOT NULL REFERENCES tracks(track_id) ON DELETE CASCADE,
    genre_id BIGINT NOT NULL REFERENCES genres(genre_id) ON DELETE CASCADE,
    PRIMARY KEY (track_id, genre_id)
);

CREATE INDEX IF NOT EXISTS idx_tracks_popularity ON tracks(popularity);
CREATE INDEX IF NOT EXISTS idx_tracks_track_name ON tracks(track_name);
CREATE INDEX IF NOT EXISTS idx_artists_artist_name ON artists(artist_name);
CREATE INDEX IF NOT EXISTS idx_albums_release_date ON albums(release_date);
CREATE INDEX IF NOT EXISTS idx_track_audio_features_tempo ON track_audio_features(tempo);
CREATE INDEX IF NOT EXISTS idx_track_artists_artist_id ON track_artists(artist_id);
CREATE INDEX IF NOT EXISTS idx_album_artists_artist_id ON album_artists(artist_id);
CREATE INDEX IF NOT EXISTS idx_track_genres_genre_id ON track_genres(genre_id);
