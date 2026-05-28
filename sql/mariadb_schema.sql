CREATE TABLE IF NOT EXISTS artists (
    artist_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    artist_name VARCHAR(255) NOT NULL,
    spotify_artist_id VARCHAR(255) UNIQUE
);

CREATE TABLE IF NOT EXISTS albums (
    album_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    album_name VARCHAR(255) NOT NULL,
    release_date DATE,
    total_tracks INT,
    album_type VARCHAR(100),
    UNIQUE KEY uq_albums_album_name (album_name)
);

CREATE TABLE IF NOT EXISTS tracks (
    track_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    spotify_track_id VARCHAR(255) UNIQUE,
    track_name VARCHAR(255) NOT NULL,
    duration_ms INT,
    explicit BOOLEAN,
    popularity INT,
    album_id BIGINT,
    CONSTRAINT fk_tracks_album FOREIGN KEY (album_id) REFERENCES albums(album_id)
);

CREATE TABLE IF NOT EXISTS genres (
    genre_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    genre_name VARCHAR(120) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS track_audio_features (
    track_id BIGINT PRIMARY KEY,
    danceability DOUBLE,
    energy DOUBLE,
    `key` INT,
    loudness DOUBLE,
    `mode` INT,
    speechiness DOUBLE,
    acousticness DOUBLE,
    instrumentalness DOUBLE,
    liveness DOUBLE,
    valence DOUBLE,
    tempo DOUBLE,
    time_signature INT,
    CONSTRAINT fk_taf_track FOREIGN KEY (track_id) REFERENCES tracks(track_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS track_artists (
    track_id BIGINT NOT NULL,
    artist_id BIGINT NOT NULL,
    PRIMARY KEY (track_id, artist_id),
    CONSTRAINT fk_track_artists_track FOREIGN KEY (track_id) REFERENCES tracks(track_id) ON DELETE CASCADE,
    CONSTRAINT fk_track_artists_artist FOREIGN KEY (artist_id) REFERENCES artists(artist_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS album_artists (
    album_id BIGINT NOT NULL,
    artist_id BIGINT NOT NULL,
    PRIMARY KEY (album_id, artist_id),
    CONSTRAINT fk_album_artists_album FOREIGN KEY (album_id) REFERENCES albums(album_id) ON DELETE CASCADE,
    CONSTRAINT fk_album_artists_artist FOREIGN KEY (artist_id) REFERENCES artists(artist_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS track_genres (
    track_id BIGINT NOT NULL,
    genre_id BIGINT NOT NULL,
    PRIMARY KEY (track_id, genre_id),
    CONSTRAINT fk_track_genres_track FOREIGN KEY (track_id) REFERENCES tracks(track_id) ON DELETE CASCADE,
    CONSTRAINT fk_track_genres_genre FOREIGN KEY (genre_id) REFERENCES genres(genre_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_tracks_popularity ON tracks(popularity);
CREATE INDEX IF NOT EXISTS idx_tracks_track_name ON tracks(track_name);
CREATE INDEX IF NOT EXISTS idx_artists_artist_name ON artists(artist_name);
CREATE INDEX IF NOT EXISTS idx_albums_release_date ON albums(release_date);
CREATE INDEX IF NOT EXISTS idx_track_audio_features_tempo ON track_audio_features(tempo);
CREATE INDEX IF NOT EXISTS idx_track_artists_artist_id ON track_artists(artist_id);
CREATE INDEX IF NOT EXISTS idx_album_artists_artist_id ON album_artists(artist_id);
CREATE INDEX IF NOT EXISTS idx_track_genres_genre_id ON track_genres(genre_id);
