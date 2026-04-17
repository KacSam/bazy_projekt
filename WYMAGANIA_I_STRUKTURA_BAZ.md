# Projekt porównawczy SZBD – wymagania i struktura baz danych

## 1. Wymagania na ocenę 3.0 (checklista)

Poniżej pełna lista wymagań, które muszą znaleźć się w projekcie:

1. Cel i zakres pracy – jasno określony temat oraz zakres analiz.
2. Opis wybranych systemów zarządzania bazami danych (SZBD).
3. Zalety i wady wybranych baz danych, w tym udogodnienia oraz ograniczenia.
4. Awaryjność, bezpieczeństwo, migracje, integracje i skalowalność – część teoretyczna.
5. Obszary biznesowych zastosowań wybranych systemów zarządzania bazami danych.
6. Opis zbioru danych – co najmniej 5 tabel w systemie relacyjnym.
7. Krótki opis aplikacji testowej, obejmujący:
   - zdefiniowanie wymagań,
   - wykorzystane technologie i narzędzia,
   - opis działania aplikacji.
8. Opis przeprowadzonych testów wydajnościowych oraz porównanie operacji CRUD dla:
   - małego,
   - średniego,
   - dużego zbioru danych
   (np. 10 000, 100 000, 1 000 000 rekordów).
9. Porównanie co najmniej 4 systemów baz danych:
   - 2 systemów relacyjnych,
   - 2 systemów nierelacyjnych.
10. Co najmniej 12 scenariuszy testowych,
    w tym minimum 3 scenariusze dla każdej operacji CRUD.
11. Średnią z 3 prób dla każdej operacji CRUD
    (minimum 3 próby dla każdego z 12 scenariuszy testowych).
12. Opracowanie wyników testów w formie:
    - opisu,
    - wizualizacji (np. wykresów),
    przedstawionych jako sprawozdanie oraz prezentacja.

---

## 2. Aktualny etap: projekt struktury baz danych

### 2.1. Dataset źródłowy

Źródło: `rodolfofigueroa/spotify-12m-songs` (Kaggle).

Przykładowe ładowanie danych (Python + kagglehub):

```python
# pip install kagglehub[pandas-datasets]
import kagglehub
from kagglehub import KaggleDatasetAdapter

file_path = ""  # np. nazwa konkretnego pliku CSV

df = kagglehub.load_dataset(
    KaggleDatasetAdapter.PANDAS,
    "rodolfofigueroa/spotify-12m-songs",
    file_path,
)

print(df.head())
```

Uwaga: przed finalnym mapowaniem trzeba potwierdzić rzeczywiste nazwy kolumn (`df.columns`), bo mogą się różnić zależnie od pliku.

---

## 3. Wybrane SZBD do porównania (min. 4)

### Relacyjne
1. PostgreSQL
2. MySQL

### Nierelacyjne
3. MongoDB (dokumentowa)
4. Cassandra (kolumnowa, wide-column)

---

## 4. Proponowany model relacyjny (PostgreSQL / MySQL)

Minimalnie 5 tabel (tu: 8 tabel), z normalizacją pod testy CRUD i analitykę.

### 4.1. Tabele główne

1. `artists`
- `artist_id` (PK)
- `artist_name`
- `spotify_artist_id` (UNIQUE, jeśli dostępne)

2. `albums`
- `album_id` (PK)
- `album_name`
- `release_date`
- `total_tracks`
- `album_type`

3. `tracks`
- `track_id` (PK)
- `spotify_track_id` (UNIQUE)
- `track_name`
- `duration_ms`
- `explicit` (BOOLEAN)
- `popularity`
- `album_id` (FK -> albums)

4. `genres`
- `genre_id` (PK)
- `genre_name` (UNIQUE)

5. `track_audio_features`
- `track_id` (PK, FK -> tracks)
- `danceability`
- `energy`
- `key`
- `loudness`
- `mode`
- `speechiness`
- `acousticness`
- `instrumentalness`
- `liveness`
- `valence`
- `tempo`
- `time_signature`

### 4.2. Tabele relacji M:N

6. `track_artists`
- `track_id` (FK -> tracks)
- `artist_id` (FK -> artists)
- PK (`track_id`, `artist_id`)

7. `album_artists`
- `album_id` (FK -> albums)
- `artist_id` (FK -> artists)
- PK (`album_id`, `artist_id`)

8. `track_genres`
- `track_id` (FK -> tracks)
- `genre_id` (FK -> genres)
- PK (`track_id`, `genre_id`)

### 4.3. Kluczowe indeksy (pod testy wydajności)

- `tracks(popularity)`
- `tracks(track_name)`
- `artists(artist_name)`
- `albums(release_date)`
- `track_audio_features(tempo)`
- indeksy po FK w tabelach łącznikowych

---

## 5. Proponowany model nierelacyjny – MongoDB

Kolekcje:

1. `songs`
- `_id`
- `spotify_track_id`
- `track_name`
- `duration_ms`
- `explicit`
- `popularity`
- `album`: obiekt z polami `album_id`, `album_name`, `release_date`
- `artists`: tablica obiektów (`artist_id`, `artist_name`)
- `genres`: tablica stringów
- `audio_features`: obiekt (danceability, energy, tempo, ...)

2. `artists`
- `_id`
- `artist_name`
- `spotify_artist_id`
- opcjonalnie: `top_tracks` (tablica)

3. `albums`
- `_id`
- `album_name`
- `release_date`
- `artists`

Indeksy:
- `songs.spotify_track_id` (UNIQUE)
- `songs.popularity`
- `songs.track_name`
- `songs.artists.artist_name`
- `songs.audio_features.tempo`

Zaleta do testów: naturalne osadzanie danych (`embedded docs`) i szybkie odczyty bez joinów.

---

## 6. Proponowany model nierelacyjny – Cassandra

Tabela w Cassandrze powinna być projektowana pod zapytania, nie pod normalizację.

### 6.1. Przykładowe tabele zapytaniowe

1. `songs_by_track_id`
- PK: `track_id`
- kolumny: `track_name`, `album_name`, `duration_ms`, `popularity`, `explicit`, `tempo`, `energy`, ...

2. `songs_by_popularity`
- PK: `(popularity_bucket, popularity, track_id)`
- użycie: szybkie pobieranie top utworów per bucket

3. `songs_by_artist`
- PK: `(artist_id, track_id)`
- kolumny: `artist_name`, `track_name`, `album_name`, `release_date`, `popularity`

4. `songs_by_genre`
- PK: `(genre, popularity, track_id)`
- użycie: ranking utworów w gatunku

### 6.2. Uwagi praktyczne

- Denormalizacja jest celowa i wymagana.
- Często tworzy się kilka tabel z tymi samymi danymi dla różnych wzorców odczytu.
- Dla testów CRUD warto mierzyć osobno wydajność insertów batch i odczytów po kluczu partycji.

---

## 7. Mapowanie danych z datasetu do wspólnego modelu testowego

1. Wczytaj dane i wylistuj kolumny (`df.columns`).
2. Zidentyfikuj kolumny z listami (np. wielu artystów, wiele gatunków).
3. Przygotuj etap ETL:
   - czyszczenie nulli,
   - konwersja typów,
   - rozbijanie pól wielowartościowych,
   - deduplikacja rekordów.
4. Załaduj dane do każdej bazy zgodnie z docelowym modelem.
5. Zweryfikuj spójność liczby rekordów pomiędzy bazami.

---

## 8. Minimalny pakiet scenariuszy CRUD (szkic pod wymagania)

Dla każdej bazy uruchom testy na 10k / 100k / 1M rekordów.

- CREATE: insert pojedynczy, insert batch, insert z walidacją.
- READ: odczyt po ID, odczyt z filtrem (np. popularność), odczyt agregacyjny.
- UPDATE: aktualizacja pojedynczego pola, aktualizacja wielu pól, aktualizacja wsadowa.
- DELETE: usunięcie po ID, usunięcie po warunku, miękkie usunięcie (tam, gdzie zasadne).

Każdy scenariusz: minimum 3 próby, raportowana średnia.

---

## 9. Co dalej (najbliższe kroki)

1. Potwierdzić kolumny źródłowe datasetu i doprecyzować mapowanie 1:1.
2. Przygotować DDL SQL dla PostgreSQL i MySQL.
3. Przygotować skrypty tworzenia kolekcji/indeksów dla MongoDB.
4. Przygotować CQL pod Cassandra (tabele pod zapytania).
5. Zaimplementować skrypt ETL oraz generator danych 10k/100k/1M.
