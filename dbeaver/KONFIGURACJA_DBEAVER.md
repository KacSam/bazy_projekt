# Konfiguracja DBeaver dla projektu bazy_projekt

Ten dokument zawiera komplet ustawien DBeaver dla 4 baz:
- PostgreSQL
- MariaDB
- MongoDB
- Cassandra

## 1. Warunek startowy

Upewnij sie, ze kontenery dzialaja:

- docker compose up -d
- docker compose ps

Aktualne porty z docker compose:
- PostgreSQL: localhost:5432
- MariaDB: localhost:3306
- MongoDB: localhost:27017
- Cassandra: localhost:9042

## 2. PostgreSQL (spotify_db)

### New Connection
- Database type: PostgreSQL
- Host: localhost
- Port: 5432
- Database: spotify_db
- Username: admin
- Password: password

### JDBC URL (opcjonalnie)
- jdbc:postgresql://localhost:5432/spotify_db

### Test
- Kliknij Test Connection
- Powinno zwrocic sukces

### Sanity query
- SELECT COUNT(*) AS tracks_count FROM tracks;
- SELECT track_id, track_name, popularity FROM tracks ORDER BY track_id LIMIT 10;

## 3. MariaDB (spotify_db)

### New Connection
- Database type: MariaDB
- Host: localhost
- Port: 3306
- Database: spotify_db
- Username: admin
- Password: password

### JDBC URL (opcjonalnie)
- jdbc:mariadb://localhost:3306/spotify_db

### Test
- Kliknij Test Connection

### Sanity query
- SELECT COUNT(*) AS tracks_count FROM tracks;
- SELECT track_id, track_name, popularity FROM tracks ORDER BY track_id LIMIT 10;

## 4. MongoDB (spotify_db)

Uwaga: jesli nie widzisz MongoDB na liscie typow baz, doinstaluj rozszerzenie MongoDB w DBeaver (Database -> Driver Manager / Marketplace).

### New Connection
- Database type: MongoDB
- Host: localhost
- Port: 27017
- Authentication database: admin
- Username: admin
- Password: password
- Default database: spotify_db

### Connection URL (opcjonalnie)
- mongodb://admin:password@localhost:27017/admin

### Test
- Kliknij Test Connection

### Sanity query (Mongo shell)
- use spotify_db
- db.songs.countDocuments({})
- db.songs.find({}, { id: 1, name: 1, artists: 1 }).limit(10)

## 5. Cassandra (spotify_db)

Uwaga: jesli Cassandra nie jest dostepna na liscie, doinstaluj odpowiedni sterownik/extension w DBeaver.

### New Connection
- Database type: Cassandra
- Host: localhost
- Port: 9042
- Keyspace: spotify_db
- Username: puste
- Password: puste
- Datacenter / local DC: datacenter1

### Dodatkowo
- Cluster name: SpotifyCluster (informacyjnie)
- SSL: off

### Test
- Kliknij Test Connection

### Sanity query (CQL)
- SELECT COUNT(*) FROM spotify_db.songs_by_track_id;
- SELECT track_id, track_name, popularity FROM spotify_db.songs_by_track_id LIMIT 10;

## 6. Struktura folderow w DBeaver (propozycja)

Stworz 1 projekt, np. MAGISTER-BAZY, a w nim 4 connection foldery:
- 01-relacyjne
- 02-nierelacyjne

Przypnij:
- PostgreSQL i MariaDB do 01-relacyjne
- MongoDB i Cassandra do 02-nierelacyjne

## 7. Szybki test poprawnosci importu (oczekiwane wyniki)

Dla aktualnie zaladowanej probki:
- PostgreSQL tracks_count: 10000
- MariaDB tracks_count: 10000
- MongoDB songs countDocuments: 10000
- Cassandra songs_by_track_id count: 10000

Jesli liczby sa inne:
1. Sprawdz docker compose ps
2. Sprawdz, czy laczysz sie na localhost i wlasciwe porty
3. Wykonaj ponownie import skryptem scripts/load_spotify_data.py

## 8. Export/Import ustawien DBeaver (dla kolegi)

Aby przekazac konfiguracje koledze:
1. DBeaver -> File -> Export
2. Wybierz DBeaver -> Project
3. Zaznacz:
   - Connections
   - Driver settings (opcjonalnie)
   - Tasks (jesli dodasz)
4. Wyeksportuj do ZIP

Kolega:
1. DBeaver -> File -> Import
2. Wskazuje ZIP
3. Potwierdza import Connections

Po imporcie moze byc potrzebna podmiana hasel (zalezy od ustawien secure storage).
