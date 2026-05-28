# Prezentacja: Analiza porownawcza SZBD

## Slajd 1: Temat

Analiza porownawcza czterech systemow zarzadzania bazami danych na danych Spotify.

Systemy: PostgreSQL, MariaDB, MongoDB, Cassandra.

## Slajd 2: Cel i zakres

Celem jest porownanie operacji CRUD dla dwoch baz relacyjnych i dwoch nierelacyjnych.

Zakres poziomu 3.0: opis systemow, model danych, aplikacja testowa, 12 scenariuszy CRUD, 3 proby dla kazdego scenariusza, wyniki i wykresy.

## Slajd 3: Dataset

Zrodlo: `rodolfofigueroa/spotify-12m-songs`.

Dane dotycza utworow, albumow, artystow, gatunkow i cech audio, takich jak tempo, energia, popularnosc i czas trwania.

## Slajd 4: Model relacyjny

PostgreSQL i MariaDB korzystaja z 8 tabel:

`artists`, `albums`, `tracks`, `genres`, `track_audio_features`, `track_artists`, `album_artists`, `track_genres`.

Model zawiera klucze glowne, klucze obce i tabele lacznikowe dla relacji M:N.

## Slajd 5: Modele NoSQL

MongoDB przechowuje dane jako dokumenty w kolekcji `songs`, z zagniezdzonym albumem, artystami, gatunkami i cechami audio.

Cassandra uzywa tabel zapytaniowych: po ID utworu, popularnosci, artyscie i gatunku.

## Slajd 6: Aplikacja testowa

Srodowisko dziala w Docker Compose.

Loader danych: `scripts/load_spotify_data.py`.

Benchmark CRUD: `scripts/run_crud_benchmarks.py`.

Generator wykresow: `scripts/generate_result_assets.py`.

## Slajd 7: Scenariusze CREATE i READ

CREATE:

- insert pojedynczy
- insert wsadowy
- insert z walidacja

READ:

- odczyt po ID
- odczyt z filtrem popularnosci
- odczyt agregacyjny

## Slajd 8: Scenariusze UPDATE i DELETE

UPDATE:

- aktualizacja jednego pola
- aktualizacja wielu pol
- aktualizacja wsadowa

DELETE:

- usuniecie po ID
- usuniecie po warunku
- miekkie usuniecie

## Slajd 9: Metodyka pomiaru

Testy sa wykonywane dla 10 000, 100 000 i 1 000 000 rekordow.

Kazdy scenariusz jest uruchamiany 3 razy.

Raportowana jest srednia z prob, lista prob oraz przepustowosc w rekordach na sekunde.

## Slajd 10: Wyniki

Wstaw wykresy wygenerowane przez:

```powershell
python scripts/generate_result_assets.py
```

Wykresy:

- sredni czas operacji wedlug bazy
- sredni czas wedlug rozmiaru zbioru
- przepustowosc wedlug operacji CRUD

## Slajd 11: Porownanie systemow

PostgreSQL i MariaDB pokazuja zachowanie klasycznych baz relacyjnych.

MongoDB pokazuje zachowanie modelu dokumentowego.

Cassandra pokazuje model zapytaniowy i denormalizacje pod konkretne odczyty.

## Slajd 12: Wnioski

Ostateczne wnioski nalezy oprzec na wygenerowanych wynikach.

Nalezy wskazac najlepszy system dla CREATE, READ, UPDATE i DELETE oraz opisac, jak zmieniaja sie wyniki dla 10k, 100k i 1M rekordow.
