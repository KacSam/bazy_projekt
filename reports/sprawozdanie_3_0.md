# Analiza porownawcza systemow zarzadzania bazami danych

## 1. Cel i zakres pracy

Celem projektu jest porownanie wydajnosci oraz cech praktycznych czterech systemow zarzadzania bazami danych na wspolnym zbiorze danych muzycznych Spotify. Zakres obejmuje dwie bazy relacyjne, PostgreSQL i MariaDB, oraz dwie bazy nierelacyjne, MongoDB i Cassandra.

Analiza dla poziomu 3.0 obejmuje opis systemow, zalety i ograniczenia, podstawowe aspekty awaryjnosci, bezpieczenstwa, migracji, integracji i skalowalnosci, zastosowania biznesowe oraz testy CRUD dla trzech rozmiarow danych: 10 000, 100 000 i 1 000 000 rekordow.

## 2. Wybrane systemy SZBD

PostgreSQL jest relacyjnym systemem baz danych nastawionym na zgodnosc z SQL, transakcje ACID, integralnosc danych i rozbudowane typy danych. W projekcie reprezentuje dojrzaly silnik relacyjny uzywany w aplikacjach biznesowych i analitycznych.

MariaDB jest relacyjnym systemem zgodnym z ekosystemem MySQL. W projekcie pelni role drugiego silnika relacyjnego, dobrego do porownania klasycznych operacji tabelarycznych, indeksow i prostoty wdrozenia.

MongoDB jest nierelacyjna baza dokumentowa. Dane sa przechowywane jako dokumenty zagniezdzone, co dobrze pasuje do rekordow utworow zawierajacych album, artystow, gatunki i cechy audio w jednym dokumencie.

Cassandra jest nierelacyjna baza szerokokolumnowa projektowana pod wysoka dostepnosc i zapytania oparte o klucze partycji. W projekcie dane sa denormalizowane do tabel odpowiadajacych konkretnym wzorcom odczytu.

## 3. Zalety, wady i zastosowania

PostgreSQL oferuje silna integralnosc danych, dobre wsparcie transakcji i szerokie mozliwosci zapytan SQL. Ograniczeniem jest wieksza zlozonosc konfiguracji przy bardzo duzej skali horyzontalnej.

MariaDB jest prosta we wdrozeniu, popularna i szybka w typowych aplikacjach webowych. Ograniczeniem moze byc mniejsza elastycznosc modelowania zlozonych danych niz w dokumentowych bazach NoSQL.

MongoDB ulatwia prace z danymi polustrukturalnymi i zmiennym schematem. Dobrze sprawdza sie w katalogach, profilach uzytkownikow i aplikacjach z dokumentami JSON. Ograniczeniem jest mniejsza naturalnosc dla zlozonych relacji M:N niz w bazach relacyjnych.

Cassandra dobrze nadaje sie do duzej liczby zapisow, wysokiej dostepnosci i rozproszonego przechowywania danych. Wymaga jednak projektowania tabel pod konkretne zapytania, co zwieksza duplikacje danych.

## 4. Awaryjnosc, bezpieczenstwo, migracje, integracje i skalowalnosc

PostgreSQL i MariaDB zapewniaja mechanizmy backupu, replikacji, transakcji i kontroli dostepu przez role oraz uprawnienia. Migracje schematu sa typowo realizowane skryptami SQL lub narzedziami aplikacyjnymi.

MongoDB oferuje uwierzytelnianie, role, repliki i latwa integracje z aplikacjami JSON. Migracje czesto polegaja na stopniowej zmianie struktury dokumentow, poniewaz schemat jest bardziej elastyczny.

Cassandra jest projektowana z mysla o odpornosci na awarie wezlow i skalowaniu horyzontalnym. Bezpieczenstwo obejmuje uwierzytelnianie, autoryzacje i konfiguracje klastra. Migracje wymagaja ostroznosci, bo model danych jest scisle powiazany z zapytaniami.

## 5. Opis zbioru danych i modelu

Zrodlem danych jest dataset Kaggle `rodolfofigueroa/spotify-12m-songs`. Dane sa ladowane skryptem `scripts/load_spotify_data.py`, ktory wykrywa plik CSV, normalizuje kolumny i mapuje rekordy do wspolnego modelu testowego.

Model relacyjny zawiera 8 tabel: `artists`, `albums`, `tracks`, `genres`, `track_audio_features`, `track_artists`, `album_artists` i `track_genres`. Spelnia to wymaganie minimum 5 tabel w systemie relacyjnym.

MongoDB przechowuje glowne dane w kolekcji `songs`, gdzie album, artysci, gatunki i cechy audio sa czescia dokumentu utworu. Dodatkowo tworzone sa kolekcje `artists` i `albums`.

Cassandra przechowuje dane w tabelach zapytaniowych: `songs_by_track_id`, `songs_by_popularity`, `songs_by_artist` i `songs_by_genre`.

## 6. Aplikacja testowa

Aplikacja testowa sklada sie z kontenerow Docker oraz skryptow Python. `docker-compose.yaml` uruchamia cztery bazy danych na lokalnych portach. `scripts/load_spotify_data.py` laduje dane Spotify do wszystkich baz. `scripts/run_crud_benchmarks.py` wykonuje testy CRUD i zapisuje wyniki do `results/crud_benchmarks.csv` oraz `results/crud_benchmarks.json`.

Wymagania aplikacji testowej: Python z zaleznosciami z `requirements.txt`, Docker, dostep do datasetu Kaggle oraz uruchomione kontenery baz danych.

Podstawowa sekwencja uruchomienia:

```powershell
docker compose up -d
python scripts/load_spotify_data.py --rows 10000
python scripts/run_crud_benchmarks.py --sizes 10000,100000,1000000 --repeats 3
python scripts/generate_result_assets.py
```

Jesli dataset jest juz pobrany lokalnie albo Kaggle blokuje pobieranie przez SSL, import mozna uruchomic bez sieci:

```powershell
python scripts/load_spotify_data.py --rows 10000 --csv C:\sciezka\do\spotify.csv
```

## 7. Scenariusze testowe CRUD

Testy obejmuja 12 scenariuszy, po 3 dla kazdej operacji CRUD.

CREATE: `insert_single`, `insert_batch`, `insert_validated`.

READ: `by_id`, `filter_popularity`, `aggregate`.

UPDATE: `single_field`, `multi_field`, `batch`.

DELETE: `by_id`, `by_condition`, `soft_delete`.

Kazdy scenariusz jest wykonywany 3 razy. Do wynikow zapisywany jest sredni czas w milisekundach, przepustowosc w rekordach na sekunde oraz lista prob pomiarowych.

## 8. Opracowanie wynikow

Po wykonaniu benchmarku nalezy wygenerowac wykresy poleceniem:

```powershell
python scripts/generate_result_assets.py
```

Generator tworzy:

- `reports/assets/wyniki_podsumowanie.md`
- `reports/assets/crud_summary.csv`
- `reports/assets/charts/avg_ms_by_operation_db.svg`
- `reports/assets/charts/avg_ms_by_size_db.svg`
- `reports/assets/charts/throughput_by_operation_db.svg`

Do finalnego sprawozdania nalezy dolaczyc wygenerowane wykresy i omowic, ktore bazy byly najszybsze dla operacji CREATE, READ, UPDATE i DELETE na malym, srednim i duzym zbiorze danych.

## 9. Wnioski

Projekt na poziomie podstawowym pozwala porownac cztery systemy baz danych w jednakowym zestawie operacji CRUD. Bazy relacyjne sa oceniane w modelu tabelarycznym z relacjami i kluczami obcymi, MongoDB w modelu dokumentowym, a Cassandra w modelu zapytaniowym z denormalizacja. Ostateczne wnioski nalezy oprzec na srednich z trzech prob dla kazdego scenariusza.
