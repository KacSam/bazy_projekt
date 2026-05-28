# Podsumowanie wynikow CRUD

Plik wygenerowany automatycznie na podstawie `results/crud_benchmarks.csv`.

## Sredni czas wedlug operacji

| db | operation | avg_ms |
| --- | --- | --- |
| mongodb | CREATE | 19.108333333333334 |
| mariadb | CREATE | 38.852000000000004 |
| postgres | CREATE | 960.8125555555556 |
| cassandra | CREATE | 2613.0916666666667 |
| mariadb | DELETE | 25.270444444444447 |
| mongodb | DELETE | 78.70022222222222 |
| postgres | DELETE | 97.23822222222222 |
| cassandra | DELETE | 3785.3203333333336 |
| postgres | READ | 93.27533333333332 |
| cassandra | READ | 327.0328888888889 |
| mariadb | READ | 867.2184444444445 |
| mongodb | READ | 958.5760000000001 |
| mariadb | UPDATE | 18.28033333333333 |
| mongodb | UPDATE | 20.936999999999998 |
| postgres | UPDATE | 41.54677777777778 |
| cassandra | UPDATE | 1244.3477777777778 |

## Sredni czas wedlug rozmiaru zbioru

| db | size | avg_ms |
| --- | --- | --- |
| mariadb | 10000 | 241.10533333333333 |
| mongodb | 10000 | 300.50308333333334 |
| postgres | 10000 | 313.56025 |
| cassandra | 10000 | 2089.0800833333333 |
| mariadb | 100000 | 242.01275 |
| mongodb | 100000 | 247.03183333333334 |
| postgres | 100000 | 296.53125 |
| cassandra | 100000 | 1983.8048333333334 |
| mariadb | 1000000 | 229.09783333333334 |
| mongodb | 1000000 | 260.45625 |
| postgres | 1000000 | 284.5631666666667 |
| cassandra | 1000000 | 1904.4595833333333 |

## Wykresy

- `charts/avg_ms_by_operation_db.svg`
- `charts/avg_ms_by_size_db.svg`
- `charts/throughput_by_operation_db.svg`
