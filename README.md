# money-laundering-analysis-tp
Sistema distribuido multicliente, escalable y tolerante a fallas que analiza la información de transacciones bancarias para detectar lavado de activos.

## Protocolo de comunicacion (cliente-servidor)

Todos los enteros se serializan como uint32 en big-endian. Los strings se serializan como largo (uint32) + bytes utf-8.

Tipos de mensaje:

- `TRANSACTIONS_BATCH`: `client_id` (string), lista de filas (cada fila es lista de strings)
- `ACCOUNTS_BATCH`: `client_id` (string), lista de filas (cada fila es lista de strings)
- `END_TRANSACTIONS`: `client_id` (string)
- `END_ACCOUNTS`: `client_id` (string)
- `QUERY_RESULT_BATCH`: `client_id` (string), `query_id` (uint32), lista de filas
- `END_QUERY`: `client_id` (string), `query_id` (uint32)
- `END_RESULTS`: `client_id` (string)
- `ACK`: `client_id` (string)


Flujo esperado:

1) Cliente envia `ACCOUNTS_BATCH` en batches y luego `END_ACCOUNTS`.
2) Cliente envia `TRANSACTIONS_BATCH` en batches y luego `END_TRANSACTIONS`.
3) Servidor responde con multiples `QUERY_RESULT_BATCH` intercalados para las 5 consultas.
4) Servidor envia `END_QUERY` por cada consulta y finalmente `END_RESULTS`.
5) Cliente responde `ACK` (con `client_id`) por cada mensaje recibido.
6) Servidor responde `ACK` (con `client_id`) por cada mensaje recibido.

Todos los mensajes incluyen `client_id` para validar errores de ruteo.

## Script de muestreo de datasets

El script [scripts/reduce_dataset.py](scripts/reduce_dataset.py) permite reducir cualquier CSV con header usando muestreo aleatorio. No carga todo el archivo en memoria.

Uso:

```bash
python scripts/reduce_dataset.py --input datasets/transactions_full.csv --output datasets/transactions_reduced.csv --size 100000
```

Opcionalmente, podes fijar una semilla para resultados reproducibles:

```bash
python scripts/reduce_dataset.py --input datasets/transactions_full.csv --output datasets/transactions_reduced.csv --size 100000 --seed 123
```

## Resultados

Los resultados de cada cliente se escriben como CSV en:

```
results/client_{id}/results_q{n}.csv
```

Ejemplo:

```
results/client_0/results_q1.csv
```
