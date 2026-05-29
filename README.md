# money-laundering-analysis-tp
Sistema distribuido multicliente, escalable y tolerante a fallas que analiza la información de transacciones bancarias para detectar lavado de activos.

## Protocolo de comunicacion (cliente-gateway)

El cliente y el gateway se comunican por TCP. Todos los mensajes empiezan con
un `msg_type` serializado como `uint32` big-endian. Los strings se serializan
como `uint32 length` + bytes UTF-8.

Tipos de mensaje:

- `TRANSACTIONS_BATCH` (`1`): `client_id` + payload CSV de transacciones.
- `ACCOUNTS_BATCH` (`2`): `client_id` + payload CSV de cuentas.
- `END_TRANSACTIONS` (`3`): `client_id`.
- `END_ACCOUNTS` (`4`): `client_id`.
- `QUERY_RESULT_BATCH` (`5`): `client_id` + `query_id` + payload CSV de resultados.
- `END_QUERY` (`6`): `client_id` + `query_id`.
- `END_RESULTS` (`7`): `client_id`.
- `ACK` (`8`): `client_id`.

### Batches CSV

`TRANSACTIONS_BATCH`, `ACCOUNTS_BATCH` y `QUERY_RESULT_BATCH` usan un unico
payload CSV por batch:

```
uint32 msg_type
string client_id
uint32 query_id      # solo para QUERY_RESULT_BATCH
uint32 payload_size
bytes csv_payload
```

El payload CSV no incluye header. Para transacciones, el cliente proyecta y
envia solo las columnas que usa el sistema:

```
Timestamp, From Bank, Account, To Bank, Account.1,
Amount Paid, Payment Currency, Payment Format
```

Este formato evita serializar cada celda por separado y reduce el costo de CPU del cliente y del gateway al procesar datasets grandes.


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
