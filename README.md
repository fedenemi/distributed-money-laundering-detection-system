# money-laundering-analysis-tp

A fault-tolerant, scalable, multi-client distributed system that analyzes banking transaction data to detect money laundering patterns. Built as part of the Distributed Systems course (75.74) at UBA FIUBA.

The system ingests ~170 million banking transactions from the IBM AML dataset and runs five analytical queries in parallel across a pipeline of ~80 containerized workers coordinated via RabbitMQ. It is designed to survive arbitrary process crashes mid-execution: each worker persists its state to disk and recovers automatically after being restarted by a distributed monitor. The monitoring subsystem itself uses the Chang-Roberts ring election algorithm to tolerate monitor failures without a single point of failure.

**Key technical highlights:**
- Distributed pipeline across 80+ Docker containers orchestrated with docker-compose
- RabbitMQ-based message passing with at-least-once delivery and idempotent deduplication
- Crash recovery via disk persistence (atomic writes with CRC32 checksums) and RabbitMQ requeue
- Leaderless fault detection using Chang-Roberts ring election across 3 monitor nodes
- Chaos Monkey testing to validate fault tolerance under arbitrary SIGKILL events

---

## Client-gateway communication protocol

The client and gateway communicate over TCP. All messages start with a `msg_type` serialized as a `uint32` big-endian. Strings are serialized as `uint32 length` + UTF-8 bytes.

Message types:

- `TRANSACTIONS_BATCH` (`1`): `client_id` + CSV payload of transactions.
- `ACCOUNTS_BATCH` (`2`): `client_id` + CSV payload of accounts.
- `END_TRANSACTIONS` (`3`): `client_id`.
- `END_ACCOUNTS` (`4`): `client_id`.
- `QUERY_RESULT_BATCH` (`5`): `client_id` + `query_id` + CSV payload of results.
- `END_QUERY` (`6`): `client_id` + `query_id`.
- `END_RESULTS` (`7`): `client_id`.
- `ACK` (`8`): `client_id`.

### CSV batches

`TRANSACTIONS_BATCH`, `ACCOUNTS_BATCH` and `QUERY_RESULT_BATCH` use a single CSV payload per batch:

```
uint32 msg_type
string client_id
uint32 query_id      # only for QUERY_RESULT_BATCH
uint32 payload_size
bytes csv_payload
```

The CSV payload does not include a header. For transactions, the client projects and sends only the columns used by the system:

```
Timestamp, From Bank, Account, To Bank, Account.1,
Amount Paid, Payment Currency, Payment Format
```

This format avoids serializing each cell individually and reduces CPU overhead on both the client and the gateway when processing large datasets.

Expected flow:

1) Client sends `ACCOUNTS_BATCH` in batches, then `END_ACCOUNTS`.
2) Client sends `TRANSACTIONS_BATCH` in batches, then `END_TRANSACTIONS`.
3) Server responds with multiple `QUERY_RESULT_BATCH` messages interleaved across the 5 queries.
4) Server sends `END_QUERY` for each query and finally `END_RESULTS`.
5) Client responds `ACK` (with `client_id`) for each message received.
6) Server responds `ACK` (with `client_id`) for each message received.

All messages include `client_id` to validate routing errors.

## Dataset sampling script

The [scripts/reduce_dataset.py](scripts/reduce_dataset.py) script allows reducing any CSV with a header using random sampling. It does not load the entire file into memory.

Usage:

```bash
python scripts/reduce_dataset.py --input datasets/transactions_full.csv --output datasets/transactions_reduced.csv --size 100000
```

Optionally, you can fix a seed for reproducible results:

```bash
python scripts/reduce_dataset.py --input datasets/transactions_full.csv --output datasets/transactions_reduced.csv --size 100000 --seed 123
```

## Chaos monkey

The [scripts/chaos_monkey.py](scripts/chaos_monkey.py) script allows interrupting docker-compose services to test fault tolerance. Configuration lives in [scripts/chaos_monkey.yaml](scripts/chaos_monkey.yaml).

First, validate that the configuration selects the expected services:

```bash
python scripts/chaos_monkey.py --dry-run --once
```

To run it continuously while the system is up:

```bash
python scripts/chaos_monkey.py
```

To limit it to a single real event:

```bash
python scripts/chaos_monkey.py --once
```

To test a specific node without modifying the YAML:

```bash
python scripts/chaos_monkey.py --once --service q2_banks_name_adder_0
```

You can also select by regex:

```bash
python scripts/chaos_monkey.py --once --pattern '^q3_avg_and_transactions_joiner_0$'
```

The configuration allows defining:

- `allowed_services`: exact services that may be stopped.
- `allowed_patterns`: regex patterns of allowed services.
- `exclude_services` and `exclude_patterns`: services that should never be touched.
- `action`: `restart`, `stop`, `kill`, `stop_start` or `kill_start`.
- `interval_seconds`: wait range between failures.
- `downtime_seconds`: range of time a service stays stopped.
- `max_events`: maximum number of events before stopping.

By default, `rabbitmq`, `gateway`, `client_*` and the Q5 API client are never interrupted.

## Results

Results for each client are written as CSV files to:

```
results/client_{id}/results_q{n}.csv
```

Example:

```
results/client_0/results_q1.csv
```
