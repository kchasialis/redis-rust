# Redis Server Clone

A Redis-compatible server implemented from scratch in Rust. The server implements the Redis Serialization Protocol (RESP) and handles multiple concurrent clients via an **asynchronous event loop** powered by [Tokio](https://tokio.rs/).

## Architecture

- **Async I/O** — Each client connection runs in its own Tokio task. There is no thread-per-client overhead; the Tokio runtime multiplexes thousands of connections over a small thread pool.
- **Shared state** — In-memory storage is a `HashMap` wrapped in `Arc<RwLock<...>>`, allowing safe concurrent reads and serialized writes across tasks.
- **Inter-task communication** — Blocking commands (`BLPOP`, blocking `XREAD`) use Tokio `mpsc` channels. A per-key queue is used for senders so that the first writer wakes the first waiter in FIFO order.
- **RESP parser** — A handwritten RESP v2/v3 parser handles serialization and deserialization of all protocol types.
- **Replication** — A master-replica model is implemented using the PSYNC handshake protocol. The master propagates write commands to all connected replicas over persistent TCP connections. Replicas track their byte offset and respond to `REPLCONF GETACK` so the master can verify replication progress via `WAIT`.

## Supported Commands

### General

| Command | Syntax | Description |
|---------|--------|-------------|
| `PING` | `PING` | Returns `PONG` |
| `ECHO` | `ECHO <message>` | Echoes the message back |

### Strings

| Command | Syntax | Description |
|---------|--------|-------------|
| `SET` | `SET <key> <value> [EX seconds \| PX milliseconds]` | Set a key with optional TTL |
| `GET` | `GET <key>` | Get the value of a key |
| `INCR` | `INCR <key>` | Atomically increment an integer value by 1 |
| `TYPE` | `TYPE <key>` | Returns the type stored at a key (`string`, `stream`, `none`) |

### Lists

| Command | Syntax | Description |
|---------|--------|-------------|
| `RPUSH` | `RPUSH <key> <value> [value ...]` | Append values to the tail of a list |
| `LPUSH` | `LPUSH <key> <value> [value ...]` | Prepend values to the head of a list |
| `LPOP` | `LPOP <key> [count]` | Remove and return element(s) from the head |
| `LRANGE` | `LRANGE <key> <start> <stop>` | Get a range of elements (supports negative indices) |
| `LLEN` | `LLEN <key>` | Return the length of a list |
| `BLPOP` | `BLPOP <key> <timeout>` | Blocking left-pop; waits up to `timeout` seconds (0 = indefinite) |

### Streams

| Command | Syntax | Description |
|---------|--------|-------------|
| `XADD` | `XADD <key> <id\|-\|*> <field> <value> [field value ...]` | Append an entry to a stream. ID can be `*` (auto), `<ms>-*` (auto sequence), or explicit `<ms>-<seq>` |
| `XRANGE` | `XRANGE <key> <start> <end>` | Return entries in an ID range (`-` = min, `+` = max) |
| `XREAD` | `XREAD [BLOCK <ms>] STREAMS <key> [key ...] <id> [id ...]` | Read new entries from one or more streams. Use `$` as ID to read only future entries |

### Transactions

| Command | Syntax | Description |
|---------|--------|-------------|
| `MULTI` | `MULTI` | Begin a transaction block |
| `EXEC` | `EXEC` | Execute all commands queued since `MULTI` |
| `DISCARD` | `DISCARD` | Abort the transaction and discard the command queue |

Commands issued after `MULTI` return `QUEUED` and are executed atomically when `EXEC` is called. `DISCARD` clears the queue and exits the transaction block. Errors in individual queued commands (e.g. `INCR` on a non-integer) are reported per-command in the `EXEC` response without aborting the rest.

### Replication

| Command | Syntax | Description |
|---------|--------|-------------|
| `INFO` | `INFO replication` | Returns role, replication ID, and byte offset |
| `REPLCONF` | `REPLCONF <option> <value>` | Exchanged during the replication handshake and for ACK tracking |
| `PSYNC` | `PSYNC <replid> <offset>` | Initiates full replication sync and RDB transfer |
| `WAIT` | `WAIT <numreplicas> <timeout>` | Blocks until `numreplicas` replicas have acknowledged the current offset, or `timeout` ms elapses |

## Replication

The server supports a **master-replica** topology. A replica is started by passing `--replicaof <host> <port>`.

### Handshake sequence

When a replica starts it performs a three-step handshake with the master:

1. Sends `PING` → expects `PONG`
2. Sends `REPLCONF listening-port <port>` and `REPLCONF capa psync2` → expects `OK` for each
3. Sends `PSYNC ? -1` → master responds with `FULLRESYNC <replid> <offset>` followed by an RDB snapshot

After the handshake the master keeps the TCP connection open and streams every write command (currently `SET`) to the replica as serialized RESP arrays. The replica applies each command to its local storage and advances its `repl_offset` by the number of bytes consumed.

### Offset tracking and WAIT

The master tracks a `repl_offset` that advances by the serialized byte length of each propagated command. When a client issues `WAIT <n> <timeout>`, the master sends `REPLCONF GETACK *` to every replica and waits (up to `timeout` ms) for at least `n` replicas to respond with an offset that meets or exceeds the master's current offset. The count of qualifying replicas is returned to the client.

If the master offset is still 0 (no writes have occurred), `WAIT` returns the total number of connected replicas immediately without sending `GETACK`.

### Starting a replica

```bash
# Terminal 1 – start the master
./target/release/redis-rs --port 6379

# Terminal 2 – start a replica
./target/release/redis-rs --port 6380 --replicaof "localhost 6379"
```

## Key Implementation Details

- **Stream IDs** are monotonically ordered `(milliseconds, sequence)` pairs stored in a `BTreeMap` for efficient range queries.
- **TTL** is implemented lazily: expiry is checked on read rather than via a background sweeper.
- **Blocking operations** (`BLPOP`, `XREAD BLOCK`) register a Tokio `mpsc::Sender` in a per-key registry. The writing command pops the first waiter and sends the data directly through the channel.
- **Transactions** are tracked per-connection. A command queue is maintained for each client in `MULTI` state and flushed on `EXEC`.
- **Replication state** is stored behind an `Arc<RwLock<ReplicationState>>`. Each replica connection is stored as a split `OwnedReadHalf`/`OwnedWriteHalf` pair so the master can write propagated commands and simultaneously read `REPLCONF ACK` responses.
- **RDB transfer** — The master sends a minimal valid empty RDB file immediately after `FULLRESYNC` using the standard `$<len>\r\n<bytes>` framing (no trailing `\r\n`, as per the Redis wire format).
- **RESP protocol** — Full RESP v2 support plus v3 types (Boolean, Double, BigNumber, BulkError, Map, Set, VerbatimString).

## Build & Run

```bash
git clone https://github.com/kchasialis/redis-rust.git
cd redis-rust
cargo build --release

./target/release/redis-rs

./target/release/redis-rs --port 6385

./target/release/redis-rs --port 6380 --replicaof "localhost 6379"
```

## Connect with redis-cli

```bash
redis-cli ping
redis-cli set foo bar EX 60
redis-cli get foo
redis-cli incr counter
redis-cli rpush mylist a b c
redis-cli blpop mylist 5
redis-cli xadd mystream '*' name Alice age 30
redis-cli xrange mystream - +
redis-cli xread block 0 streams mystream $
redis-cli multi
redis-cli set foo 1
redis-cli incr foo
redis-cli exec

# Replication info
redis-cli info replication
redis-cli wait 1 500
```

## Testing

Tests live in the `tests/` directory and are organized by module. Run the full suite or a specific group:

```bash
# Full suite
cargo test

# Specific module
cargo test ping
cargo test xread
cargo test replication_state
cargo test replconf
cargo test set_propagation
```

| Module | Tests |
|---|---|
| `resp_serialization` | `simple_string`, `bulk_string`, `integer`, `null_bulk_string_wire_format`, `null_array_wire_format`, `nested_array`, `simple_error` |
| `stream_id` | `ordering`, `display` |
| `ping` | `returns_pong`, `stateless_repeated`, `concurrent_clients` |
| `echo` | `echoes_argument`, `preserves_spaces` |
| `set_get` | `basic_set_then_get`, `get_missing_key_returns_null`, `set_overwrites_existing_key`, `px_expiry`, `ex_expiry_alive_within_window` |
| `type_cmd` | `string_type`, `missing_key_is_none`, `stream_type` |
| `push` | `rpush_creates_list_of_one`, `rpush_increments_length`, `rpush_multiple_values`, `lpush_prepends_in_order` |
| `lrange` | `positive_indexes`, `negative_indexes` |
| `lpop` | `single_pop`, `pop_with_count`, `missing_key_returns_null` |
| `llen` | `existing_and_missing` |
| `blpop` | `timeout_returns_null_array`, `wakes_on_rpush`, `only_one_waiter_woken_per_push` |
| `xadd` | `explicit_id_is_returned`, `rejects_zero_zero_id`, `rejects_non_monotonic_ids`, `partial_auto_sequence`, `full_auto_id_format` |
| `xrange` | `explicit_bounds`, `minus_lower_bound`, `plus_upper_bound`, `missing_key_returns_empty` |
| `xread` | `single_stream`, `multiple_streams`, `block_wakes_on_xadd`, `block_timeout_returns_null`, `block_dollar_reads_only_future_entries` |
| `incr` | `increments_from_zero_when_missing`, `increments_existing_integer_key`, `errors_on_non_integer_value` |
| `info` | `master_role_and_offset`, `replica_role` |
| `replconf` | `listening_port_returns_ok`, `capa_psync2_returns_ok`, `getack_returns_replconf_ack_offset`, `getack_reflects_replica_offset` |
| `replication_state` | `master_starts_with_unique_replid`, `master_and_replica_start_at_zero_offset`, `replica_stores_master_info` |
| `set_propagation` | `offset_advances_after_set`, `offset_advances_cumulatively` |

## License

MIT