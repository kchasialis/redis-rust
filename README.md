# Redis Server Clone

A Redis-compatible server implemented from scratch in Rust. The server uses the Redis Serialization Protocol (RESP) and handles multiple concurrent clients via an **asynchronous event loop**, using [Tokio](https://tokio.rs/).

## Architecture

- **Async I/O** — Each client connection runs in its own Tokio task. There is no thread-per-client overhead; instead, the Tokio runtime multiplexes thousands of connections over a small thread pool.
- **Shared state** — In-memory storage is a `HashMap` wrapped in `Arc<RwLock<...>>`, allowing safe concurrent reads and serialised writes across tasks.
- **Inter-task communication** — Blocking commands (`BLPOP`, blocking `XREAD`) use Tokio `mpsc` channels. A per-key queue is used for senders so that the first writer wakes the first waiter in FIFO order.
- **RESP parser** — A handwritten RESP v2/v3 parser handles serialization and deserialization.

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
| `TYPE` | `TYPE <key>` | Returns the type of the value stored (`string`, `stream`, `none`) |

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
| `XREAD` | `XREAD [BLOCK <ms>] STREAMS <key> [key ...] <id> [id ...]` | Read new entries from one or more streams. Use `$` as ID to read only new entries |

## Key Implementation Details

- **Stream IDs** are monotonically ordered `(milliseconds, sequence)` pairs stored in a `BTreeMap` for efficient range queries.
- **TTL** is implemented lazily: expiry is checked on read rather than via a background sweeper.
- **Blocking operations** (`BLPOP`, `XREAD BLOCK`) register a Tokio `mpsc::Sender` in a per-key registry. The writing command pops the first waiter and sends the data directly through the channel, avoiding a second storage lookup where possible.
- **RESP protocol** — Full RESP v2 support plus v3 types (Boolean, Double, BigNumber, BulkError, Map, Set, VerbatimString).

### Build & Run

```bash
git clone https://github.com/kchasialis/redis-rust.git
cd redis-rust
cargo build --release
./target/release/redis-rs
```

The server listens on `127.0.0.1:6379`.

### Connect with redis-cli

```bash
redis-cli ping
redis-cli set foo bar EX 60
redis-cli get foo
redis-cli rpush mylist a b c
redis-cli blpop mylist 5
redis-cli xadd mystream '*' name Alice age 30
redis-cli xrange mystream - +
redis-cli xread block 0 streams mystream $
```

## Testing

Several tests can be found under `tests` directory. 

| Module | Tests |
|---|---|
| `resp_serialization` | RESP wire format round-trips for all types |
| `stream_id` | `StreamId` ordering and `Display` |
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

### Running the tests

```bash
# Run the full suite
cargo test

# Run a specific group
cargo test ping
cargo test xread

## License

MIT
