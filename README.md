# Redis Server Clone

A Redis server clone implemented from scratch in Rust. The server implements the Redis Serialization Protocol (RESP) and handles multiple concurrent clients via an asynchronous event loop using [Tokio](https://tokio.rs/).

## Architecture

- **Async I/O** — Each client connection runs in its own Tokio task and yields CPU when waiting for I/O to other tasks. 
- **Shared state** — In-memory storage is a `HashMap` wrapped in `Arc<RwLock<...>>`, allowing safe concurrent reads and serialized writes across tasks.
- **Inter-task communication** — Blocking commands (`BLPOP`, blocking `XREAD`) use Tokio `mpsc` channels. A per-key queue is used for senders so that the first writer wakes the first waiter in FIFO order.
- **RESP parser** — A handwritten RESP v2/v3 parser handles serialization and deserialization of all protocol types.
- **Replication** — A master-replica model is implemented using the PSYNC handshake protocol. The master propagates write commands to all connected replicas over persistent TCP connections. Replicas track their byte offset and respond to `REPLCONF GETACK` so the master can verify replication progress via `WAIT`.
- **Pub/Sub** — A global `Subscriptions` registry maps channel names to Tokio `broadcast::Sender`s. Each `SUBSCRIBE` call spawns a per-channel task that forwards received messages over a per-connection `mpsc` channel, which the connection loop selects alongside incoming client reads.

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

### Pub/Sub

| Command | Syntax | Description |
|---------|--------|-------------|
| `SUBSCRIBE` | `SUBSCRIBE <channel>` | Subscribe to a channel; receives a confirmation and subsequent `message` pushes |
| `UNSUBSCRIBE` | `UNSUBSCRIBE <channel>` | Unsubscribe from a channel |
| `PUBLISH` | `PUBLISH <channel> <message>` | Publish a message to all subscribers; returns subscriber count |

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
- **Pub/Sub** — A client entering subscribe mode is restricted to `SUBSCRIBE`, `UNSUBSCRIBE`, and `PING`. The connection loop uses `tokio::select!` to simultaneously wait for new client commands and for messages forwarded from the broadcast channel, ensuring message delivery without blocking.

## Build & Run

```bash
git clone https://github.com/kchasialis/redis-rust.git
cd redis-rust
cargo build --release

# Standalone master on default port 6379
./target/release/redis-rs

# Custom port
./target/release/redis-rs --port 6385

# Replica of another instance
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

Tests are located in the `tests/` directory and are organized by module. Run the full suite or a specific group:

```bash
# Full suite
cargo test

# Specific module
cargo test ping
cargo test persistence
cargo test pub_sub
```

## License

MIT