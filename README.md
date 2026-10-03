# miniredis

A distributed key value store in Go, built from scratch. No dependencies
outside the standard library.

It speaks the real Redis wire protocol, so `redis-cli` and `redis-benchmark`
connect to it without knowing the difference.

```
$ redis-cli -p 6380 SET user rudransh
OK
$ redis-cli -p 6380 GET user
"rudransh"
```

## What's in it

**The server.** An in-memory store behind a `sync.RWMutex`, one goroutine per
connection, with TTLs expired lazily on read the way Redis does it. The
protocol is five types separated by `\r\n`.

```
redis-benchmark -n 50000 -t set,get

real redis   SET 211,864/s   GET 239,234/s
this         SET 176,678/s   GET 213,675/s
```

**Durability.** Every write is appended to a log before the reply goes out, and
the log is replayed on boot. A `kill -9` mid write leaves a partial command at
the tail, so replay stops at the last complete one and keeps everything before
it.

```
fsync every write        278 writes/s
fsync off             66,445 writes/s
```

**Sharding.** A router in front of N instances, placing each node at 150 points
on a hash ring and sending a key to the next point clockwise. Commands with no
key to hash (`KEYS`, `FLUSHALL`) fan out to every node and come back merged.

```
100,000 keys across 3 shards      34,595 / 30,976 / 34,429

adding a 4th shard
  hash ring        22,847 moved
  hash % n         75,143 moved
```

## Running it

```
go build -o miniredis .
./miniredis -addr :6380

go build -o shard ./shard
./shard -addr :6390 -shards 127.0.0.1:6380,127.0.0.1:6381,127.0.0.1:6382
```

Flags: `-aof` for the log path, `-fsync=false` to trade durability for speed,
`-replicas` for points per node on the ring.

## Layout

```
resp/      the wire protocol, shared by the server and the router
server.go  command dispatch
store.go   the map, TTLs, lazy expiry
aof.go     the append only log
shard/     the router and the hash ring
```

## Next

Raft, so the cluster survives a node dying instead of losing that shard.

## Tests

```
go test -race ./...
```
