# miniredis

A distributed key value store in Go, built from scratch with nothing outside
the standard library. It speaks the real Redis wire protocol, so `redis-cli`
and `redis-benchmark` connect to it without knowing the difference, and it
benchmarks at around 89% of real Redis on GET. Writes go to an append only log
before the reply is sent, so it survives a `kill -9` and replays on boot. A
router in front of several instances places each node at 150 points on a hash
ring and sends a key to the next point clockwise, which means adding a fourth
shard to three moves 23k of 100k keys instead of the 75k that `hash % n` would.
Raft is next, so the cluster survives a node dying rather than losing that
shard.

```
go test -race ./...
go build -o miniredis . && ./miniredis -addr :6380
go build -o shard ./shard && ./shard -addr :6390
```
