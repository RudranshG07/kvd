# miniredis

A key value store in Go, written from scratch with only the standard library.

It speaks the real Redis protocol, so redis-cli connects to it and doesn't know
the difference. Writes go to a log before the reply is sent, so it survives
being killed and picks up where it left off. A router in front of a few
instances hashes each key onto a ring and sends it to the node that owns it, so
adding a node only moves the keys right before it instead of nearly all of
them.

There is also a Raft implementation with leader election and log replication.
Kill the leader and the others elect a new one in a few hundred milliseconds,
and every node ends up applying the same entries in the same order.
