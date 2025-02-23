# SUPERMASSIVE
SuperMassive is a highly scalable, in-memory, distributed, sharded, fault-tolerant, and self-healing key-value database.

## Features
- **Highly scalable** Scale horizontally with ease.
- **Distributed** Data is distributed across multiple nodes.
- **Sharded** Data is sharded across multiple nodes.
- **Robust Health Checking System** Health checks are performed on all nodes.
- **Smart Data Distribution** uses a sequence-based round-robin approach for distributing writes across primary nodes.
- **Automatic Fail-over** Automatic fail-over of primary nodes on write failure.
- **Parallel Read Operations** Read operations are performed in parallel.
- **Consistency Management** Timestamp-based version control to handle conflicts.
- **Fault-tolerant** Replication and fail-over are supported.
- **Self-healing** Automatic data recovery.
- **Simple Protocol** Simple protocol PUT, GET, DEL, INCR, DECR
- **Journaling** used for node recovery.
- **Multi-platform** Linux, Windows, MacOS

## Example
For testing purposes start cluster with ``--local`` to start with no user authentication.

You can use TLS, configure in your yaml configuration files.  A cluster can be started with TLS and so can other instance types.
If nodes and their replicas are set to use TLS make sure to configure client connections to these nodes to be using TLS.

When starting a cluster instance you provide a `--username` and `--password`.  When accessing through a client like netcat you now need to authenticate with `AUTH user\0password`.

The `user\0password` should be encoded in base64.
```bash
$ ( echo -n "AUTH " && echo -n $'user\0password' | base64 ) | nc -C localhost 4000
OK authenticated
```

`-C` is used for CRLF line endings.  This is required for the protocol.

```bash
PUT key1 value1
OK key-value written

GET key1
key1 value1

DEL key1
OK key-value deleted

GET key1
ERR key-value not found

PUT key1 100
OK key-value written

INCR key1
OK 101

DECR key1
key1 100

INCR key1 10
key1 110

DECR key1 10
key1 100

PUT key2 1.5
OK key-value written

INCR key2 1.1
key2 2.6

DECR key2 1.1
key2 1.5
```

There are NO transactions.  All commands except PUT, PING are ran in parallel.  PUT selects 1 node based on current sequence and writes to it.
On get, we always return the most recent value of a key.  If there are multiple values for a key only 1 value lives on if this occurs, rest are deleted.


## Replica consistency?
When a replica is down, the primary node will not be able to write to it.  The primary node will continue to write to the other replicas.
When the replica comes back up, the primary node will send the missing data to the replica.  The replica will then be in sync with the primary node.

This is using the journal pages and a specific piece of the protocol
A replica will once connected to will send a `SYNCFROM pgnum` where pgnum is the last page number in the replica journal.  The primary will then send missing operations to the replica.

Communication looks like this
```
-- Replica goes online
-- Replica sends `SYNCFROM pgnum` to primary
-- Primary starts sending pages to replica until it reaches end of its journal
-- Replica writes pages to its journal
-- PRIMARY is done sending pages to replica once `SYNCDONE` is sent to replica
-- Primary and replica are now in sync
```
