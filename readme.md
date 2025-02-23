<div>
    <h1 align="left"><img width="286" src="artwork/supermassive-logo-colored-v1.0.png"></h1>
</div>

SuperMassive is a massively scalable, in-memory, distributed, sharded, fault-tolerant, and self-healing key-value database.

> [!IMPORTANT]
> SuperMassive is in active development and is not ready for production use.

## Features

- **Highly scalable** Scale horizontally with ease.  Simply add more nodes to the cluster.
- **Distributed** Data is distributed across multiple nodes in a sharded fashion.
- **Robust Health Checking System** Health checks are performed on all nodes, if any node is marked unhealthy we will try to recover it.
- **Smart Data Distribution** uses a sequence-based round-robin approach for distributing writes across primary nodes.  This ensures that all primary nodes get an equal share of writes.
- **Automatic Fail-over** Automatic fail-over of primary nodes on write failure. If a primary node is unavailable for a write, we go to the next available primary node.
- **Parallel Read Operations** Read operations are performed in parallel.
- **Consistency Management** Timestamp-based version control to handle conflicts. The most recent value is always returned, the rest are deleted.
- **Fault-tolerant** Replication and fail-over are supported. If a node goes down, the cluster will continue to function.
- **Self-healing** Automatic data recovery.  A node can recover from a journal.  A node replica can recover from a primary node via a check point like algorithm.
- **Simple Protocol** Simple protocol PUT, GET, DEL, INCR, DECR, REGX
- **Async Node Journal** Operations are written to a journal asynchronously.  This allows for fast writes and recovery.
- **Multi-platform** Linux, Windows, MacOS

## Discord
Join the SuperMassive Discord server to chat with the maintainers and other users.  We are always looking for feedback, bugs and discussion.

[![Discord](https://img.shields.io/discord/1343082076324495402?color=yellow&label=Discord&logo=discord&style=for-the-badge)](https://discord.gg/yfE5MV5w4d)

## Getting Started

You can use TLS for your client-cluster communication and cluster-node node-replica communication.  A cluster can be started with TLS and so can other instance types based on configuration files.

When starting a cluster instance you provide a `--username` and `--password`.  When accessing through a client like netcat you now need to authenticate with `AUTH user\0password`.

The `user\0password` should be encoded in base64.

There is only 1 user for the system.  You set it, you can change the user credentials any restart.
It is advisable to call an environment variable to populate these flags or another method to keep the password secure.

Starting either a cluster, node, or node replica you always also provide a `--shared-key` flag which is used to authentication cluster to node, node to node replica communication.
All keys in the chain must match for communication to be successful.  This should also be kept secure.

```bash
(echo -n "AUTH " && echo -n $"username\\0password" | base64 && cat) | nc -C localhost 4000
OK authenticated
```

`-C` is used for CRLF line endings.  This is required for the protocol.

A cluster requires nodes to write to.  By default `.cluster`, `.node`, `.nodereplica` yaml configs are created.
These configurations give you an example of how a cluster is setup with 1 node and 1 replica.

### Configurations

**Cluster**

```yaml
health-check-interval: 2
server-config:
    address: localhost:4000
    use-tls: false
    cert-file: /
    key-file: /
    read-timeout: 10
    buffer-size: 1024
node-configs:
    - node:
        server-address: localhost:4001
        use-tls: false
        ca-cert-file: ""
        connect-timeout: 5
        write-timeout: 5
        read-timeout: 5
        max-retries: 3
        retry-wait-time: 1
        buffer-size: 1024
      replicas:
        - server-address: localhost:4002
          use-tls: false
          ca-cert-file: ""
          connect-timeout: 5
          write-timeout: 5
          read-timeout: 5
          max-retries: 3
          retry-wait-time: 1
          buffer-size: 1024

```
You can add more nodes and replicas to the cluster by adding more `node-configs`.
A `node` acts as a primary shard and a `replica` acts as a read replica to the primary shard.

**Node**

```yaml
health-check-interval: 2
server-config:
    address: localhost:4001
    use-tls: false
    cert-file: /
    key-file: /
    read-timeout: 10
    buffer-size: 1024
read-replicas:
    - server-address: localhost:4002
      use-tls: false
      ca-cert-file: /
      connect-timeout: 5
      write-timeout: 5
      read-timeout: 5
      max-retries: 3
      retry-wait-time: 1
      buffer-size: 1024

```

**Node Replica**

```yaml
server-config:
    address: localhost:4002
    use-tls: false
    cert-file: /
    key-file: /
    read-timeout: 10
    buffer-size: 1024

```

### Examples

```bash
PUT key1 value1
OK key-value written

GET key1
key1 value1

DEL key1
OK key-value deleted

GET key1
ERR key-value not found

-- Match keys with REGEX
--- Match keys starting with prefix 'user_'
REGX ^user_
--- Match keys ending with a suffix '2024'
REGX _2024$
--- Contains 'session'
REGX session
--- Match keys with 'user' and '2024'
REGX user.*2024
--- Match keys with 'user' or '2024'
REGX user|2024
--- Range match keys between 'log_10', 'log_15'
REGX log_(1[0-5])
-- Results are returned OK KEY VALUE CRLF KEY VALUE CRLF...

-- You can offset and limit the results
REGX user.*2024 0 10 -- Offset 0, Limit 10
REGX user.*2024 10 10 -- Offset 10, Limit 10

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

STAT -- get stats on all nodes in the cluster
OK
PRIMARY localhost:4001 -- get stats on a specific node
DISK
    sync_enabled true
    sync_interval 128ms
    avg_page_size 1024.00
    file_mode -rwxrwxr-x
    is_closed false
    last_page 99
    storage_efficiency 0.9846
    file_name .journal
    page_size 1024
    total_pages 100
    total_header_size 1600
    total_data_size 102400
    page_utilization 1.0000
    header_overhead_ratio 0.0154
    file_size 104000
    modified_time 2025-02-23T04:39:31-05:00
MEMORY
    load_factor 0.3906
    grow_threshold 0.7500
    max_probe_length 2
    empty_buckets 156
    utilization 0.3906
    needs_grow false
    needs_shrink false
    size 256
    used 100
    shrink_threshold 0.2500
    avg_probe_length 0.2600
    empty_bucket_ratio 0.6094
REPLICA localhost:4002 -- Will list primary, then all replica stats under each primary
.. more

RCNF -- reload configuration files, will reload for entire cluster, nodes, and replicas.  Good when you want to change configurations without restarting the cluster.
OK configs reloaded

```

> [!NOTE]
> There are NO transactions.  All commands except PUT, PING are ran in parallel.  PUT selects 1 node based on current sequence and writes to it.  On get, we always return the most recent value of a key.  If there are multiple values for a key only 1 value lives on if this occurs, rest are deleted.


## Replica consistency?

When a replica is down, the primary node will not be able to write to it.  The primary node will continue to write to the other replicas.
When the replica comes back up, the primary node will send the missing data to the replica.  The replica will then be in sync with the primary node.

This is using the journal pages and a specific piece of the protocol
A primary after connected to replica will send a `STARTSYNC`, a replica will then send a `SYNCFROM pgnum` where pgnum is the last page number in the replica journal.  The primary will then send missing operations to the replica.

**Communication looks like this**
1. Replica goes online
2. Replica sends `SYNCFROM pgnum` to primary
3. Primary starts sending pages to replica until it reaches end of its journal
4. Replica writes pages to its journal
5. PRIMARY is done sending pages to replica once `SYNCDONE` is sent to replica
6. Primary and replica are now in sync

## All nodes are full?
Add more nodes to the cluster.  The cluster will automatically distribute the data across the new nodes.
Primaries can shrink based on deletes allowing more data to be written over time based on new values taking precedence.

## Contributing
To contribute to SuperMassive simply fork, make your changes, and submit a pull request with a detailed description on why, what changes were made.

## License
BSD-3-Clause
