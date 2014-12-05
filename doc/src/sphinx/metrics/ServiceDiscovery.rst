Name Resolution
<<<<<<<<<<<<<<<

Finagle clients resolve :doc:`names <Names>` into sets of network
addresses to which sockets can be opened. A number of the moving parts
involved in this process are cached (i.e. `Dtab`\s, `Name`\s, and
`NameTree`\s). The following stats are recorded under the
`interpreter/{dtabcache,namecache,nametreecache}` scopes to provide
visibility into this caching.

**misses**
  A counter of the number of cache misses

**misstime_ms**
  A histogram of the latency, in milliseconds, of invocation of
  `Service`\s created on misses

**evicts**
  A counter of the number of cache evictions

**idle**
  A gauge of the number of cached idle `ServiceFactory`\s

**oneshots**
  A counter of the number of "one-off" `ServiceFactory`\s that are
  created in the event that no idle `ServiceFactory`\s are cached

Address Stabilization
<<<<<<<<<<<<<<<<<<<<<

Resolved addresses (represented as an instance of
`com.twitter.finagle.Addr`) are stabilized in two ways:

1. ZooKeeper failures will not cause a previously bound address to fail.
2. When a member leaves a cluster, its removal is delayed.

Note that hosts added to a cluster are reflected immediately.

The following metrics are scoped under the concatenation of `zk2/` and
the ServerSet's ZooKeeper path.

**limbo**
  A gauge tracking the number of endpoints that are in "limbo". When a
  member leaves a cluster, it is placed in limbo. Hosts in limbo are
  still presented to the load balancer as belonging to the cluster,
  but are staged for removal. They are removed if they do not recover
  within an interval bound by the ZooKeeper session timeout.

**size**
  A gauge tracking the total size of the live cluster, not including
  members in limbo.

**observed_serversets**
  A gauge tracking the number of clusters whose membership status is
  *currently* been tracked within the process. This metric differs from
  `session_cache_size` below in that it tracks live clusters rather
  than the total number of cached sessions.

ZooKeeper Diagnostics
<<<<<<<<<<<<<<<<<<<<<

The following stats reflect diagnostic information about the ZooKeeper
sessions opened for the purposes of service discovery.

Under the \`zk2\` scope
~~~~~~~~~~~~~~~~~~~~~~~

**session_cache_size**
  A gauge tracking the number of distinct logical clusters whose
  membership status has been tracked within the process.

**entries/read_ms**
  A histogram of the latency, in milliseconds, of reading entry znodes

**entries/parse_ms**
  A histogram of the latency, in milliseconds, of parsing the data
  within entry znodes

**vectors/read_ms**
  A histogram of the latency, in milliseconds, of reading vector znodes

**vectors/parse_ms**
  A histogram of the latency, in milliseconds, of parsing the data
  within vector znodes

Under the \`zkclient\` scope
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**ephemeral_successes**
  A counter of the number successful ephemeral node creations

**ephemeral_failures**
  A counter of the number failed ephemeral node creations

**ephemeral_latency_ms**
  A histogram of the latency, in milliseconds, of ephemeral node creation

**watch_successes**
  A counter of the number successful watch-related operations
  (i.e. "watch exists", "get watch data", and "get child watches"
  operations)

**watch_failures**
  A counter of the number failed watch-related operations

**watch_latency_ms**
  A histogram of the latency, in milliseconds, of watch-related operations

**read_successes**
  A counter of the number successful ZooKeeper reads

**read_failures**
  A counter of the number failed ZooKeeper reads

**read_latency_ms**
  A histogram of the latency, in milliseconds, of ZooKeeper reads

**write_successes**
  A counter of the number successful ZooKeeper writes

**write_failures**
  A counter of the number failed ZooKeeper writes

**write_latency_ms**
  A histogram of the latency, in milliseconds, of ZooKeeper writes

**multi_successes**
  A counter of the number successful transactional operations

**multi_failures**
  A counter of the number failed transactional operations

**multi_latency_ms**
  A histogram of the latency, in milliseconds, of transactional operations

**session_connects**
  A counter of the number of successful session connection operations

**session_connects_readonly**
  A counter of the number of successful read-only session connection operations

**session_auth_failures**
  A counter of the number of session authentication failures

**session_disconnects**
  A counter of the number of session disconnections

**session_expirations**
  A counter of the number of session expirations
