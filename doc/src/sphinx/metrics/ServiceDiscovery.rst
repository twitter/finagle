Name Resolution
<<<<<<<<<<<<<<<

Finagle clients resolve :doc:`names <Names>` into sets of network
addresses to which sockets can be opened. A number of the moving parts
involved in this process are cached (i.e.
:src:`Dtabs <com/twitter/finagle/Dtab.scala>`,
:src:`Names <com/twitter/finagle/Name.scala>`, and
:src:`NameTrees <com/twitter/finagle/NameTree.scala>`).
The following stats are recorded under the
`namer/{dtabcache,namecache,nametreecache}` scopes to provide
visibility into this caching.

**misses**
  A counter of the number of cache misses.

**evicts**
  A counter of the number of cache evictions.

**expires**
  A counter of the number of idle ``ServiceFactory``\s
  that were actively evicted.

**idle**
  A gauge of the number of cached idle ``ServiceFactory``\s.

**oneshots**
  A counter of the number of "one-off" ``ServiceFactory``\s that are
  created in the event that no idle ``ServiceFactory``\s are cached.

**namer/bind_latency_us**
  A stat of the total time spent resolving ``Name``\s.

Initial Resolution
<<<<<<<<<<<<<<<<<<

**finagle/clientregistry/initialresolution_ms**

  A counter of the time spent waiting for client resolution via
  :src:`ClientRegistry.expAllRegisteredClientsResolved <com/twitter/finagle/client/ClientRegistry.scala>`.

Address Stabilization
<<<<<<<<<<<<<<<<<<<<<

Resolved addresses (represented as an instance of
:src:`Addr <com/twitter/finagle/Addr.scala>`) are stabilized in two ways:

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

**zkHealth**
  A gauge tracking the health of the underlying zk client as seen by the resolver.
  Unknown(0), Healthy(1), Unhealthy(2), Probation(3)

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
  A histogram of the latency, in milliseconds, of reading entry znodes.

**entries/parse_ms**
  A histogram of the latency, in milliseconds, of parsing the data
  within entry znodes.

**vectors/read_ms**
  A histogram of the latency, in milliseconds, of reading vector znodes.

**vectors/parse_ms**
  A histogram of the latency, in milliseconds, of parsing the data
  within vector znodes.

Under the \`zkclient\` scope
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**ephemeral_successes**
  A counter of the number successful ephemeral node creations.

**ephemeral_failures**
  A counter of the number failed ephemeral node creations.

**ephemeral_latency_ms**
  A histogram of the latency, in milliseconds, of ephemeral node creation.

**watch_successes**
  A counter of the number successful watch-related operations
  (i.e. "watch exists", "get watch data", and "get child watches"
  operations).

**watch_failures**
  A counter of the number failed watch-related operations.

**watch_latency_ms**
  A histogram of the latency, in milliseconds, of watch-related operations.

**read_successes**
  A counter of the number successful ZooKeeper reads.

**read_failures**
  A counter of the number failed ZooKeeper reads.

**read_latency_ms**
  A histogram of the latency, in milliseconds, of ZooKeeper reads.

**write_successes**
  A counter of the number successful ZooKeeper writes.

**write_failures**
  A counter of the number failed ZooKeeper writes.

**write_latency_ms**
  A histogram of the latency, in milliseconds, of ZooKeeper writes.

**multi_successes**
  A counter of the number successful transactional operations.

**multi_failures**
  A counter of the number failed transactional operations.

**multi_latency_ms**
  A histogram of the latency, in milliseconds, of transactional operations.

**session_sync_connected**
  A counter of the number of read-write session transitions.

**session_connected_read_only**
  A counter of the number of read-only session transitions.

**session_no_sync_connected**
  Unused (should always be 0).

**session_sasl_authenticated**
  A counter of the number of sessions upgraded to SASL.

**session_auth_failed**
  A counter of the number of session authentication failures.

**session_disconnected**
  A counter of the number of temporary session disconnects.

**session_expired**
  A counter of the number of session expirations.

**session_closed**
  A counter of the number of closed sessions.
