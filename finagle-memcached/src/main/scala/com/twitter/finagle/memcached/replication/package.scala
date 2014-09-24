package com.twitter.finagle.memcached

/**

Package replication implements a base cache client that can manage multiple cache replicas.

The base replication client will forward cache command to all replicas, as well as collect and
aggregate each replica's response into a ReplicationStatus object representing the replication
consistency. The BaseReplicationClient will not handle the consistency between replicas in anyway,
but only to report its view of the replication state. For instance, BaseReplicationClient provides
interfaces similar to generic memcache client but always returns ReplicationStatus object which
can be one of these three forms:
  - ConsistentReplication, indicating a consistent state across all replicas
  - InconsistentReplication, indicating an inconsistent state across all replicas
  - FailedReplication, indicating a failure state

By checking the returned ReplicationStatus object, one can tell the cache replication status and
then handle it with application specific logic.

In addition to a base replication client, a simple replication client wrapper that's compatible
with generic cache client interface is also provided. The SimpleReplicationClient only supports
a subset of all memcached commands for now, and will succeed only if the command succeed on all
cache replicas. In a more complicate caching scenario, this simple/naive replication client may
not be applicable.
 */
package object replication
