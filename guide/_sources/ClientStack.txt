Client Stack
============

As of :doc:`6.x <changelog>`, the :src:`DefaultClient <com/twitter/finagle/client/DefaultClient.scala>`
is the recommended api for building clients. The DefaultClient decorates a client
with many prominent components: load balancing over multiple hosts, connection
pooling per host, a failure management mechanism, and much more. Client implementations
are encouraged to provide sensible defaults and leave room
for application specific behavior to be built on top of the base
layer via filters or synchronization mechanisms.

Load Balancing
^^^^^^^^^^^^^^

.. _heap_balancer:

The heap balancer maintains a collection of hosts, each represented by a
ServiceFactory, and equally distributes requests over the hosts. The default
balancing strategy is to pick the host with the least number of outstanding requests,
which is similar to a least connections strategy in other load balancers. Additionally,
the load balancer deliberately introduces jitter to avoid synchronicity (and thundering herds)
in a distributed system and to ensure even balancing when request concurrency is low.

Connection Pooling
^^^^^^^^^^^^^^^^^^

.. _watermark_pool:

Finagle provides a generic pool that maintains a collection of
service instances. Each endpoint the client connects to has an independent
pool with high and low watermarks. The :src:`WatermarkPool <com/twitter/finagle/pool/WaterMarkPool.scala>` keeps
persistent services up to the lower bound. It will keep making new services up
to upper bound if you checkout more than lower bound services, but when
you release those services above the lower bound, it immediately tries
to close them. This, however, creates a lot of connection churn if your
application consistantly requires more than lower bound connections.

.. _caching_pool:

As a result, there is a separate facility for caching, with some TTL,
services above the lower bound. The :src:`CachingPool <com/twitter/finagle/pool/CachingPool.scala>`
caches *regardless* of whether there are more than lower-bound open services;
it's always caching up to (upper-bound - lower-bound) services. The cache reaches
its peak value when you reach your peak concurrency (i.e. "load"),
and then slowly decays, based on the TTL.

The DefaultClient layers both pools which amounts to
maintaining the low watermark (as long as request concurrency exists),
queueing requests above the high watermark, and applying a TTL for
services that are between [low, high].

Note, finagle exposes :ref:`pool counts <pool_counts>` to observe the pool behavior.

Fail Fast
^^^^^^^^^

The :src:`FailFastFactory <com/twitter/finagle/service/FailFastFactory.scala>`
attempts to reduce the amount of requests dispatched to endpoints that are likely to fail.
It works by marking downed hosts when a connection fails, and launching a background process that
repeatedly attempts to reconnect with a given backoff schedule. During the time that a host is marked down,
the factory is marked unavailable (and thus the load balancer
above it will avoid its use). The factory becomes available
again on success or when the backoff schedule runs out.

Timeouts
^^^^^^^^

Finagle provides timeout facilities with fine granularity. The simplest and most
common way to enforce a timeout is per-request using Future#within [#]_:

::

  val f = client(request)
  f.within(1.seconds) onSuccess { ... } onFailure { ... }

The DefaultClient exposes parameters that enforce timeouts at varying
levels of the client stack:

**maxIdletime** - The maximum time for which any Service is permitted to be idle.

**maxLifetime** - The maximum lifetime for any Service.

**serviceTimeout** - The maximum amount of time allowed for acquiring a Service.

By default these are disabled and DefaultClient implementations should
only enforced them with care.

.. [#] Future.within creates a new future that does not
       cancel the original future if a timeout occurs.
       When a timeout occurs, calling Future#raise
       on the original future raises an advisory exception
       and may potentially cancel a pending request.

Retries
^^^^^^^

Finagle provides a configurable :src:`RetryingFilter <com/twitter/finagle/service/RetryingFilter.scala>`.
The filter can be configured either to retry a specific number of times or to adhere to a backoff strategy.
By default, the RetryingFilter *does not assume your RPC service is idempotent*. Retries occur only when they
are known to be safe. That is, when Finagle can guarantee the request was never delivered to the
server.

