Metrics
-------

Here are some of the metrics reported by finagle. All times are in milliseconds. A metric is either
a count (C) or a statistic (S).

+-------------------------+-+----------------------------------------------------------------------+
|closechan                |C|This counter is incremented every time a connection is closed. Timed  |
|                         | |out connections are closed. By default an idle connection times out in|
|                         | |5 seconds (hostConnectionIdleTime).                                   |
+-------------------------+-+----------------------------------------------------------------------+
|connection_duration      |S|A stat representing the distribution of the duration                  |
|                         | |of a connection. closechan` and `connection_duration.count` must be   |
|                         | |equal.                                                                |
+-------------------------+-+----------------------------------------------------------------------+
|connection_received_bytes|S|bytes received per connection                                         |
+-------------------------+-+----------------------------------------------------------------------+
|connection_requests      |S|Number of requests per connection, observed after it closes.          |
+-------------------------+-+----------------------------------------------------------------------+
|connection_sent_bytes    |S|Bytes sent per connection                                             |
+-------------------------+-+----------------------------------------------------------------------+
|connections              |C|The current number of connections between client and server.          |
+-------------------------+-+----------------------------------------------------------------------+
|handletime_us            |S|The walltime elapsed while handling a request.                        |
+-------------------------+-+----------------------------------------------------------------------+
|requests                 |S|The number of requests dispatched.                                    |
+-------------------------+-+----------------------------------------------------------------------+
|pending                  |C|Number of pending requests (i.e. requests without responses).         |
+-------------------------+-+----------------------------------------------------------------------+
|request_latency_ms       |S|The time from the beginning of a request until the response is        |
|                         | |received                                                              |
+-------------------------+-+----------------------------------------------------------------------+


Connection Pool Stats
^^^^^^^^^^^^^^^^^^^^^

.. _pool_counts:

A finagle client pools tcp connections via a :ref:`WatermarkPool <watermark_pool>`
and :ref:`CachingPool <caching_pool>`. The following metrics expose the state
of the pools in your application.

**pool_cached** - represents the number of cached tcp connections to a particular host. This caching behavior is helpful to eliminate unnecessary connection churn as exposed by the default behavior of the watermark pool.

**pool_size** - represents the number of connections open to the host. It should be between the lower and upper bounds of your client's watermark pool configuration.

**pool_waiters** - the number of requests that are queued while waiting for a connection. Note, if you observe that "pool_waiters" is exactly zero, then there is no queueing waiting to get a connection from the pool. This means your pool is not under-sized with respect to the current workload. That's a good thing!

Load Balancer Stats
^^^^^^^^^^^^^^^^^^^

.. _load_balancer_counts:

A finagle client connected to multiple hosts load balances requests via a :ref:`HeapBalancer <heap_balancer>`. The following metrics expose the state of the heap balancer.

**size** - represents the current number of nodes used by the balancer.

**adds** - the cumulative node additions over the life time of the client.

**removes** - the cumulative node removals over the life time of the client.



