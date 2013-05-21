Metrics
-------

Here are some of the metrics reported by finagle. All times are in milliseconds. A metric is either
a count (C) or a statistic (S).

+-------------------------+-+----------------------------------------------------------------------+
|closechan                |C|This counter is incremented every time a connection is closed. Timed  |
|                         | |out connections are closed. By default an idle connection times out in|
|                         | |5 seconds (hostConnectionIdleTime).                                   |
+-------------------------+-+----------------------------------------------------------------------+
|connection_duration      |S|A stats (an Histogram) representing the distribution of the duration  |
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
					
Pool Counts
^^^^^^^^^^^

The pool is a collection of tcp connections to a single host.

pool_cached
  finagle's default connection pool is called a "watermark" pool because it has a lower-bound and
  upper-bound (high and low water marks). The WaterMarkPool keeps persistent connections up the
  lower-bound. It will keep making connections up to upper-bound if you checkout more than
  lower-bound connections, but when you release those connections above the lower-bound, it
  immediately tries to close them. This creates a lot of connection churn if you keep needing more
  than lower-bound connections. As a result, there is a separate facility for caching for a few
  seconds (with some TTL) those connections above the lower bound and not closing them and
  re-opening them after every request. It caches REGARDLESS of whether there are more than
  lower-bound open connections; it's always caching UP TO (upper-bound - lower-bound)
  connections. The cache reaches its peak value when you reach your peak concurrency (i.e., "load"),
  and then slowly decays, based on the TTL. (The default TTL is 5 seconds).

pool_size
  represents the number of connections open to the host, as seen by the connection pool. It should
  be between the min and max of your client builder spec.

  If you can see that "pool_waiters" is exactly zero, then there is no queueing waiting to get a
  connection from the pool. This means your pool is not under-sized under the current
  workload. That's a good thing.

pool_waiters
  the number of requests that are queued while waiting for a connection.
