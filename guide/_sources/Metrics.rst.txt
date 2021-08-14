Metrics
=======

This section aims to be a comprehensive list of all of the metrics that Finagle
exposes. The metrics are organized by layer and then by class.

Finagle leverages `verbosity levels`_ and defines some of its low-utility metrics as "debug".
Unless explicitly stated, assume ``Verbosity.Default`` is used to define a given metric.

.. NOTE::

   Some of the metrics are only for clients, some only for servers, and some are for both.

   Some metrics are only visible when certain optional classes are used.

NB: Finagle sometimes uses ``RollupStatsReceivers`` internally, which will take
stats like "failures/twitter/TimeoutException" and roll them up, aggregating
into "failures/twitter" and also "failures". For example, if there are 3
"failures/twitter/TimeoutException" counted, and 4
"failures/twitter/ConnectTimeoutException", then it will count 7 for
"failures/twitter".

Public
------

.. _public_stats:

These stats come from the public interface, and are the ones that you should look at first
to figure out whether a client is abusing you, or you are misusing a downstream service.
They are also useful in diagnosing what contributes to request latency.

.. include:: metrics/Public.rst

Construction
------------

.. _construction_stats:

These stats are about setting up services in Finagle, and expose whether you are
having trouble making services.

.. include:: metrics/Construction.rst

Finagle
-------

.. _finagle_stats:

These metrics track various Finagle internals.

.. include:: metrics/Finagle.rst

Load Balancing
--------------

.. _loadbalancer_stats:

The client stats under the `loadbalancer` scope expose the innards of
what's going on with load balancing, and the management of equivalent
groups of hosts.

.. include:: metrics/LoadBalancing.rst

Fail Fast
----------

.. _fail_fast_stats:

The client stats under the `failfast` scope give insight into how
Finagle handles services where it can't establish a connection.

.. include:: metrics/FailFast.rst

Failure Accrual
---------------

.. _failure_accrual_stats:

The client stats under the `failure_accrual` scope track how
:src:`FailureAccrualFactory <com/twitter/finagle/liveness/FailureAccrualFactory.scala>`
manages failures.

.. include:: metrics/FailureAccrual.rst

Idle Apoptosis
--------------

.. _idle_apoptosis_stats:

These client stats keep track of how frequently
:doc:`Services <ServicesAndFilters>` are closed due to prolonged idleness.

.. include:: metrics/IdleApoptosis.rst

Rate Limiting
-------------

.. _rate_limiting_stats:

These client stats show how much you're hitting your rate limit if you're using rate limiting.

.. include:: metrics/RateLimiting.rst

Pooling
-------

.. _pool_stats:

These client stats help you keep track of connection churn.

.. include:: metrics/Pooling.rst

PendingRequestFilter
--------------------

.. _pending_request_filter:

These stats represent information about the behavior of PendingRequestFilter.

**pending_requests/rejected**
  A counter of the number of requests that have been rejected by this filter.

Retries
-------

.. _retries:

.. include:: metrics/Retries.rst

Backup Requests
---------------

.. _backup_requests:

These stats provide information on the state and behavior of
`com.twitter.finagle.client.BackupRequestFilter`.

**backups/send_backup_after_ms**
  A histogram of the time, in  milliseconds, after which a request will be re-issued if it has
  not yet completed.

**backups/backups_sent**
  A counter of the number of backup requests sent.

**backups/backups_won**
  A counter of the number of backup requests that completed successfully before the original
  request.

**backups/budget_exhausted**
  A counter of the number of times the backup request budget (computed using the current value
  of the `maxExtraLoad` param) or client retry budget was exhausted, preventing a backup from being
  sent.

Dispatching
-----------

.. _dispatching:

The client metrics scoped under `dispatcher` represent information about a
client's dispatching layer.

Depending on the underlying protocol, dispatchers may have different request
queueing rules.

**serial/queue_size**
  A gauge used by serial dispatchers that can only have a single request
  per connection at a time that represents the number of pending requests.


Server Thread Usage
-------------------

.. _threadusage:

Metrics scoped under "thread_usage/requests" can be used as a signal for
seeing if your connections or threads are imbalanced on a server.

There are caveats which can make these metrics unreliable or not applicable:

- Work is done on a ``FuturePool`` instead of the server's thread.
- The amount of work done per request is highly inconsistent.
- Low number of requests.

**relative_stddev**
  A gauge of the relative standard deviation, or coefficient of variation, for
  the number of requests handled by each thread. Put another way, the closer this
  is to 0.0 the less variance there is in the number of requests handled per thread.

  If this value is large, before taking action, you may want to first verify the metric
  by looking at the node's thread utilization. Examples include `mpstat -P ALL`
  and `top -p $pid -H`. You may also want to look at the debug metrics for
  "mean" and "stdev" to help quantify the amount of imbalance. One solution to
  mitigate imbalance is to move work to a ``FuturePool``.

**mean** `verbosity:debug`
  A gauge of the arithemetic mean, or average, of the number of requests handled
  by each thread.

**stddev** `verbosity:debug`
  A gauge of the standard of deviation of the number of requests handled
  by each thread.

**per_thread/<thread_name>** `verbosity:debug`
 A counter which indicates the number of requests that a specific thread has received.

Admission Control
-----------------

.. _admission_control_stats:

The stats under the `admission_control` scope show stats for the different admission control
strategies.

.. include:: metrics/AdmissionControl.rst

Threshold Failure Detector
--------------------------

.. _failure_detector:

The client metrics under the `mux/failuredetector` scope track the behavior of
out-of-band RTT-based failure detection. They only apply to the mux
protocol.

.. include:: metrics/FailureDetector.rst

Method Builder
--------------

Client metrics that are created when using :ref:`MethodBuilder <methodbuilder>`.

.. include:: metrics/MethodBuilder.rst

Transport
---------

.. _transport_stats:

These metrics pertain to where the Finagle abstraction ends and the bytes are sent over the wire.
Understanding these stats often requires deep knowledge of the protocol, or individual transport
(e.g. Netty) internals.

.. include:: metrics/Transport.rst


Service Discovery
-----------------

.. _service_discovery:

These metrics track the state of name resolution and service discovery.

.. include:: metrics/ServiceDiscovery.rst

JVM
---

.. _jvm_metrics:

.. include:: metrics/Jvm.rst

Toggles
-------

These metrics correspond to :ref:`feature toggles <toggles>`.

**toggles/<libraryName>/checksum**
  A gauge summarizing the current state of a `ToggleMap` which may be useful
  for comparing state across a cluster or over time.

Streaming
---------

.. _streaming_metrics:

.. include:: metrics/Streaming.rst

HTTP
----
.. _http_stats:

These stats pertain to the HTTP protocol.

**nacks**
  A counter of the number of retryable HTTP 503 responses the Http server returns. Those
  responses are automatically retried by Finagle HTTP client.

**nonretryable_nacks**
  A counter of the number of non-retryable HTTP 503 responses the HTTP server returns. Those
  responses are not automatically retried.

**Deprecated: stream/failures/<exception_name>**
  The replacement is `stream/request/failures/<exception_name>` and `stream/response/failures/<exception_name>`.
  A counter of the number of times a specific exception has been thrown in the middle of a stream.

**Deprecated: stream/failures**
  The replacement is `stream/request/failures` and `stream/response/failures`.
  A counter of the number of times any failure has been observed in the middle of a stream.

**http/cookie/samesite_failures** `verbosity:debug`
  A counter of the number of failed attempts to decode the SameSite Cookie attribute.

**rejected_invalid_header_names**
  A counter of the number of rejected requests by a server due to an invalid (as seen by RFC-7230)
  header name.

**rejected_invalid_header_values**
  A counter of the number of rejected requests by a server due to an invalid (as seen by RFC-7230)
  header value.

These metrics are added by
:finagle-http-src:`StatsFilter <com/twitter/finagle/http/filter/StatsFilter.scala>` and can be enabled by
using `.withHttpStats` on `Http.Client` and `Http.Server`.

**status/<statusCode>**
  A counter of the number of responses received, or returned for servers, that had this
  statusCode.

**status/<statusClass>**
  Same as **status/statusCode** but aggregated per category, e.g. all 500 range responses
  count as 5XX for this counter.

**time/<statusCode>**
  A histogram on duration in milliseconds per HTTP status code.

**time/<statusCategory>**
  A histogram on duration in milliseconds per HTTP status code category.

HTTP2
-----
These stats pertain to HTTP2 only.

.. include:: metrics/Http2.rst

Memcached
---------

.. _memcached_stats:

These stats pertain to the Memcached protocol.

.. include:: metrics/Memcached.rst

Mux
---

.. _mux_stats:

These stats pertain to the :ref:`Mux <mux>` protocol.

.. include:: metrics/Mux.rst

Mysql
-----

.. _mysql_stats

These stats pertain to the finagle-mysql implementation.

.. include:: metrics/Mysql.rst

ThriftMux
---------

.. _thriftmux_stats:

These stats pertain to the :ref:`ThriftMux <whats_thriftmux>` protocol.

.. include:: metrics/ThriftMux.rst

.. _verbosity levels: https://twitter.github.io/util/guide/util-stats/basics.html#verbosity-levels


PerEndpoint StatsFilter
-----------------------
.. include:: metrics/PerEndpoint.rst

Partitioning
------------

See :doc:`here <PartitionAwareClient>` for how to enable ThriftMux Partition Aware Client.

.. include:: metrics/Partitioning.rst
