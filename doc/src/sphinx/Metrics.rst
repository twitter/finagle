Metrics
=======

This section aims to be a comprehensive list of all of the metrics that Finagle
exposes. The metrics are organized by layer and then by class.

Some of the stats are only for clients, some only for servers, and some are for both.
Some stats are only visible when certain optional classes are used.

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
:src:`FailureAccrualFactory <com/twitter/finagle/service/FailureAccrualFactory.scala>`
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
  a counter of the number of requests that have been rejected by this filter.

Retries
-------

.. _retries:

.. include:: metrics/Retries.rst

Dispatching
-----------

.. _dispatching:

Metrics scoped under `dispatcher` represent information about a client's dispatching
layer.

Depending on the underlying protocol, dispatchers may have different request
queueing rules.

**serial/queue_size**
  a gauge used by serial dispatchers that can only have a single request
  per connection at a time that represents the number of pending requests.

**pipelining/pending**
  a gauge used by pipelining dispatchers that represents how many
  pipelined requests are currently outstanding.

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

Toggles
-------

These metrics correspond to :ref:`feature toggles <toggles>`.

**toggles/<libraryName>/checksum**
  A gauge summarizing the current state of a `ToggleMap` which may be useful
  for comparing state across a cluster or over time.

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

These metrics are added by
:src:`StatsFilter <com/twitter/finagle/http/filter/StatsFilter.scala>` and can be enabled by
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

ThriftMux
---------

.. _thriftmux_stats:

These stats pertain to the :ref:`ThriftMux <whats_thriftmux>` protocol.

.. include:: metrics/ThriftMux.rst
