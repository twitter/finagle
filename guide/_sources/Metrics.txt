Metrics
=======

This section aims to be a comprehensive list of all of the stats that finagle
exposes.  The stats are organized by layer and then by class.

Some of the stats are only for clients, some only for servers, and some are for both.
Some stats are only visible when certain optional classes are used.

NB: Finagle uses RollupStatsReceiver internally, which will take stats like
"failures/twitter/TimeoutException" and roll them up, aggregating into "failures/twitter"
and also "failures".  For example, if there are 3 "failures/twitter/TimeoutException" counted,
and 4 "failures/twitter/ConnectTimeoutException", then it will cound 7 "failures/twitter".

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

These stats are about setting up services in finagle, and expose whether you are
having trouble making services.

.. include:: metrics/Construction.rst

Finagle
-------

.. _finagle_stats:

These metrics track various Finagle internals.

.. include:: metrics/Finagle.rst

Service Discovery
-----------------

.. _service_discovery:

These metrics track the state of name resolution and service discovery.

.. include:: metrics/ServiceDiscovery.rst

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
finagle handles services where it can't make a connection.

.. include:: metrics/FailFast.rst

Failure Accrual
---------------

.. _failure_accrual_stats:

The client stats under the `failure_accrual` scope track how `FailureAccrualFactory`
manages failures.

.. include: metrics/FailureAccrual.rst

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

Retries
-------

.. _retries:

.. include:: metrics/Retries.rst

Transport
---------

.. _transport_stats:

These metrics pertain to where the finagle abstraction ends and the bytes are sent over the wire.
Understanding these stats often requires deep knowledge of the protocol, or individual transport
(e.g. Netty) internals.

.. include:: metrics/Transport.rst

Mux
---

.. _mux_stats:

These stats pertain to :ref:`Mux <mux>`.

.. include:: metrics/Mux.rst

Threshold Failure Detector
--------------------------

.. _failure_detector:

The client metrics under the `mux/failuredetector` scope track the behavior of out-of-band ping
based failure detection. They only apply to the mux protocol.

.. include:: metrics/FailureDetector.rst