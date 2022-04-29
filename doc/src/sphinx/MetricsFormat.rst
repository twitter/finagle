.. _format:

Metrics Format
==============
This guideline is for the `finagle-stats` implementation of metrics only. The
format of metrics that Finagle exports may differ for other metrics implementations.

.. tip::

   See `this list <Metrics.html>`__ for a comprehensive view of all the metrics from finagle-core.

Verbosity
---------
Finagle leverages `verbosity levels`_ and defines some of its low-utility metrics as "debug".
Unless explicitly stated, assume ``Verbosity.Default`` is used to define a given metric.

Scope Separator
---------------
The default separator for each scope of the metric is `/`. For example,
"clnt/foo/requests". Change the scope separator with the flag
`com.twitter.finagle.stats.scopeSeparator`.

Types of Metrics
----------------
Finagle exports three different types of metrics: counters, gauges, and histograms. 


Counters
<<<<<<<<
Counters are **unlatched** by default, which means that the absolute value of the
counter is exported. The counter does not reset to zero after sampling. To
calculate how much the counter incremented per second, use a metric collection
system that samples every second and subtract the value of the counter between
two consecutive seconds.

Latched counters can be enabled with the flag `com.twitter.finagle.stats.useCounterDeltas`.
The delta for the counter is exported instead of the absolute value.

.. code-block:: javascript

    "requests": 10

Gauges
<<<<<<
The value of a gauge is a float.

.. code-block:: javascript

    "pending": 200.0

Histogram
<<<<<<<<<
For `finagle-stats`, there is a trailing 0 in .p9990.

.. code-block:: javascript

    "request_latency_ms.avg": 4.9,
    "request_latency_ms.count": 83,
    "request_latency_ms.max": 14,
    "request_latency_ms.min": 2,
    "request_latency_ms.p50": 5,
    "request_latency_ms.p90": 7,
    "request_latency_ms.p95": 8,
    "request_latency_ms.p99": 9,
    "request_latency_ms.p9990" : 14,
    "request_latency_ms.p9999" : 14,
    "request_latency_ms.sum" : 409

If the histogram is empty, only the count is exported.

.. code-block:: javascript

    "request_latency_ms.count" : 0


.. _verbosity levels: https://twitter.github.io/util/guide/util-stats/basics.html#verbosity-levels

RollupStatsReceivers
--------------------
Finagle sometimes uses ``RollupStatsReceivers`` internally, which will take
stats like "failures/twitter/TimeoutException" and roll them up, aggregating
into "failures/twitter" and also "failures". For example, if there are 3
"failures/twitter/TimeoutException" counted, and 4
"failures/twitter/ConnectTimeoutException", then it will count 7 for
"failures/twitter".
