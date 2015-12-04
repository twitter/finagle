.. _finagle_clients:

Clients
=======

Finagle clients adheres to a simple :src:`interface <com/twitter/finagle/Client.scala>` for
construction:

.. code-block:: scala

  def newClient(dest: Name, label: String): ServiceFactory[Req, Rep]

That is, given a logical destination and an identifier, return a function
that produces a typed `Service` over which requests can be dispatched.
There are variants of this constructor for stateless clients that create a simple
`Service`, for example:

.. code-block:: scala

  def newService(dest: Name, label: String): Service[Req, Rep]

As of :doc:`6.x <changelog>`, client implementations are encouraged to expose
this interface on a Scala object named after the protocol implementation. This
results in a uniform way to construct clients, ``Protocol.newClient(...)``. For
example:

.. code-block:: scala

    Http.newClient(...)
    Memcached.newClient(...)

Clients can further furnish the resulting `ServiceFactory` with protocol
specific API's. A common pattern is to expose a ``newRichClient`` method that
does exactly this. For cases like Thrift, where IDLs are part of
the rich API, a more specialized API is exposed. See the protocols section on
:ref:`Thrift <thrift_and_scrooge>` for more details.

.. _client_modules:

Client Modules
--------------

A default Finagle client is designed to maximize success and minimize latency.
Each request dispatched through a client will flow through various modules that
help it achieve these goals. The modules are logically separated into three
stacks: the `client stack` manages `name` resolution and balances requests
across multiple endpoints, the `endpoint stack` provides session qualification
and connection pooling, and the `connection stack` provides connection life-cycle
management and implements the wire protocol.

.. figure:: _static/clientstack.svg

    Fig. 1: A visual representation of each module in a default Finagle client
    that is configured with three endpoints and connections.


Module Composition
~~~~~~~~~~~~~~~~~~

A materialized Finagle client is a :ref:`ServiceFactory <service_factory>`. It produces
:ref:`Services <services>` over which requests can be dispatched. The modules in
`Fig. 1` are defined in terms of a `ServiceFactory` and thus are composed via the usual
:ref:`combinators <composing_services_filters>`. An important consequence of this is that
modules deeper in the stack can affect the behavior and availability of the client. For example,
this is how failure management modules mark entire endpoints as unavailable.

Observability
~~~~~~~~~~~~~

The ``Observe``, ``Monitor``, and ``Trace`` modules export useful information about the internals and
behavior of a Finagle client. Client metrics are exported using a :util-stats-src:`StatsReceiver <com/twitter/finagle/stats/StatsReceiver.scala>`
(See the :ref:`metrics <public_stats>` section for more details). Generic exception handling can
be installed via the :src:`MonitorFilter <com/twitter/finagle/filter/MonitorFilter.scala>`.
Finally, clients have built-in support for `Zipkin <http://twitter.github.com/zipkin/>`_.

Timeouts & Expiration
~~~~~~~~~~~~~~~~~~~~~

Finagle provides timeout facilities with fine granularity:

The ``Service Timeout`` module defines a timeout for service acquisition. That is,
it defines the maximum time allotted to a request to wait for an available service. Requests
that exceed this timeout are failed with a `ServiceTimeoutException`. This module
is implemented by the :src:`TimeoutFactory <com/twitter/finagle/factory/TimeoutFactory.scala>`

The ``Request Timeout`` module is a filter and thus gives an upper bound on the amount of
time allowed for a request to be outstanding. An important implementation detail of the
:src:`TimeoutFilter <com/twitter/finagle/service/TimeoutFilter.scala>` is that it attempts
to cancel the request when a timeout is triggered. With most protocols, if the request has
already been dispatched, the only way to cancel the request is to terminate the connection.

The ``Expiration`` module is attached at the connection level and expires a service after a
certain amount of idle time. The module is implemented by
:src:`ExpiringService <com/twitter/finagle/service/ExpiringService.scala>`.

:ref:`Related stats <idle_apoptosis_stats>`

Finally, timeouts can be enforced outside of these modules on a per-request level using
`Future#within` [#raise]_:

.. code-block:: scala

  val f = client(request)
  f.within(1.seconds) onSuccess { ... } onFailure { ... }

.. [#raise] The `Future#raiseWithin` variant creates a new future
            that invokes raise on the future when the timeout occurs.
            The affects of which are dependent on the producer of the
            future. In most cases, Finagle will attempt to cancel the
            request if it hasn't already been dispatched. If it has been
            dispatched, the behavior is dependent on the protocol (without
            protocol support Finagle needs to tear down the session to signal
            cancellation).

Request Draining
~~~~~~~~~~~~~~~~

The ``Drain`` module guarantees that the client delays closure until all
outstanding requests have been completed. It wraps each produced service with
a :src:`RefCountedService <com/twitter/finagle/service/RefcountedService.scala>`.

Load Balancer
~~~~~~~~~~~~~

.. _load_balancer:

Finagle clients come equipped with a load balancer, a pivotal component in the client stack, whose
responsibility is to dynamically distribute load across a collection of interchangeable endpoints.
This gives Finagle an opportunity to maximize success and optimize request distribution in an attempt
to tighten the client's tail latencies. To achieve this in a non-cooperative distributed environment,
the balancer must pass accurate judgments about endpoints based only on its local view. An effective
feedback mechanism in such environments is latency; the balancers load metrics make use of this
either implicitly or explicitly.

Balancer implementations are split into two parts: A `load metric` and a `distributor`. Each node in the
balancer maintains the load metric and a distributor uses the data to select an endpoint. The following
distributor and load metric configurations are available:

Heap + Least Loaded
^^^^^^^^^^^^^^^^^^^
The distributor is a heap which is shared across requests. Each node in the heap maintains a count of
outstanding request. The count is incremented when a request is dispatched and decremented when we
receive a response (note the dependence on latency). The heap is min-ordered to allow for
efficient access to the least loaded. The distributor inherits all the nice properties of the heap
(i.e. selecting the top of the heap is constant time and other common operations take `O(log n)`).
This configuration has some limitations. In particular, it’s difficult to use weighted nodes or
swap out a load metric without sacrificing the performance of the heap. What’s more, the heap must be
updated atomically by each request and thus represents a highly contended resource.

Power of Two Choices (P2C) + Least Loaded
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The P2C distributor solves many of the limitations that are inherent with the Heap distributor and
is the default Balancer for Finagle clients. By employing an elegant (and surprising) mathematical
phenomenon [#p2c]_, the algorithm randomly picks two nodes from the collection of endpoints and selects
the least loaded of the two. By repeatedly using this strategy, we can expect a manageable upper bound on
the maximum load of any server [#p2c_bounds]_. The default load metric for the P2C balancer is least
loaded, however, because P2C is fully concurrent [#p2c_jmh]_, it allows us to efficiently implement
weighted nodes [#weights_api]_ or different load metrics with minimal per-request costs.

Experimental
^^^^^^^^^^^^
The following balancer configurations were developed to target specific problems we encounter at
Twitter. They are considered experimental, so they may change as we continue to understand their
place in the client stack.

P2C + Peak Ewma
^^^^^^^^^^^^^^^^^^^^
Backed by the P2C distributor, Peak EWMA uses a moving average over an endpoint's round-trip time (RTT)
that is highly sensitive to peaks. This average is then weighted by the number of outstanding requests,
effectively increasing our resolution per-request. It is designed to react to slow endpoints more quickly than
`least loaded` by penalizing them when they exhibit slow response times. This load metric operates under
the assumption that a loaded endpoint takes time to recover and so it is generally safe for the
advertised load to incorporate an endpoint's history. However, this assumption breaks down in the
presence of long polling clients.

Aperture + Least Loaded
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
All the previously mentioned configurations operate optimally under high load. That is, without
sufficient concurrent load, the previous distributors can degrade to random selection. The Aperture
distributor aims to remedy this among other things. By employing a simple feedback controller based
on the client's load, the distributor balances across a subset of servers to meet a specified
target load band. The principle of hysteresis is applied to the aperture to avoid rapid fluctuations
and dampen the effects of large load spikes.

The benefits of Aperture are promising:

1. A client uses resources commensurate to offered load. In particular,
   it does not have to open sessions with every service in a large cluster.
   This is especially important when offered load and cluster capacity
   are mismatched.
2. It balances over fewer, and thus warmer, services. This also means that
   clients pay the penalty of session establishment less frequently.
3. It increases the efficacy of least-loaded balancing which, in order to
   work well, requires concurrent load.

Role of Balancers in Resiliency
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The balancer's primary goal is to attempt to optimize request latency. Coincidentally, to do this
well, it also needs to properly qualify sessions. These two concerns are treated separately in the
client stack. Finagle has dedicated modules which track failures and control the `com.twitter.finagle.Status`
of an endpoint. The balancers selection process takes this status into account. However, without
protocol support the qualification happens in-band with requests (i.e. it requires failed requests).
We are exploring better (explicit) session qualification with protocol support (e.g. Mux).

Additionally, clients must be resilient to instabilities in the service discovery system.
Historically, Finagle has employed solutions like `com.twitter.finagle.addr.StabilizingAddr` and
`com.twitter.finagle.serverset2.Stabilizer` to validate changes to the balancers endpoint collection.
Since we have information about the availability of an endpoint in the balancer, it may represent
a viable intersection to validate such changes. Balancers have a "probation" capability built-in
behind a client parameter [#probation]_.

:ref:`Related stats <loadbalancer_stats>`

.. [#p2c]
   Michael Mitzenmacher. 2001. The Power of Two Choices in
   Randomized Load Balancing. IEEE Trans. Parallel Distrib. Syst. 12, 10 (October 2001), 1094-1104.

.. [#p2c_bounds]
   The maximum load on any server is roughly bound by `ln(ln(n))` where n is the number
   of requests.

.. [#p2c_jmh]
   Our microbenchmark exposes the stark differences:
   ::

      HeapBalancerBench.getAndPut                  1000  avgt   10  8686.479 ± 261.360  ns/op
      P2CBalancerBench.leastLoadedGetAndPut        1000  avgt   10  1692.388 ± 103.164  ns/op

.. [#weights_api]
   Weights are built into all the balancers except for the HeapBalancer. The API exposed
   for this is in `com.twitter.finagle.WeightedSocketAddress`. The name resolver that translates
   logical destinations to `com.twitter.finagle.Addr`s can wrap concrete address with a `Double`
   which influences the balancer's distributor during the selection process.

.. [#probation]
   See `com.twitter.finagle.loadbalancer.LoadBalancerFactory#EnableProbation`

Session Qualification
~~~~~~~~~~~~~~~~~~~~~

The following modules aim to preemptively disable sessions that will likely fail requests.
From the perspective of the load balancer, they act as circuit breakers which, when
triggered, temporarily suspend the use of a particular endpoint.

Failure Accrual
^^^^^^^^^^^^^^^

The ``Failure Accrual`` module marks itself as unavailable based on the number of observed
failures. The module remains unavailable for a predefined duration. Recall
that the availability is propagated through the stack. Thus the load balancer
will avoid using an endpoint where the failure accrual module is unavailable.
The module is implemented by :src:`FailureAccrualFactory <com/twitter/finagle/service/FailureAccrualFactory.scala>`.

.. _client_fail_fast:

Fail Fast
^^^^^^^^^

The :src:`FailFast <com/twitter/finagle/service/FailFastFactory.scala>` module
attempts to reduce the number of requests dispatched to endpoints that are likely
to fail. It works by marking downed hosts when a connection fails, and launching a
background process that repeatedly attempts to reconnect with a given backoff schedule.
During the time that a host is marked down, the factory is marked unavailable (and thus
the load balancer above it will avoid its use). The factory becomes available
again on success or when the back-off schedule runs out.

See the FAQ to :ref:`better understand <faq_failedfastexception>` why clients
might be seeing ``com.twitter.finagle.FailedFastException``'s.

:ref:`Related stats <fail_fast_stats>`

Pooling
~~~~~~~

.. _watermark_pool:

Watermark Pool
^^^^^^^^^^^^^^

Finagle provides a generic pool that maintains a collection of
service instances. Each endpoint the client connects to has an independent
pool with high and low watermarks. The :src:`WatermarkPool <com/twitter/finagle/pool/WatermarkPool.scala>` keeps
persistent services up to the lower bound. It will keep making new services up
to upper bound if you checkout more than lower bound services, but when
you release those services above the lower bound, it immediately tries
to close them. This, however, creates a lot of connection churn if your
application consistently requires more than lower bound connections.

.. _caching_pool:

Caching Pool
^^^^^^^^^^^^

To reduce connection churn, there is a separate facility for caching, with some TTL,
services above the lower bound. The :src:`CachingPool <com/twitter/finagle/pool/CachingPool.scala>`
caches *regardless* of whether there are more than lower-bound open services;
it's always caching up to (upper-bound - lower-bound) services. The cache reaches
its peak value when you reach your peak concurrency (i.e. "load"),
and then slowly decays, based on the TTL.

The default client stack layers both pools which amounts to
maintaining the low watermark (as long as request concurrency exists),
queuing requests above the high watermark, and applying a TTL for
services that are between [low, high].

:ref:`Related stats <pool_stats>`

Retries
~~~~~~~

Finagle provides a configurable :src:`RetryExceptionsFilter <com/twitter/finagle/service/RetryingFilter.scala>`.
The filter can be configured either to retry a specific number of times or to adhere to a back-off strategy.
By default, the RetryingFilter *does not assume your RPC service is idempotent*. Retries occur only when they
are known to be safe. That is, when Finagle can guarantee the request was never delivered to the
server.

There is no direct protocol or annotation support for marking endpoints as idempotent.
A common workaround is to create separate client instances for issuing non-idempotent requests.
For example, one could keep separate client objects for reads and writes, the former configured to retry on
any request failure and the latter being more conservative in order to avoid conflicting writes.

Configuration
-------------

Prior to :doc:`6.x <changelog>`, the `ClientBuilder` was the primary method for configuring
the modules inside a Finagle client. We've moved away from this model for various
:ref:`reasons <configuring_finagle6>`.
