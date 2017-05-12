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
results in a uniform way to construct clients, ``Protocol.newClient(...)`` or
``Protocol.newService(...)``. For example:

.. code-block:: scala

  import com.twitter.finagle.Service
  import com.twitter.finagle.Http
  import com.twitter.finagle.http.{Request, Response}

  val twitter: Service[Request, Response] = Http.client.newService("twitter.com")

Clients can further furnish the resulting `ServiceFactory` with protocol
specific API's. A common pattern is to expose a ``newRichClient`` method that
does exactly this. For cases like Thrift, where IDLs are part of
the rich API, a more specialized API is exposed. See the protocols section on
:ref:`Thrift <thrift_and_scrooge>` for more details.

Transport
---------

Finagle clients come with a variety of transport-level parameters that not only wire up TCP socket
options, but also upgrade the transport protocol to support encryption (e.g. TLS/SSL) and proxy
servers (e.g. HTTP, SOCKS5).

HTTP Proxy
~~~~~~~~~~

There is built-in support for `tunneling TCP-based protocols <http://www.web-cache.com/Writings/Internet-Drafts/draft-luotonen-web-proxy-tunneling-01.txt>`_
through web proxy servers in a default Finagle client that might be used with any TCP traffic, not
only HTTP(S). See `Squid documentation <http://wiki.squid-cache.org/Features/HTTPS>`_ on this feature.

The following example enables tunneling HTTP traffic through a web proxy server `my-proxy-server.com`
to `twitter.com`.

.. code-block:: scala

  import com.twitter.finagle.{Service, Http}
  import com.twitter.finagle.http.{Request, Response}
  import com.twitter.finagle.client.Transporter
  import java.net.SocketAddress

  val twitter: Service[Request, Response] = Http.client
    .configured(Transporter.HttpProxy(Some(new InetSocketAddress("my-proxy-server.com", 3128))))
    .withSessionQualifier.noFailFast
    .newService("twitter.com")


.. note::

  The web proxy server, represented as a static `SocketAddress`, acts as a single point of failure
  for a client. Whenever a proxy server is down, no traffic is served through the client no matter
  how big its replica set. This is why :ref:`Fail Fast <client_fail_fast>` is disabled on this
  client.

  There is better HTTP proxy support coming to Finagle along with the Netty 4 upgrade.

SOCKS5 Proxy
~~~~~~~~~~~~

SOCKS5 proxy support in Finagle is designed and implemented exclusively for testing/development
(assuming that SOCKS proxy is provided via `ssh -D`), not for production usage. For production
traffic, an HTTP proxy should be used instead.

Use the following CLI flags to enable SOCKS proxy on every Finagle client on a given JVM instance
(username and password are optional).

.. code-block:: shell

  -com.twitter.finagle.socks.socksProxyHost=localhost \
  -com.twitter.finagle.socks.socksProxyPort=50001 \
  -com.twitter.finagle.socks.socksUsername=$TheUsername \
  -com.twitter.finagle.socks.socksPassword=$ThePassword

.. _client_modules:

Client Modules
--------------

A default Finagle client is designed to maximize success and minimize latency.
Each request dispatched through a client will flow through various modules that
help it achieve these goals. The modules are logically separated into three
stacks: the `client stack` manages `name` resolution and balances requests
across multiple endpoints, the `endpoint stack` provides circuit breakers
and connection pooling, and the `connection stack` provides connection life-cycle
management and implements the wire protocol.

.. figure:: _static/clientstack.svg

    Fig. 1: A visual representation of each module in a default Finagle client
    that is configured with three endpoints and connections. Requests flow from
    left to right.


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

The `Observe`, `Monitor`, and `Trace` modules export useful information about the internals and
behavior of a Finagle client. Client metrics are exported using a
:util-stats-src:`StatsReceiver <com/twitter/finagle/stats/StatsReceiver.scala>`
(see the :ref:`metrics <public_stats>` section for more details about specific metric values).

Unhandled exceptions can be handled by installing a custom
:src:`MonitorFilter <com/twitter/finagle/filter/MonitorFilter.scala>` or overriding the default monitor
instance. The default setting for ``MonitorFilter`` is to log all the unhandled exception onto standard
output. To override this, use the following sample.

.. _configuring_monitors:

.. code-block:: scala

  import com.twitter.finagle.Service
  import com.twitter.finagle.Http
  import com.twitter.finagle.http.{Request, Response}
  import com.twitter.util.Monitor

  val monitor: Monitor = new Monitor {
    def handle(t: Throwable): Boolean = {
      // do something with the exception
      true
    }
  }

  val twitter: Service[Request, Response] = Http.client
    .withMonitor(monitor)
    .newService("twitter.com")

Finally, clients have built-in support for `Zipkin <http://zipkin.io/>`_.

.. _client_retries:

Retries
~~~~~~~

Every Finagle client contains a `Retries` module in the top (above load balancers) of its
stack so it can retry failures from the underlying modules: circuit breakers, timeouts,
load balancers and connection pools.

Failures that are known to be safe to retry (for example, exceptions that occurred before the
bytes were written to the wire and protocol level NACKs) will be automatically retried by Finagle.
These retries come out of a ``RetryBudget`` that allows for approximately 20% of the total requests
to be retried on top of 10 retries per second in order to accommodate clients that have just started
issuing requests or clients that have a low rate of requests per second.

Some failures may also be known as unsafe to retry. If a :src:`Failure <com/twitter/finagle/Failure.scala>`
is flagged ``NonRetryable``, the `Retries` module will not make any attempts to retry the request and
pass along the failure as is. A `NonRetryable` failure may be used in situations where a client
determines that a service is unhealthy and wishes to signal that the normal pattern of retries should
be skipped. Additionally, a service may reject a request that is malformed and thus pointless to retry.
While Finagle respects the `NonRetryable` flag internally, users should also take care to respect it
when creating retry filters of their own.

The `Retries` module is configured with two parameters:

1. ``RetryBudget`` - determines whether there is available budget to retry a request
2. ``Stream[Duration]`` - the backoff [#backoff]_ policy used to requeue the failed request

By default, the ``RetryBudget`` allows for about 20% of the total requests to be immediately (no backoff)
retried on top of 10 retries per second in order to accommodate clients that have just started issuing
requests or clients that have a low rate of requests per second.

To override this default use the following code snippet.

.. code-block:: scala

  import com.twitter.conversions.time._
  import com.twitter.finagle.Http
  import com.twitter.finagle.service.{Backoff, RetryBudget}

  val budget: RetryBudget = ???

  val twitter = Http.client
    .withRetryBudget(budget)
    .withRetryBackoff(Backoff.const(10.seconds))
    .newService("twitter.com")

The following example [#example]_ shows how to use a factory method ``RetryBudget.apply`` in order to
construct a new instance of ``RetryBudget`` backed by *leaky token bucket*.

.. code-block:: scala

  import com.twitter.conversions.time._
  import com.twitter.finagle.service.RetryBudget

  val budget: RetryBudget = RetryBudget(
    ttl = 10.seconds,
    minRetriesPerSec = 5
    percentCanRetry = 0.1
  )

The ``RetryBudget`` factory method takes three arguments:

1. `ttl` - a time to live for deposited tokens
2. `minRetriesPerSec` - the minimum rate of retries allowed
3. `percentCanRetry` - the percentage of requests that might be retried

While the :src:`RequeueFilter <com/twitter/finagle/service/RequeueFilter.scala>` (configured via ``Retries``)
is inserted into every client's stack by default so all the failures from the underlying modules will be
retried, the :src:`RetryFilter <com/twitter/finagle/service/RetryFilter.scala>` handling application level
exceptions from the remote server should be applied explicitly.

.. code-block:: scala

  import com.twitter.conversions.time._
  import com.twitter.finagle.Http
  import com.twitter.finagle.util.DefaultTimer
  import com.twitter.finagle.service.{RetryBudget, RetryFilter, RetryPolicy}
  import com.twitter.finagle.stats.NullStatsReceiver
  import com.twitter.util.Try

  val twitter = Http.client.newService("twitter.com")
  val budget: RetryBudget = RetryBudget()
  val policy: RetryPolicy[Try[Nothing]] = ???

  val retry = new RetryFilter(
    retryPolicy = policy,
    timer = DefaultTimer.twitter,
    statsReceiver = NullStatsReceiver,
    retryBudget = budget
  )

  val retryTwitter = retry.andThen(twitter)

The ``RetryPolicy`` instance might be constructed in several ways:

1. ``RetryPolicy.tries`` - retries using jittered backoff [#backoff]_ between the given number
   of maximum attempts
2. ``RetryPolicy.backoff`` - retries using the given backoff [#backoff]_ policy

The following example [#example]_ constructs an instance of ``RetryPolicy`` using the given backoff value.

.. code-block:: scala

  import com.twitter.finagle.http.{Response, Status}
  import com.twitter.finagle.service.{Backoff, RetryPolicy}
  import com.twitter.util.{Try, Return, Throw}
  import com.twitter.conversions.time._

  val policy: RetryPolicy[Try[Response]] =
    RetryPolicy.backoff(Backoff.equalJittered(10.milliseconds, 10.seconds)) {
      case Return(rep) if rep.status == Status.InternalServerError => true
    }

See :ref:`Retries metrics <retries>` for more details.

.. note::

  It's highly recommended to `share` a single instance of ``RetryBudget`` between both
  ``RetryFilter`` and ``RequeueFilter`` to prevent `retry storms`.

Timeouts & Expiration
~~~~~~~~~~~~~~~~~~~~~

Finagle provides timeout facilities with fine granularity:

The `Session Timeout` module defines a timeout for session acquisition. That is, it defines
the maximum time allotted to a request to wait for an available service/session. Requests
that exceed this timeout are failed with a ``ServiceTimeoutException``. This module is
implemented by the :src:`TimeoutFactory <com/twitter/finagle/factory/TimeoutFactory.scala>`

The default timeout value for the `Session Timeout` module is unbounded (i.e., ``Duration.Top``),
which simply means it's disabled. Although, it's possible to override the default setting with
stack params [#example]_.

.. code-block:: scala

  import com.twitter.conversions.time._
  import com.twitter.finagle.Http

  val twitter = Http.client
    .withSession.acquisitionTimeout(42.seconds)
    .newService("twitter.com")

See :ref:`Service Latency metrics <service_factory_failures>` for more details.

The `Request Timeout` module is a filter and thus gives an upper bound on the amount of
time allowed for a request to be outstanding. An important implementation detail of the
:src:`TimeoutFilter <com/twitter/finagle/service/TimeoutFilter.scala>` is that it attempts
to cancel the request when a timeout is triggered. With most protocols, if the request has
already been dispatched, the only way to cancel the request is to terminate the connection.

The default timeout for the `Request Timeout` module is unbounded (i.e., ``Duration.Top``).
Here is an example [#example]_ of how to override that default.

.. _configuring_timeouts:

.. code-block:: scala

  import com.twitter.conversions.time._
  import com.twitter.finagle.Http

  val twitter = Http.client
    .withRequestTimeout(42.seconds)
    .newService("twitter.com")

See :ref:`Request Latency metrics <metrics_stats_filter>` for more details.

.. note:: Requests timed out by the `Request Timeout` module are not retried by default
          given it's not known whether or not they were written to the wire.

The `Expiration` module is attached at the connection level and expires a service/session
after a certain amount of idle time. The module is implemented by
:src:`ExpiringService <com/twitter/finagle/service/ExpiringService.scala>`.

The default setting for the `Expiration` module is to never expire a session. Here is how
it can be configured [#example]_.

.. code-block:: scala

  import com.twitter.conversions.time._
  import com.twitter.finagle.Http

  val twitter = Http.client
    .withSession.maxLifeTime(20.seconds)
    .withSession.maxIdleTime(10.seconds)
    .newService("twitter.com")

The `Expiration` module takes two parameters:

1. `maxLifeTime` - the maximum duration for which a session is considered alive
2. `maxIdleTime` - the maximum duration for which a session is allowed to idle
   (not sending any requests)

See :ref:`Expiration metrics <idle_apoptosis_stats>` for more details.

Finally, timeouts can be enforced outside of these modules on a per-request level using
``Future#within`` or ``Future#raiseWithin``.  [#raise]_:

.. code-block:: scala

  import com.twitter.conversions.time._
  import com.twitter.finagle.http.{Request, Response}
  import com.twitter.util.Future

  val response: Future[Response] = twitter(request).within(1.second)

Request Draining
~~~~~~~~~~~~~~~~

The `Drain` module guarantees that the client delays closure until all
outstanding requests have been completed. It wraps each produced service with
a :src:`RefCountedService <com/twitter/finagle/service/RefcountedService.scala>`.

Load Balancing
~~~~~~~~~~~~~~

.. _load_balancer:

Finagle clients come equipped with a load balancer, a pivotal component in the client stack, whose
responsibility is to dynamically distribute load across a collection of interchangeable endpoints.
This gives Finagle an opportunity to maximize success and optimize request distribution in an attempt
to tighten the client's tail latencies. To achieve this in a non-cooperative distributed environment,
the balancer must pass accurate judgments about endpoints based only on its local view. An effective
feedback mechanism in such environments is latency; the balancers load metrics make use of this
either implicitly or explicitly.

Balancer implementations are split into two parts: A `load metric` and a `distributor`. Each node in the
balancer maintains the load metric and a distributor uses the data to select an endpoint.

The default setup for a Finagle client is to use P2C algorithm to distribute load across endpoints, while
picking the least loaded one. See :ref:`P2C + Least Loaded <p2c_least_loaded>`
for more details.

There are plenty of useful stats exported from the `Load Balancing` module.
See :ref:`Load Balancing metrics <loadbalancer_stats>` for more details.

Use the following code snippet to override the default load balancing strategy for a particular Finagle
client (see :src:`Balancers <com/twitter/finagle/loadbalancer/Balancers.scala>` on how to construct
instances of ``LoadBalancerFactory``).

.. code-block:: scala

  import com.twitter.finagle.Http
  import com.twitter.finagle.loadbalancer.LoadBalancerFactory

  val balancer: LoadBalancerFactory = ???
  val twitter = Http.client
    .withLoadBalancer(balancer)
    .newService("twitter.com:8081,twitter.com:8082")

In addition to the default configuration (i.e., ``Balancers.p2c``), the following setups are available.

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

Use ``Balancers.heap`` to construct an instance of ``LoadBalancerFactory``.

.. code-block:: scala

  import com.twitter.finagle.loadbalancer.{Balancers, LoadBalancerFactory}

  val balancer: LoadBalancerFactory = Balancers.heap()

.. note::

  In order to take an advantage of any of the load balancers supported in Finagle, a client
  should be configured to talk to a `replica set` (the default finagle stack doesn't do `sharding`
  and assumes all hosts in the set are interchangeable.)
  (see :ref:`Names and Naming in Finagle <finagle_names>` for more details) rather than a single
  endpoint.

.. _p2c_least_loaded:

Power of Two Choices (P2C) + Least Loaded
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The P2C distributor solves many of the limitations that are inherent with the Heap distributor and
is the default balancer for Finagle clients. By employing an elegant (and surprising) mathematical
phenomenon [#p2c]_, the algorithm randomly picks two nodes from the collection of endpoints and selects
the least loaded of the two. By repeatedly using this strategy, we can expect a manageable upper bound on
the maximum load of any server [#p2c_bounds]_. The default load metric for the P2C balancer is least
loaded, however, because P2C is fully concurrent [#p2c_jmh]_, it allows us to efficiently implement
weighted nodes [#weights_api]_ or different load metrics with minimal per-request costs.

Use ``Balancers.p2c`` to construct an instance of ``LoadBalancerFactory`` [#example]_.

.. code-block:: scala

  import com.twitter.finagle.loadbalancer.{Balancers, LoadBalancerFactory}

  val balancer: LoadBalancerFactory = Balancers.p2c(maxEffort = 100)

.. _max_effort:

The ``maxEffort`` param (default value is 5) from the example above is the maximum amount of "effort"
we're willing to expend on a load balancing decision without rebuilding its internal state. Simply
speaking this is the number of times a load balancer is able to retry because the previously picked
node was *marked unavailable* (i.e., an underlying circuit breaker is activated). If the ``maxEffort``
is exhausted and the *alive* node still hasn't been found, the load balancer will send a request to
the last picked one.

Power of Two Choices (P2C) + Peak EWMA [#experimental]_
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Backed by the P2C distributor, Peak EWMA uses a moving average over an endpoint's round-trip time (RTT)
that is highly sensitive to peaks. This average is then weighted by the number of outstanding requests,
effectively increasing our resolution per-request. It is designed to react to slow endpoints more quickly than
`least loaded` by penalizing them when they exhibit slow response times. This load metric operates under
the assumption that a loaded endpoint takes time to recover and so it is generally safe for the
advertised load to incorporate an endpoint's history. However, this assumption breaks down in the
presence of long polling clients.

Use ``Balancers.p2cPeakEwma`` to construct an instance of ``LoadBalancerFactory`` [#example]_.

.. code-block:: scala

  import com.twitter.conversions.time._
  import com.twitter.finagle.loadbalancer.{Balancers, LoadBalancerFactory}

  val balancer: LoadBalancerFactory =
    Balancers.p2cPeakEwma(maxEffort = 100, decayTime = 100.seconds)

The ``p2cPeakEwma`` factory method takes two arguments:

1. `maxEffort` (default: 5) - see :ref:`P2C's max effort <max_effort>`
2. `decayTime` (default: 10 seconds) - the window of latency observations

Aperture + Least Loaded [#experimental]_
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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

Use ``Balancers.aperture`` to construct an instance of ``LoadBalancerFactory`` [#example]_.

.. code-block:: scala

  import com.twitter.conversions.time._
  import com.twitter.finagle.loadbalancer.{Balancers, LoadBalancerFactory}

  val balancer: LoadBalancerFactory =
    Balancers.aperture(
      maxEffort = 10
      smoothWin = 32.seconds,
      lowLoad = 1.0,
      highLoad = 2.0,
      minAperture = 10
    )

The ``aperture`` factory method takes five arguments:

1. `maxEffort` (default: 5) - see :ref:`P2C's max effort <max_effort>`
2. `smoothWin` (default: 5 seconds) - the window of concurrent load observation
3. [`lowLoad`, `highLoad`] (default: [0.5, 2]) - the load band used to adjust an aperture size
   such that a concurrent load for each endpoint stays within the given interval
4. `minAperture` (default: 1) - the minimum size of the aperture

.. note::

  The Aperture load balancer should rarely be configured and we are working to provide broadly
  applicable defaults.

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

Behavior when no nodes are available
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
When there are no nodes in the `com.twitter.finagle.Status.Open` state, the balancers
must make a decision. One approach is to fail the request at this point. Instead,
Finagle makes an optimistic decision that its view of the nodes may be out-of-date
and picks a node it hopes has become available.

:ref:`Related stats <loadbalancer_stats>`

.. _client_circuit_breaking:

Circuit Breaking
~~~~~~~~~~~~~~~~

The following modules aim to preemptively disable sessions that will likely fail requests.
From the perspective of the load balancer, they act as circuit breakers which, when
triggered, temporarily suspend the use of a particular endpoint.

There are at least two modules in the client stacks that might be viewed as circuit breakers:

1. `Fail Fast` - a session-driven circuit breaker
2. `Failure Accrual` - a `request-driven circuit breaker <http://martinfowler.com/bliki/CircuitBreaker.html>`_

In addition to `Fail Fast` and `Failure Accrual`, some of the protocols (i.e., `Mux`) in
Finagle support `Ping-based Failure Detectors` [#failure_detectors]_
(i.e., :finagle-mux-src:`ThresholdFailureDetector <com/twitter/finagle/mux/ThresholdFailureDetector.scala>`).

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

.. _disabling_fail_fast:

The `Fail Fast` module is enabled by default for all of the Finagle clients except for
``Memcached.client`` one. The following example demonstrates how to explicitly disable it for a
particular client.

.. code-block:: scala

  import com.twitter.finagle.Http

  val twitter = Http.client
    .withSessionQualifier.noFailFast
    .newService("twitter.com")

.. note::

  It's important to disable `Fail Fast` when only have one host in the replica set because
  Finagle doesn't have any other path to choose.

:ref:`Related stats <fail_fast_stats>`

.. _client_failure_accrual:

Failure Accrual
^^^^^^^^^^^^^^^

The `Failure Accrual` module marks itself as unavailable based on the number of observed
failures. The module remains unavailable for a predefined duration. Recall
that the availability is propagated through the stack. Thus the load balancer
will avoid using an endpoint where the failure accrual module is unavailable.
The module is implemented by :src:`FailureAccrualFactory <com/twitter/finagle/liveness/FailureAccrualFactory.scala>`.

See :ref:`Failure Accrual Stats <failure_accrual_stats>` for stats exported from the
``Failure Accrual`` module.

The ``FailureAccrualFactory`` is configurable in terms of used policy to determine whether to mark
an endpoint dead upon a request failure. At this point, there are two setups available out of
the box.

1. A policy based on the requests success rate meaning (i.e, an endpoint marked dead if its success rate
   goes bellow the given threshold)
2. A policy based on the number of consecutive failures occurred in the endpoint (i.e., an endpoint marked
   dead if there are at least ``N`` consecutive failures occurred in this endpoint)

The default setup for the `Failure Accrual` module is to use a policy based on the
number of consecutive failures (default is 5) accompanied by equal jittered backoff [#backoff]_ producing
durations for which an endpoint is marked dead.

Use ``FailureAccrualFactory.Param`` [#experimental]_ to configure Failure Accrual` based on requests
success rate [#example]_.

.. code-block:: scala

  import com.twitter.conversions.time._
  import com.twitter.finagle.Http
  import com.twitter.finagle.liveness.{FailureAccrualFactory, FailureAccrualPolicy}
  import com.twitter.finagle.service.Backoff

  val twitter = Http.client
    .configured(FailureAccrualFactory.Param(() => FailureAccrualPolicy.successRate(
      requiredSuccessRate = 0.95,
      window = 100,
      markDeadFor = Backoff.const(10.seconds)
    )))
    .newService("twitter.com")

The ``successRate`` factory method takes three arguments:

1. `requiredSuccessRate` - the minimally required success rate bellow which an endpoint marked dead
2. `window` - the size of the window to tack success rate on
3. `markDeadFor` - the backoff policy (an instance of ``Stream[Duration]``) used to mark an endpoint
   dead for

To configure `Failure Accrual` based on a number of consecutive failures [#experimental]_, use the
following snippet [#example]_.

.. code-block:: scala

  import com.twitter.conversions.time._
  import com.twitter.finagle.Http
  import com.twitter.finagle.liveness.{FailureAccrualFactory, FailureAccrualPolicy}
  import com.twitter.finagle.service.Backoff

  val twitter = Http.client
    .configured(FailureAccrual.Param(() => FailureAccrualPolicy.consecutiveFailures(
      numFailures = 10,
      markDeadFor = Backoff.const(10.seconds)
    )))
    .newService("twitter.com")

The ``consecutiveFailures`` method takes two arguments:

1. `consecutiveFailures` - the number of failures after which an endpoint is marked dead
2. `markDeadFor` - the backoff policy (an instance of ``Stream[Duration]``) used to mark an endpoint
   dead for

.. note::

  It's highly recommended to use :src:`Backoff <com/twitter/finagle/service/Backoff.scala>`
  API for constructing instances of ``Stream[Duration]`` instead of using the error-prone Stream API directly.

Finally, it's possible to completely disable the `Failure Accrual` module for a given
client.

.. code-block:: scala

  import com.twitter.finagle.Http

  val twitter = Http.client
    .withSessionQualifier.noFailureAccrual
    .newService("twitter.com")

Pooling
~~~~~~~

Many protocols benefit from having persistent connections that are reused across requests.
Pooling is designed to balance connection churn and service acquisition latency at the cost of holding
resources open.

Depending on the configuration, a Finagle client's stack might contain up to _three_ connection pools
stacked on each other: watermark, caching and buffering pools.

The only Finagle protocol that doesn't require any connection pooling (a multiplexing protocol) is
`Mux` so it uses :src:`SingletonPool <com/twitter/finagle/pool/SingletonPool.scala>` that maintains
a single connection per endpoint. For every other Finagle-supported protocol (i.e., HTTP/1.1, Thrift),
there a connection pooling setup built with watermark and caching pools.

The default client stack layers caching and watermark pools which amounts to maintaining the low
watermark (i.e., ``0``, as long as request concurrency exists), queuing requests above the unbounded high
watermark (i.e., ``Int.MaxValue``), and applying an unbounded TTL (i.e., ``Duration.Top``) for services
that are between [low, high].

The override the default settings for connection pooling in a Finagle client, use the following
example [#example]_.

.. code-block:: scala

  import com.twitter.conversions.time._
  import com.twitter.finagle.Http

  val twitter = Http.client
    .withSessionPool.minSize(10)
    .withSessionPool.maxSize(20)
    .withSessionPool.maxWaiters(100)
    .newService("twitter.com")

Thus all the three pools are configured with a single param that takes the following arguments:

1. `minSize` and `maxSize` - low and high watermarks for the watermark pool (note that a Finagle
   client will not maintain more connections than `maxSize`)
2. `maxWaiters` - the maximum number of connection requests that are queued when the connection
   concurrency exceeds the high watermark

:ref:`Related stats <pool_stats>`

Buffering Pool
^^^^^^^^^^^^^^

The simplest connection pool implementation available in Finagle is
:src:`BufferingPool <com/twitter/finagle/pool/BufferingPool.scala>` that just buffers up to
``bufferSize`` connections and produces/closes new ones above it. This pool is disabled by
default.

.. _watermark_pool:

Watermark Pool
^^^^^^^^^^^^^^

Finagle provides a generic pool that maintains a collection of service instances. Each endpoint
the client connects to has an independent pool with high and low watermarks.
The :src:`WatermarkPool <com/twitter/finagle/pool/WatermarkPool.scala>` keeps persistent services
up to the lower bound. It will keep making new services up to upper bound if you checkout more
than lower bound services, but when you release those services above the lower bound, it
immediately tries to close them. This, however, creates a lot of connection churn if your
application consistently requires more than lower bound connections.

Caching Pool
^^^^^^^^^^^^

.. _caching_pool:

To reduce connection churn, there is a separate facility for caching, with some TTL,
services above the lower bound. The :src:`CachingPool <com/twitter/finagle/pool/CachingPool.scala>`
caches *regardless* of whether there are more than lower-bound open services;
it's always caching up to (upper-bound - lower-bound) services. The cache reaches
its peak value when you reach your peak concurrency (i.e. "load"),
and then slowly decays, based on the TTL.

:ref:`Related stats <pool_stats>`

Admission Control
-----------------

Clients are configured with the :src:`NackAdmissionFilter <com/twitter/finagle/filter/NackAdmissionFilter.scala>`
which will probabilistically drop some requests to unhealthy clusters. This aims
to decrease the request volume to those clusters with little to no effect on a
client's already unhealthy success rate. The filter works by keeping a moving
average of the fraction of requests that are :ref:`nacked <glossary_nack>`. When
this fraction hits a given threshold, the filter will probabilistically drop
requests in proportion to that fraction.
The filter can be configured with the following parameters:

1. ``window`` The duration over which the average is calculated. Default is 2
   minutes.
2. ``nackRateThreshold`` The rate of rejected requests at which the filter kicks
   in. Default is 0.5.

:ref:`Related stats <admission_control_stats>`

.. _response_classification:

.. include:: shared-modules/ResponseClassification.rst

MethodBuilder
-------------

.. warning:: These APIs are experimental and subject to change.

.. note:: Currently there is ``MethodBuilder`` support for HTTP and ThriftMux.
          We are waiting on user interest before expanding to more protocols.

``MethodBuilder`` is a collection of APIs for client configuration at a higher
level than the  :ref:`Finagle 6 APIs <finagle6apis>` while improving upon the deprecated
``ClientBuilder``. ``MethodBuilder`` provides:

- :ref:`Logical <mb_logical_req>` success rate metrics.
- Retries based on application-level requests and responses (e.g. an HTTP
  503 response code or a Thrift exception).
- Configuration of per-attempt and total timeouts.

:doc:`Learn more <MethodBuilder>` about ``MethodBuilder``.

.. rubric:: Footnotes

.. [#backoff] Most of the backoff strategies implemented in Finagle are inspired by Mark
   Brooker's `blog post <http://www.awsarchitectureblog.com/2015/03/backoff.html>`_.

.. [#experimental] This configuration was developed to target specific problems we encounter
   at Twitter and should be considered experimental. Note that its API may change as we continue
   to understand its place in the stack.

.. [#example] Configuration parameters/values provided in this example are only to demonstrate
   the API usage, not the real world values. We do not recommend blindly applying those values
   to production systems.

.. [#raise] The `Future#within` variant creates a new future that invokes raise on the future
   when the timeout occurs. The affects of which are dependent on the producer of the future.
   In most cases, Finagle will attempt to cancel the request if it hasn't already been dispatched.
   If it has been dispatched, the behavior is dependent on the protocol (without protocol
   support Finagle needs to tear down the session to signal cancellation).

.. [#p2c] Michael Mitzenmacher. 2001. The Power of Two Choices in Randomized Load Balancing.
   IEEE Trans. Parallel Distrib. Syst. 12, 10 (October 2001), 1094-1104.

.. [#p2c_bounds] The maximum load variance between any two servers is bound by `ln(ln(n))`
   where n is the number of servers in the cluster.

.. [#p2c_jmh] Our micro benchmark exposes the stark differences:

::

  HeapBalancer.getAndPut           1000  avgt  10 8686.479 ± 261.360 ns/op
  P2CBalancer.leastLoadedGetAndPut 1000  avgt  10 1692.388 ± 103.164 ns/op

.. [#weights_api] Weights are built into all the balancers except for the ``HeapBalancer``.
   The API exposed for this is in ``com.twitter.finagle.WeightedSocketAddress``. The name
   resolver that translates logical destinations to ``com.twitter.finagle.Addr`` can wrap
   concrete address with a `Double` which influences the balancer's distributor during the
   selection process.

.. [#probation] See ``com.twitter.finagle.loadbalancer.LoadBalancerFactory#EnableProbation``.

.. [#failure_detectors] See `Failure Detectors` section from
   Alvaro Videla's `blog post <http://videlalvaro.github.io/2015/12/learning-about-distributed-systems.html>`_.
