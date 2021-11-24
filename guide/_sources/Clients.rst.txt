.. _finagle_clients:

Clients
=======

Finagle clients adheres to a simple :src:`interface <com/twitter/finagle/Client.scala>` for
construction. The following API supports the creation of both stateful and stateless clients.
You want a stateful client if you require requests and response sequences over the same connection.
The following illustrates the difference.

Constructor for a **stateless** client:

.. code-block:: scala

  def newService(dest: Name, label: String): Service[Req, Rep]

That is, given a logical destination and an identifier, return a function that produces
a typed `Service` over which requests can be dispatched. Dispatched requests will be
load balanced across all of the resolved hosts utilizing Finagle's configured load balancer.

An alternative constructor for a **stateful** client producing a typed `ServiceFactory`:

.. code-block:: scala

  def newClient(dest: Name, label: String): ServiceFactory[Req, Rep]

Each `Service` acquired from the returned `ServiceFactory` represents a distinct session. Requests
dispatched on this `Service` will reuse the established connection to the picked host. Load balancing
occurs only per-session, `ServiceFactory.apply`, while per-request on `newService`.  Depending on
the configured `Pooling`_ strategy, `Service.close` typically returns its connection to
the pool and does not cut the connection. As a result, it is important to close sessions after use
to avoid resource leaks. For example:

.. code-block:: scala

  import com.twitter.finagle.{Service, ServiceFactory}
  import com.twitter.finagle.Http
  import com.twitter.finagle.http.{Request, Response}
  import com.twitter.util.Future

  val sessionFactory: ServiceFactory[Request, Response] = Http.client.newClient("example.com:80")

  // we establish a session, represented by `svc`
  sessionFactory().onSuccess { svc: Service[Request, Response] =>

    // both requests will land on the same host
    val rep1: Future[Response] = svc(Request("/some/path"))
    val rep2: Future[Response] = svc(Request("/some/other/path"))

    // clean up the session so the connection is released into the pool
    svc.close()
  }

  // session establishment is load balanced. No guarantee as to which endpoint is selected by the load balancer
  sessionFactory().onSuccess { ... }


Client Protocol Implementation
------------------------------

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


Transport Security
------------------

Finagle has robust support for TLS. The most common options such as server validation are accessible
directly via the `tls` members of :src:`ClientTransportParams <com/twitter/finagle/param/ClientTransportParams.scala>`
as follows:


.. code-block:: scala

  import com.twitter.finagle.{Service, Http}
  import com.twitter.finagle.http.{Request, Response}

  val twitter: Service[Request, Response] = Http.client
    .withTransport.tls("twitter.com")
    .newService("twitter.com:443")

There are further configuration options including client authentication accessible via :src:`ClientTransportParams <com/twitter/finagle/param/ClientTransportParams.scala>`.


Finagle also supports `SPNEGO <https://en.wikipedia.org/wiki/SPNEGO>`_ which is an HTTP specific
extension for negotiating security schemes. A common use case for SPNEGO is for authentication in `Kerberos <https://en.wikipedia.org/wiki/Kerberos_(protocol)>`_
secured environments.


.. code-block:: scala

  import com.twitter.finagle.{Service, Http}
  import com.twitter.finagle.http.{Request, Response}
  import com.twitter.finagle.http.SpnegoAuthenticator.ClientFilter
  import com.twitter.finagle.http.SpnegoAuthenticator.Credentials.{ClientSource, JAASClientSource}

  val jaas: ClientSource = new JAASClientSource(
    loginContext = "com.sun.security.jgss.krb5.initiate",
    _serverPrincipal = "HTTP/SOME_HOST@SOME_DOMAIN"
  )

  val client: Service[Request, Response] =
    new ClientFilter(jaas).andThen(Http.client.newService("host:port"))


HTTP Proxy
~~~~~~~~~~

There is built-in support for `tunneling TCP-based protocols <https://tools.ietf.org/html/draft-luotonen-web-proxy-tunneling-01>`_
through web proxy servers in a default Finagle client that might be used with any TCP traffic, not
only HTTP(S). See `Squid documentation <https://wiki.squid-cache.org/Features/HTTPS>`_ on this feature.

The following example enables tunneling HTTP traffic through a web proxy server `my-proxy-server.com`
to `twitter.com`.

.. code-block:: scala

  import com.twitter.finagle.{Service, Http}
  import com.twitter.finagle.http.{Request, Response}
  import com.twitter.finagle.client.Transporter
  import java.net.SocketAddress

  val twitter: Service[Request, Response] = Http.client
    .withTransport.httpProxyTo(
      host = "twitter.com:443",
      credentials = Transporter.Credentials("user", "password")
    )
    .newService("inet!my-proxy-server.com:3128") // using local DNS to resolve proxy

While this setup may look somewhat counter intuitive with regards to where the ultimate destination
and the proxy server address are applied, it enables a variety of resiliency features by utilizing
Finagle's naming and load balancing subsystems. Given a web proxy server address/name falls under
a standard name resolution process, it might be (and should be) backed by a replica set (multiple
hosts) to get the greatest out of a client.

.. note::

  There is also a legacy support to web proxy servers available in Finagle via the
  `Transporter.HttpProxy` stack param. In that case, proxy server is forced to represented as a single
  `SocketAddress`, which not only introduces a single point of failure within a client (i.e., a
  client goes offline if a web proxy server is down), but also disables Finagle's resiliency features
  such as failure detection and load balancing.

SOCKS5 Proxy
~~~~~~~~~~~~

SOCKS5 proxy support in Finagle is designed and implemented exclusively for testing/development
(assuming that SOCKS proxy is provided via `ssh -D`), not for production usage. For production
traffic, an HTTP(S) proxy should be used instead.

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

.. raw:: html
    :file: _static/clientstack.svg


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

Finally, clients have built-in support for `Tracing <Tracing.html>`_.

.. _client_retries:

Retries
~~~~~~~

Every Finagle client contains a `Retries` module in the stack, above load balancers,
so that it can retry failures from the underlying modules: circuit breakers, timeouts,
load balancers and connection pools. Retries can help improve the client's logical success
rate when subsequent attempts succeed.

For the most part, service owners will be interested in the logical success rate of a clients.
Logical requests represent the result of the initial request, after any retries have occurred.
Concretely, should a request result in a retryable failure on the first attempt, but succeed upon
retry, this is considered a single successful logical request. By default, a Finagle client's
success rate metrics include the individual attempts and this can cause confusion.
:ref:`MethodBuilder <methodbuilder>` offers logical metrics scoped to "logical" for both
success rate and latency. The deprecated ``ClientBuilder`` code also offers similar metrics
scoped to "tries".

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

  import com.twitter.conversions.DurationOps._
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

  import com.twitter.conversions.DurationOps._
  import com.twitter.finagle.service.RetryBudget

  val budget: RetryBudget = RetryBudget(
    ttl = 10.seconds,
    minRetriesPerSec = 5,
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

  import com.twitter.conversions.DurationOps._
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
    timer = DefaultTimer,
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
  import com.twitter.conversions.DurationOps._

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

  import com.twitter.conversions.DurationOps._
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
Note that HTTP/2 and Mux both have first-class support for request cancellation without
needing to tear down the connection.

The default timeout for the `Request Timeout` module is unbounded (i.e., ``Duration.Top``).
Here is an example [#example]_ of how to override that default.

.. _configuring_timeouts:

.. code-block:: scala

  import com.twitter.conversions.DurationOps._
  import com.twitter.finagle.Http

  val twitter = Http.client
    .withRequestTimeout(42.seconds)
    .newService("twitter.com")

Regarding :ref:`retries <client_retries>`, the timeout is given to each attempt.

As Finagle does not know whether or not a request is idempotent, request timeouts
are not retried by default. However this can be configured through a
:ref:`retry policy <client_retries>`.

See :ref:`Request Latency metrics <metrics_stats_filter>` for more details.

.. note:: This module only works with request/response usage and does not
          support streaming (such as with HTTP).

The `Expiration` module is attached at the connection level and expires a service/session
after a certain amount of time. The module is implemented by
:src:`ExpiringService <com/twitter/finagle/service/ExpiringService.scala>`.

The default setting for the `Expiration` module is to never expire a session. Here is how
it can be configured [#example]_.

.. code-block:: scala

  import com.twitter.conversions.DurationOps._
  import com.twitter.finagle.Http

  val twitter = Http.client
    .withSession.maxLifeTime(20.seconds)
    .newService("twitter.com")

The `Expiration` module for clients takes one parameter: `maxLifeTime` - the maximum duration for
which a session is considered alive.

See :ref:`Expiration metrics <idle_apoptosis_stats>` for more details.

Finally, timeouts can be enforced outside of these modules on a per-request level using
``Future#within`` or ``Future#raiseWithin``.  [#raise]_:

.. code-block:: scala

  import com.twitter.conversions.DurationOps._
  import com.twitter.finagle.http.{Request, Response}
  import com.twitter.util.Future

  val response: Future[Response] = twitter(request).within(1.second)

Request Draining
~~~~~~~~~~~~~~~~

The `RequestDraining` module guarantees that the client delays closure until all
outstanding requests have been completed. It wraps each produced `Service` with
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

  val balancer: LoadBalancerFactory = Balancers.p2c()

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

  import com.twitter.conversions.DurationOps._
  import com.twitter.finagle.loadbalancer.{Balancers, LoadBalancerFactory}

  val balancer: LoadBalancerFactory =
    Balancers.p2cPeakEwma(decayTime = 100.seconds)

The ``p2cPeakEwma`` factory method takes two arguments:

1. `decayTime` (default: 10 seconds) - the window of latency observations

Aperture + Least Loaded [#experimental]_
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::

  The aperture load balancers can be more challenging to reason about than the P2C family of
  balancers. See :ref:`Aperture Load Balancers <aperture_load_balancers>` section for more
  information.

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

  import com.twitter.conversions.DurationOps._
  import com.twitter.finagle.loadbalancer.{Balancers, LoadBalancerFactory}

  val balancer: LoadBalancerFactory =
    Balancers.aperture(
      smoothWin = 32.seconds,
      lowLoad = 1.0,
      highLoad = 2.0,
      minAperture = 10
    )

The ``aperture`` factory method takes five arguments:

1. `smoothWin` (default: 5 seconds) - the window of concurrent load observation
2. [`lowLoad`, `highLoad`] (default: [0.5, 2]) - the load band used to adjust an aperture size
   such that a concurrent load for each endpoint stays within the given interval
3. `minAperture` (default: 1) - the minimum size of the aperture

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
must make a decision. Finagle's default behavior makes an optimistic decision
that its view of the nodes may be out-of-date and picks a node it hopes has become available.
This can be customized to fail the request immediately through
``LoadBalancerFactory.WhenNoNodesOpenParam`` which will cause clients to see
a non-retryable ``c.t.f.loadbalancer.NoNodesOpenException``:

.. code-block:: scala

  import com.twitter.finagle.loadbalancer.LoadBalancerFactory.WhenNoNodesOpenParam
  import com.twitter.finagle.loadbalancer.WhenNoNodesOpen
  import com.twitter.finagle.Http

  Http.client
    .configured(WhenNoNodesOpenParam(WhenNoNodesOpen.FailFast))

:ref:`Related stats <loadbalancer_stats>`

.. _client_circuit_breaking:

Circuit Breaking
~~~~~~~~~~~~~~~~

The following modules aim to preemptively disable sessions that will likely fail requests.
From the perspective of the load balancer, they act as circuit breakers which, when
triggered, temporarily suspend the use of a particular endpoint.

There are at least two modules in the client stacks that might be viewed as `circuit breakers <https://martinfowler.com/bliki/CircuitBreaker.html>`_:

1. `Fail Fast` - a session-driven circuit breaker
2. `Failure Accrual` - a request-driven circuit breaker

In addition to `Fail Fast` and `Failure Accrual`, some of the protocols (i.e., `Mux`, `HTTP/2`) in
Finagle support `Ping-based Failure Detectors` [#failure_detectors]_
(i.e., :src:`ThresholdFailureDetector <com/twitter/finagle/liveness/ThresholdFailureDetector.scala>`).

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

This module fails closed and returns an exception when it detects a failure. See the
FAQ to :ref:`better understand <faq_failedfastexception>` why clients might be
seeing ``com.twitter.finagle.FailedFastException``'s.

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

  Because this module fails closed, Finagle will automatically disable `Fail Fast` when only
  one host is present in the replica set. This is because, without more replicas, Finagle can
  not meaningfully handle the failure when breaking the circuit.

:ref:`Related stats <fail_fast_stats>`

.. _client_failure_accrual:

Failure Accrual
^^^^^^^^^^^^^^^

The `Failure Accrual` module marks itself as unavailable per-endpoint based on a configurable :src:`policy <com/twitter/finagle/liveness/FailureAccrualPolicy.scala>`.
Unlike `Fail Fast`, this module fails open. That is, even if it transitions into an unavailable state, requests
will still be allowed to flow through it. However, recall that the availability is propagated through the stack,
so the load balancer will avoid using an endpoint where the failure accrual module is unavailable.

When transitioning from an unavailable to an available state, the module is conservative and only
allows for a probe request. If the probe fails, it goes back to unavailable regardless of the policy.
Put differently, at least one request must succeed before the module starts to apply the policy
again.

There are two types of policies out of the box:

1. A policy based on the requests success rate meaning (i.e, an endpoint marked dead if its success rate
   goes below the given threshold).
2. A policy based on the number of consecutive failures occurred in the endpoint (i.e., an endpoint marked
   dead if there are at least ``N`` consecutive failures occurred in this endpoint)

The default setup for the `Failure Accrual` module is a hybrid policy based on the number of consecutive
failures (default is 5) and required success rate (default is 80%). The policy is accompanied by an equal
jittered backoff [#backoff]_ (5 to 300 seconds) producing durations for which an endpoint is marked dead.

Use ``FailureAccrualFactory.Param`` [#experimental]_ to configure Failure Accrual`. The following snippets
illustrate some examples [#example]_.

.. code-block:: scala

  import com.twitter.conversions.DurationOps._
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

1. `requiredSuccessRate` - the minimally required success rate below which an endpoint marked dead
2. `window` - the window of *requests* to measure success rate on; measured using an exponentially
   weighted moving average
3. `markDeadFor` - the backoff policy (an instance of ``Stream[Duration]``) used to mark an endpoint
   dead for

.. code-block:: scala

  import com.twitter.conversions.DurationOps._
  import com.twitter.finagle.Http
  import com.twitter.finagle.liveness.{FailureAccrualFactory, FailureAccrualPolicy}
  import com.twitter.finagle.service.Backoff

  val twitter = Http.client
    .configured(FailureAccrualFactory.Param(() => FailureAccrualPolicy.successRateWithinDuration(
      requiredSuccessRate = 0.95,
      window = 5.minutes,
      markDeadFor = Backoff.const(10.seconds),
      minRequestThreshold = 100
    )))
    .newService("twitter.com")

The ``successRateWithinDuration`` factory method takes four arguments:

1. `requiredSuccessRate` - the minimally required success rate below which an endpoint is marked dead
2. `window` - duration over which the success rate is tracked over.
3. `markDeadFor` - the backoff policy (an instance of ``Stream[Duration]``) used to mark an endpoint
   dead for
4. `minRequestThreshold` - the minimum number of requests required within the past ``window`` before considering
   the measured success rate

To configure `Failure Accrual` based on a number of consecutive failures [#experimental]_, use the
following snippet [#example]_.

.. code-block:: scala

  import com.twitter.conversions.DurationOps._
  import com.twitter.finagle.Http
  import com.twitter.finagle.liveness.{FailureAccrualFactory, FailureAccrualPolicy}
  import com.twitter.finagle.service.Backoff

  val twitter = Http.client
    .configured(FailureAccrual.Param(() => FailureAccrualPolicy.consecutiveFailures(
      numFailures = 10,
      markDeadFor = Backoff.const(10.seconds)
    )))
    .newService("twitter.com")

The ``consecutiveFailures`` factory method takes two arguments:

1. `consecutiveFailures` - the number of failures after which an endpoint is marked dead
2. `markDeadFor` - the backoff policy (an instance of ``Stream[Duration]``) used to mark an endpoint
   dead for

FailureAccrualPolicys can also be composed together via the ``orElse`` method. If multiple policies return a duration on `markDeadOnFailure()`,
the maximum duration is used.

.. code-block:: scala
  import com.twitter.conversions.DurationOps._
  import com.twitter.finagle.Http
  import com.twitter.finagle.liveness.{FailureAccrualFactory, FailureAccrualPolicy}
  import com.twitter.finagle.service.Backoff

  val twitter = Http.client
    .configured(FailureAccrual.Param(() =>
      FailureAccrualPolicy.consecutiveFailures(
        numFailures = 10,
        markDeadFor = Backoff.const(10.seconds)
        ).orElse(
          FailureAccrualPolicy.successRate(
            requiredSuccessRate = 0.95,
            window = 100,
            markDeadFor = Backoff.const(10.seconds)
          )
        )
      )
    )
    .newService("twitter.com")

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

The module is implemented by :src:`FailureAccrualFactory <com/twitter/finagle/liveness/FailureAccrualFactory.scala>`.
See :ref:`Failure Accrual Stats <failure_accrual_stats>` for stats exported from the
``Failure Accrual`` module.

Pooling
~~~~~~~

Many protocols benefit from having persistent connections that are reused across requests.
Pooling is designed to balance connection churn and service acquisition latency at the cost of holding
resources open.

Depending on the configuration, a Finagle client's stack might contain up to _three_ connection pools
stacked on each other: watermark, caching and buffering pools.

The two Finagle protocols that don't require any connection pooling (multiplexing protocols) are
Mux and HTTP/2 as they both maintain just one connection per remote peer. For every other Finagle-
supported protocol (i.e., HTTP/1.1, Thrift), there a connection pooling setup built with watermark
and caching pools in front of each remote peer.

.. note::

  When HTTP/2 is enabled on an HTTP client (and the transport is successfully upgraded), the session
  pool caches streams (not connections) multiplexed over a single connection.

The default client stack layers caching and watermark pools which amounts to maintaining the low
watermark (i.e., ``0``, as long as request concurrency exists), queuing requests above the unbounded high
watermark (i.e., ``Int.MaxValue``), and applying an unbounded TTL (i.e., ``Duration.Top``) for services
that are between [low, high].

The override the default settings for connection pooling in a Finagle client, use the following
example [#example]_.

.. code-block:: scala

  import com.twitter.conversions.DurationOps._
  import com.twitter.finagle.Http

  val twitter = Http.client
    .withSessionPool.minSize(10)
    .withSessionPool.maxSize(20)
    .withSessionPool.maxWaiters(100)
    .withSessionPool.ttl(5.seconds)
    .newService("twitter.com")

.. note::

  All session pool settings are applied to each host in the replica set. Put this way, these settings
  are per-host as opposed to per-client.

Thus all the three pools are configured with a single param that takes the following arguments:

1. `minSize` and `maxSize` - low and high watermarks for the watermark pool (note that a Finagle
   client will not maintain more connections than `maxSize` per host)
2. `maxWaiters` - the maximum number of connection requests that are queued per host when the
   connection concurrency exceeds the high watermark
3. `ttl`- the maximum amount of time a per-host session is allowed to be cached in a pool

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

.. code-block:: scala

  import com.twitter.conversions.DurationOps._
  import com.twitter.finagle.Http

  val twitter = Http.client
    .withAdmissionControl.nackAdmissionControl(
      window = 10.minutes,
      nackRateThreshold = 0.75
    )

The filter can be configured with the following parameters:

1. ``window`` The duration over which the average is calculated. Default is 2
   minutes.
2. ``nackRateThreshold`` The rate of rejected requests at which the filter kicks
   in. Default is 0.5.

:ref:`Related stats <admission_control_stats>`

Here is a brief summary of the configurable params.

    A configuration with a ``nackRateThreshold`` of N% and a ``window`` of duration
    W roughly translates as, "start dropping some requests to the cluster when
    the nack rate averages at least N% over a window of duration W."

Here are some examples of situations with param values chosen to make the
filter useful:

- Owners of Service A examine their service's nack rate over several days
  and find that it is almost always under 10% and rarely above 1% (e.g.,
  during traffic spikes) or 5% (e.g., during a data center outage). They
  do not want to preemptively drop requests unless the cluster sees an
  extreme overload situation so they choose a nack rate threshold of 20%.
  And in such a situation they want the filter to act relatively quickly,
  so they choose a window of 30 seconds.

- Owners of Service B observe that excess load typically causes peak nack
  rates of around 25% for up to 60 seconds. They want to be aggressive
  about avoiding cluster overload and don’t mind dropping some innocent
  requests during mild load so they choose a window of 10 seconds and a
  threshold of 0.15 (= 15%).


.. note:: Client-side admission control may not work well with clients that only very sporadically
          send requests to their backends. In this case, the view that each client has of the state
          of the backend is reduced drastically, and its efficiency is degraded. It's recommended to
          disable nack admission control (via `withAdmissionControl.noNackAdmissionControl`) for
          clients experiencing bursty and very low volume (i.e., single digit RPS) traffic.

.. _response_classification:

.. include:: shared-modules/ResponseClassification.rst

MethodBuilder
-------------

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

Partition Aware Client
----------------------

Partition Aware Client is a client configured with a
partitioning layer in the client stack that understands the service partitioning strategy.
It routes requests to the partitioned service shards and supports scatter-gather.
Finagle Memcached client and Redis PartitionedClient have the consistent hashing partitioning
strategy configuration by default, ThriftMux client has partitioning support as an opt-in
configuration by calling ``.withPartitioning``.

:doc:`Learn more <PartitionAwareClient>` about ThriftMux Partition Aware Client.

.. rubric:: Footnotes

.. [#backoff] Most of the backoff strategies implemented in Finagle are inspired by Mark
   Brooker's `blog post <https://www.awsarchitectureblog.com/2015/03/backoff.html>`_.

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
   Alvaro Videla's `blog post <https://videlalvaro.github.io/2015/12/learning-about-distributed-systems.html>`_.
