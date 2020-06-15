FAQ
===

General Finagle FAQ
-------------------

.. _propagate_failure:

What are CancelledRequestException, CancelledConnectionException, and ClientDiscardedRequestException?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a client connected to a Finagle server disconnects, the server raises
a *cancellation* interrupt on the pending Future. This is done to
conserve resources and avoid unnecessary work: the upstream
client may have timed the request out, for example. Interrupts on
futures propagate, and so if that server is in turn waiting for a response
from a downstream server it will cancel this pending request, and so on.

The topology, visually:

``Upstream ---> (Finagle Server -> Finagle Client) ---> Downstream``

Interrupts propagate between the Finagle Server and Client only if the
Future returned from the Server is chained [#]_ to the Client.

A simplified code snippet that exemplifies the intra-process structure:

.. code-block:: scala

  import com.twitter.finagle.Mysql
  import com.twitter.finagle.mysql
  import com.twitter.finagle.Service
  import com.twitter.finagle.Http
  import com.twitter.finagle.http
  import com.twitter.util.Future

  val client: Service[mysql.Request, mysql.Result] =
    Mysql.client.newService(...)

  val service: Service[http.Request, http.Response] =
    new Service[http.Request, http.Response] {
      def apply(req: http.Request): Future[http.Response] = {
        client(...).map(req: mysql.Result => http.Response())
      }
    }

  val server = Http.server.serve(..., service)

.. [#] "Chained" in this context means that calling `Future#raise`
       will reach the interrupt handler on the Future that represents
       the RPC call to the client. This is clearly the case in the above
       example where the call to the client is indeed the returned Future.
       However, this will still hold if the client call was in the context
       of a Future combinator (e.g. `Future#select`, `Future#join`, etc.)

This is the source of the :API:`CancelledRequestException <com/twitter/finagle/CancelledRequestException>` –
when a Finagle client receives the cancellation interrupt while a request is pending, it
fails that request with this exception. A special case of this is when a request is in the process
of establishing a session and is instead interrupted with a :API:`CancelledConnectionException <com/twitter/finagle/CancelledConnectionException>`

Note, in mux, when work is interrupted/cancelled on behalf of a request we encode this failure with
a :API:`ClientDiscardedRequestException <com/twitter/finagle/mux/ClientDiscardedRequestException>`. The concept
is the same as above though.

Server operators may prefer to measure their server side success rates excluding these classes of
exceptions. The rationale is that these failures don't necessarily represent a server side issue
and are dependent on client side configuration (e.g. timeouts). The recommended way to achieve this
is to configure a custom `com.twitter.finagle.service.ResponseClassifier` via `$Protocol.server.withResponseClassifier(...)`.

An important note is that these failures may sometimes show up in client stats! The interrupt will
propagate through the call graph until it finds an unsatisfied future. In some cases, that outstanding
future will have originated from a client backend within a server (i.e. `Finagle Client` in the topology
above). Thus, when the interrupt reaches the client future, all references to the future will see
the interrupt with the cancellation exception.

You can disable this behavior by using the :API:`MaskCancelFilter <com/twitter/finagle/filter/MaskCancelFilter>`:

.. code-block:: scala

  import com.twitter.finagle.filter.MaskCancelFilter
  import com.twitter.finagle.Http
  import com.twitter.finagle.http.

  val service: Service[http.Request, http.Response] =
    Http.client.newService("http://twitter.com")
  val masked = new MaskCancelFilter[http.Request, http.Response]

  val maskedService = masked.andThen(service)

.. note:: Most protocols do not natively support request cancellations (though modern RPC
          protocols like :doc:`Mux <Protocols>` do). In practice, this means that for these
          protocols, we need to disconnect the client to signal cancellation, which in turn
          can cause undue connection churn.

.. _configuring_finagle6:

How do I configure clients and servers with Finagle 6 APIs?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As of :doc:`Finagle 6.x <changelog>`, we introduced a new, preferred API for constructing Finagle
``Client``\s and ``Server``\s. Where the old API used ``ServerBuilder``\/``ClientBuilder``,
the new APIs use ``$Protocol.client.newClient`` and ``$Protocol.server.serve`` [#]_.

Old ``ClientBuilder`` APIs:

.. code-block:: scala

  import com.twitter.finagle.Http
  import com.twitter.finagle.builder.ClientBuilder
  import com.twitter.finagle.stats.StatsReceiver
  import com.twitter.finagle.tracing.Tracer
  import com.twitter.util.Duration

  val statsReceiver: StatsReceiver = ???
  val tracer: Tracer = ???
  val requestTimeout: Duration = ???
  val connectTimeout: Duration = ???

  val client = ClientBuilder()
    .stack(Http.client)
    .name("clientname")
    .reportTo(statsReceiver)
    .tracer(tracer)
    .requestTimeout(requestTimeout)
    .connectTimeout(connectTimeout)
    .hostConnectionLimit(1)
    .hosts("localhost:10000,localhost:10001,localhost:10003")
    .build()

New ``Stack`` APIs:

.. code-block:: scala

  import com.twitter.finagle.Http
  import com.twitter.finagle.stats.StatsReceiver
  import com.twitter.finagle.tracing.Tracer
  import com.twitter.util.Duration

  val statsReceiver: StatsReceiver = ???
  val tracer: Tracer = ???
  val requestTimeout: Duration = ???
  val connectTimeout: Duration = ???

  val client = Http.client
    .withLabel("clientname")
    // if `withStatsReceiver` is not specified, it will use the
    // `c.t.f.stats.DefaultStatsReceiver` scoped to the value of `newClient` or
    // `newService`'s label. If that is not provided, it will be scoped to the
    // value of `withLabel`.
    .withStatsReceiver(statsReceiver)
    .withTracer(tracer)
    .withRequestTimeout(requestTimeout)
    .withSession.acquisitionTimeout(connectTimeout)
    .withSessionPool.maxSize(1)
    .newService("localhost:10000,localhost:10001")

More configuration options and the details about them are available for
:ref:`clients <finagle_clients>` and :ref:`servers <finagle_servers>`.
Additionally, the Scaladocs for most methods on ``ServerBuilder`` and
``ClientBuilder`` include the Stack-based API's alternative. A few methods do
not yet have one-to-one equivalents, such as ``ClientBuilder.retries`` and
for these you should :ref:`migrate <mb_cb_migration>` to using ``MethodBuilder``.

.. [#] Protocol implementors are encouraged to provide sensible
       defaults and leave room for application specific behavior
       to be built on top of the base layer via ``Filters`` or
       synchronization mechanisms.

.. _faq_failedfastexception:

Why do clients see com.twitter.finagle.FailedFastException's?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

While the :src:`FailFast <com/twitter/finagle/service/FailFastFactory.scala>` service
factory generally shields clients from downed hosts, sometimes clients will see
:src:`FailedFastExceptions <com/twitter/finagle/Exceptions.scala>`.
A common cause is when all endpoints in the load balancer's pool are
marked down as fail fast, then the load balancer will pass requests through, resulting in a
``com.twitter.finagle.FailedFastException``.

See :ref:`this example <disabling_fail_fast>` on how to disable `Fail Fast` for a given client.

Refer to the :ref:`fail fast <client_fail_fast>` section for further context.

What is a com.twitter.finagle.service.ResponseClassificationSyntheticException?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

While typically, a :src:`StatsFilter <com/twitter/finagle/service/StatsFilter.scala>` counts
`Exceptions` as failures, a user may supply a
`ResponseClassifier <https://twitter.github.io/finagle/guide/Clients.html#response-classification>`_
that treats non-Exceptions as failures. In that case, while no exceptions have occurred, a
`ResponseClassificationSyntheticException` is used as a "synthetic" exception for
bookkeeping purposes.

One specific example can be seen when using the ThriftResponseClassifier.ThriftExceptionsAsFailures.
Successful ThriftResponses which deserialize into Thrift Exceptions use this exception to
be counted as failures in StatsFilter.

How long should my Clients live?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One client should be made per set of fungible services.  You should not be reinstantiating
your client on every request, and you should not have a different client per instance--finagle
can handle load-balancing for you.

There are a few use cases, like link shortening, or web crawling, where a service must communicate
with many other non-fungible services, in which it makes sense to proliferate clients that are
created, used, and thrown away, but in the vast majority of cases, clients should be persistent,
not ephemeral.

When can I use a null?
~~~~~~~~~~~~~~~~~~~~~~

None of Finagle's APIs admits nulls unless noted otherwise.  Finagle is written in Scala, and by
convention, we use Scala `Options` when a parameter or a result is optional.

Where is time spent in the client stack?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Finagle's :ref:`clients  <client_modules>` and :ref:`servers <server_modules>`
have many modules that are tasked with a wide assortment of jobs. When there
is unexpected latency, it can be useful to have visibility into where time
is spent. Finagle's `RequestLogger` can help with this. It can be enabled by
setting the ``com.twitter.finagle.request.Logger`` level to ``TRACE`` and
enabling the stack param:

.. code-block:: scala

  // scala
  import com.twitter.finagle.filter.RequestLogger

  Protocol.client.configured(RequestLogger.Enabled)
  Protocol.server.configured(RequestLogger.Enabled)

.. code-block:: java

  // java
  import com.twitter.finagle.filter.RequestLogger;

  Protocol.client.configured(RequestLogger.Enabled().mk());
  Protocol.server.configured(RequestLogger.Enabled().mk());

The logs include synchronous and asynchronous time for each stack module's
`Filter`. Synchronous here means the time spent from the beginning of the
`Filter.apply` call to when the `Future` is returned from the `Filter`.
Asynchronous here is how long it takes from the beginning of the
`Filter.apply` call to when the returned `Future` is satisfied.

As an example, given this stack module with the name "slow-down-module":

.. code-block:: scala

  import com.twitter.conversions.DurationOps._
  import com.twitter.finagle.Filter
  import com.twitter.finagle.util.DefaultTimer

  class SlowFilterDoNotUse extends Filter[Int, Int, Int, Int] {
    def apply(request: Int, service: Service[Int, Int]): Future[Int] = {
      // this delays the synchronous path
      Thread.sleep(1.second.inMilliseconds)

      // the call to `Future.delayed` delays the asynchronous path
      service(request).delayed(500.milliseconds)(DefaultTimer)
    }
  }

The output of `RequestLogger` would look something like:

.. code-block:: none

  traceId=b07d63561ed1a9b9.b07d63561ed1a9b9<:b07d63561ed1a9b9 server-name slow-down-module begin
  traceId=b07d63561ed1a9b9.b07d63561ed1a9b9<:b07d63561ed1a9b9 server-name slow-down-module end cumulative sync elapsed 1000025 us
  traceId=b07d63561ed1a9b9.b07d63561ed1a9b9<:b07d63561ed1a9b9 server-name slow-down-module end cumulative async elapsed 1500045 us

There will be these lines for every stack module and the log format is:
*traceId=$traceId $client-or-server-label $module-name*.

Mux-specific FAQ
----------------

What service behavior will change when upgrading to Mux?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*Connecting Pooling Metrics*

With Mux, Finagle multiplexes several requests onto a single connection. As a
consequence, traditional forms of connection-pooling are no longer required. Thus
Mux employs :API:`SingletonPool <com/twitter/finagle/pool/SingletonPool>`,
which exposes new stats:

- ``connects``, ``connections``, and ``closes`` stats should drop, since
  there will be less channel opening and closing.
- ``connection_duration``, ``connection_received_bytes``, and
  ``connection_sent_bytes`` stats should increase, since connections become more
  long-lived.
- ``connect_latency_ms`` and ``failed_connect_latency_ms`` stats may become
  erratic because their sampling will become more sparse.
- ``pool_cached``, ``pool_waiters``, ``pool_num_waited``, ``pool_size`` stats all
  pertain to connection pool implementations not used by Mux, so they disappear
  from stats output.

*ClientBuilder configuration*

Certain :API:`ClientBuilder <com/twitter/finagle/builder/ClientBuilder>`
settings related to connection pooling become obsolete:
``hostConnectionCoresize``, ``hostConnectionLimit``, ``hostConnectionIdleTime``,
``hostConnectionMaxWaiters``, and ``expHostConnectionBufferSize``

*Server Connection Stats*

The server-side connection model changes as well. Expect the following stats to
be impacted:

- ``connects``, ``connections``, and ``closes`` stats should drop.
- ``connection_duration``, ``connection_received_bytes``, and
  ``connection_sent_bytes`` should increase.
- Obsolete stats: ``idle/idle``, ``idle/refused``, and ``idle/closed``

*ServerBuilder configuration*
Certain :API:`ServerBuilder <com/twitter/finagle/builder/ServerBuilder>`
connection management settings become obsolete: ``openConnectionsThresholds``.

What is ThriftMux?
~~~~~~~~~~~~~~~~~~

.. _whats_thriftmux:

:API:`ThriftMux <com/twitter/finagle/ThriftMux$>`
is an implementation of the Thrift protocol built on top of Mux.
