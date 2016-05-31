FAQ
===

General Finagle FAQ
-------------------

.. _propagate_failure:

What are CancelledRequestException and CancelledConnectionException?
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
       of a Future combinator (ex. `Future#select`, `Future#join`, etc.)

This is the source of the :API:`CancelledRequestException <com.twitter.finagle.CancelledRequestException>` --
when a Finagle client receives the cancellation interrupt while a request is pending, it
fails that request with this exception. A special case of this is when a request is in the process
of establishing a session and is instead interrupted with a :API:`CancelledConnectException <com.twitter.finagle.CancelledConnectException>`

You can disable this behavior by using the :API:`MaskCancelFilter <com.twitter.finagle.filter.MaskCancelFilter>`:

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

Why is com.twitter.common.zookeeper#server-set not found?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some of our libraries still aren't published to maven central. If you add

.. code-block:: scala

	resolvers += "twitter" at "https://maven.twttr.com"

to your sbt configuration, it will be able to pick up the libraries which are
published externally, but not yet to maven central.

.. _configuring_finagle6:

How do I configure clients and servers with Finagle 6 APIs?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As of :doc:`6.x <changelog>`, We introduced a new, preferred API for constructing Finagle
``Client``\s and ``Server``\s. Where the old API used ``ServerBuilder``\/``ClientBuilder``
with ``Codec``\s, the new APIs use ``Protocol.client.newClient`` and ``Protocol.server.serve`` [#]_.

Old ``ClientBuilder`` APIs:

.. code-block:: scala

  import com.twitter.finagle.builder.ClientBuilder
  import com.twitter.finagle.http.Http
  import com.twitter.finagle.stats.StatsReceiver
  import com.twitter.finagle.tracing.Tracer
  import com.twitter.util.Duration

  val statsReceiver: StatsReceiver = ???
  val tracer: Tracer = ???
  val requestTimeout: Duration = ???
  val connectTimeout: Duration = ???

  val client = ClientBuilder()
    .codec(Http)
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
for these you should continue to use ``ClientBuilder`` along with the
``ClientBuilder.stack`` method.

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

A related issue is when the load balancer's pool is a single endpoint that is itself a
load balancer (for example an Nginx server or a hardware load balancer).
It is important to disable fail fast as the remote load balancer has
the visibility into which endpoints are up.

See :ref:`this example <disabling_fail_fast>` on how to disable `Fail Fast` for a given client.

Refer to the :ref:`fail fast <client_fail_fast>` section for further context.

How long should my Clients live?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One client should be made per set of fungible services.  You should not be reinstantiating
your client on every request, and you should not have a different client per instance--finagle
can handle load-balancing for you.

There are a few use cases, like link shortening, or web crawling, where a service must communicate
with many other non-fungible services, in which it makes sense to proliferate clients that are
created, used, and thrown away, but in the vast majority of cases, clients should be persistent,
not ephemeral.

Mux-specific FAQ
----------------

What service behavior will change when upgrading to Mux?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*Connecting Pooling Metrics*

With Mux, Finagle multiplexes several requests onto a single connection. As a
consequence, traditional forms of connection-pooling are no longer required. Thus
Mux employs `com.twitter.finagle.pool.SingletonPool <http://twitter.github.io/finagle/docs/#com.twitter.finagle.pool.SingletonPool>`_,
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

Certain `ClientBuilder <http://twitter.github.io/finagle/docs/#com.twitter.finagle.builder.ClientBuilder>`_
settings related to connection pooling become obsolete:
``hostConnectionCoresize``, ``hostConnectionLimit``, ``hostConnectionIdleTime``,
``hostConnectionMaxWaiters``, ``hostConnectionMaxIdleTime``,
``hostConnectionMaxLifeTime``, and ``hostConnectionBufferSize``

*Server Connection Stats*

The server-side connection model changes as well. Expect the following stats to
be impacted:

- ``connects``, ``connections``, and ``closes`` stats should drop.
- ``connection_duration``, ``connection_received_bytes``, and
  ``connection_sent_bytes`` should increase.
- Obsolete stats: ``idle/idle``, ``idle/refused``, and ``idle/closed``

*ServerBuilder configuration*
Certain `ServerBuilder <http://twitter.github.io/finagle/docs/#com.twitter.finagle.builder.ServerBuilder>`_
connection management settings become obsolete: ``openConnectionsThresholds``,
``hostConnectionMaxIdleTime``, and ``hostConnectionMaxLifeTime``.

What is ThriftMux?
~~~~~~~~~~~~~~~~~~

`ThriftMux <http://twitter.github.io/finagle/docs/#com.twitter.finagle.ThriftMux$>`_
is an implementation of the Thrift protocol built on top of Mux.
