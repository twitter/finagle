.. _methodbuilder:

MethodBuilder
=============

.. note:: Currently there is ``MethodBuilder`` support for HTTP and ThriftMux.
          We are waiting on user interest before expanding to more protocols.

``MethodBuilder`` is a collection of APIs for client configuration at a higher
level than the  :ref:`Finagle 6 APIs <finagle6apis>` while improving upon the deprecated
``ClientBuilder``. ``MethodBuilder`` provides:

- :ref:`Idempotent <mb_idempotency>` classification providing a good default retry
  policy. It also offers a single knob, max extra load, that can help reduce tail latency
  through :ref:`backup requests <mb_backup_requests>`.
- :ref:`Logical <mb_logical_req>` success rate metrics.
- Retries based on application-level requests and responses (e.g. an HTTP
  503 response code or a Thrift exception).
- Configuration of per-attempt and total timeouts.

All of these can be customized per method (or endpoint) while sharing a single
underlying Finagle client. Concretely, a single service might offer both
`GET statuses/show/:id` as well as `POST statuses/update`, whilst each having wildly
different characteristics. The `GET` is idempotent and has a tight latency distribution
while the `POST` is not idempotent and has a wide latency distribution. If users want
different configurations, without ``MethodBuilder`` they must create separate
Finagle clients for each grouping. While long-lived clients in Finagle are not
expensive, they are not free. They create duplicate metrics and waste heap,
file descriptors, and CPU.

``MethodBuilder`` can also be used without scoping to a ``methodName``, allowing users to wrap
an entire client with ``MethodBuilder``, as with ``totalService`` in the example below.

.. _mb_example_http:

Turning the example into code, first in Scala:

.. code-block:: scala

  import com.twitter.conversions.PercentOps._
  import com.twitter.conversions.DurationOps._
  import com.twitter.finagle.{http, Http, Service}
  import com.twitter.finagle.service.{ReqRep, ResponseClass}
  import com.twitter.util.Return

  // other StackClient configuration done here, e.g. `withSessionQualifier`
  val stackClient = Http.client
    .withLabel("example_client")

  val builder = stackClient.methodBuilder("inet!localhost:8080")

  // a `Service` for "GET statuses/show/:id" with relatively tight timeouts
  // and a liberal retry policy.
  val statusesShow: Service[http.Request, http.Response] = builder
    // 25 millisecond timeout per attempt. this applies to the initial
    // attempt and each retry, if any.
    .withTimeoutPerRequest(25.milliseconds)
    // 50 milliseconds timeout in total, including retries
    .withTimeoutTotal(50.milliseconds)
    // retry all HTTP 4xx and 5xx responses
    .withRetryForClassifier {
      case ReqRep(_, Return(rep)) if rep.statusCode >= 400 && rep.statusCode <= 599 =>
        ResponseClass.RetryableFailure
    }
    // can reduce tail latency by sending 1% extra load to the backend
    .idempotent(maxExtraLoad = 1.percent)
    // build the service
    .newService(methodName = "get_statuses")

  // a `Service` for "POST statuses/update", which is non-idempotent and has
  // relatively loose timeouts
  val statusesUpdate: Service[http.Request, http.Response] = builder
    // 200 millisecond timeouts per attempt. this applies to the initial
    // attempt and each retry, if any.
    .withTimeoutPerRequest(200.milliseconds)
    // 500 milliseconds timeouts in total, including retries
    .withTimeoutTotal(500.milliseconds)
    // no retries (except on write exceptions), and no backup requests
    .nonIdempotent
    // build the service
    .newService(methodName = "update_status")

  // A `Service` that is not scoped to a method
  val totalService: Service[http.Request, http.Response] = builder
    .newService

A similar example, for Java:

.. code-block:: java

  import com.twitter.finagle.Http;
  import com.twitter.finagle.Service;
  import com.twitter.finagle.http.Request;
  import com.twitter.finagle.http.Response;
  import com.twitter.util.Duration;

  Service<Request, Response> exampleService =
    Http.client().methodBuilder("localhost:8080")
      .withTimeoutTotal(Duration.fromMilliseconds(50))
      .withTimeoutPerRequest(Duration.fromMilliseconds(25))
      .idempotent(0.01)
      .newService("java_example");

Retries
-------

``MethodBuilder`` defaults to using the client's :ref:`classifier <response_classification>`
to retry failures that are marked as retryable
(``com.twitter.finagle.service.ResponseClass.RetryableFailure``).

A budget is used to prevent retries from overwhelming
the backend service. The budget is shared across clients created from
an initial ``MethodBuilder``. As such, even if the retry rules
deem the request retryable, it may not be retried if there is insufficient
budget.

Finagle automatically retries failures that are known to be safe
to retry via :src:`RequeueFilter <com/twitter/finagle/service/RequeueFilter.scala>`.
This includes ``com.twitter.finagle.WriteException WriteExceptions`` and
:ref:`retryable nacks <glossary_nack>`. As these should have already been retried,
``MethodBuilder`` will avoid retrying them again at this layer.

The :ref:`classifier <response_classification>` set by ``withRetryForClassifier`` is
used to determine which requests are successful. This is the basis for measuring
the :ref:`logical <mb_logical_req>` success metrics of the method and for logging_
unsuccessful requests.

Since setting the response classifier overrides how retries are handled, setting
``withRetryForClassifier`` may clobber the way that the ``idempotent``, described below,
handles retries, rendering them non-idempotent.  If you want to reclassify failures as
successes or successes as failures as well as mark the endpoint as idempotent, set
``withRetryForClassifier`` before setting ``idempotent``.

.. _mb_idempotency:

Idempotency
-----------

``MethodBuilder`` provides ``idempotent`` and ``nonIdemptotent`` methods for a client to signal
whether it's safe to resend requests that have already been sent.

If a client is configured with ``idempotent``, a protocol-dependent
:src:`ResponseClassifier <com/twitter/finagle/service/ResponseClassifier.scala>` is combined with
any existing classifier, in particular what's set by ``withRetryForClassifier`` to also reissue
requests on failure (Thrift exceptions for ThriftMux clients, and 500s for HTTP clients). The
parameter to ``idempotent``, ``maxExtraLoad``, is used to configure
:ref:`backup requests <mb_backup_requests>` and may be useful in reducing tail latency. Backup
requests can be disabled by setting ``maxExtraLoad`` to `0.0`.

If a client is configured with ``nonIdempotent``, any existing configured
:src:`ResponseClassifier <com/twitter/finagle/service/ResponseClassifier.scala>` is removed
and replaced with the default
:src:`ResponseClassifier <com/twitter/finagle/service/ResponseClassifier.scala>`, which only retries
on write exceptions (wherein the request was never sent to the server). Any configured
:ref:`backup requests <mb_backup_requests>` are also disabled,
since it's not safe to reissue requests.

.. _mb_backup_requests:

Backup Requests
~~~~~~~~~~~~~~~

Backup requests, or hedged requests, are a means of reducing the tail latency of
requests that are known to be safe to issue multiple times. This is done by sending an extra
copy of a request, or a backup, to the backend after some threshold of time has elapsed
before receiving a response for the initial request. This helps tail latency when the
backend service has high variability in its response times and the additional backend load
is deemed worth the cost of improving tail latency.

``MethodBuilder`` makes backup requests easy through the ``idempotent`` method's ``maxExtraLoad``
parameter. This parameter represents the maximum extra load, expressed as a fraction from
`[0.0 to 1.0)`, you are willing to send to the backend. If a response for the original request
has not been received within some duration, a second, backup, request will be issued so long as
the extra load constraints have not been violated. That duration is derived from ``maxExtraLoad``;
it is the n-th percentile latency of requests, where n is `100 * (1  - maxExtraLoad)`. For example,
if ``maxExtraLoad`` is `0.01`, no more than 1% additional requests will be sent and
it will be sent at the p99 latency.

To disable backup requests, set ``maxExtraLoad`` to `0.0`.

Latency is calculated using a windowed history of responses. In order to protect the backend from
excessive backup requests should the latency shift suddenly, a `RetryBudget` based on
``maxExtraLoad`` is used. When determining whether or not to send a backup, this local budget
is combined with the underlying client's retry budget; this means that the backend will
not receive more extra load than that permitted by the budget, whether through
retries due to failures or backup requests.

If you are using TwitterServer along with finagle-stats, a good starting point for
determining a value for ``maxExtraLoad`` is looking at the details of the
`PDF histogram for request latency <https://twitter.github.io/twitter-server/Admin.html#admin-histograms>`_.
If you choose a ``maxExtraLoad`` of `0.01`, for example, you can expect your p999 and p9999
latencies to come in towards the p99 latency. For `0.05`, those latencies would shift
towards your p95 latency. You should also ensure that your backend can tolerate the increased load.

Here's an example of how to send backup requests for one endpoint
and disable them for another, first in Scala:

.. code-block:: scala

  import com.twitter.conversions.PercentOps._
  import com.twitter.finagle.{http, Http, Service}

  val builder = Http.client.methodBuilder("inet!localhost:8080")

  val withBackups: Service[http.Request, http.Response] = builder
    .idempotent(maxExtraLoad = 1.percent)
    .newService(methodName = "with_backups")

  val noBackups: Service[http.Request, http.Response] = builder
    .idempotent(maxExtraLoad = 0.percent)
    .newService(methodName = "no_backups")

A similar example, for Java:

.. code-block:: java

  import com.twitter.finagle.Http;
  import com.twitter.finagle.Service;
  import com.twitter.finagle.http.Request;
  import com.twitter.finagle.http.Response;

  Service<Request, Response> withBackups =
    Http.client().methodBuilder("localhost:8080")
      .idempotent(0.01)
      .newService("with_backups");

  Service<Request, Response> noBackups =
    Http.client().methodBuilder("localhost:8080")
      .idempotent(0.0)
      .newService("no_backups");

.. _mb_backups_requests_no_mb:

While backup requests are integrated nicely with ``MethodBuilder``, this is not a requirement.
The functionality is encapsulated in a :ref:`Filter <filters>` that can be composed with
your other `Filters` and `Services`. Take a look at
:src:`BackupRequestFilter <com/twitter/finagle/client/BackupRequestFilter.scala>`,
but note there are subtleties regarding where it is placed and the retry budgets.

.. note::

   Backup requests were popularized by Google in Dean, J. and Barroso, L.A. (2013),
   `The Tail at Scale <https://cacm.acm.org/magazines/2013/2/160173-the-tail-at-scale/fulltext>`_,
   Communications of the ACM, Vol. 56 No. 2, Pages 74-80.
   Non-paywalled slides `here <https://static.googleusercontent.com/media/research.google.com/en//people/jeff/Berkeley-Latency-Mar2012.pdf>`_.

Timeouts
--------

For per-request timeouts the defaults come from the client's configuration
for :src:`TimeoutFilter.Param <com/twitter/finagle/service/TimeoutFilter.scala>`
which is typically set on a client via ``com.twitter.finagle.$Protocol.withRequestTimeout``.

For total total timeouts, the defaults come from the client's configuration
for :src:`TimeoutFilter.TotalTimeout <com/twitter/finagle/service/TimeoutFilter.scala>`.

The total timeout is how long the :ref:`logical request <mb_logical_req>` is given to complete. This includes
the time spent on developer configured retries as well as automatic retries issued by
Finagle. Per request timeouts apply to each attempt issued, irrespective of if
it is the initial request, a Finagle requeue, or a retry based on the developer's policy.

Take a ``MethodBuilder`` configured with 100 ms per-request timeout,
150 ms total timeout, and a policy that will retry all timeouts as an example.
If the first request to the backend gets a retryable nack back in 10 ms,
Finagle will automatically issue a retry with 100 ms for its timeout.
If this retry happens to time out, the application level retry policy on
the ``MethodBuilder`` applies, and this retry will have 40 ms remaining (150 ms total
- 10 ms - 100 ms).

Metrics
-------

.. include:: metrics/MethodBuilder.rst

For example:

.. code-block:: scala

  import com.twitter.conversions.PercentOps._
  import com.twitter.finagle.Http

  val builder = Http.client
    .withLabel("example_client")
    .methodBuilder("inet!localhost:8080")
    .idempotent(maxExtraLoad = 1.percent)
  val statusesShow = builder.newService(methodName = "get_statuses")

Will produce the following metrics:

- `clnt/example_client/get_statuses/logical/requests`
- `clnt/example_client/get_statuses/logical/success`
- `clnt/example_client/get_statuses/logical/failures`
- `clnt/example_client/get_statuses/logical/failures/exception_name`
- `clnt/example_client/get_statuses/logical/request_latency_ms`
- `clnt/example_client/get_statuses/retries`
- `clnt/example_client/get_statuses/backups/send_backup_after_ms`
- `clnt/example_client/get_statuses/backups/backups_sent`
- `clnt/example_client/get_statuses/backups/backups_won`
- `clnt/example_client/get_statuses/backups/budget_exhausted`

``MethodBuilder`` adds itself into the process registry which allows
for introspection of runtime configuration via TwitterServer's `/admin/registry.json`
`endpoint <https://twitter.github.io/twitter-server/Admin.html#admin-registry-json>`_.

.. _logging:

Logging
-------

Unsuccessful requests, as determined by the :ref:`classifier <response_classification>`
set by ``withRetryForClassifier``, are logged at ``com.twitter.logging.Level.DEBUG``
level. Further details, including the request and response, are available at ``TRACE``
level. There is a ``Logger`` per method, named with the format
`"com.twitter.finagle.client.MethodBuilder.$clientName.$methodName"`.

Lifecycle
---------

A ``MethodBuilder`` is tied to a single logical destination via a
:ref:`Name <finagle_names>`, though using :ref:`dtabs <dtabs>` allows
clients to talk to different physical locations.

Because ``MethodBuilder`` is immutable, its methods chain together, and create
new instances backed by the original underlying client. This allows for common
customizations to be shared across endpoints:

.. code-block:: scala

  import com.twitter.conversions.DurationOps._
  import com.twitter.finagle.{http, Http, Service}
  import com.twitter.finagle.service.{ReqRep, ResponseClass}
  import com.twitter.util.Return

  // the `Services` below will use these settings unless they are
  // explicitly changed.
  val base = Http.client.methodBuilder("inet!localhost:8080")
    .withRetryDisabled
    .withTimeoutPerRequest(200.milliseconds)

  val longerTimeout: Service[http.Request, http.Response] = base
    // changes the timeout, while leaving retries disabled
    .withTimeoutTotal(500.milliseconds)
    .newService(methodName = "longer_timeout")

  val retryOn418s: Service[http.Request, http.Response] = base
    // keeps the 200 ms timeout, while changing the retry policy
    .withRetryForClassifier {
      case ReqRep(_, Return(rep)) if rep.statusCode == 418 =>
        ResponseClass.RetryableFailure
    }
    .newService(methodName = "retry_teapots")

As a consequence of the Finagle client being shared, its underlying
resources (e.g. connections) are shared as well. Specifically, all
``Service``\s constructed by a ``MethodBuilder`` must be ``close``-ed
for the underlying resources to be closed.

One other effect of sharing the Finagle client is that the load balancer
and connection pool (when applicable, e.g. HTTP/1.1) are shared resources
as well. For most usage patterns this is unlikely to be an issue. In some cases,
it may manifest as poor distribution of the different method's requests across
backends. Should it be an issue, we recommend creating and using
separate Finagle clients for those methods.

.. _mb_cb_migration:

Migrating from ClientBuilder
----------------------------

``MethodBuilder`` is in part intended as a replacement for ``ClientBuilder`` and
as such there is relatively easy migration path. Users should prefer using the
Finagle 6 style ``StackClient``\s directly for creating a ``MethodBuilder`` and
work on migrating their code off of ``ClientBuilder``.

Notes and caveats:

- Metrics will be scoped to the ``ClientBuilder.name`` and then the method name.
- Total timeout defaults to using the ``ClientBuilder.timeout`` configuration.
- Per-request timeout defaults to using the ``ClientBuilder.requestTimeout`` configuration.
- The ``ClientBuilder`` metrics scoped to "tries" are not included. These
  are superseded by the logical ``MethodBuilder`` metrics.
- The ``ClientBuilder`` retry policy will **not** be applied and users must migrate
  to using ``withRetryForClassifier``.
- The ``ClientBuilder`` must have a destination set via one of
  ``hosts``, ``addrs``, ``dest``, ``cluster``, or ``group``.

.. code-block:: scala

  import com.twitter.finagle.client.ClientBuilder
  import com.twitter.finagle.{http, Http}

  val stackClient = Http.client()
  val clientBuilder = ClientBuilder()
    .name("example_client")
    .stack(stackClient)
    .hosts("localhost:8080")

  val methodBuilder = http.MethodBuilder.from(clientBuilder)

Application-level failure handling
----------------------------------

While ``MethodBuilder`` encourages developers to consider
failure modes in the broadest sense through response classification,
this is often insufficient for application developers who need to
do more than that. Examples include logging, fallback to a different
data source, hiding functionality, and more. As ``MethodBuilder``
gives you a standard :ref:`Service <services>`, developers are encouraged
to compose them with :ref:`Filters <filters>` and/or transform the
``Service``\s returned :ref:`Future <future_failure>` to handle more
granular failures.

Using with ThriftMux
--------------------

:ref:`Above <mb_example_http>` we saw an example using HTTP. Next
let's walk through a ThriftMux example, using a hypothetical
social graph service with two endpoints, `followers` and `follow`,
where `followers` is idempotent and has a tight latency profile
and `follow` is only retryable for a specific error code and has a wide
latency distribution. Given the IDL:

.. code-block:: none

  #@namespace scala com.twitter.finagle.example.graph

  exception NotFoundException { 1: i32 code }

  service GraphService {
    i32 followers(1: i64 user_id) throws (1: NotFoundException ex)
    i32 follow(1: i64 follower, 2: i64 followee) throws (1: NotFoundException ex)
  }

We create ``MethodBuilder``\s which work on Scrooge's generated
Service-per-method, ``ServicePerEndpoint``.

.. note:: Scrooge does not yet generate ``ServicePerEndpoint`` for Java users,
          so this is limited to Scala.

.. code-block:: scala

  import com.twitter.conversions.DurationOps._
  import com.twitter.finagle.{Service, ThriftMux}
  import com.twitter.finagle.example.graph._
  import com.twitter.finagle.service.{ReqRep, ResponseClass}
  import com.twitter.finagle.thriftmux.service.ThriftMuxResponseClassifier
  import com.twitter.util.Throw

  val stackClient = ThriftMux.client
    .withLabel("thriftmux_example")
  val builder = stackClient.methodBuilder("inet!localhost:8989")

  // `Service` for "followers" with tight timeouts and liberal retry policy
  val followers: Service[GraphService.Followers.Args, Int] =
    builder
      .withTimeoutPerRequest(20.milliseconds)
      .withTimeoutTotal(50.milliseconds)
      .withRetryForClassifier(ThriftMuxResponseClassifier.ThriftExceptionsAsFailures)
      .servicePerEndpoint[GraphService.ServicePerEndpoint](methodName = "followers")
      .followers

  // `Service` for "follow"
  val follow: Service[GraphService.Follow.Args, Int] =
    builder
      .withTimeoutPerRequest(200.milliseconds)
      .withTimeoutTotal(300.milliseconds)
      .withRetryForClassifier {
        case ReqRep(_, Throw(NotFoundException(code))) if code == 5 =>
          ResponseClass.RetryableFailure
      }
      .servicePerEndpoint[GraphService.ServicePerEndpoint](methodName = "follow")
      .follow

If you are working with code that prefers Scrooge's ``MethodPerEndpoint``
or ``FutureIface`` you can convert from a ``ServicePerEndpoint`` by wrapping it
with a ``MethodPerEndpoint``.

.. code-block:: scala

  import com.twitter.conversions.DurationOps._
  import com.twitter.finagle.{Filter, ThriftMux}
  import com.twitter.finagle.example.graph._
  import com.twitter.util.Future

  val stackClient = ThriftMux.client.withLabel("thriftmux_example")
  val servicePerEndpoint: GraphService.ServicePerEndpoint =
    stackClient
      .methodBuilder("inet!localhost:8989")
      .withTimeoutPerRequest(20.milliseconds)
      .servicePerEndpoint[GraphService.ServicePerEndpoint](methodName = "followers")

  // `MethodPerEndpoint` is a collection of methods that return `Futures`.
  // It will use the configuration from the `ServicePerEndpoint` which allows
  // you to decorate the endpoints with `Filters`.
  val loggingFilter: Filter[GraphService.Follow.Args, GraphService.Follow.SuccessType] = ???
  val filtered: GraphService.ServicePerEndpoint =
    servicePerEndpoint.withFollow(loggingFilter.andThen(servicePerEndpoint.follow))
  val methodPerEndpoint: GraphService.MethodPerEndpoint =
    new GraphService.MethodPerEndpoint(filtered)

  val result: Future[Int] =
    methodPerEndpoint.follow(follower = 568825492L, followee = 4196983835L)

Further details on the differences between ``ServicePerEndpoint`` and ``MethodPerEndpoint``
and how to work with them are in
`Scrooge's Finagle docs <https://twitter.github.io/scrooge/Finagle.html>`_.

.. _mb_logical_req:

Logical request definition
--------------------------

``MethodBuilder``\'s logical requests represent the result of the
initial request, after any retries have occurred. Concretely, should a request result
in a retryable failure on the first attempt, but succeed upon retry, this is considered
a single successful logical request while the logical request latency is the sum of
both the initial attempt and the retry.
