MethodBuilder
=============

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

All of these can be customized per method (or endpoint) while sharing a single
underlying Finagle client. Concretely, a single service might offer both
`GET statuses/show/:id` as well as `POST statuses/update`, whilst each having wildly
different characteristics. The `GET` is idempotent and has a tight latency distribution
while the `POST` is not idempotent and has a wide latency distribution. If users want
different configurations, without ``MethodBuilder`` they must create separate
Finagle clients for each grouping. While long-lived clients in Finagle are not
expensive, they are not free. They create duplicate metrics and waste heap,
file descriptors, and CPU.

.. _mb_example_http:

Turning the example into code, first in Scala:

.. code-block:: scala

  import com.twitter.conversions.time._
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
    // build the service
    .newService(methodName = "get_statuses")

  // a `Service` for "POST statuses/update" with relatively loose timeouts
  // and a retry policy tailored to this endpoint.
  val statusesUpdate: Service[http.Request, http.Response] = builder
    // 200 millisecond timeouts per attempt. this applies to the initial
    // attempt and each retry, if any.
    .withTimeoutPerRequest(200.milliseconds)
    // 500 milliseconds timeouts in total, including retries
    .withTimeoutTotal(500.milliseconds)
    // retry only a specific HTTP response code
    .withRetryForClassifier {
      case ReqRep(_, Return(rep)) if rep.statusCode == 418 => ResponseClass.RetryableFailure
    }
    // build the service
    .newService(methodName = "update_status")

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

The :ref:`classifier <response_classification>` set by ``withRetryForClassifier`` is also used
to determine the :ref:`logical <mb_logical_req>` success metrics of the client.

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

Metrics are scoped to your client's label and method name.

- `clnt/your_client_label/method_name/logical/requests` — A counter of the total
  number of :ref:`logical <mb_logical_req>` successes and failures.
  This does not include any retries.
- `clnt/your_client_label/method_name/logical/success` — A counter of the total
  number of :ref:`logical <mb_logical_req>` successes.
- `clnt/your_client_label/method_name/logical/request_latency_ms` — A stat of
  the latency of the :ref:`logical <mb_logical_req>` requests, in milliseconds.
- `clnt/your_client_label/method_name/retries` — A stat of the number of times
  requests are retried.

For example:

.. code-block:: scala

  import com.twitter.finagle.Http

  val builder = Http.client
    .withLabel("example_client")
    .methodBuilder("inet!localhost:8080")
  val statusesShow = builder.newService(methodName = "get_statuses")

Will produce the following metrics:

- `clnt/example_client/get_statuses/logical/requests`
- `clnt/example_client/get_statuses/logical/success`
- `clnt/example_client/get_statuses/logical/request_latency_ms`
- `clnt/example_client/get_statuses/retries`

``MethodBuilder`` adds itself into the process registry which allows
for introspection of runtime configuration via TwitterServer's `/admin/registry.json`
`endpoint <https://twitter.github.io/twitter-server/Admin.html#admin-registry-json>`_.

Lifecycle
---------

A ``MethodBuilder`` is tied to a single logical destination via a
:ref:`Name <finagle_names>`, though using :ref:`dtabs <dtabs>` allows
clients to talk to different physical locations.

Because ``MethodBuilder`` is immutable, its methods chain together, and create
new instances backed by the original underlying client. This allows for common
customizations to be shared across endpoints:

.. code-block:: scala

  import com.twitter.conversions.time._
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
- The ``ClientBuilder`` retry policy will be applied and users must migrate
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
Service-per-method, ``ServiceIface``.

.. note:: Scrooge does not yet generate ``ServiceIface`` for Java users,
          so this is limited to Scala.

.. code-block:: scala

  import com.twitter.conversions.time._
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
      .newServiceIface[GraphService.ServiceIface](methodName = "followers")
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
      .newServiceIface[GraphService.ServiceIface](methodName = "follow")
      .follow

.. _mb_logical_req:

Logical request definition
--------------------------

``MethodBuilder``\'s logical requests represent the result of the
initial request, after any retries have occurred. Concretely, should a request result
in a retryable failure on the first attempt, but succeed upon retry, this considered
a single successful logical request while the logical request latency is the sum of
both the initial attempt and the retry.
