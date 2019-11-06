Unsuccessful Responses
======================

In normal operation, the majority of requests follow the "successful
response" code path. This is generally easy to reason about and receives lots of
developer attention. However, many things can and do go wrong in complex systems
that are caused by subtle behaviors which can be hard to detect and plan for.
Finagle aims to help services gracefully degrade whenever possible.

It is important to understand that there are many ways a service can respond to
a request unsuccessfully. Some, but not all of those responses may be due to
exceptional cases. In Finagle, each response is represented as a
:util-core-src:`Future <com/twitter/util/Future.scala>` which may be satisfied
with either a `Return` or `Throw`. From a user's point of view, either case could be
considered successful or not, even though considering a `Throw` result as a
successful response is an anti-pattern.

For instance, an unsuccessful HTTP request may be satisfied with a
`Return(HttpResponse)` where the response code is 5XX or some kind of
`Throw(LostConnection)`. Another possibility is that a service takes far too
long to reply or never replies at all. In this case, Finagle may transform the
lack of reply into a timeout exception (if a timeout is configured. There is
no timeout by default). This aims to provide some clarity around unsuccessful
responses and how Finagle deals with them.

Types of Unsuccessful Responses
-------------------------------

Since Finagle is based on `Futures` which may be satisfied with arbitrary
`Throwables`, the state space of possible unsuccessful responses is very large.
They fall into the following categories.

**Throwables with FailureFlags**
  :src:`FailureFlags <com/twitter/finagle/FailureFlags.scala>` allow for
  signaling about how to handle the response. Some `FailureFlags` are propagated
  across service boundaries over ThriftMux and Http. See `Failure Flags`_ below.

**Throwables with Sources**
  Sources contain additional information about what was happening when the
  exception was encountered. A throwable may have both Sources and FailureFlags.

  - Note: :src:`com.twitter.finagle.Failures <com/twitter/finagle/Failure.scala>`,
    which can sometimes be seen in logs, are un-named exceptions which can contain
    both sources and FailureFlags. Failures lack specificity of named exceptions.

**Finagle Exceptions**
  Internally, Finagle has exceptions for many different
  occasions. Many of these can be found in
  :src:`Exceptions.scala <com/twitter/finagle/Exceptions.scala>`. Occasionally,
  these will be defined elsewhere, such as in a relevant module. These exceptions
  cover common cases that Finagle may encounter when servicing a request, such as
  `ConnectionRefusedException` when a connection to a given address is refused,
  or a `RequestTimeoutException` when a service fails to respond in a prescribed
  amount of time.

**Protocol Specific Unsuccessful Responses**
  Some protocols have built-in support for a wide range of unsuccessful responses.
  For example HTTP provides many response codes such as 2XX: success, 4XX: failed
  due to client error, or 5XX: failed due to server error. Wikipedia provides a
  comprehensive list of `http response codes`_.

**User-Specified Unsuccessful Responses**
  Users may also wish to treat certain successful-looking responses as failures.
  For example, a Thrift struct representing the response to a query may return
  no results. From the perspective of the service, this is normal, but the client
  may wish to consider this empty result set situation as an exceptional case.
  In those cases, Finagle is unable to act appropriately unless a
  `ResponseClassifier` or `RetryPolicy` is configured. See
  `Handling Unsuccessful Responses`_ below for details.


Handling Unsuccessful Responses
-------------------------------

Finagle clients [#standard]_ make decisions about behavior by examining
`FailureFlags`. For exceptions that originate from within Finagle,
these flags are set appropriately. However, for user specified unsuccessful
responses, this information does not automatically exist.

Users have two mechanisms available to provide this information:
:ref:`ResponseClassifiers <response_classification>` and
:ref:`RetryPolicies <client_retries>`. Response classification is used for stats
recording and :ref:`circuit breaking <client_circuit_breaking>`. `RetryPolicies`
are used to control which responses will be retried.

Users should also handle exceptions in application code via `Future#handle` or
`Future#rescue`. This may also take place in a filter which transforms responses,
allowing for additional composition.

.. code-block:: scala

  import com.twitter.finagle.{SimpleFilter, Service, Protocol}
  import com.twitter.finagle.protocol.{Response, Request, NotFoundError}
  import com.twitter.finagle.service.StatsReceiver
  import com.twitter.util.Future

  ...

  val rawService: Service[Request, Response] = Protocol.client.newService("service.com")

  val defaultValueFilter = new SimpleFilter[Request, Response] {
    def apply(req: Request, service: Service[Request, Response]): Future[Response] = {
      service(req).handle {
        case _: NotFoundError => MyDefaultValueResponse
      }
    }
  }

  // Record stats after custom exception handling
  val logicalRequestStats = new StatsReceiver(stats.scope("logical"))

  val lookupService: Service[Request, Response] = logicalRequestStats
    .andThen(defaultValueFilter)
    .andThen(rawService)


Sending Unsuccessful Responses
------------------------------

A service that wishes to tap into Finagle's client-side exception handling
mechanisms should respond with a `Throwable` that extends `FailureFlags` with
flags set appropriately. An easy way to create a generic unnamed response like
this is by using the convenience methods in :src:`Failure.scala <com/twitter/finagle/Failure.scala>`

In a situation where a service would normally be able to process a request but
is temporarily overloaded, a :ref:`nack <glossary_nack>` response is appropriate.
The client receiving this will automatically retry the request via the `Retries`
module. This is the recommended way of exerting
:ref:`back pressure <glossary_back_pressure>` on clients.

.. caution::

  A nack response implies that no work has been done to process the request.
  This contract should be adhered to when using back pressure.

.. code-block:: scala

  import com.twitter.finagle.Failure
  import com.twitter.util.Future

  // Using Failure.rejected - Creates an exception with FailureFlags
  // that's flagged Rejected and Retryable. This is a typical Nack response.
  Future.exception(Failure.rejected("Too busy to handle the request"))

Sometimes it is useful to explicitly disable retries on a request on the client
side. By constructing a response as shown below, the NonRetryable flag will
propagate back across service boundaries. This can be used to mitigate
:ref:`retry storms<glossary_retry_storm>`.

.. code-block:: scala

  import com.twitter.finagle.Failure
  import com.twitter.util.Future

  // Create a rejected response, and flag it as non-retryable.
  val exn = Failure.rejected("Retry limit exceeded to service X").asNonRetryable
  Future.exception(exn)

If a service wishes to send an application-level exception, Finagle will deliver
it to the client without any special processing. As mentioned above, clients can
this behavior by setting up `ResponseClassifiers` and `RetryPolicies`. For
instance, if a client has set `ThriftExceptionsAsFailures` as its response
classifier, those application level Thrift exceptions will be treated as
non-retryable failures.

Exception or FailureFlags?
~~~~~~~~~~~~~~~~~~~~~~~~~~

For application-level exceptions, such as those defined in Thrift interfaces,
services should use those exceptions. For situations where the response should
propagate across multiple server/client boundaries or to use any signaling with
`FailureFlags`, respond with a `Throwable` that extends `FailureFlags` as
suggested above. A quick way to create such a response (if its name is not
important) is to use `Failure`'s convenience methods.

If a custom `Throwable` is used, only its message and `FailureFlags` (if any) will
propagate across multiple service boundaries. Any other information,
such as stack traces, will be discarded. Due to Finagle's asynchronous nature,
stack traces are not particularly useful. Instead Finagle provides built-in
support for distributed tracing systems.

Failure Flags
-------------

`FailureFlags` can signal what should be done with an unsuccessful response or
provide extra information for stats gathering or other measurements. This list
of flags is not comprehensive as some flags are stripped out as they leave
Finagle clients and are not exposed to users. The following table describes these
flags, what they mean, how they're used internally, and which protocols support
passing them through.

.. csv-table::
  :header: Flag, Indicates, Protocols, Used by

  "Rejected","No attempt was made to do any work on this request.","ThrifMux, Http","NackAdmissionFilter, HttpNackFilter, RequeueFilter"
  "Interrupted","Something intentionally stopped this request, so it will not be retried.","None","RequeueFilter"
  "NonRetryable","This request should not be retried","ThriftMux, Http","HttpNackFilter, RetryFilter, RequeueFilter"

.. _http response codes: https://en.wikipedia.org/wiki/List_of_HTTP_status_codes

.. [#standard] "Finagle client" or "Finagle server" here refers to the typical
   stack client or server that one creates by following methods outlined in the
   user's guide.
