Response Classification
-----------------------

To give Finagle visibility into application level success and failure
developers can provide classification of responses on clients and servers by using
:src:`response classifiers <com/twitter/finagle/service/package.scala>`.
This gives Finagle the proper domain knowledge and improves the efficacy of
:ref:`failure accrual <client_failure_accrual>` and more accurate
:ref:`success rate stats <metrics_stats_filter>`.

For HTTP clients and servers, using ``HttpResponseClassifier.ServerErrorsAsFailures`` often works
great as it classifies any HTTP 5xx response code as a failure. For Thrift/ThriftMux
clients you may want to use ``ThriftResponseClassifier.ThriftExceptionsAsFailures``
which classifies any deserialized Thrift Exception as a failure. For a large set of
use cases these should suffice. Classifiers get wired up to your client and server in a
straightforward manner, for example, in a ThriftMux client:

.. code-block:: scala

  import com.twitter.finagle.ThriftMux
  import com.twitter.finagle.thrift.service.ThriftResponseClassifier

  ThriftMux.client
    ...
    .withResponseClassifier(ThriftResponseClassifier.ThriftExceptionsAsFailures)

In an HTTP server:

.. code-block:: scala

  import com.twitter.finagle.Http
  import com.twitter.finagle.http.service.HttpResponseClassifier

  Http.server
    ...
    .withResponseClassifier(HttpResponseClassifier.ServerErrorsAsFailures)

If a classifier is not specified on a client or server or if a user's classifier isn't
defined for a given request/response pair then ``ResponseClassifier.Default``
is used. This gives us the simple classification rules of responses that are
``Returns`` are successful and ``Throws`` are failures.

Custom Classifiers
~~~~~~~~~~~~~~~~~~

Writing a custom classifier requires understanding of the few classes used. A
``ResponseClassifier`` is a ``PartialFunction`` from ``ReqRep`` to
``ResponseClass``.

Let's work our way backwards through those, beginning with ``ResponseClass``.
This can be either ``Successful`` or ``Failed`` and those values are
self-explanatory. There are three constants which will cover the vast majority
of usage: ``Success``, ``NonRetryableFailure`` and ``RetryableFailure``. While
as of today there is no distinction made between retryable and non-retryable
failures, this lays the groundwork for use in the future.

A ``ReqRep`` is a request/response struct with a request of type ``Any`` and a
response of type ``Try[Any]``. While all of this functionality is called
response classification, you’ll note that classifiers make judgements on both a
request and response.

Creating a custom ``ResponseClassifier`` is fairly straightforward for HTTP
as the ``ReqRep`` is an ``http.Request`` and ``Try[http.Response]`` pair.
Here is an example that counts HTTP 503s as failures:

.. code-block:: scala

  import com.twitter.finagle.http
  import com.twitter.finagle.service.{ReqRep, ResponseClass, ResponseClassifier}
  import com.twitter.util.Return

  val classifier: ResponseClassifier = {
    case ReqRep(_, Return(r: http.Response)) if r.statusCode == 503 =>
      ResponseClass.NonRetryableFailure
  }

Note that this ``PartialFunction`` isn't total which is ok due to Finagle
always using user defined classifiers in combination with
``ResponseClassifier.Default`` which will cover all cases.

Thrift and ThriftMux Classifiers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Thrift and ThriftMux classifiers require a bit more care as the request and
response types are not as obvious. This is because there is only a single
``Service`` from ``Array[Byte]`` to ``Array[Byte]`` for all the methods of an
IDL's service. To make this workable, there is support in Scrooge and
``Thrift/ThriftMux.newService`` and ``Thrift/ThriftMux.newClient`` code to
deserialize the responses into the expected application types so that
classifiers can be written in terms of the Scrooge generated request type,
``$Service.$Method.Args``, and the method's response type. Given an IDL:

.. code-block:: none

  exception NotFoundException { 1: string reason }

  service SocialGraph {
    i32 follow(1: i64 follower, 2: i64 followee) throws (1: NotFoundException ex)
  }

One possible classifier would be:

.. code-block:: scala

  import com.twitter.finagle.service.{ReqRep, ResponseClass, ResponseClassifier}

  val classifier: ResponseClassifier = {
    // #1
    case ReqRep(_, Throw(_: NotFoundException)) =>
      ResponseClass.NonRetryableFailure

    // #2
    case ReqRep(_, Return(x: Int)) if x == 0 =>
      ResponseClass.NonRetryableFailure

    // #3
    case ReqRep(SocialGraph.Follow.Args(a, b), _) if a <= 0 =>
      ResponseClass.NonRetryableFailure
  }

If you examine that classifier you'll note a few things. First (#1), the
deserialized ``NotFoundException`` can be treated as a failure. Next (#2), a
"successful" response can be examined to enable services using status codes to
classify errors. Lastly (#3), the request can be introspected to make the
decision.

Other Details
~~~~~~~~~~~~~

If you have a response classifier that categorizes ``Returns`` as
failures, note that they will be counted in the ``StatsFilter``
as a ``com.twitter.finagle.service.ResponseClassificationSyntheticException`` in the
``StatsReceiver`` to indicate when this happens. See the
`FAQ <https://twitter.github.io/finagle/guide/FAQ.html#what-is-a-com-twitter-finagle-service-responseclassificationsyntheticexception>`_
for more details.
