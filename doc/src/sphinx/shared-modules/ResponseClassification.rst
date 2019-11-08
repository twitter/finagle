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
clients and servers you may want to use ``ThriftResponseClassifier.ThriftExceptionsAsFailures``
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

``ResponseClassifier`` is a ``PartialFunction`` from ``ReqRep`` to
``ResponseClass``. Custom classifiers allow the user to tell Finagle what
constitutes a failed outcome, and also what to do about it. Users define
classifiers in terms of ``ReqRep`` and ``Try``.

``rc``, defined below, is a classifier that tells Finagle that ``Throw`` means
failure, and ``Return`` means success.

.. code-block:: scala

  val rc: ResponseClassifier = {
    case ReqRep(req, Throw(exc)) => ResponseClass.RetryableFailure
    case ReqRep(req, Return(rep)) => ResponseClass.Success
  }

A ``ReqRep`` is a request-response pair. This is so that classifiers can make
judgements on both a request and response.

More than just telling Finagle if this ``ReqRep`` is a successful or failed
outcome, it also gives Finagle a hint about what it should do next. Finagle can
respond to failed outcomes with some nuance, for example, it may retry the
operation.

``ResponseClass`` defines three classes of failure:

- ``NonRetryableFailure``: Something went wrong, don't retry.
- ``RetryableFailure``: Something went wrong, consider retrying the operation.
- ``Ignorable``: Something went wrong, but it can be ignored.

And, of course, ``Success`` means that the operation succeeded.
``Ignorable`` does not apply to ``Success`` because it is a mapping from
``FailureFlags.Ignorable`` which only applies to ``Failure`` and not any
arbitrary response.

It's important to note that classifiers are only consulted but not obeyed. For
example, a classifier may emit ``Ignorable`` for a given ``ReqRep`` but what
actually happens depends on how the caller chooses to interpret ``Ignorable``.
Similarly, just because a classifier emits ``RetryableFailure`` does not mean
the caller will retry the operation.

Now that we've covered the basics, let's look at an example in HTTP. Here is
an example that counts HTTP 503s as failures:

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
IDL's service. To make this workable, there is support in Scrooge,
``Thrift/ThriftMux.newService``, ``Thrift/ThriftMux.newClient`` and ``Thrift/ThriftMux.serve``
code to deserialize the responses into the expected application types so that
classifiers can be written in terms of the Scrooge generated request type,
``$Service.$Method.Args``, and the method's response type. Given an IDL:

.. code-block:: none

  exception NotFoundException { 1: string reason }
  exception InvalidQueryException {
    1: i32 errorCode
  }

  service SocialGraph {
    i32 follow(1: i64 follower, 2: i64 followee) throws (
      1: NotFoundException ex1,
      2: InvalidQueryException ex2
    )
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

    // #3 *Caution*
    case ReqRep(SocialGraph.Follow.Args(a, b), _) if a <= 0 =>
      ResponseClass.NonRetryableFailure

    // #4
    case ReqRep(_, Throw(_: InvalidQueryException)) =>
      ResponseClass.Success
  }

If you examine that classifier you'll note a few things. First (#1), the
deserialized ``NotFoundException`` can be treated as a failure. Second (#2), a
"successful" response can be examined to enable services using status codes to
classify errors. Next (#3), the request can be introspected to make the
decision - *HOWEVER* - if an exception is thrown at the Mux layer
(ex: ``c.t.f.mux.ClientDiscardedRequestException``) there will **NOT**
be a match against (#3). This style (#3) should be avoided for Thrift and ThriftMux.
Instead, prefer to handle request specific details at the application layer, such as creating
a `Filter <ServicesAndFilters.html#filters>`_ to reject the request, and reserve Response
Classification to deal with wire level response concerns. Lastly (#4), the deserialized
``InvalidQueryException`` can be treated as a successful response.

Other Details
~~~~~~~~~~~~~

If you have a response classifier that categorizes non-Exceptions as failures, this includes
Thrift Responses (#2) or embedded Thrift Exceptions (#1), note that they will be counted in
the ``StatsFilter`` as a ``com.twitter.finagle.service.ResponseClassificationSyntheticException``
in the ``StatsReceiver`` to indicate when this happens. See the
`FAQ <https://twitter.github.io/finagle/guide/FAQ.html#what-is-a-com-twitter-finagle-service-responseclassificationsyntheticexception>`_
for more details.
