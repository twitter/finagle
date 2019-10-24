Signaling Failure within Finagle
================================

For a combination of historical and practical reasons, there are many ways to
signal an unsuccessful response in Finagle. When extending Finagle, it is
important to use the correct signaling mechanisms.

For more information on how these are presented to users outside of Finagle, see
the :doc:`../UnsuccessfulResponses` page in the User's Guide.

History
-------

Historically, Finagle relied exclusively on a hierarchy of classes of exceptions
defined in :src:`Exceptions.scala <com/twitter/finagle/Exceptions.scala>`, with
each exception corresponding to a specific problem. Pattern matching on
the exception type was used to classify the exceptions and make decisions about
how to handle them. As the number of exceptions grew, pattern matching became
unwieldy and `Failures`, were introduced as uniform way for Finagle to signal
failure internally. Failures are more general and the successor to ad-hoc
Exceptions. They are composable, immutable, and allow us to send signals across
process boundaries.

However, when fleshing out Finagle's back pressure strategy, it became useful
for exceptions other than just `Failure` to also be able to be flagged with
various attributes that propagate across service boundaries. In order to
accomplish this, the :src:`FailureFlags <com/twitter/finagle/FailureFlags.scala>`
trait was introduced. `FailureFlags` can be applied to any `Throwable`, which
simplifies the migration strategy as the type of `Throwable` no longer has to
change.

Currently, a `Failure` can be thought of as a "Finagle runtime exception" as
it contains no type-based information. `Failure` provides a convenient API for
building `FailureFlag`-style exceptions, but should not be used for inspection.


Creating exceptions with FailureFlags
-------------------------------------

Any new module should ensure that the exceptions it creates have `FailureFlags`.
This can be done by creating a new class that extends `FailureFlags` or by using
convenience methods in
:src:`Failure.scala <com/twitter/finagle/Failure.scala>`.

**If the class name of the exception is helpful to users** use a custom
exception class that extends `FailureFlags`. The class name will show up in logs
and stats. This is preferred.

**If class name of the exception is unimportant or it is truly a runtime-exception
type error**, creating a new `Failure` with flags set correctly is appropriate.

Responding with an arbitrary Throwable or Exception is not
recommended as they will be converted generic exceptions. For instance,
Thrift will convert them to `TApplicationExceptions`.


Examining and Modifying FailureFlags
------------------------------------

To inspect a `Throw` response for flags, pattern match against the FailureFlags trait.

.. code-block:: scala

  import com.twitter.finagle.FailureFlags
  import com.twitter.util.Throw

  response match {
    case Throw(f: FailureFlags[_]) if f.isFlagged(FailureFlags.$FLAG) =>
      // Logic here.
    case ...
  }

`FailureFlags` responses should be immutable. All of the convenience methods for
flagging and unflagging make copies. Use these methods to modify flags.

.. code-block:: scala

  import com.twitter.finagle.FailureFlags
  import com.twitter.util.Throw

  // Flag a response as "nonRetryable"
  response match {
    case Throw(f: FailureFlags[_]) => Future.exception(f.asNonRetryable)
  }

FailureFlags and Failures outside of Finagle
--------------------------------------------

In a typically-configured client, the :src:`ProcessFailures
<com/twitter/finagle/Failure.scala>` filter will unwrap (See `Failure#unwrap`)
Failures and mask out `FailureFlags` that are unsafe to pass along to users.

Unwrapping a `Failure` will transform the `Failure` into whatever Throwable it
had wrapped, discarding any additional information carried by the `Failure`. The
reason for this unwrapping is because of the large API burden of moving over to
`Failure` completely. By switching to a `FailureFlags` trait, this problem is
avoided.

If a Failure does not wrap anything, the `ProcessFailures` filter will only mask
off unsafe `FailureFlags`.

Propagating Responses
---------------------

There's some nuance as to how `FailureFlags` and unflagged exceptions traverse
client/service boundaries and differ across protocols. These tables cover
how unsuccessful responses propagate from service to client over the two most
common protocols: HTTP and ThriftMux. The columns indicate how the response is
transformed along the way.

ThriftMux
~~~~~~~~~

ThriftMux will currently pass the following `FailureFlags`: `Rejected`,
`Retryable`, `NonRetryable`. See :finagle-mux-src:`MuxFailure.scala <com/twitter/finagle/mux/transport/MuxFailure.scala>`.

.. csv-table::
  :header: Cause,Finagle (as Throw),ThriftMux,Mux,Client Sees (as Throw)

  "`SomeException(message)` raised in handling request","`SomeException`","`TApplicationException`","`RdispatchOk`","`TApplicationException(message)`"
  "`Failure.rejected(message)`","`Failure`","`Failure`","`RdispatchNack`","`Failure(message)` flagged `Rejected` & `NonRetryable`"
  "any `Throwable with HasFailureFlags`","`T: HasFailureFlags`","`T: HasFailureFlags`","`RdispatchError`","`Failure(throwable.toString)` with supported `FailureFlags`"

`Failure.rejected` creates a `Failure` with `Rejected` and `Retryable` flags.

If a custom `Throwable` is used, its message and `FailureFlags` (if any) will
propagate but it will be converted in to a `Failure`. Any other information,
such as stack traces, will be discarded. Due to Finagle's asynchronous nature,
stack traces are not particularly useful. Instead Finagle provides built in
support for distributed tracing systems.

When sending these responses across service boundaries, `toString` is called on
each hop. This means that messages will appear like
"Failure(Failure(Failure(Underlying cause... " when logged. This may change in
the future.

HTTP
~~~~

Http will pass the flag `Rejected` (a retryable nack) or a combination of `Rejected`
and `NonRetryable` (a nonretryable nack) via headers. HTTP will completely discard
any information (stack trace, message, etc) except for the flags. See
:finagle-base-http-src:`HttpNackFilter.scala
<com/twitter/finagle/http/filter/HttpNackFilter.scala>`

.. csv-table::
  :header: Cause,Finagle (as Throw),Http,Client Sees (as Throw)

  "`SomeException(message)` raised in handling request","`SomeException`","`500 Internal Server Error`","`Return(Http.Response(500 w/ message))`"
  "any `Throwable with HasFailureFlags` flagged `Rejected` and `NonRetryable`","`T: HasFailureFlags`","`503 Service Unavailable` w/ 'finagle-http-nack' header","`Failure` flagged `Rejected` and `NonRetryable`"
  "any `Throwable with HasFailureFlags` flagged `Rejected`","`T: HasFailureFlags`","`503 Service Unavailable` w/ 'finagle-http-nonretryable-nack' header","`Failure` flagged `Rejected` and `NonRetryable`"

