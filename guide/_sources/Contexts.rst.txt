Contexts
========

Finagle uses Contexts_, which give you something akin to Java's ThreadLocals_
across :doc:`asynchronous <Futures>` code.
They give you access to request-scoped state, such as a request's deadline,
throughout the logical life of a request without requiring them to be explicitly passed.
Finagle explicitly manages them for you across threads and execution
contexts such as ``Future`` composition, ``FuturePools``, ``Timers``,
and in some cases — across the client/server boundary.

Contexts can be either *local* or *broadcast*.
Local contexts do not cross process boundaries while broadcast
contexts may be marshalled and transmitted across process boundaries.

Most ``Context`` usage is hidden behind other APIs, which means you may not
realize you are using this functionality. This example shows how a ``ClientId``
is available within ``Timer`` functions:

.. code-block:: scala

  scala> import com.twitter.conversions.DurationOps._, com.twitter.finagle.thrift.ClientId, com.twitter.finagle.util.HashedWheelTimer, com.twitter.util.{Await, Future}
  import com.twitter.conversions.DurationOps._
  import com.twitter.finagle.thrift.ClientId
  import com.twitter.finagle.util.HashedWheelTimer
  import com.twitter.util.{Await, Future}

  scala> val aClientId = ClientId("test-client") // Create a ClientId to use
  aClientId: com.twitter.finagle.thrift.ClientId = ClientId(test-client)

  scala> val clientIdInTimer: Future[String] =
           // Put that ClientId into scope and schedule
           // work on a Timer
           aClientId.asCurrent {
             HashedWheelTimer.Default.doLater(100.milliseconds) {
               // Check the value when the Timer's function is
               // evaluated 100 milliseconds later
               ClientId.current match {
                 case Some(cId) => cId.name
                 case None => "no-client-id"
               }
             }
           }
  clientIdInTimer: com.twitter.util.Future[String] = Promise@2147108131(state=Interruptible(List(),<function1>))

  scala> println(s"Timer saw: '${Await.result(clientIdInTimer)}'")
  Timer saw: 'test-client'

Commonly used instances
-----------------------

Because ``Context``\s are not passed directly into methods, discovery of which
ones exist is challenging.
To aid with this, here is a listing of some commonly used instances that Finagle
makes available:

Current trace id
~~~~~~~~~~~~~~~~

``com.twitter.finagle.tracing.Trace.id`` —
A broadcast ``Context`` that represents this request's distributed tracing ``TraceId``.

Current client id
~~~~~~~~~~~~~~~~~

``com.twitter.finagle.thrift.ClientId.current`` —
A broadcast ``Context`` that represents the client identifier of a request.

Current request deadline
~~~~~~~~~~~~~~~~~~~~~~~~

``com.twitter.finagle.context.Deadline.current`` —
A broadcast ``Context`` that represents when the request should be completed by.

Current requeue attempt
~~~~~~~~~~~~~~~~~~~~~

``com.twitter.finagle.context.Requeues.current`` —
A broadcast ``Context`` that represents which requeue attempt this request is. 
Requeues are retries on write exceptions (i.e. the original request was never sent to the server).
Will have ``attempt`` set to 0 if the request is not a requeue.

Current TLS session
~~~~~~~~~~~~~~~~~~~

``com.twitter.finagle.transport.Transport.sslSessionInfo`` _
A local ``Context`` that represents the ``Transport``\s
``com.twitter.finagle.ssl.session.SslSessionInfo``. If a TLS session is established,
the ``SslSessionInfo`` provides access to the ``javax.net.ssl.SSLSession``, along with
the ``sessionId``, ``cipherSuite``, and both ``local`` and ``peer`` certificates. This
is an encompassing replacement for ``com.twitter.finagle.transport.Transport.peerCertificate``.

``com.twitter.finagle.transport.Transport.peerCertificate`` —
A local ``Context`` that represents the ``Transport``\s
``java.security.cert.Certificate`` if a TLS session is established.

Upstream Address
~~~~~~~~~~~~~~~~
``com.twitter.finagle.context.RemoteInfo.Upstream.addr`` —
A local ``Context`` that represents the upstream (ingress)
``java.net.SocketAddress`` of the current request.

Backup request indicator
~~~~~~~~~~~~~~~~~~~~~~~~
``com.twitter.finagle.context.BackupRequest.wasInitiated`` —
A broadcast ``Context`` that indicates if the request was initiated by a backup
request.

Creating new Contexts
---------------------

Instances should be immutable or must provide proper memory visibility
so that changes will be seen across thread boundaries.

Care should be taken when creating new broadcast ``Context``\s as they
will be sent across the entire downstream request graph. Considerations
should include serialization/deserialization costs, serialized size, and
schema evolution.

.. _Contexts: https://github.com/twitter/finagle/blob/release/finagle-core/src/main/scala/com/twitter/finagle/context/Contexts.scala

.. _ThreadLocals: https://docs.oracle.com/javase/8/docs/api/java/lang/ThreadLocal.html
