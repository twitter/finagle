FAQ
===

What's a `CancelledRequestException`?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When a client connected to a Finagle server disconnects, the server raises
a *cancellation* interrupt on the pending Future. This is done to
conserve resources and avoid unnecessary work: the upstream
client may have timed the request out, for example. Interrupts on
futures propagate, and so if that server is in turn waiting for a response
from a downstream server it will cancel this pending request, and so on.

This is the source of the :API:`CancelledRequestException <com.twitter.finagle.CancelledRequestException>` --
when a Finagle client receives the cancellation interrupt while a request is pending, it
fails that request with this exception.

You can disable this behavior by using the :API:`MaskCancelFilter <com.twitter.finagle.filter.MaskCancelFilter>`:

::

	val service: Service[Req, Rep] = ...
	val masked = new MaskCancelFilter[Req, Rep]

	val maskedService = masked andThen service

Note that most protocols do not natively support request cancellations
(though modern RPC protocols like :API:`Mux <com.twitter.finagle.Mux$>`
do). In practice, this means that for these protocols, we need to disconnect
the client to signal cancellation, which in turn can cause undue connection
churn.

Why is "com.twitter.common.zookeeper#server-set" not found?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Some of our libraries still aren't published to maven central.  If you add

::

	resolvers += "twitter" at "http://maven.twttr.com"

to your sbt configuration, it will be able to pick up the libraries which are
published externally, but not yet to maven central.

How do I change my timeouts in the Finagle 6 APIs?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We introduced a new, preferred API for constructing Finagle `Client`s and `Server`s.
Where the old API used `ServerBuilder`/`ClientBuilder` with Codecs, the new APIs use
`Proto.newClient`.

Old APIs:

::

	val client = ClientBuilder
	  .codec(Http)
	  .hosts("localhost:10000,localhost:10001,localhost:10003")
	  .hostConnectionLimit(1)
	  .build()

New APIs:

::

	val client = Http.newService("localhost:10000,localhost:10001")

The new APIs make timeouts more explicit, but we think we have a pretty good reason
for changing the API this way.

Timeouts are typically used in two cases:

A.  Liveness detection (tcp connect timeout)
B.  Application requirements (global timeout)

For liveness detection, it is actually fine for timeouts to be long.  We have a
default of 1 second.

For application requirements, you can use a service normally and then use
`Future#raiseWithin`.

::

	val get: Future[HttpResponse] = Http.fetchUrl("https://twitter.com/")
	get.raiseWithin(1.ms)

We found that having all of the extremely granular timeouts was making it harder
for people to use Finagle, since it was hard to reason about what all of the
timeouts did without knowledge of Finagle internals.  How is `tcpConnectTimeout`
different from `connectTimeout`?  How is a `requestTimeout` different from a
`timeout`?  What is an `idleReaderTimeout`?  How is it different from
`idleWriterTimeout`?  People would often cargo-cult bad configuration settings,
and it would be difficult to recover from the bad situation.  We also found that
they were rarely being used correctly, and usually only by very sophisticated
users.

We're encouraging users to avoid encoding application requirements in Finagle,
which was previously too easy to do via methods like `ClientBuilder#retries`, or
`ClientBuilder#timeout`.  These are fundamentally application-level concerns--
you're trying to meet an SLA, etc.  In general, in order to do what Finagle is
for, which is to deliver an rpc message to a cluster, we don't think you should
need a lot of configuration at all.  You should need to specify your protocol,
and a few details about the transport (ssl?  no ssl?), but that's about it.

Of course, there are some points where there are rough edges, and we haven't
figured out exactly what the right default should be.  We're actively looking
for input, and would love for the greater Finagle community to help us find good
defaults.
