FAQ
===

What's a `CancelledRequestException`?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When a client to a Finagle server disconnects, the server raises
a *cancellation* interrupt on the pending Future. This is done to
conserve resources and avoid unnecessary work: the upstream
client may have timed the request out, for example. Interrupts on
futures propagate, and so if that server is in turn waiting for a response
from a downstream server it will cancel this pending request, and so on.

This is the source of the :api:`CancelledRequestException <com.twitter.finagle.CancelledRequestException>` -- 
when a Finagle client receives the cancellation interrupt while a request is pending, it 
fails that request with this exception.

You can disable this behavior by using the :api:`MaskCancelFilter <com.twitter.finagle.filter.MaskCancelFilter>`:

::

	val service: Service[Req, Rep] = ...
	val masked = new MaskCancelFilter[Req, Rep]

	val maskedService = masked andThen service

Note that most protocols do not natively support request cancellations
(though modern RPC protocols like :api:`Mux <com.twitter.finagle.Mux$>`
do). In practice, this means that for these protocols, we need to disconnect
the client to signal cancellation, which in turn can cause undue connection
churn.
