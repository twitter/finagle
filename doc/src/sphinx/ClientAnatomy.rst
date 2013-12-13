Anatomy of a Client
===================

In this section we'll examine the internals of a Finagle client by
outlining a typical process for integrating a new codec into
the finagle stack. We'll build a client in a bottom up fashion,
arriving at a robust :ref:`Service <services>` that can send and receive
strings to and from a :doc:`symmetric server<ServerAnatomy>`.

Note, this section aims to expose the nuts and bolts used to build
finagle clients. These concepts aren't particularly neccessary
in understanding how to use a finagle client.

Client Protocol
---------------

We'll use a `Netty Channel Pipeline <http://netty.io/3.6/api/org/jboss/netty/channel/ChannelPipeline.html>`_
to frame our network traffic. Our client pipeline defines a
UTF-8 newline delimited protocol:

.. includecode:: code/client-server-anatomy/Netty3.scala#clientpipeline

Transporter
-----------

Transporters are responsible for connecting :ref:`Transports <transport_interface>`
to peers — they establish a session. Our client uses a
:src:`Netty3Transporter <com/twitter/finagle/netty3/client.scala>`, however
the use of other Transporters is fully supported.

.. includecode:: code/client-server-anatomy/EchoClient.scala#transporter

Request Dispatcher
------------------

A dispatcher turns a Transport (a stream of objects) into a Service
(request-response pairs). It must manage all outstanding requests,
pairing incoming responses to their respective requests.
The simplest kind of dispatcher is called a :src:`SerialClientDispatcher <com/twitter/finagle/dispatch/ClientDispatcher.scala>`,
which allows only a single outstanding request (concurrent requests are queued) [#]_.

Our client will employ the SerialClientDispatcher.

.. [#] Note that finagle also includes a dispatcher that can
       pipeline requests, i.e., allow more than one outstanding request.
       It's possible to create a custom dispatcher as well. For example,
       :api:`Mux <com.twitter.finagle.Mux$>`, which support true mutliplexing,
       defines a custom dispatcher.

A Basic Client
--------------

Given a defined transporter and request dispatching strategy, we can compose the
two and create a client:

.. includecode:: code/client-server-anatomy/EchoClient.scala#explicitbridge

This sort of wiring is exactly what the :src:`Bridge <com/twitter/finagle/client/Bridge.scala>`
object does for us with the additional caveat that the resulting
Future[Service[Req, Rep]] is converted into a ServiceFactory (a factory pattern
defined for Services).

Finally, we can dispatch requests over our client,

.. includecode:: code/client-server-anatomy/EchoClient.scala#basicclientexample

Assuming we have a server willing to listen, we can expect
a response:

::

  $ ./sbt run
  > hello

A Robust Client
---------------

Our client is a Service, so we can supply additional
behavior to make our client more robust using
filters:

.. includecode:: code/client-server-anatomy/EchoClient.scala#filters

composing these filters [#]_ with our basic client demonstrates
the composable components used throughout finagle.

.. includecode:: code/client-server-anatomy/EchoClient.scala#robustclient

This client is a good start, but we cannot dispatch concurrent requests
to a single host, nor load balance over multiple hosts. A typical Finagle client
affords us the ability to dispatch a large number of concurrent requests.

.. [#] The use of the MaskCancelFilter in the example filter stack
       ensures that timeout exceptions don't propagate to our
       bottom most service which, in this case, represents a dispatcher.
       Without this guarantee, the service would be closed after the first
       timeout exception. This becomes unnecessary when we wrap a DefaultClient
       with the same filters because the semantics of Service#close() change
       with respect to finagle's connection pool.

The Default Client
------------------

The :src:`DefaultClient <com/twitter/finagle/client/DefaultClient.scala>`
decorates a client with many prominent features including
:ref:`load balancing <heap_balancer>` over multiple hosts
and :ref:`connection pooling <watermark_pool>` per host.
We can create a DefaultClient with the bridge defined above:

.. includecode:: code/client-server-anatomy/EchoClient.scala#defaultclient

Armed with this new client, we can connect to a :src:`Group <com/twitter/finagle/Group.scala>` (multiple hosts).

::

  client.newClient(Group[SocketAddress](
    "localhost:8080",
    "localhost:8081",
    "localhost:8082")): ServiceFactory[String, String]

Requests sent to this client are load balanced across these
hosts and each host maintains a connection pool, thus
allowing concurrent dispatches.

