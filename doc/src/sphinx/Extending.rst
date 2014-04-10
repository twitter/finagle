Extending Finagle
=================

In this section we'll examine the internals of Finagle clients and
servers by outlining a typical process for integrating a new codec into
the Finagle stack. Starting with the transport layer, we'll build
a simple client that can send strings over a network socket and a server
capable of echoing strings received over the same socket.

Note, this section aims to and exposes the nuts and bolts used to build
Finagle clients and servers. These concepts aren't particularly neccessary
in understanding how to use them.

If you haven't yet, first read the :doc:`quickstart<Quickstart>`
to understand some of Finagle's core concepts
(i.e. :doc:`Futures <Futures>`, :doc:`Services and Filters<ServicesAndFilters>`).

The entire example is available, together with a self-contained script
to launch sbt, in the Finagle git repository:

::

  $ git clone https://github.com/twitter/finagle.git
  $ cd finagle/doc/src/sphinx/code/client-server-anatomy
  $ ./sbt run

.. _transport_interface:

Transport Layer
---------------

Finagle represents the OSI transport layer as a typed stream that
may be read from and written to asynchronously. The noteworthy methods
in the interface are defined as such:

::

  trait Transport[In, Out] {
    def read(): Future[Out]
    def write(req: In): Future[Unit]
    ...
  }

Most Transports are implemented using `Netty <http://netty.io>`_
for I/O multiplexing and protocol codecs.

Server Protocol
---------------

To frame data received over the network with respect to our
protocol we use a `Netty Channel Pipeline <http://netty.io/3.6/api/org/jboss/netty/channel/ChannelPipeline.html>`_.
Our server pipeline defines a UTF-8 text-based newline delimited protocol:

.. includecode:: code/client-server-anatomy/Netty3.scala#serverpipeline

Listener
--------

The mechanics of listening over a network socket and
translating our pipeline into a typed transport are defined by the
:src:`Netty3Listener <com/twitter/finagle/netty3/Netty3Listener.scala>`.

Together with our server pipeline, we can define such a listener:

.. includecode:: code/client-server-anatomy/EchoServer.scala#serverlistener

This implements the :src:`Listener <com/twitter/finagle/server/Listener.scala>`
interface that exposes a `listen` method:

::

  def listen(addr: SocketAddress)(serveTransport: Transport[In, Out] => Unit)

That is, given a socket address to bind and listen, `serveTransport` is dispatched
for each new connection established.

For example, here is a simple echo server:

::

   val address = new java.net.InetSocketAddress("localhost", 8080)
   val echoServer = EchoListener.listen(address) { transport =>
      transport.read() flatMap { transport.write(_) } ensure transport.close()
    }

We can now send requests over this socket and have them echoed back:

::

  > echo "hello" | nc localhost 8080
  > hello

The `serveTransport` function defined above is primitive. For example,
it closes each connection after one read and write. Finagle provides tools
to provision a transport with more sophisticated behavior.

Server Dispatcher
-----------------

The :src:`server dispatcher <com/twitter/finagle/dispatch/ServerDispatcher.scala>`
queues concurrent incoming requests and serially dispatches
them over a Transport. The data read from the Transport
is funneled through a service object and the resulting value
is written back to the transport. Additionally, the
server dispatcher drains existing requests before
closing a transport.

We could translate our `serveTransport` function to use this facility:

.. includecode:: code/client-server-anatomy/EchoServer.scala#simplelisten

A nice consequence of using a :ref:`Service <services>` to process
data received over the transport is the ability to furnish our server with
additional behavior via :doc:`Filters<ServicesAndFilters>`. This is exactly
what Finagle's default server implementation does.

Default Server
--------------

Finagle's :src:`DefaultServer <com/twitter/finagle/server/DefaultServer.scala>`
provides appropriate features for building a robust server.
Using the listener and dispatcher defined above, we can define a DefaultServer:

.. includecode:: code/client-server-anatomy/EchoServer.scala#defaultserver

Then serve our echo service:

.. includecode:: code/client-server-anatomy/EchoServer.scala#defaultserverexample

Client Protocol
---------------

Again, we'll use a `Netty Channel Pipeline <http://netty.io/3.6/api/org/jboss/netty/channel/ChannelPipeline.html>`_
to frame our network traffic. Our client pipeline defines a
UTF-8 newline delimited protocol:

.. includecode:: code/client-server-anatomy/Netty3.scala#clientpipeline

Transporter
-----------

A :src:`Transporter <com/twitter/finagle/clients/Transporter.scala>` is responsible for connecting
a :ref:`Transport <transport_interface>` to a peer — it establishes a session. Our client uses a
:src:`Netty3Transporter <com/twitter/finagle/netty3/client.scala>`, however
the use of other Transporters is fully supported.

.. includecode:: code/client-server-anatomy/EchoClient.scala#transporter

Client Dispatcher
-----------------

A client dispatcher turns a Transport (a stream of objects) into a Service
(request-response pairs). It must manage all outstanding requests,
pairing incoming responses to their respective requests.
The simplest kind of dispatcher is called a :src:`SerialClientDispatcher <com/twitter/finagle/dispatch/ClientDispatcher.scala>`,
which allows only a single outstanding request (concurrent requests are queued) [#]_.

Our client will employ the SerialClientDispatcher.

.. [#] Note that Finagle also includes a dispatcher that can
       pipeline requests, i.e., allow more than one outstanding request.
       It's possible to create a custom dispatcher as well. For example,
       :doc:`Mux <Protocols>`, which support true multiplexing,
       defines a custom dispatcher.

A Basic Client
--------------

Given a defined transporter and request dispatching strategy, we can compose the
two and create a client:

.. includecode:: code/client-server-anatomy/EchoClient.scala#explicitbridge

Finally, we can dispatch requests over our client,

.. includecode:: code/client-server-anatomy/EchoClient.scala#basicclientexample

Assuming we have a server willing to listen, we can expect a response:

::

  $ ./sbt run
  > hello

A Robust Client
---------------

Our client is a Service, so we can supply additional
behavior to make our client more robust using
filters:

.. includecode:: code/client-server-anatomy/EchoClient.scala#filters

Composing these filters [#]_ with our basic client demonstrates
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
       with respect to Finagle's connection pool.

Default Client
--------------

The :src:`DefaultClient <com/twitter/finagle/client/DefaultClient.scala>`
decorates a client with many prominent features including
:ref:`load balancing <load_balancer>` over multiple hosts
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
