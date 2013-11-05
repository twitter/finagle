Anatomy of a Server
===================

In this section we'll examine the internals of a Finagle server
by outlining a typical process for integrating a new codec into
the finagle stack. Starting with the transport layer, we'll build
a simple server capable of echoing strings received over a network socket.

If you haven't yet, first read the :doc:`quickstart<Quickstart>`
to understand some of Finagle's core concepts
(i.e. :doc:`Futures <Futures>`, :doc:`Services and Filters<ServicesAndFilters>`).

Note, this section aims to expose the nuts and bolts used to build
finagle servers. These concepts aren't particularly neccessary
in understanding how to use a finagle server.

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
:src:`Netty3Listener <com/twitter/finagle/netty3/server.scala>`.

Together with our server pipeline, we can define such a listener:

.. includecode:: code/client-server-anatomy/EchoServer.scala#serverlistener

This implements the :src:`Listener <com/twitter/finagle/server/Listener.scala>`
interface that exposes a *listen* method:

::

  def listen(addr: SocketAddress)(serveTransport: Transport[In, Out] => Unit)

That is, given a socket address to bind and listen, *serveTransport* is dispatched
for each new connection established.

Thus, a simple echo server:

::

   val address = new java.net.InetSocketAddress("localhost", 8080)
   val echoServer = EchoListener.listen(address) { transport =>
      transport.read() flatMap { transport.write(_) } ensure transport.close()
    }

We can now send requests over this socket and have them echoed back:

::

  > echo "hello" | nc localhost 8080
  > hello

The *serveTransport* function defined above is primitive. For example,
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

We could translate our *serveTransport* function to use this facility:

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
