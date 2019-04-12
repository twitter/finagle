Extending Finagle
=================

In this section we'll examine the internals of Finagle by implementing
a client and server for a simple newline-delimited, string-based
protocol. We'll build a client capable of sending simple strings to a
server, which in turn may reply with another.

While we're going to dig a bit deeper here than in the rest of the
user's guide — implementing even a simple protocol requires a deeper
appreciation of Finagle's internals — the material presented here is not
necessary to understand how to use Finagle productively. However,
expert users may find the material useful.

If you haven't yet, first read the :doc:`quickstart<Quickstart>` to
understand some of Finagle's core concepts (i.e. :doc:`Futures
<Futures>`, :doc:`Services and Filters<ServicesAndFilters>`).

The entire example is available, together with a self-contained script
to launch `sbt`, in the Finagle git repository:

::

  $ git clone https://github.com/twitter/finagle.git
  $ cd finagle/doc/src/sphinx/code/client-server-anatomy
  $ ./sbt run

Stack
-----

Finagle's clients and servers comprise many relatively simple
components, arranged together in a stack. Each component is a
:api:`ServiceFactory <com/twitter/finagle/ServiceFactory>` that
composes other service factories. We've seen this before:
:doc:`filters <ServicesAndFilters>` are a kind of component.

This allows us to create simple components that we arrange into a
sophisticated whole. For example, we define timeout behavior as a
separate module; we then arrange to insert this module into the
composite stack wherever timeout behavior is required. Some of the
components included in Finagle's clients are discussed in the
:ref:`client documentation <client_modules>`.

While we can imagine stacking such modules together in a manual
fashion—for example, by building it bottoms-up, passing each
``ServiceFactory`` into the constructor of the next—this technique quickly
becomes onerous; it become very difficult to:

- parameterize each component, for example to set the sizes of
  connection pools
- inject dependencies such as
  :util:`StatsReceiver <com/twitter/finagle/stats/StatsReceiver>` implementations
- rearrange any part of the stack, for example to remove an
  unused timeout filter
- modify the stack for a specific use, for example to replace
  the connection pooling implementation or the load balancer

Traditionally, the :api:`ClientBuilder
<com/twitter/finagle/builder/ClientBuilder>` and :api:`ServerBuilder
<com/twitter/finagle/builder/ServerBuilder>` performed this duty in
the manner just described. While it worked well for a while, it
ultimately proved inflexible. With the needs and requirements of the
various protocol implementations like :ref:`Mux's <mux>`, as well as
more sophisticated new features, the builders became unworkable
monoliths.

:api:`Stack <com/twitter/finagle/Stack>` formalizes the concept of a
*stackable* component and treats a sequence of stackable components as
a first-class (immutable) value that may be manipulated like any other
collection. For example, modules may be inserted or removed from the
stack; you can map one stack to another, for example when
parameterizing individual modules.

The stack abstraction also formalizes the concept of a *parameter*—i.e.
a map of values used to construct an object. :api:`Stack.Params <com/twitter/finagle/Stack$$Params>`
is a kind of type-safe map used to hold parameters.

``Stack`` and ``Param`` work together like this: ``Stack`` represents the stack
of modules. These modules aren't *materialized*. Rather, they represent
a constructor for a ``ServiceFactory`` that accepts a ``Params`` map. Thus,
to extract the final ``ServiceFactory`` that represents the entirety of the stack,
we simply call ``stack.make(params)``.

Finagle defines default (polymorphic) stacks for both
:api:`clients <com/twitter/finagle/client/StackClient$#newStack[Req,Rep]:com.twitter.finagle.Stack[com.twitter.finagle.ServiceFactory[Req,Rep]]>` and
:api:`servers <com/twitter/finagle/server/StackServer$#newStack[Req,Rep]:com.twitter.finagle.Stack[com.twitter.finagle.ServiceFactory[Req,Rep]]>`.

We'll now discuss the constituent parts of Finagle's clients and servers.

.. _transport_interface:

Transport Layer
---------------

Finagle represents the OSI transport layer as a typed stream that may
be read from and written to asynchronously. The noteworthy methods in
the interface are defined as such:

.. code-block:: scala

  trait Transport[In, Out] {
    def read(): Future[Out]
    def write(req: In): Future[Unit]
    ...
  }

Most Transports are implemented using `Netty <https://netty.io>`_
for I/O multiplexing and protocol codecs.

You have a choice of whether to use `Netty 3 or Netty 4 <https://netty.io/wiki/new-and-noteworthy-in-4.0.html>`_
for your protocol implementation. As we have migrated our existing protocols
to Netty 4, (which is currently in beta and should be considered experimental),
our best-practice recommendations have evolved, and those will be covered in the
following section.


Decoding in the Transport
-------------------------
In the following :ref:`sections <server_protocol>`, we use Netty-provided
encoders/decoders in the ``Pipeline``, but it would be possible to have an empty
pipeline and do this work in a ``Transport``.

If you were to write your own custom decoding code, which we strongly recommend,
it's preferable to put that logic in a
:src:`Transport<com/twitter/finagle/transport/Transport.scala>`.
There are two motivations for this:

1. Being decoupled from an I/O multiplexer means that it can be easily swapped out
(e.g., for a new version of Netty).

2. Finagle and Netty present different programming models and it
isn't straight forward to reason about logic split amongst them.

In this case, the ``Pipeline`` will only contain a handler that decodes to/from the Netty buffer type
(`ChannelBuffer <https://docs.jboss.org/netty/3.2/api/org/jboss/netty/buffer/ChannelBuffer.html>`_
for Netty 3 and `ByteBuf <https://netty.io/4.0/api/io/netty/buffer/ByteBuf.html>`_ for Netty 4)
from/to the buffer type used in Finagle, :util-core-src:`Buf<com/twitter/io/Buf.scala>`,
and a framer (or framing could happen in the ``Transport``).

On the client side, the ``Pipeline`` would look like:

.. code-block:: scala

  import com.twitter.finagle.netty4.codec.BufCodec
  import io.netty.channel.ChannelPipeline

  object ClientPipeline extends ChannelPipelineFactory {
    def getPipeline = {
      val pipeline = Channels.pipeline()
      pipeline.addLast("bufCodec", new BufCodec)
      pipeline.addLast("framer", new MyFramer)
    }
  }

We'll cover two different approaches to decoding in the ``Transport``.

The first, and simplest approach, is best if your decoding logic is stateless and
has a one-to-one mapping with the frames received. If your client or server can decode
one frame into one message, without knowing anything about the previous or next
frame, this approach will work for you.

On your message type object (in this case, ``Message``), define encoding and decoding methods:

.. code-block:: scala

  object Message {
    def encode(msg: Message): Buf = {
    }

    def decode(buf: Buf): Message = {
      ...
    }
  }

  class Message {
    ...
  }

Then, on the client side, we can simply map over the
:src:`Transport<com/twitter/finagle/transport/Transport.scala>` with the encoding and decoding
functions to produce a new ``Transport`` that will do decoding and encoding
for us (to do the same thing on the server side, we'd use a
:src:`ServerDispatcher<com/twitter/finagle/dispatch/ServerDispatcher.scala>` ).

.. code-block:: scala

  protected def newDispatcher(
      transport: Transport[ChannelBuffer, ChannelBuffer]): Service[Message, Message] =
    new SerialClientDispatcher(transport.map(Message.encode, Message.decode))

The second approach to doing decoding in the ``Transport`` applies if your client or server
decodes one frame into multiple messages (or multiple frames into a single message), or
needs to maintain some state when decoding. In that case, you can use a
:src:`TransportProxy<com/twitter/finagle/transport/Transport.scala>` to wrap the underlying
`Transport` and implement your own read/write methods.

A custom client transport might look like the following; where a Decoder class
maintains state and returns a ``Message`` when sufficient frames have been read,
or null otherwise.

.. code-block:: scala

  class ClientTransport(
    underlying: Transport[Buf, Buf]
  ) extends TransportProxy {

    val decoder = new MyDecoder()
    val encoder = new MyEncoder()

    private[this] val decode: Buf => Future[Message] = buf => {
      val msg: Message = decoder.decode(buf)
      if (msg != null) {
        msg
      } else {
        readLoop()
      }
    }

    private[this] def readLoop(): Future[Message] = underlying.read().flatMap(decode)

    def read(): Future[Response] = readLoop()

    def write(msg: Message): Future[Unit] = {
      val buf: Buf = encoder.encode(msg)
      underlying.write(buf)
    }
  }

.. _server_protocol:

Server Protocol
---------------

To frame data received over the network with respect to our
protocol, we use a Netty Channel Pipeline. Our server pipeline defines a UTF-8
text-based newline delimited protocol (since Netty already provides
a `StringEncoder <https://netty.io/4.0/api/io/netty/handler/codec/string/StringEncoder.html>`_
and `StringDecoder <https://netty.io/4.0/api/io/netty/handler/codec/string/StringDecoder.html>`_,
we'll put this decoding in the Pipeline). The types have different namespaces but look
much the same in Netty 3 and Netty 4.

Using a Netty 3 `ChannelPipeline <https://netty.io/3.6/api/org/jboss/netty/channel/ChannelPipeline.html>`_:

.. includecode:: ../code/client-server-anatomy/netty3/Pipeline.scala#serverpipeline
   :language: scala

Using a Netty 4 `ChannelPipeline <https://netty.io/4.1/api/io/netty/channel/ChannelPipeline.html>`_:

.. includecode:: ../code/client-server-anatomy/netty4/Pipeline.scala#serverpipeline
   :language: scala


Listener
--------

The mechanics of listening over a network socket and
translating our pipeline into a typed transport are defined by the
Netty Listener. Finagle provides a
:src:`Netty3Listener <com/twitter/finagle/netty3/Netty3Listener.scala>`
and :finagle-netty4-src:`Netty4Listener <com/twitter/finagle/netty4/Netty4Listener.scala>`.

Here we define a :finagle-netty4-src:`Netty4Listener <com/twitter/finagle/netty4/Netty4Listener.scala>`
in our server implementation:

.. includecode:: ../code/client-server-anatomy/netty4/Echo.scala#serverlistener
   :language: scala

This implements the :src:`Listener <com/twitter/finagle/server/Listener.scala>`
interface that exposes a ``listen`` method:

.. code-block:: scala

  def listen(addr: SocketAddress)(serveTransport: Transport[In, Out] => Unit)

That is, given a socket address to bind and listen, ``serveTransport`` is dispatched
for each new connection established.

For example, here is a simple echo server using a
:finagle-netty4-src:`Netty4Listener <com/twitter/finagle/netty4/Netty4Listener.scala>`:

.. code-block:: scala

   val address = new java.net.InetSocketAddress("localhost", 8080)
   val listener = Netty4Listener(StringServerPipeline, StackServer.defaultParams)
   val echoServer = listener.listen(address) { transport =>
      transport.read() flatMap { transport.write(_) } ensure transport.close()
    }

We can now send requests over this socket and have them echoed back:

::

  > echo "hello" | nc localhost 8080
  > hello

The ``serveTransport`` function defined above is primitive. For example,
it closes each connection after one read and write. Finagle provides tools
to provision a transport with more sophisticated behavior.

Server Dispatcher
-----------------

The :src:`server dispatcher <com/twitter/finagle/dispatch/ServerDispatcher.scala>`
queues concurrent incoming requests and serially dispatches
them over a ``Transport``. The data read from the ``Transport``
is funneled through a service object and the resulting value
is written back to the ``Transport``. Additionally, the
server dispatcher drains existing requests before
closing a ``Transport``.

We could translate our ``serveTransport`` function to use this facility.

Using Netty 4:

.. includecode:: ../code/client-server-anatomy/netty4/Echo.scala#simplelisten
   :language: scala

A nice consequence of using a :ref:`Service <services>` to process
data received over the transport is the ability to furnish our server with
additional behavior via :doc:`Filters<ServicesAndFilters>`. This is exactly
what Finagle's default server implementation does.

StdStackServer
--------------

Finagle's :src:`StdStackServer
<com/twitter/finagle/server/StackServer.scala>` provides appropriate
features for building a robust server. It puts together a ``Listener``
and a ``Dispatcher`` in much the same way we just did. ``StdStackServer``
also layers a ``Stack`` on top of it (e.g. to provide timeouts, stats,
concurrency control, tracing, etc.) and takes care of graceful
shutdown, so that outstanding requests are drained before a server
exits. The resulting server is fully parameterized, providing a simple
and standard way to receive parameters and dependencies.

Using the listener and dispatcher as above, we define our full server.
The abstract type parameters ``In`` and ``Out`` are used when the type of
``Listener`` differs from the type of ``Server``. This is common when some protocol
processing is done in the ``Dispatcher``.

We'll create a server that uses Netty 4:

.. includecode:: ../code/client-server-anatomy/netty4/Echo.scala#server
   :language: scala

Finally, we make use of our service:

.. includecode:: ../code/client-server-anatomy/netty4/Echo.scala#serveruse
   :language: scala

To create a server that uses Netty 3, use a
:src:`Netty3Listener <com/twitter/finagle/netty3/Netty3Listener.scala>`
and a Netty 3 `ChannelPipeline <https://netty.io/3.6/api/org/jboss/netty/channel/ChannelPipeline.html>`_.


Client Protocol
---------------

Again, we'll use a Netty Channel Pipeline to frame our network traffic.
Our client pipeline defines a UTF-8 newline delimited protocol. As with the server
pipeline, the types have different namespaces but look much the same in Netty 3 and Netty 4.

Netty 3 `ChannelPipeline <https://netty.io/3.6/api/org/jboss/netty/channel/ChannelPipeline.html>`_:

.. includecode:: ../code/client-server-anatomy/netty3/Pipeline.scala#clientpipeline
   :language: scala

Netty 4 `ChannelPipeline <https://netty.io/4.1/api/io/netty/channel/ChannelPipeline.html>`_:

.. includecode:: ../code/client-server-anatomy/netty4/Pipeline.scala#clientpipeline
   :language: scala


Transporter
-----------

A :src:`Transporter <com/twitter/finagle/client/Transporter.scala>` is responsible for connecting
a :ref:`Transport <transport_interface>` to a peer; it establishes a session. Finagle provides
a :src:`Netty3Transporter <com/twitter/finagle/netty3/Netty3Transporter.scala>` and a
:finagle-netty4-src:`Netty4Transporter <com/twitter/finagle/netty4/Netty4Transporter.scala>`,
however the use of other Transporters is fully supported.

Using a :finagle-netty4-src:`Netty4Transporter <com/twitter/finagle/netty4/Netty4Transporter.scala>`:

.. includecode:: ../code/client-server-anatomy/netty4/Echo.scala#transporter
   :language: scala


Client Dispatcher
-----------------

A client dispatcher turns a Transport (a stream of objects) into a Service
(request-response pairs). It must manage all outstanding requests,
pairing incoming responses to their respective requests.
The simplest kind of dispatcher is called a
:src:`SerialClientDispatcher <com/twitter/finagle/dispatch/ClientDispatcher.scala>`,
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
two and create a client. We'll create a client that uses Netty 4:

.. includecode:: ../code/client-server-anatomy/netty4/Echo.scala#explicitbridge
   :language: scala

Finally, we can dispatch requests over our client.

.. includecode:: ../code/client-server-anatomy/netty4/Echo.scala#basicclientexample
   :language: scala

Assuming we have a server willing to listen, we can expect a response:

::

  $ ./sbt run
  > hello

To create a client that uses Netty 4, use a
:finagle-netty4-src:`Netty4Transporter <com/twitter/finagle/netty4/Netty4Transporter.scala>`.

A Robust Client
---------------

Our client is a ``Service``, so we can supply additional
behavior to make our client more robust using
filters:

.. includecode:: ../code/client-server-anatomy/netty4/Echo.scala#filters
   :language: scala

Composing these filters [#]_ with our basic client demonstrates
the composable components used throughout finagle.

.. includecode:: ../code/client-server-anatomy/netty4/Echo.scala#robustclient
   :language: scala

This client is a good start, but we cannot dispatch concurrent requests
to a single host, nor load balance over multiple hosts. A typical Finagle client
affords us the ability to dispatch a large number of concurrent requests.

.. [#] The use of the MaskCancelFilter in the example filter stack
       ensures that timeout exceptions don't propagate to our
       bottom most service which, in this case, represents a dispatcher.
       Without this guarantee, the service would be closed after the first
       timeout exception. This becomes unnecessary when we use a StdStackClient
       because the semantics of Service#close() change
       with respect to Finagle's connection pool.

StdStackClient
--------------

The :src:`StdStackClient <com/twitter/finagle/client/StackClient.scala>`
combines a `Transporter`, a `Dispatcher`, and a `Stack` to provide a robust,
load balanced, resource-managed client. The default stack includes many
features including
:ref:`load balancing <load_balancer>` over multiple hosts
and :ref:`connection pooling <watermark_pool>` per host. See the section
on :ref:`client modules <client_modules>` for more details.

Putting together a ``StdStackClient`` is simple:

.. includecode:: ../code/client-server-anatomy/netty4/Echo.scala#client
   :language: scala

Armed with this new client, we can connect to a destination :src:`Name
<com/twitter/finagle/Name.scala>`, representing multiple hosts:

.. code-block:: scala

  val dest = Resolver.eval(
    "localhost:8080,localhost:8081,localhost:8082")

  client.newClient(dest): ServiceFactory[String, String]

Requests sent to this client are load balanced across these
hosts and each host maintains a connection pool, thus
allowing concurrent dispatches.
