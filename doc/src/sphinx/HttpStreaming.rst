HTTP Streaming
==============

The Finagle servers and clients encountered so far send and receive fixed-length
messages. The entire message is ready at the time it is written to the socket.
In some applications, messages are large enough that it is better to send them
in pieces to avoid buffering a large entity in memory.

File uploads are a good example: the client reads a file and sends it to a
remote server. The client may choose to buffer the entire file in memory before
sending, but sending pieces of the file as they are read is an operation that
can be performed in *constant space*. This is a valuable property as it makes
the memory requirements of an application predictable.

The server can be optimized in the same way by reading from the socket while at
the same time writing it to storage.

Configuring client and server for streaming
-------------------------------------------

Streaming requires the ``RichHttp`` codec with ``aggregateChunks=false``.
(Currently, codec specification is only available through ``ServerBuilder`` and
``ClientBuilder``.)

::

  val server = ServerBuilder()
    .codec(RichHttp[Request](Http(), aggregateChunks = false))

  val client = ClientBuilder()
    .codec(RichHttp[Request](Http(), aggregateChunks = false))

The ``aggregateChunks`` flag determines how the codec should treat messages
with the header ``Transfer-Encoding: chunked``. If unspecified, the default is
``true`` meaning the codec will buffer the entire message in memory before
further operations can proceed on it. For incoming messages, this means that
the entire message is read off the socket before passing it to the ``Service``.
For outgoing messages, the entire message must be ready before writing it to
the socket. Because we want to guard against atypically large messages, as a
precaution, Finagle imposes a `limit on message size <//github.com/twitter/finagle/blob/a869209a7fe5188b74336419b0b573ecb6f42706/finagle-http/src/main/scala/com/twitter/finagle/http/Codec.scala#L33>`_.

When ``aggregateChunks`` is ``false`` this message size restriction is lifted.
Incoming and outgoing messages are not buffered in memory, but processed in
pieces (called *chunks*) via the byte stream.

Now for a short introduction to this byte stream interface.

Reader: an interface for byte streams
---------------------------------------

Byte streams are logical sequences of bytes. We can read a number of bytes from
the sequence to get a collection of bytes (specified by the type ``Buf``).
Alternatively, we can think of byte streams as a server of bytes, responding to
requests of ``Int`` with a ``Buf``.

::

  trait Reader {
    def read(n: Int): Future[Buf]
    def discard()
  }

A ``Reader`` is further specified by the following.

* Permits only one outstanding read at a time.
* ``Buf.Eof`` signifies end-of-stream, subsequent calls to read must yield
  ``Buf.Eof``.
* ``read(n)`` if successful must yield a Buf of length <= ``n``.

Consuming a byte stream from ``Reader`` is as simple as calling ``read``. To
produce a byte stream, we can use ``Reader.writable()``, such as in the
following example.

::

  val reader: Reader = {
    val writer = Reader.writable()
    writer.write("hello") before writer.write("world")
    writer
  }

``Reader.writable()`` is useful, but only recommended when the byte stream is
known in advance. For byte streams such as files or sockets that require
network access, we want the end-consumer of the ``Reader`` to control the
stream, that is to say, the work of the producer is driven by the ultimate
consumer.

In such situations, the producer must implement a `Reader
<//github.com/twitter/util/blob/master/util-core/src/main/scala/com/twitter/io/Reader.scala>`_
directly.

Implementing a Reader
---------------------

``Request`` and ``Response`` have a convenience method for writing streams.

::

  def apply(req: Request): Future[Response] = {
    val response = Response()
    response.setChunked(true)
    response.write("hello") before response.write("world")
    Future.value(response)
  }

Which corresponds to the following response.

::

  HTTP/1.1 200 OK
  Transport-Encoding: chunked

  5
  hello
  5
  world
  0

Applications should not rely on chunked transport encoding for framing; the
message above may be rechunked by intermediaries, e.g.:

::

  HTTP/1.1 200 OK
  Transport-Encoding: chunked

  3
  hel
  4
  llow
  4
  orld
  0

More advanced producers will need to implement the ``Reader`` interface. This
next example echoes the incoming byte stream in the byte stream of the
response.

::

  def apply(req: Request): Future[Response] = {
    val response = new Response {
      override val reader = req.reader
    }
    response.setChunked(true)
    Future.value(response)
  }

File upload
-----------

Now we return to the initial task of uploading a file.

To make things simpler we define an interface for file operations ``Handle``.
The implementation can specify how to handle read operations with the return
type ``Future[ByteBuffer]``. Reading 0 bytes signifies end-of-file.

::

  trait Handle {
    def read(n: Int): Future[ByteBuffer]
    def write(b: ByteBuffer): Future[Unit]
    def close(): Future[Unit]
  }

Given a ``Handle h`` we can make a new ``Reader``. This implementation of
``Reader`` is simplified for clarity in this example. The essential control
flow is dictated by ``finished`` and ``state``.

::

  def readerFromHandle(h: Handle) = new Reader {
    val finished = new Promise[Buf]
    val state = new AtomicReference[Int => Future[ByteBuffer]](n => h.read(n))
    val rexc = Future.exception(new IllegalStateException("read while reading"))

    finished.unit ensure { h.close() }

Our first concern is whether or not this ``Reader`` is finished. The ``Reader``
can finish in one of three states:

1. Discarded: the end-consumer has read enough is not interested in any more
2. Error: something bad happened during a read
3. EOF: Everything went well and there is no more data to read

We guarantee one-at-a-time reads by immediately failing any calls to read while
there is a read in progress. This is the job of ``state``. When initialized,
``state`` refers to the read function of the ``Handle``. When a read is in
progress, the reference points to a function that throws an exception.

Only after the read has completed is the ``state`` reset to the read function.
It is easy to observe that all code paths terminate with the closure of the
``Handle``.

Due to ``finished.or { readOp; ... }`` it is possible for the ``readOp`` to have
``raise`` called against it, yet our implementation leaves out an interrupt
handler. This turns out to be okay since resolving ``finished`` guarantees
``Handle`` closure, and when resolved, it becomes impossible to access
``state``.

::

    def read(n: Int) =
      if (finished.isDefined) finished
      else if (n == 0) Future.value(Buf.Empty)
      else finished.or {
        val readOp = state.getAndSet(_ => rexc)
        readOp(n) map(Buf.ByteArray(_)) respond {
          case t@Throw(exc) =>
            finished.updateIfEmpty(t)
            state.set(_ => Future.exception(exc))

          case Return(b) if b.length == 0 =>
            finished.updateIfEmpty(Return(Buf.Eof))

          case Return(_) =>
            state.set(n => h.read(n))
        }
      }

Anyone with a reference to the ``Reader`` may interrupt whatever computation is
in progress by invoking ``discard()``.

::

    def discard() {
      finished.updateIfEmpty(Throw(new Reader.ReaderDiscarded))
    }
  }

Now we are ready to prepare the request and send it via the HTTP client. This
part is straight forward. A new ``Request`` is created, the important part here
is that we override the default ``reader``, installing our own
``readerFromHandle``.

::

  val fileToRead: Handle = // ...
  val reqIn = new DefaultHttpRequest(HTTP_1_1, OK)
  val req = new Request {
    override val reader = readerFromHandle(fileToRead)

    val httpRequest = reqIn
    override val httpMessage = reqIn
    lazy val remoteSocketAddress = new InetSocketAddress(0)
  }
  req.setChunked(true)
  val res = client(req)


The implementation of the server is simpler. The only tricky part is ``writeOp``
which is recursive. This is the end-consumer, the application wants to drain the
contents of the ``reader`` into ``fileToWrite``. It does so by invoking
``read(Int.MaxValue)``. By the way, ``Int.MaxValue`` tells the producer to
return whatever is available, which also means it is okay to return less than
the requested ``n``.

The result of the ``read`` falls into two cases:

1. End-of-file: The ``reader`` is empty, the recursion terminates with ``Future.Done``
2. A ``Buf``: A successful read, the ``Buf`` is then written to the file.

::

  val fileToWrite: Handle = // ...

  Server.mk[Request, Response] { req =>
    def writeOp = req.reader.read(Int.MaxValue) flatMap {
      case buf if buf eq Buf.Eof =>
        Future.Done

      case buf =>
        fileToWrite.write(Buf.toByteBufer(buf)) before writeOp
    }
    writeOp before Future.value(Response(HTTP_1_1, OK))
  }

