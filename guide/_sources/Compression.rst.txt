Compression
===========

Compression can allow us to make smart tradeoffs between IO and CPU.  This is
necessarily protocol-specific, because different protocols use compression in
different ways.  Finagle's approach to compression is that once it's configured,
customers shouldn't need to think about it--aside the configuration setup, and
marking messages as compressed or not in some protocols, it should behave the
same way as if you were sending uncompressed messages.

HTTP
----

In HTTP, we compress messages according to the HTTP spec, using the
`Accept-Encoding` and `Content-Encoding` headers.  However, the application
doesn't need to interpret either of them manually.  By default, servers will
compress messages with text-like `Content-Type`s, but nothing else. By default,
clients will decompress the messages it knows how to decompress automatically.

To set up compression, All the application needs to do is configure
`Http.Client#withDecompression` and `Http.Server#withDecompression` or
`Http.Server#withCompressionLevel`.  -1 is the default compression level, and
will only compress messages with text-like `Content-Type`s.  Although the HTTP
spec does not officially support server-side decompression, some clients can
send compressed HTTP requests, so we support that behavior.

.. code-block:: scala

  import com.twitter.finagle.{Http, Service}
  import com.twitter.finagle.http.{Request, Response}

  def bigPayloadService: Service[Request, Response] = ???
  val server = Http.server
    .withCompressionLevel(4)
    .serve(":8080", bigPayloadService)
  val client = Http.client
    .withDecompression(enabled = true)
    .newService(":8080")

This will set up a server, and a client that can talk to it.  When the client
sets the `Accept-Encoding` header (or
`com.twitter.finagle.http.Fields.AcceptEncoding`), the server will know that
it's safe to compress the response, using the encoding that's specified in the
`Accept-Encoding`, and it will compress the response and set the
`Content-Encoding` header.

Note that on the server-side, we don't strip the `Accept-Encoding` header when
handing it to your service implementation.  This means that HTTP proxies should
manually strip the `Accept-Encoding` header if they don't want to propagate it
to the next hop.  If compression is enabled on a server, setting the
`Content-Encoding` header in the response means that the server will not attempt
to compress the response.  On the client-side, the `Content-Encoding` header
will be dropped after the message is decoded, and will be preserved if the
client does not decode it, whether it is because it is not configured to decode
it or because it is unable to do so.

Out of the box, Finagle supports "gzip" and "deflate".

ThriftMux
---------

ThriftMux supports a different kind of compression, which is for the entire
ThriftMux session, not just an individual message.  This has significant
benefits in terms of being able to compress data better, since the dictionary
that you use for compressing can collect more data on how to compress for your
mix of data.

ThriftMux negotiates compression in the Mux handshake, where the client and the
server can each say which protocols they are willing to use to compress, for
messages from the client to the server, and separately for messages from the
server to the client.

The three modes of compression are `Off`, `Accepted` and `Desired`.  If one of
the peers sets their mode as `Off`, they will not compress the session.  If
either of the peers sets their mode as `Desired`, and the other is either
`Accepted` or `Desired` and both peers can agree on at least one compression
format, then they will compress the stream.  If a peer sets their mode as
`Accepted`, it means that it is able to compress (or decompress, as the case may
be) using the specified compression formats.

The default configured compression format is `Off`, so enabling compression
means that you need to configure it as `Accepted` or `Desired` as appropriate.
Here's what configuring it looks like:

.. code-block:: scala

  import com.twitter.finagle.ThriftMux
  import com.twitter.finagle.mux.transport.{Compression, CompressionLevel}

  val compressor = Seq(Compression.lz4Compressor(highCompression = false))
  val decompressor = Seq(Compression.lz4Decompressor())
  val compressionLevel = CompressionLevel.Desired

  def bigPayload: BigPayload.MethodPerEndpoint = ???
  val server = ThriftMux.server
    .withCompressionPreferences.compression(compressionLevel, compressor)
    .withCompressionPreferences.decompression(compressionLevel, decompressor)
    .serveIface(":8080", bigPayload)
  val client = ThriftMux.client
    .withCompressionPreferences.compression(compressionLevel, compressor)
    .withCompressionPreferences.decompression(compressionLevel, decompressor)
    .build[BigPayload.MethodPerEndpoint](":8080")

Currently, lz4 is the only supported compression format for ThriftMux.  If you
would like to use compression, you should add a dependency on the
`lz4-java <https://github.com/lz4/lz4-java>`_ library.
