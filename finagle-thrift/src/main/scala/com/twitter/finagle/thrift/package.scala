package com.twitter.finagle

/**
 * =Deprecation=
 *
 * Please use the new interface, [[com.twitter.finagle.Thrift]],
 * for constructing thrift clients and servers.
 *
 * ==Thrift codecs==
 *
 * We provide both framed and buffered client protocol support, and
 * server support for the framed protocol. The public implementations
 * are:
 *
 *  - [[com.twitter.finagle.thrift.ThriftClientBufferedCodec]]
 *  - [[com.twitter.finagle.thrift.ThriftClientFramedCodec]]
 *  - [[com.twitter.finagle.thrift.ThriftServerFramedCodec]]
 *
 * The type of the server codec is `Service[Array[Byte], Array[Byte]]`
 * and the client codecs are `Service[ThriftClientRequest,
 * Array[Byte]]`. The service provided is that of a "transport" of
 * thrift messages (requests and replies) according to the protocol
 * chosen. This is why the client codecs need to have access to a
 * thrift `ProtocolFactory`.

 * These transports are used by the services produced by the
 * [[https://github.com/mariusaeriksen/thrift-0.5.0-finagle finagle thrift codegenerator]].
 *
 * {{{
 * val service: Service[ThriftClientRequest, Array[Byte]] = ClientBuilder()
 *   .hosts("foobar.com:123")
 *   .codec(ThriftClientFramedCodec())
 *   .build()
 *
 * // Wrap the raw Thrift transport in a Client decorator. The client
 * // provides a convenient procedural interface for accessing the Thrift
 * // server.
 * val client = new Hello.ServiceToClient(service, protocolFactory)
 * }}}
 *
 * In this example, `Hello` is the thrift interface, and the inner
 * class `ServiceToClient` is provided by the finagle thrift code
 * generator.
 */
package object thrift
