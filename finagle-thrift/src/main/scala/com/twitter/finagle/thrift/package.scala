package com.twitter.finagle

import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.finagle.toggle.{StandardToggleMap, ToggleMap}

/**
 * =Deprecation=
 *
 * Please use the new interface, [[com.twitter.finagle.Thrift]],
 * for constructing Thrift clients and servers.
 *
 * ==Thrift codecs==
 *
 * We provide client and server protocol support for the framed protocol.
 * The public implementations are defined on the Thrift object:
 *
 *  - [[com.twitter.finagle.Thrift.client]]
 *  - [[com.twitter.finagle.Thrift.server]]
 *
 * The type of the server codec is `Service[Array[Byte], Array[Byte]]`
 * and the client codecs are `Service[ThriftClientRequest,
 * Array[Byte]]`. The service provided is that of a "transport" of
 * thrift messages (requests and replies) according to the protocol
 * chosen. This is why the client codecs need to have access to a
 * thrift `ProtocolFactory`.
 *
 * These transports are used by the services produced by the
 * [[https://github.com/mariusaeriksen/thrift-finagle finagle thrift codegenerator]].
 *
 * {{{
 * val service: Service[ThriftClientRequest, Array[Byte]] = ClientBuilder()
 *   .hosts("foobar.com:123")
 *   .stack(Thrift.client)
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
package object thrift {

  /**
   * The name of the finagle-thrift [[ToggleMap]].
   */
  private[this] val LibraryName: String = "com.twitter.finagle.thrift"

  /**
   * The [[ToggleMap]] used for finagle-thrift.
   */
  private[finagle] val Toggles: ToggleMap =
    StandardToggleMap(LibraryName, DefaultStatsReceiver)
}
