package com.twitter.finagle.server

import com.twitter.finagle.ssl.Engine
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{ListeningServer, NullServer, Stack}
import com.twitter.util.Duration
import java.net.SocketAddress

/**
 * Listeners provide a method, `listen`, to expose a server on the
 * the given SocketAddress. `serveTransport` is called for each new
 * connection. It is furnished with a typed `Transport` representing
 * this connection.
 *
 * The returned `ListeningServer` is used to inspect the server, and
 * is also used to shut it down.
 */
trait Listener[In, Out] {
  def listen(addr: SocketAddress)(serveTransport: Transport[In, Out] => Unit): ListeningServer
}

/**
 * An empty Listener that can be used as a placeholder.
 */
object NullListener extends Listener[Any, Any] {
  def listen(addr: SocketAddress)(serveTransport: Transport[Any, Any] => Unit) = NullServer
}

/**
 * @define $param a [[com.twitter.finagle.Stack.Param]] used to configure
 */
object Listener {
  /**
   * $param the `addr` that is bound for a `Listener`.
   */
  case class BindTo(addr: SocketAddress)
  implicit object BindTo extends Stack.Param[BindTo] {
    val default = BindTo(new SocketAddress {
      override val toString = "unknown"
    })
  }

  /**
   * $param the buffer sizes of a `Listener`.
   *
   * @param send An option indicating the size of the send buffer.
   * If None, the implementation default is used.
   *
   * @param recv An option indicating the size of the receive buffer.
   * If None, the implementation default is used.
   */
  case class BufferSize(send: Option[Int], recv: Option[Int])
  implicit object BufferSize extends Stack.Param[BufferSize] {
    val default = BufferSize(None, None)
  }

  /**
   * $param the `Listener` backlog.
   *
   * @param value An option indicating the backlog size. If None,
   * the implementation default is used.
   */
  case class Backlog(value: Option[Int])
  implicit object Backlog extends Stack.Param[Backlog] {
    val default = Backlog(None)
  }

  /**
   * $param the liveness of a `Listener`.
   *
   * @param readTimeout A maximum duration a listener is allowed
   * to read a request.
   *
   * @param writeTimeout A maximum duration a listener is allowed to
   * write a response.
   *
   * @param keepAlive An option indicating if the keepAlive is on or off.
   * If None, the implementation default is used.
   */
  case class Liveness(
    readTimeout: Duration,
    writeTimeout: Duration,
    keepAlive: Option[Boolean]
  )
  implicit object Liveness extends Stack.Param[Liveness] {
    val default = Liveness(Duration.Top, Duration.Top, None)
  }

  /**
   * $param the verbosity of a `Listener`. Listener activity is
   * written to [[com.twitter.finagle.param.Logger]].
   */
  case class Verbose(b: Boolean)
  implicit object Verbose extends Stack.Param[Verbose] {
    val default = Verbose(false)
  }

  /**
   * $param the SSL engine of a `Listener`.
   */
  case class SslEngine(e: Option[() => com.twitter.finagle.ssl.Engine])
  implicit object SslEngine extends Stack.Param[SslEngine] {
    val default = SslEngine(None)
  }
}