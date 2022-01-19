package com.twitter.finagle.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.Stack
import com.twitter.finagle.Status
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.finagle.ssl.server.SslServerConfiguration
import com.twitter.finagle.ssl.session.NullSslSessionInfo
import com.twitter.finagle.ssl.session.SslSessionInfo
import com.twitter.io.Buf
import com.twitter.io.Pipe
import com.twitter.io.Reader
import com.twitter.io.ReaderDiscardedException
import com.twitter.io.Writer
import com.twitter.io.StreamTermination
import com.twitter.util._
import java.net.SocketAddress
import java.security.cert.Certificate
import scala.runtime.NonLocalReturnControl
import scala.util.control.NonFatal

/**
 * A transport is a representation of a stream of objects that may be
 * read from and written to asynchronously. Transports are connected
 * to some endpoint, typically via a channel pipeline that performs
 * encoding and decoding.
 */
trait Transport[In, Out] extends Closable { self =>
  type Context <: TransportContext

  /**
   * Write `req` to this transport; the returned future
   * acknowledges write completion.
   */
  def write(req: In): Future[Unit]

  /**
   * Read a message from the transport.
   */
  def read(): Future[Out]

  /**
   * The status of this transport; see [[com.twitter.finagle.Status]] for
   * status definitions.
   */
  def status: Status

  /**
   * The channel closed with the given exception. This is the
   * same exception you would get if attempting to read or
   * write on the Transport, but this allows clients to listen to
   * close events.
   */
  def onClose: Future[Throwable]

  /**
   * The locally bound address of this transport.
   */
  @deprecated("Please use Transport.context.localAddress instead", "2017-08-21")
  final def localAddress: SocketAddress = context.localAddress

  /**
   * The remote address to which the transport is connected.
   */
  @deprecated("Please use Transport.context.remoteAddress instead", "2017-08-21")
  final def remoteAddress: SocketAddress = context.remoteAddress

  /**
   * Maps this transport to `Transport[In1, Out2]`. Note, exceptions
   * in `f` and `g` are lifted to a [[com.twitter.util.Future]].
   *
   * @param f The function applied to `write`s input.
   * @param g The function applied to the result of a `read`
   */
  def map[In1, Out1](f: In1 => In, g: Out => Out1): Transport[In1, Out1] =
    new Transport[In1, Out1] {
      type Context = self.Context

      def write(in1: In1): Future[Unit] =
        try self.write(f(in1))
        catch {
          case NonFatal(t) => Future.exception(t)
          case nlrc: NonLocalReturnControl[_] =>
            Future.exception(new FutureNonLocalReturnControl(nlrc))
        }

      def read(): Future[Out1] = self.read().map(g)
      def status: Status = self.status
      def onClose: Future[Throwable] = self.onClose
      def close(deadline: Time): Future[Unit] = self.close(deadline)
      def context: Context = self.context
      override def toString: String = self.toString
    }

  /**
   * Maps the context of this transport to a new context.
   * @param f A function to provide a new context
   */
  def mapContext[Ctx1 <: TransportContext](f: self.Context => Ctx1): Transport[In, Out] {
    type Context <: Ctx1
  } = {
    val newCtx = f(self.context)
    new TransportProxyWithoutContext[In, Out](self) {
      type Context = Ctx1

      override def write(req: In): Future[Unit] = self.write(req)
      override def read(): Future[Out] = self.read()
      override def context: Context = newCtx
    }
  }

  /**
   * The control panel for the Transport.
   */
  def context: Context
}

/**
 * A collection of [[com.twitter.finagle.Stack.Param]]'s useful for configuring
 * a [[com.twitter.finagle.transport.Transport]].
 *
 * @define $param a [[com.twitter.finagle.Stack.Param]] used to configure
 */
object Transport {

  private[finagle] val sslSessionInfoCtx = new Contexts.local.Key[SslSessionInfo]

  /**
   * Retrieve the [[Transport]]'s [[SslSessionInfo]] from
   * [[com.twitter.finagle.context.Contexts.local]] if available. If none exists,
   * a [[NullSslSessionInfo]] is returned instead.
   */
  def sslSessionInfo: SslSessionInfo = Contexts.local.get(sslSessionInfoCtx) match {
    case Some(info) => info
    case None => NullSslSessionInfo
  }

  /**
   * Retrieve the peer certificate of the [[Transport]], if
   * one exists.
   */
  def peerCertificate: Option[Certificate] = sslSessionInfo.peerCertificates.headOption

  /**
   * $param the buffer sizes of a `Transport`.
   *
   * @param send An option indicating the size of the send buffer.
   * If None, the implementation default is used.
   *
   * @param recv An option indicating the size of the receive buffer.
   * If None, the implementation default is used.
   */
  case class BufferSizes(send: Option[Int], recv: Option[Int]) {
    def mk(): (BufferSizes, Stack.Param[BufferSizes]) =
      (this, BufferSizes.param)
  }
  object BufferSizes {
    implicit val param = Stack.Param(BufferSizes(None, None))
  }

  /**
   * $param the liveness of a `Transport`. These properties dictate the
   * lifecycle of a `Transport` and ensure that it remains relevant.
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
  case class Liveness(readTimeout: Duration, writeTimeout: Duration, keepAlive: Option[Boolean]) {
    def mk(): (Liveness, Stack.Param[Liveness]) =
      (this, Liveness.param)
  }
  object Liveness {
    implicit val param = Stack.Param(Liveness(Duration.Top, Duration.Top, None))
  }

  /**
   * $param the verbosity of a `Transport`. Transport activity is
   * written to [[com.twitter.finagle.param.Logger]].
   */
  case class Verbose(enabled: Boolean) {
    def mk(): (Verbose, Stack.Param[Verbose]) =
      (this, Verbose.param)
  }
  object Verbose {
    implicit val param = Stack.Param(Verbose(enabled = false))
  }

  /**
   * $param the SSL/TLS client configuration for a `Transport`.
   */
  case class ClientSsl(sslClientConfiguration: Option[SslClientConfiguration]) {
    def mk(): (ClientSsl, Stack.Param[ClientSsl]) =
      (this, ClientSsl.param)
  }
  object ClientSsl {
    implicit val param = Stack.Param(ClientSsl(None))
  }

  /**
   * $param the SSL/TLS server configuration for a `Transport`.
   */
  case class ServerSsl(sslServerConfiguration: Option[SslServerConfiguration]) {
    def mk(): (ServerSsl, Stack.Param[ServerSsl]) =
      (this, ServerSsl.param)
  }
  object ServerSsl {
    implicit val param = Stack.Param(ServerSsl(None))
  }

  /**
   * $param the options (i.e., socket options) of a `Transport`.
   *
   * @param noDelay enables or disables `TCP_NODELAY` (Nagle's algorithm)
   *                option on a transport socket (`noDelay = true` means
   *                disabled). Default is `true` (disabled).
   *
   * @param reuseAddr enables or disables `SO_REUSEADDR` option on a
   *                  transport socket. Default is `true`.
   *
   * @param reusePort enables or disables `SO_REUSEPORT` option on a
   *                  transport socket (Linux 3.9+ only). This option is only
   *                  available when using finagle-netty4 and native epoll support
   *                  is enabled. Default is `false`.
   */
  case class Options(noDelay: Boolean, reuseAddr: Boolean, reusePort: Boolean) {
    def this(noDelay: Boolean, reuseAddr: Boolean) = this(noDelay, reuseAddr, reusePort = false)

    def mk(): (Options, Stack.Param[Options]) = (this, Options.param)
  }

  object Options {
    implicit val param: Stack.Param[Options] =
      Stack.Param(Options(noDelay = true, reuseAddr = true, reusePort = false))

    def apply(noDelay: Boolean, reuseAddr: Boolean): Options =
      this.apply(noDelay = noDelay, reuseAddr = reuseAddr, reusePort = false)
  }

  /**
   * Serializes the object stream from a `Transport` into a
   * [[com.twitter.io.Writer]].
   *
   * The serialization function `f` can return `Future.None` to interrupt the
   * stream to facilitate using the transport with multiple writers and vice
   * versa.
   *
   * Both transport and writer are unmanaged, the caller must close when
   * done using them.
   *
   * {{{
   * copyToWriter(trans, w)(f).ensure {
   *   trans.close()
   *   w.close()
   * }
   * }}}
   *
   * @param trans The source Transport.
   *
   * @param w The destination [[com.twitter.io.Writer]].
   *
   * @param f A mapping from `A` to `Future[Option[Buf]]`.
   */
  private[finagle] def copyToWriter[A](
    trans: Transport[_, A],
    w: Writer[Buf]
  )(
    f: A => Future[Option[Buf]]
  ): Future[Unit] = {
    trans.read().flatMap(f).flatMap {
      case None => Future.Done
      case Some(buf) => w.write(buf).before(copyToWriter(trans, w)(f))
    }
  }

  /**
   * Collates a transport, using the collation function `chunkOfA`,
   * into a [[com.twitter.io.Reader]].
   *
   * Collation completes when `chunkOfA` returns `Future.None`. The returned
   * [[com.twitter.io.Reader]] is also a Unit-typed
   * [[com.twitter.util.Future]], which is satisfied when collation
   * is complete, or else has failed.
   *
   * @note This deserves its own implementation, independently of
   * using copyToWriter. In particular, in today's implementation,
   * the path of interrupts are a little convoluted; they would be
   * clarified by an independent implementation.
   */
  private[finagle] def collate[A](
    trans: Transport[_, A],
    chunkOfA: A => Future[Option[Buf]]
  ): Reader[Buf] with Future[Unit] = new Promise[Unit] with Reader[Buf] {
    private[this] val rw = new Pipe[Buf]

    // Ensure that collate's future is satisfied _before_ its reader
    // is closed. This allows callers to observe the stream completion
    // before readers do.
    private[this] val writes = copyToWriter(trans, rw)(chunkOfA)
    forwardInterruptsTo(writes)
    writes.respond {
      case ret @ Throw(t) =>
        updateIfEmpty(ret)
        rw.fail(t)
      case r @ Return(_) =>
        updateIfEmpty(r)
        rw.close()
    }

    def read(): Future[Option[Buf]] = rw.read()

    def discard(): Unit = {
      rw.discard()
      raise(new ReaderDiscardedException)
    }

    def onClose: Future[StreamTermination] = rw.onClose
  }

  /**
   * Casts an object transport to `Transport[In1, Out1]`. Note that this is
   * generally unsafe: only do this when you know the cast is guaranteed safe.
   * This is useful when coercing a netty object pipeline into a typed transport,
   * for example.
   *
   * @see [[Transport.cast(Class[Out], transport)]] for Java users.
   */
  def cast[In1, Out1](
    trans: Transport[Any, Any]
  )(
    implicit m: Manifest[Out1]
  ): Transport[In1, Out1] = {
    val cls = m.runtimeClass.asInstanceOf[Class[Out1]]
    cast[In1, Out1](cls, trans)
  }

  /**
   * Casts an object transport to `Transport[In1, Out1]`. Note that this is
   * generally unsafe: only do this when you know the cast is guaranteed safe.
   * This is useful when coercing a netty object pipeline into a typed transport,
   * for example.
   *
   * @see [[Transport.cast(trans)]] for Scala users.
   */
  def cast[In1, Out1](cls: Class[Out1], trans: Transport[Any, Any]): Transport[In1, Out1] = {

    if (cls.isAssignableFrom(classOf[Any])) {
      // No need to do any dynamic type checks on Any!
      trans.asInstanceOf[Transport[In1, Out1]]
    } else
      new Transport[In1, Out1] {
        type Context = trans.Context

        def write(req: In1): Future[Unit] = trans.write(req)
        def read(): Future[Out1] = trans.read().flatMap(readFn)
        def status: Status = trans.status
        def onClose: Future[Throwable] = trans.onClose
        def close(deadline: Time): Future[Unit] = trans.close(deadline)
        def context: Context = trans.context.asInstanceOf[Context]
        override def toString: String = trans.toString

        private val readFn: Any => Future[Out1] = {
          case out1 if cls.isAssignableFrom(out1.getClass) => Future.value(out1.asInstanceOf[Out1])
          case other =>
            val msg = s"Transport.cast failed. Expected type ${cls.getName} " +
              s"but found ${other.getClass.getName}"
            val ex = new ClassCastException(msg)
            Future.exception(ex)
        }
      }
  }

}

/**
 * A factory for transports: they are specially encoded as to be
 * polymorphic.
 */
trait TransportFactory {
  def apply[In, Out](): Transport[In, Out]
}

/**
 * A [[Transport]] that defers all methods except `read` and `write`
 * to `self`. The context is unspecified and must be provided by the
 * implementor.
 */
abstract class TransportProxyWithoutContext[In, Out](_self: Transport[In, Out])
    extends Transport[In, Out] {
  def status: Status = _self.status
  def onClose: Future[Throwable] = _self.onClose
  def close(deadline: Time): Future[Unit] = _self.close(deadline)
  override def toString: String = _self.toString
}

/**
 * A [[Transport]] that defers all methods except `read` and `write`
 * to `self`. The context is copied from `self`.
 */
abstract class TransportProxy[In, Out](_self: Transport[In, Out])
    extends TransportProxyWithoutContext[In, Out](_self) {
  val self: Transport[In, Out] = _self

  type Context = self.Context

  def context: Context = self.context
}

/**
 * A `Transport` interface to a pair of queues (one for reading, one
 * for writing); useful for testing.
 */
class QueueTransport[In, Out](writeq: AsyncQueue[In], readq: AsyncQueue[Out])
    extends Transport[In, Out] {
  type Context = TransportContext

  private[this] val closep = new Promise[Throwable]

  def write(input: In): Future[Unit] = {
    writeq.offer(input)
    Future.Done
  }

  def read(): Future[Out] =
    readq.poll() onFailure { exc => closep.updateIfEmpty(Throw(exc)) }

  def status: Status = if (closep.isDefined) Status.Closed else Status.Open

  def close(deadline: Time): Future[Unit] = {
    val ex = new Exception("QueueTransport is now closed")
    closep.updateIfEmpty(Return(ex))
    Future.Done
  }

  val onClose: Future[Throwable] = closep
  val context: TransportContext =
    new SimpleTransportContext(new SocketAddress {}, new SocketAddress {}, NullSslSessionInfo)
}
