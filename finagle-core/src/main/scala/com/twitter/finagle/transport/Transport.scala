package com.twitter.finagle.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.{Stack, Status}
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.finagle.ssl.server.SslServerConfiguration
import com.twitter.io.{Buf, Reader, Writer}
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
  @deprecated("Please use Transport.context.status instead", "2017-08-21")
  def status: Status

  /**
   * The channel closed with the given exception. This is the
   * same exception you would get if attempting to read or
   * write on the Transport, but this allows clients to listen to
   * close events.
   */
  @deprecated("Please use Transport.context.onClose instead", "2017-08-21")
  def onClose: Future[Throwable]

  /**
   * The locally bound address of this transport.
   */
  @deprecated("Please use Transport.context.localAddress instead", "2017-08-21")
  def localAddress: SocketAddress

  /**
   * The remote address to which the transport is connected.
   */
  @deprecated("Please use Transport.context.remoteAddress instead", "2017-08-21")
  def remoteAddress: SocketAddress

  /**
   * The peer certificate if a TLS session is established.
   */
  @deprecated("Please use Transport.context.peerCertificate instead", "2017-08-21")
  def peerCertificate: Option[Certificate]

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
      def localAddress: SocketAddress = self.localAddress
      def remoteAddress: SocketAddress = self.remoteAddress
      def peerCertificate: Option[Certificate] = self.peerCertificate
      def close(deadline: Time): Future[Unit] = self.close(deadline)
      def context: Context = self.context
      override def toString: String = self.toString
    }

  /**
   * The control panel for the Transport.
   */
  def context: Context
}

private[finagle] abstract class ContextBasedTransport[In, Out, Ctx <: TransportContext](
  val context: Ctx
) extends Transport[In, Out] {
  type Context = Ctx

  def status: Status = context.status
  def onClose: Future[Throwable] = context.onClose
  def localAddress: SocketAddress = context.localAddress
  def remoteAddress: SocketAddress = context.remoteAddress
  def peerCertificate: Option[Certificate] = context.peerCertificate
  def close(deadline: Time): Future[Unit] = context.close(deadline)
  override def toString: String = context.toString
}

/**
 * A collection of [[com.twitter.finagle.Stack.Param]]'s useful for configuring
 * a [[com.twitter.finagle.transport.Transport]].
 *
 * @define $param a [[com.twitter.finagle.Stack.Param]] used to configure
 */
object Transport {

  private[finagle] val peerCertCtx = new Contexts.local.Key[Certificate]

  /**
   * Retrieve the transport's SSLSession (if any) from
   * [[com.twitter.finagle.context.Contexts.local]]
   */
  def peerCertificate: Option[Certificate] = Contexts.local.get(peerCertCtx)

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
  case class Liveness(
    readTimeout: Duration,
    writeTimeout: Duration,
    keepAlive: Option[Boolean]
  ) {
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
   */
  case class Options(noDelay: Boolean, reuseAddr: Boolean) {
    def mk(): (Options, Stack.Param[Options]) = (this, Options.param)
  }

  object Options {
    implicit val param: Stack.Param[Options] =
      Stack.Param(Options(noDelay = true, reuseAddr = true))
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
    w: Writer
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
  ): Reader with Future[Unit] = new Promise[Unit] with Reader {
    private[this] val rw = Reader.writable()

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

    def read(n: Int): Future[Option[Buf]] = rw.read(n)

    def discard(): Unit = {
      rw.discard()
      raise(new Reader.ReaderDiscarded)
    }
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
  )(implicit m: Manifest[Out1]): Transport[In1, Out1] = {
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
  def cast[In1, Out1](
    cls: Class[Out1],
    trans: Transport[Any, Any]
  ): Transport[In1, Out1] = {

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
        def localAddress: SocketAddress = trans.localAddress
        def remoteAddress: SocketAddress = trans.remoteAddress
        def peerCertificate: Option[Certificate] = trans.peerCertificate
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
 * to `self`.
 */
abstract class TransportProxy[In, Out](_self: Transport[In, Out]) extends Transport[In, Out] {

  type Context = self.Context

  val self: Transport[In, Out] = _self
  def status: Status = self.status
  def onClose: Future[Throwable] = self.onClose
  def localAddress: SocketAddress = self.localAddress
  def remoteAddress: SocketAddress = self.remoteAddress
  def peerCertificate: Option[Certificate] = self.peerCertificate
  def close(deadline: Time): Future[Unit] = self.close(deadline)
  def context: Context = self.context
  override def toString: String = self.toString
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
    readq.poll() onFailure { exc =>
      closep.updateIfEmpty(Throw(exc))
    }

  def status: Status = if (closep.isDefined) Status.Closed else Status.Open

  def close(deadline: Time): Future[Unit] = {
    val ex = new Exception("QueueTransport is now closed")
    closep.updateIfEmpty(Return(ex))
    Future.Done
  }

  val onClose: Future[Throwable] = closep
  val localAddress: SocketAddress = new SocketAddress {}
  val remoteAddress: SocketAddress = new SocketAddress {}
  def peerCertificate: Option[Certificate] = None
  val context: TransportContext = new LegacyContext(this)
}
