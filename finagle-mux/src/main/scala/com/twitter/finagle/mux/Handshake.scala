package com.twitter.finagle.mux

import com.twitter.finagle.mux.transport.{MuxFramer, Message}
import com.twitter.finagle.transport.{Transport, TransportProxy}
import com.twitter.finagle.{Failure, Status}
import com.twitter.io.Charsets
import com.twitter.util.{Future, Return, Throw, Time}
import java.net.SocketAddress
import java.security.cert.Certificate
import java.util.concurrent.atomic.AtomicBoolean
import org.jboss.netty.buffer.ChannelBuffer

/**
 * Implements mux session negotiation. The mux spec allows for (re)negotiation to happen
 * arbitrarily throughout a session, but for simplicity, our implementation assumes
 * it happens at the start of a session. It is implemented in terms of a [[Transport]] so that
 * negotiation can sit transparently below client and server dispatchers and easily install
 * features based on the exchanged version and headers.
 */
private[finagle] object Handshake {
  type Headers = Seq[(ChannelBuffer, ChannelBuffer)]

  /**
   * A function which transforms a transport based on a session's
   * version and headers. Note the input exposes the framed byte stream
   * rather than mux `Message` types to more easily allow for features that need
   * to operate on the raw byte frame (e.g. compression, checksums, etc.).
   */
  type Negotiator = Headers =>
    (Transport[ChannelBuffer, ChannelBuffer] => Transport[Message, Message])

  /**
   * Returns true if `key` exists in `headers`.
   */
  def valueOf(key: ChannelBuffer, headers: Headers): Option[ChannelBuffer] = {
    val iter = headers.iterator
    while (iter.hasNext) {
      val (k, v) = iter.next()
      if (k == key) return Some(v)
    }
    None
  }

  /**
   * We can assign tag 1 without worry of any tag conflicts because we gate
   * all messages until the handshake is complete (or fails).
   */
  val TinitTag = 1

  /**
   * The messages and exceptions exchanged when there is a version
   * mismatch during handshaking.
   */
  val VerMismatchMsg = "unsupported mux version"
  val VerMismatchExc = Future.exception(Failure(VerMismatchMsg))

  /**
   * The current version exchanged between client and server and vice versa.
   * Clients initiated the handshake and a server supports a single version
   * and responds with errors on anything else.
   */
  val LatestVersion: Short = 0x0001

  /**
   * The default headers exchanged between client and server and
   * vice versa.
   */
  val DefaultHeaders: Headers = Seq(
    MuxFramer.Header.KeyBuf -> MuxFramer.Header.DefaultWindowBuf
  )

  /**
   * The default negotiation function used when servers and clients
   * resolve their transport features.
   */
  val DefaultNegotiator: Negotiator = headers => {
    trans => {
      val window = valueOf(MuxFramer.Header.KeyBuf, headers)
      if (window.isEmpty) trans.map(Message.encode, Message.decode)
      else MuxFramer(trans, MuxFramer.Header.decodeWindow(window.get))
    }
  }

  /**
   * Returns a [[Transport]] that handles session negotiation from a client's
   * perspective. The client initiates the handshake via a `Tinit` message.
   * If the server responds appropriately with an `Rinit`, `trans` is transformed
   * via `negotiate` otherwise it's returned unchanged.
   */
  def client(
    trans: Transport[ChannelBuffer, ChannelBuffer],
    version: Short = LatestVersion,
    headers: Headers = DefaultHeaders,
    negotiate: Negotiator = DefaultNegotiator
  ): Transport[Message, Message] = {
    // Since the handshake happens at the start of a session, we can safely enc/dec
    // messages without having to worry about any special features (e.g. fragments).
    val msgTrans = trans.map(Message.encode, Message.decode)
    val handshake: Future[Transport[Message, Message]] =
      msgTrans.write(Message.Tinit(TinitTag, version, headers)).before {
        msgTrans.read().transform {
          case Return(Message.Rinit(_, v, hdrs)) if v == version =>
            Future(negotiate(hdrs)(trans))
          case Return(Message.Rerr(_, `VerMismatchMsg`)) =>
            VerMismatchExc
          // We don't handle any other response. Instead, we return
          // the session as is and assume that we can speak mux pre
          // handshaking. Any subsequent failures will be handled by
          // the layers above (i.e. the dispatcher). This is a workaround
          // since our initial implementation of mux didn't implement
          // handshaking. In theory, any Rerr should result in an
          // exception.
          case _ => Future.value(msgTrans)
        }
      }
    handshake.onFailure { _ => msgTrans.close() }
    new DeferredTransport(handshake)
  }

  /**
   * Returns a [[Transport]] that handles session negotiation from a server's
   * perspective. It reads the first message from the `trans` and if it is
   * an `Rinit`, transforms the transport via `negotiate`. If the client doesn't
   * support negotiation, the original `trans` is returned, making sure to replace
   * any messages we eagerly read from the transport.
   */
  def server(
    trans: Transport[ChannelBuffer, ChannelBuffer],
    version: Short = LatestVersion,
    headers: Headers = DefaultHeaders,
    negotiate: Negotiator = DefaultNegotiator
  ): Transport[Message, Message] = {
    // Since the handshake happens at the start of a session, we can safely enc/dec
    // messages without having to worry about any special features (e.g. fragments).
    val msgTrans = trans.map(Message.encode, Message.decode)
    val handshake: Future[Transport[Message, Message]] =
      msgTrans.read().transform {
        // A Tinit with a matching version
        case Return(Message.Tinit(tag, ver, hdrs)) if ver == version =>
          msgTrans.write(Message.Rinit(tag, version, headers)).before {
            Future(negotiate(hdrs)(trans))
          }
        // A Tinit with a version mismatch. Write an Rerr and then return
        // a failed future.
        case Return(Message.Tinit(tag, ver, _)) =>
          msgTrans.write(Message.Rerr(tag, VerMismatchMsg))
            .before { VerMismatchExc }
        // Client doesn't support negotiating a session and we've consumed a
        // message from the readq. Transparently put `msg` back into the readq.
        case Return(msg) => Future.value(new TransportProxy(msgTrans) {
          private[this] val first = new AtomicBoolean(true)
          def read(): Future[Message] =
            if (first.compareAndSet(true, false)) Future.value(msg)
            else msgTrans.read()
          def write(req: Message): Future[Unit] = msgTrans.write(req)
        })

        case Throw(_) => Future.value(msgTrans)
      }

    handshake.onFailure { _ => msgTrans.close() }
    new DeferredTransport(handshake)
  }
}

/**
 * Implements a [[Transport]] in terms of a future transport (`trans`). All async
 * operations are composed via future composition and callers can safely
 * interrupt the returned Futures without affecting the result of `trans`.
 * The synchronous operations return sane defaults until `trans` is resolved.
 */
private class DeferredTransport(trans: Future[Transport[Message, Message]])
  extends Transport[Message, Message] {

  import DeferredTrans._

  // we create a derivative promise while `trans` is not defined
  // because the transport is multiplexed and interrupting on one
  // stream shouldn't affect the result of the handshake.
  private[this] def gate() = trans.interruptible()

  def write(msg: Message): Future[Unit] = gate().flatMap(_.write(msg))

  private[this] val read0: Transport[Message, Message] => Future[Message] = _.read()
  def read(): Future[Message] = gate().flatMap(read0)

  def status: Status = trans.poll match {
    case Some(Return(t)) => t.status
    case None => Status.Busy
    case _ => Status.Closed
  }

  val onClose: Future[Throwable] = gate().flatMap(_.onClose)

  def localAddress: SocketAddress = trans.poll match {
    case Some(Return(t)) => t.localAddress
    case _ => UnknownSocketAddress
  }

  def remoteAddress: SocketAddress = trans.poll match {
    case Some(Return(t)) => t.remoteAddress
    case _ => UnknownSocketAddress
  }

  def peerCertificate: Option[Certificate] = trans.poll match {
    case Some(Return(t)) => t.peerCertificate
    case _ => None
  }

  def close(deadline: Time): Future[Unit] = gate().flatMap(_.close(deadline))
}

private object DeferredTrans {
  object UnknownSocketAddress extends SocketAddress
}