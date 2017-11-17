package com.twitter.finagle.mux

import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.transport.{LegacyContext, Transport, TransportContext, TransportProxy}
import com.twitter.finagle.{Failure, Status}
import com.twitter.io.Buf
import com.twitter.util.{Future, Return, Throw, Time, Try}
import java.net.SocketAddress
import java.security.cert.Certificate
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Implements mux session negotiation. The mux spec allows for (re)negotiation
 * to happen arbitrarily throughout a session, but for simplicity, our
 * implementation assumes it happens at the start of a session. It is implemented
 * in terms of a [[Transport]] so that negotiation can sit transparently below
 * client and server dispatchers and easily install features based on the
 * exchanged version and headers.
 */
private[finagle] object Handshake {
  type Headers = Seq[(Buf, Buf)]

  /**
   * A function which transforms or installs features atop a transport based
   * on a session's headers. Note, the input exposes the framed byte stream rather
   * than mux `Message` types to more easily allow for features that need to
   * operate on the raw byte frame (e.g. compression, checksums, etc).
   */
  type Negotiator = (Option[Headers], Transport[Buf, Buf]) => Transport[Message, Message]

  /**
   * Returns Some(value) if `key` exists in `headers`, otherwise None.
   */
  def valueOf(key: Buf, headers: Headers): Option[Buf] = {
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
   * Unfortunately, `Rerr` messages don't have error codes in the current
   * version of mux. This means that we need to match strings to distinguish
   * between important `Rerr` messages. This one is particularly important
   * because it allows us to roll out handshakes without a coordinated
   * upgrade path.
   */
  val CanTinitMsg = "tinit check"

  /**
   * A noop negotiator returns a transport that ignores the headers and
   * encodes / decodes mux messages.
   */
  val NoopNegotiator: Negotiator = (_, trans) => {
    trans.map(Message.encode, Message.decode)
  }

  /**
   * In order to simplify the rollout of handshakes, we need to make
   * sure that our remote can understand Tinits before sending them.
   * This is a hack since we didn't launch mux with handshakes.
   *
   * 1. Send an Rerr which we are certain can be interpreted by the first
   * implementations of mux.
   *
   * 2. If we receive a marker Rerr which echos back our message, we know
   * we can Tinit.
   */
  def canTinit(trans: Transport[Message, Message]): Future[Boolean] =
    trans.write(Message.Rerr(TinitTag, CanTinitMsg)).before {
      trans.read().transform {
        case Return(Message.Rerr(`TinitTag`, `CanTinitMsg`)) =>
          Future.True
        case _ =>
          Future.False
      }
    }

  /**
   * Returns a [[Transport]] that handles session negotiation from a client's
   * perspective. The client initiates the handshake via a `Tinit` message.
   * If the server responds appropriately with an `Rinit`, `trans` is transformed
   * via `negotiate` otherwise it's returned unchanged.
   *
   * @param trans the original transport established at the start of a mux
   * session (with no messages dispatched).
   *
   * @param version the version the client sends to the server.
   *
   * @param headers the headers the client sends to the server.
   *
   * @param negotiate a function which furnishes a transport based on the
   * the headers received from the server.
   */
  def client(
    trans: Transport[Buf, Buf],
    version: Short,
    headers: Headers,
    negotiate: Negotiator
  ): Transport[Message, Message] = {
    // Since the handshake happens at the start of a session, we can safely
    // enc/dec messages without having to worry about any special session
    // features.
    val msgTrans = trans.map(Message.encode, Message.decode)
    val handshake: Future[Transport[Message, Message]] =
      canTinit(msgTrans).transform {
        // We can start the official Tinit/Rinit handshake
        case Return(true) =>
          msgTrans.write(Message.Tinit(TinitTag, version, headers)).before {
            msgTrans.read().transform {
              case Return(Message.Rinit(_, v, serverHeaders)) if v == version =>
                Future(negotiate(Some(serverHeaders), trans))

              case Return(Message.Rerr(_, msg)) =>
                Future.exception(Failure(msg))

              case t @ Throw(_) =>
                Future.const(t.cast[Transport[Message, Message]])
            }
          }

        // If we can't init. Negotiation may be required for features like TLS
        // negotiation where this client demands encryption. It's important to
        // distinguish between receiving an Rinit without headers (Some(Seq.empty))
        // vs not negotiating (None) since we implicitly assume that if the server
        // can negotiate it supports fragmenting.
        case Return(false) => Future.value(negotiate(None, trans))

        case t @ Throw(_) =>
          Future.const(t.cast[Transport[Message, Message]])
      }

    handshake.onFailure { _ =>
      msgTrans.close()
    }
    new DeferredTransport(msgTrans, handshake)
  }

  /**
   * Returns a [[Transport]] that handles session negotiation from a server's
   * perspective. It reads the first message from the `trans` and if it is
   * an `Rinit`, transforms the transport via `negotiate`. If the client doesn't
   * support handshakes, the original `trans` is returned, making sure to replace
   * any messages we eagerly read from the transport.
   *
   * @param trans the original transport established at the start of a mux
   * session (with no outstanding messages).
   *
   * @param version the version sent to the client.
   *
   * @param headers a function which resolves the server headers with respect
   * to the client headers. This is structured this way since the headers the
   * server responds with are typically based on the clients.
   *
   * @param negotiate a function which transforms `trans` based on the
   * negotiated headers.
   */
  def server(
    trans: Transport[Buf, Buf],
    version: Short,
    headers: Headers => Headers,
    negotiate: Negotiator
  ): Transport[Message, Message] = {
    // Since the handshake happens at the start of a session, we can safely enc/dec
    // messages without having to worry about any special features (e.g. fragments).
    val msgTrans = trans.map(Message.encode, Message.decode)
    val handshake: Future[Transport[Message, Message]] =
      msgTrans.read().transform {
        // A Tinit with a matching version
        case Return(Message.Tinit(tag, ver, clientHeaders)) if ver == version =>
          val serverHeaders = headers(clientHeaders)
          msgTrans.write(Message.Rinit(tag, version, serverHeaders)).before {
            Future(negotiate(Some(clientHeaders), trans))
          }

        // A Tinit with a version mismatch. Write an Rerr and then return
        // a failed future.
        case Return(Message.Tinit(tag, ver, _)) =>
          val msg = s"unsupported version $ver, expected $version"
          msgTrans
            .write(Message.Rerr(tag, msg))
            .before { Future.exception(Failure(msg)) }

        // A marker Rerr that queries whether or not we can do handshaking.
        // Echo back the Rerr message to indicate that we can and recurse
        // so we can be ready to handshake again.
        case Return(rerr @ Message.Rerr(_, _)) =>
          msgTrans.write(rerr).before {
            Future.value(server(trans, version, headers, negotiate))
          }

        // Client did not start a session with negotiation. We need to run the
        // negotiation logic regardless since the configuration may demand
        // negotiable feature like TLS.
        // After completing negotiation, we need to inject the message that has
        // been consumed from the transport back into the message stream for
        // the session to consume.
        case Return(msg) =>
          Try(negotiate(None, trans)) match {
            case Return(t) =>
              Future.value(new TransportProxy(t) {
                private[this] val first = new AtomicBoolean(true)
                def read(): Future[Message] =
                  if (first.compareAndSet(true, false)) Future.value(msg)
                  else msgTrans.read()
                def write(req: Message): Future[Unit] = msgTrans.write(req)
              })

            case Throw(t) =>
              val errorMessage = s"Negotiation failed: ${t.getMessage}"
              msgTrans.write(Message.Rerr(msg.tag, errorMessage)).before {
                Future.exception(t)
              }
          }


        case Throw(_) => Future.value(msgTrans)
      }

    handshake.onFailure { _ =>
      msgTrans.close()
    }
    new DeferredTransport(msgTrans, handshake)
  }
}

/**
 * Implements a [[Transport]] in terms of a future transport. All async
 * operations are composed via future composition and callers can safely
 * interrupt the returned futures without affecting the result of `underlying`.
 *
 * @param init the transport to proxy synchronous operations to.
 *
 * @param underlying the transport which will be used once its containing future
 * is satisfied.
 */
private class DeferredTransport(
  init: Transport[Message, Message],
  underlying: Future[Transport[Message, Message]]
) extends Transport[Message, Message] {

  type Context = TransportContext

  // we create a derivative promise while `underlying` is not defined
  // because the transport is multiplexed and interrupting on one
  // stream shouldn't affect the result of the handshake.
  private[this] def gate(): Future[Transport[Message, Message]] =
    underlying.interruptible()

  def write(msg: Message): Future[Unit] = gate().transform {
    case Return(trans) => trans.write(msg)
    case t @ Throw(_) => Future.const(t.cast[Unit])
  }

  private[this] val read0: Try[Transport[Message, Message]] => Future[Message] = {
    case Return(trans) => trans.read()
    case t @ Throw(_) => Future.const(t.cast[Message])
  }
  def read(): Future[Message] = gate().transform(read0)

  def status: Status = underlying.poll match {
    case Some(Return(t)) => t.context.status
    case None => Status.Busy
    case _ => Status.Closed
  }

  val onClose: Future[Throwable] = gate().flatMap(_.context.onClose)

  def localAddress: SocketAddress = init.context.localAddress
  def remoteAddress: SocketAddress = init.context.remoteAddress
  def peerCertificate: Option[Certificate] = init.context.peerCertificate

  def close(deadline: Time): Future[Unit] = gate().flatMap(_.close(deadline))
  val context: TransportContext = new LegacyContext(this)
}
