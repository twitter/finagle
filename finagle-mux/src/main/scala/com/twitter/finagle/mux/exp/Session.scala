package com.twitter.finagle.mux.exp

import com.twitter.finagle.{Context, ListeningServer, MuxListener, MuxTransporter}
import com.twitter.finagle.mux._
import com.twitter.finagle.netty3.{BufChannelBuffer, ChannelBufferBuf}
import com.twitter.finagle.stats.ClientStatsReceiver
import com.twitter.finagle.tracing.{Trace, Annotation, TraceId}
import com.twitter.finagle.transport.Transport
import com.twitter.io.Buf
import com.twitter.util._
import java.net.SocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import java.util.logging.Logger
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import scala.collection.JavaConverters._

object Session {
  /**
   * Upon connecting to `addr` a Session is established by providing a
   * MuxService which will handle incoming messages from the other
   * party.
   */
  def connect(addr: SocketAddress): Future[SessionFactory] =
    MuxTransporter(addr, ClientStatsReceiver) map { transport =>
      (serverImpl: MuxService) => {
        val clientDispatcher = new ClientDispatcher(transport)
        new Session(clientDispatcher, serverImpl, transport)
      }
    }

  /**
   * Listen for connections on addr. For each connection,
   * `sessionHandler` is invoked which should establish a Session by
   * providing a MuxService to the SessionFactory which will handle
   * incoming messages from the other party.
   */
  def listen(
    addr: SocketAddress,
    sessionHandler: SessionHandler
  ): ListeningServer with CloseAwaitably =
    new ListeningServer with CloseAwaitably {
      private[this] val serverDeadlinep = new Promise[Time]
      private[this] val listener = MuxListener.listen(addr) { transport =>
        sessionHandler { receiver =>
          val clientDispatcher = new ClientDispatcher(transport)
          val session = new Session(clientDispatcher, receiver, transport)
          serverDeadlinep onSuccess { deadline =>
            clientDispatcher.drain() before session.close(deadline)
          }
          session
        }
      }

      def boundAddress = listener.boundAddress

      def closeServer(deadline: Time) = closeAwaitably {
        serverDeadlinep.setValue(deadline)
        listener.close(deadline)
      }
    }
}

/**
 * Session repeatedly calls recv() on transport, and dispatches the messages
 * to the appropriate Session instance.
 */
class Session private[finagle](
  clientDispatcher: ClientDispatcher,
  service: MuxService,
  trans: Transport[ChannelBuffer, ChannelBuffer]
) extends Closable {
  import Message._

  private[this] val log = Logger.getLogger(getClass.getName)
  private[this] val pending = new ConcurrentHashMap[Int, Future[_]]

  def client: MuxService = clientDispatcher
  def close(deadline: Time) =
    Closable.sequence(
      Closable.all(client, service),
      trans
    ).close(deadline)

  def onClose: Future[Throwable] = trans.onClose
  def remoteAddress = trans.remoteAddress
  def localAddress = trans.localAddress

  private[this] def dispatch(message: Message) {
    message match {
      // A one-way message
      case Treq(MarkerTag, None, buf) =>
        service.send(ChannelBufferBuf(buf))

      case Tdispatch(tag, contexts, _dst, _dtab, req) =>
        if (clientDispatcher.isDraining()) {
          trans.write(encode(RdispatchNack(tag, Seq.empty)))
        } else {
          for ((k, v) <- contexts)
            Context.handle(ChannelBufferBuf(k), ChannelBufferBuf(v))
          Trace.record(Annotation.ServerRecv())
          val f = service(ChannelBufferBuf(req))
          pending.put(tag, f)
          f respond {
            case Return(rsp) =>
              pending.remove(tag)
              Trace.record(Annotation.ServerSend())
              trans.write(encode(RdispatchOk(tag, Seq.empty, BufChannelBuffer(rsp))))
            case Throw(exc) =>
              trans.write(encode(RdispatchError(tag, Seq.empty, exc.toString)))
          }
        }

      case Treq(tag, traceId, req) =>
        if (clientDispatcher.isDraining()) {
          trans.write(encode(RreqNack(tag)))
          return
        }
        val saved = Trace.state
        try {
          for (traceId <- traceId)
            Trace.setId(traceId)
          Trace.record(Annotation.ServerRecv())
          val f = service(ChannelBufferBuf(req))
          pending.put(tag, f)
          f respond {
            case Return(rsp) =>
              pending.remove(tag)
              Trace.record(Annotation.ServerSend())
              trans.write(encode(RreqOk(tag, BufChannelBuffer(rsp))))
            case Throw(exc) =>
              trans.write(encode(RreqError(tag, exc.toString)))
          }
        } finally {
          Trace.state = saved
        }

      case Tdiscarded(tag, why) =>
        pending.get(tag) match {
          case null => ()
          case f => f.raise(new ClientDiscardedRequestException(why))
        }

      case Tping(tag) =>
        service.ping() onSuccess { _ =>
          trans.write(encode(Rping(tag)))
        }

      case Tdrain(tag) =>
        service.drain() onSuccess { _ =>
          trans.write(encode(Rdrain(tag)))
        }

      case m@Tmessage(tag) =>
        log.warning("Did not understand Tmessage[tag=%d] %s".format(tag, m))
        trans.write(encode(Rerr(tag, "badmessage")))

      case rmsg@Rmessage(tag) =>
        clientDispatcher.recv(rmsg)
    }
  }

  private[this] def loop(): Future[Nothing] =
    trans.read() flatMap { buf =>
      val save = Local.save()
      try {
        val m = decode(buf)
        dispatch(m)
        loop()
      } catch {
        case exc: BadMessageException =>
          // We could just ignore this message, but in reality it
          // probably means something is really FUBARd.
          Future.exception(exc)
      } finally {
        Local.restore(save)
      }
    }

  loop() onFailure { case cause =>
    // We know that if we have a failure, we cannot from this point forward
    // insert new entries in the pending map.
    val exc = ClientHangupException(cause)
    for ((_, f) <- pending.asScala)
      f.raise(exc)
    pending.clear()
    trans.close()
    clientDispatcher.failSent(cause)
  }
}
