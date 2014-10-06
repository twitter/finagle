package com.twitter.finagle.mux.exp

import com.twitter.concurrent.{Spool, SpoolSource}
import com.twitter.finagle.mux._
import com.twitter.finagle.netty3.{
  BufChannelBuffer, ChannelBufferBuf, Netty3Listener, Netty3Transporter}
import com.twitter.finagle.stats.ClientStatsReceiver
import com.twitter.finagle.tracing.{Trace, Annotation}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Dtab, CancelledRequestException, Context, ListeningServer, Stack}
import com.twitter.io.Buf
import com.twitter.util._
import java.net.SocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger
import org.jboss.netty.buffer.ChannelBuffer
import scala.collection.JavaConverters._

object Session {
  /**
   * Upon connecting to `addr` a Session is established by providing a
   * MuxService which will handle incoming messages from the other
   * party.
   */
  def connect(addr: SocketAddress): Future[SessionFactory] = {
    val transporter = new Netty3Transporter[ChannelBuffer, ChannelBuffer]("mux", PipelineFactory)
    transporter(addr, ClientStatsReceiver) map { transport =>
      (serverImpl: MuxService) => {
        val clientDispatcher = new ClientDispatcher(transport)
        new Session(clientDispatcher, serverImpl, transport)
      }
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
      private[this] val listener = new Netty3Listener[ChannelBuffer, ChannelBuffer]("mux", PipelineFactory)
      private[this] val server = listener.listen(addr) { transport =>
        sessionHandler { receiver =>
          val clientDispatcher = new ClientDispatcher(transport)
          val session = new Session(clientDispatcher, receiver, transport)
          serverDeadlinep onSuccess { deadline =>
            clientDispatcher.drain() before session.close(deadline)
          }
          session
        }
      }

      def boundAddress = server.boundAddress

      def closeServer(deadline: Time) = closeAwaitably {
        serverDeadlinep.setValue(deadline)
        server.close(deadline)
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
  import Spool.*::

  private[this] val log = Logger.getLogger(getClass.getName)
  private[this] val pending = new ConcurrentHashMap[Int, Future[_]]
  private[this] val incoming = new ConcurrentHashMap[Int, SpoolSource[Buf]].asScala

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
      // One-way message
      case Tdispatch(tag, contexts, /*ignore*/_dst, dtab, buf) if tag == MarkerTag =>
        for ((k, v) <- contexts)
          Context.handle(ChannelBufferBuf(k), ChannelBufferBuf(v))
        if (dtab.length > 0)
          Dtab.local ++= dtab

        service.send(ChannelBufferBuf(buf))

      // RPC
      case Tdispatch(tag, contexts, /*ignore*/_dst, dtab, req) =>
        val masked = tag & MaxTag
        if (clientDispatcher.isDraining && !incoming.contains(masked)) {
          trans.write(encode(RdispatchNack(masked, Seq.empty)))
        } else {
          for ((k, v) <- contexts)
            Context.handle(ChannelBufferBuf(k), ChannelBufferBuf(v))
          if (dtab.length > 0)
            Dtab.local ++= dtab

          val source = incoming.getOrElseUpdate(masked, {
            val source = new SpoolSource[Buf]
            val head = source()
            val f = head flatMap { service(_) }
            pending.put(masked, f)
            f respond { rsp => writeResponseFragments(masked, rsp) }
            source
          })

          if ((tag & TagMSB) == 0) {
            incoming.remove(masked)
            Trace.record(Annotation.ServerRecv())
            source.offerAndClose(ChannelBufferBuf(req))
          } else {
            Trace.record(Annotation.ServerRecvFragment())
            source.offer(ChannelBufferBuf(req))
          }
        }

      case Tdiscarded(tag, why) =>
        val exc = new ClientDiscardedRequestException(why)

        // Note: we abuse Tdiscarded and fail any streaming request
        // associated with `tag`.  A future revision of the protocol
        // should solve this with a status byte in Tdispatch.
        for (source <- incoming.remove(tag))
          source.raise(exc)

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

  private[this] def writeResponseFragments(
    masked: Int,
    rsp: Try[Spool[Buf]]
  ) {
    def continue(buf: Buf): ChannelBuffer =
      encode(RdispatchOk(masked | TagMSB, Seq.empty, BufChannelBuffer(buf)))

    def terminal(buf: Buf): ChannelBuffer =
      encode(RdispatchOk(masked, Seq.empty, BufChannelBuffer(buf)))

    def error(exc: Throwable): ChannelBuffer =
      encode(RdispatchError(masked, Seq.empty, exc.toString))

    def loop(s: Spool[Buf]) { s match {
      case Spool.Empty =>
        pending.remove(masked)
        Trace.record(Annotation.ServerSend())
        trans.write(terminal(Buf.Empty))

      case buf *:: Future(Return(Spool.Empty)) =>
        pending.remove(masked)
        Trace.record(Annotation.ServerSend())
        trans.write(terminal(buf))

      case buf *:: Future(Return(tail)) =>
        Trace.record(Annotation.ServerSendFragment())
        trans.write(continue(buf)) onSuccess { case () =>
          loop(tail)
        }

      case buf *:: deferred =>
        Trace.record(Annotation.ServerSendFragment())
        trans.write(continue(buf)) onSuccess { case () =>
          deferred respond {
            case Return(tail) =>
              loop(tail)
            case Throw(exc) =>
              pending.remove(masked)
              trans.write(error(exc))
          }
        }
    }}

    rsp match {
      case Return(Spool.Empty) =>
        pending.remove(masked)
        trans.write(error(new Exception("Server returned an empty sequence")))

      case Return(s) =>
        loop(s)

      case Throw(exc) =>
        pending.remove(masked)
        trans.write(error(exc))
    }
  }

  private[this] def loop(): Future[Nothing] =
    trans.read() flatMap { buf =>
      val save = Local.save()
      val m = try decode(buf) catch {
        case exc: BadMessageException =>
          // We could just ignore this message, but in reality it
          // probably means something is really FUBARd.
          return Future.exception(exc)
      }

      dispatch(m)
      Local.restore(save)
      loop()
    }

  loop() onFailure { case cause =>
    // We know that if we have a failure, we cannot from this point forward
    // insert new entries in the pending map.
    val exc = new CancelledRequestException(cause)
    for ((_, f) <- pending.asScala)
      f.raise(exc)
    pending.clear()
    trans.close()
    clientDispatcher.failSent(cause)
  }
}
