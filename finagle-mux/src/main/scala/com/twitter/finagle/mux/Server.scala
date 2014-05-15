package com.twitter.finagle.mux

import com.twitter.finagle.{Context, Dtab, Service, WriteException}
import com.twitter.finagle.tracing.{Trace, Annotation}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.util.DefaultLogger
import com.twitter.util.{Closable, Future, Local, Promise, Return, Throw, Time}
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import scala.collection.JavaConverters._

case class ClientHangupException(cause: Throwable) extends Exception(cause)
case class ClientDiscardedRequestException(why: String) extends Exception(why)

object ServerDispatcher {
  val ServerEnabledTraceMessage = "finagle.mux.serverEnabled"
}

/**
 * A ServerDispatcher for the mux protocol.
 */
private[finagle] class ServerDispatcher(
  trans: Transport[ChannelBuffer, ChannelBuffer],
  service: Service[ChannelBuffer, ChannelBuffer],
  canDispatch: Boolean
) extends Closable {
  import Message._

  // Used to buffer requests with in-progress local service invocation.
  private[this] val pending = new ConcurrentHashMap[Int, Future[_]]

  // TagMap used specifically for tag-unmapping on receipt of Rdrain.
  private[this] val tags = TagSet()
  private[this] val log = DefaultLogger

  private[this] val localAddress = trans.localAddress match {
    case ia: InetSocketAddress => ia
    case _ => new InetSocketAddress(0)
  }

  // TODO: rewrite Treqs into Tdispatches?

  @volatile private[this] var receive: Message => Unit = {
    case Tdispatch(tag, contexts, /*ignore*/_dst, dtab, req) =>
      if (!canDispatch) {
        // There seems to be a bug in the Scala pattern matcher:
        // 	case Tdispatch(..) if canDispatch => ..
        // results in a match error. Somehow the clause
        // 	case m@Tmessage(tag) =>
        // doesn't seem to be evaluated.
        val msg = Rerr(tag, "Tdispatch not enabled")
        trans.write(encode(msg))
      } else {
        for ((k, v) <- contexts)
          Context.handle(ChannelBufferBuf(k), ChannelBufferBuf(v))
        Trace.record(Annotation.ServerRecv())
        if (dtab.length > 0)
          Dtab.delegate(dtab)
        val f = service(req)
        pending.put(tag, f)
        f respond {
          case Return(rep) =>
            pending.remove(tag)

              // Record tracing info to track Mux adoption across clusters.
              Trace.record(ServerDispatcher.ServerEnabledTraceMessage)
            Trace.record(Annotation.ServerSend())
            trans.write(encode(RdispatchOk(tag, Seq.empty, rep)))
          case Throw(exc) =>
            trans.write(encode(RdispatchError(tag, Seq.empty, exc.toString)))
        }
      }

    case Treq(tag, traceId, req) =>
      val saved = Trace.state
      try {
        for (traceId <- traceId)
          Trace.setId(traceId)
        Trace.record(Annotation.ServerRecv())

        // Record tracing info to track Mux adoption across clusters.
        Trace.record(ServerDispatcher.ServerEnabledTraceMessage)

        val f = service(req)
        pending.put(tag, f)
        f respond {
          case Return(rep) =>
            pending.remove(tag)
            Trace.record(Annotation.ServerSend())
            trans.write(encode(RreqOk(tag, rep)))
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
      trans.write(encode(Rping(tag)))

    case m@Tmessage(tag) =>
      val msg = Rerr(tag, "Did not understand Tmessage %d".format(m.typ))
      trans.write(encode(msg))
  }

  private[this] def loop(): Future[Nothing] =
    trans.read() flatMap { buf =>
      val save = Local.save()
      try {
        val m = decode(buf)
        receive(m)
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
  }

  private[this] val closing = new AtomicBoolean(false)
  private[this] val closep = new Promise[Unit]

  /**
   * Close the mux Server. Any further messages received will be either ignored
   * or else responded to with the message type's corresponding Nack message.
   * A drainage message is issued to the client. The Future result of this
   * method is not satisfied until a closure acknowledgement is received from
   * the client.
   */
  def close(deadline: Time): Future[Unit] = {
    if (!closing.compareAndSet(false, true))
      closep
    else {
      receive = {
        case Treq(tag, _, _) =>
          trans.write(encode(RreqNack(tag)))
        case Tdispatch(tag, _, _, _, _) =>
          trans.write(encode(RdispatchNack(tag, Seq.empty)))
        case Rdrain(1) =>
          closep.setDone()
        case Rerr(tag, error) =>
          log.warning("Error received for tag=%d after server close: %s".format(tag, error))
        case unexpected =>
          trans.write(encode(Rerr(
            unexpected.tag,
            "Unexpected message of type %s received after server shutdown".format(
              unexpected.getClass.getName
            )
          )))
      }

      // Tdrain is the only T-typed message that servers ever send, so we don't
      // need to allocate a distinct tag to differentiate messages.
      trans.write(encode(Tdrain(1))).map(Function.const(closep))
    }
  }
}
