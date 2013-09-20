package com.twitter.finagle.mux

import com.twitter.finagle.{Service, Context}
import com.twitter.finagle.tracing.{Trace, Annotation}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.util.{Future, Return, Throw, Time, Closable, Local}
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import org.jboss.netty.buffer.ChannelBuffer
import scala.collection.JavaConverters._

case class ClientHangupException(cause: Throwable) extends Exception(cause)
case class ClientDiscardedRequestException(why: String) extends Exception(why)

/**
 * A ServerDispatcher for the mux protocol.
 */
private[finagle] class ServerDispatcher(
  trans: Transport[ChannelBuffer, ChannelBuffer],
  service: Service[ChannelBuffer, ChannelBuffer],
  canDispatch: Boolean
) extends Closable {
  import Message._

  private[this] val pending = new ConcurrentHashMap[Int, Future[_]]
  private[this] val tags = TagSet()
  private[this] val localAddress = trans.localAddress match {
    case ia: InetSocketAddress => ia
    case _ => new InetSocketAddress(0)
  }

  // TODO: rewrite Treqs into Tdispatches?

  @volatile private[this] var receive: Message => Unit = { 
    case Tdispatch(tag, contexts, req) =>
      if (!canDispatch) {
        // There seems to be a bug in the Scala pattern matcher:
        // 	case Tdispatch(..) if canDispatch => ..
        // results in a match error. Somehow the clause
        // 	case m@Tmessage(tag) =>
        // doesn't seem to be evaluated.
        val msg = Rerr(tag, "Tdispatch not enabled")
        trans.write(encode(msg))
      } else {
        val save = Local.save()
        try {
          for ((k, v) <- contexts)
            Context.handle(ChannelBufferBuf(k), ChannelBufferBuf(v))
          Trace.record(Annotation.ServerRecv())
          val f = service(req)
          pending.put(tag, f)
          f respond {
            case Return(rep) =>
              pending.remove(tag)
              Trace.record(Annotation.ServerSend())
              trans.write(encode(RdispatchOk(tag, Seq.empty, rep)))
            case Throw(exc) =>
              trans.write(encode(RdispatchError(tag, Seq.empty, exc.toString)))
          }
        } finally {
          Local.restore(save)
        }
      }

    case Treq(tag, traceId, req) =>
      val saved = Trace.state
      try {
        for (traceId <- traceId)
          Trace.setId(traceId)
        Trace.record(Annotation.ServerRecv())
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
      try {
        val m = decode(buf)
        receive(m)
        loop()
      } catch {
        case exc: BadMessageException =>
          // We could just ignore this message, but in reality it
          // probably means something is really FUBARd.
          Future.exception(exc)
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

  def close(deadline: Time): Future[Unit] = {
    receive = {
      case Treq(tag, _, _) =>
        trans.write(encode(RreqNack(tag)))
      case Tdispatch(tag, _, _) =>
        trans.write(encode(RdispatchNack(tag, Seq.empty)))
      case _ =>
        // Drop everything else. Is this OK?
    }

    for (tag <- tags.acquire())
      trans.write(encode(Tdrain(tag)))

    // TODO.
    Future.Done
  }
}

