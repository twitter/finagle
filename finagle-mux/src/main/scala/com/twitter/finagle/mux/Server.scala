package com.twitter.finagle.mux

import com.twitter.finagle.{CancelledRequestException, Context, Dtab, Service}
import com.twitter.finagle.mux.lease.exp.{Lessor, Lessee}
import com.twitter.finagle.tracing.{Trace, Annotation}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.util.{DefaultLogger, DefaultTimer}
import com.twitter.util._
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import org.jboss.netty.buffer.ChannelBuffer
import scala.collection.JavaConverters._

/**
 * Indicates that a client requested that a given request be discarded.
 *
 * This implies that the client issued a Tdiscarded message for a given tagged
 * request, as per [[com.twitter.finagle.mux]].
 */
case class ClientDiscardedRequestException(why: String) extends Exception(why)

object ServerDispatcher {
  val ServerEnabledTraceMessage = "finagle.mux.serverEnabled"
}

/**
 * A ServerDispatcher for the mux protocol.
 */
private[finagle] class ServerDispatcher private[finagle](
    trans: Transport[ChannelBuffer, ChannelBuffer],
    service: Service[ChannelBuffer, ChannelBuffer],
    canDispatch: Boolean, // XXX: a hack to indicate whether a server understand {T,R}Dispatch
    lessor: Lessor // the lessor that the dispatcher should register with in order to get leases
) extends Closable with Lessee {
  def this(
    trans: Transport[ChannelBuffer, ChannelBuffer],
    service: Service[ChannelBuffer, ChannelBuffer],
    canDispatch: Boolean
  ) = this(trans, service, canDispatch, Lessor.nil)

  import Message._

  lessor.register(this)

  // Used to buffer requests with in-progress local service invocation.
  private[this] val pending = new ConcurrentHashMap[Int, Future[_]]

  private[this] val log = DefaultLogger

  // TODO: rewrite Treqs into Tdispatches?

  @volatile private[this] var receive: Message => Unit = {
    case Tdispatch(tag, contexts, /*ignore*/_dst, dtab, req) =>
      lessor.observeArrival()
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
          Dtab.local ++= dtab
        val elapsed = Stopwatch.start()
        val f = service(req)
        pending.put(tag, f)
        f respond { tr =>
          pending.remove(tag)
          tr match {
            case Return(rep) =>
              lessor.observe(elapsed())
              // Record tracing info to track Mux adoption across clusters.
              Trace.record(ServerDispatcher.ServerEnabledTraceMessage)
              Trace.record(Annotation.ServerSend())
              trans.write(encode(RdispatchOk(tag, Seq.empty, rep)))
            case Throw(exc) =>
              trans.write(encode(RdispatchError(tag, Seq.empty, exc.toString)))
          }
        }
      }

    case Treq(tag, traceId, req) =>
      lessor.observeArrival()
      val saved = Trace.state
      try {
        for (traceId <- traceId)
          Trace.setId(traceId)
        Trace.record(Annotation.ServerRecv())

        // Record tracing info to track Mux adoption across clusters.
        Trace.record(ServerDispatcher.ServerEnabledTraceMessage)

        val elapsed = Stopwatch.start()
        val f = service(req)
        pending.put(tag, f)
        f respond { tr =>
          pending.remove(tag)
          tr match {
            case Return(rep) =>
              lessor.observe(elapsed())
              Trace.record(Annotation.ServerSend())
              trans.write(encode(RreqOk(tag, rep)))
            case Throw(exc) =>
              trans.write(encode(RreqError(tag, exc.toString)))
          }
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
    val exc = new CancelledRequestException(cause)
    pending.asScala.foreach { case (tag, f) =>
      pending.remove(tag)
      f.raise(exc)
    }

    lessor.unregister(this)
    trans.close()
  }

  trans.onClose ensure {
    service.close()
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
      trans.write(encode(Tdrain(1))) before {
        closep.within(DefaultTimer.twitter, deadline - Time.now) transform { _ =>
          lessor.unregister(this)
          trans.close()
        }
      }
    }
  }

  /**
   * Emit a lease to the clients of this server.
   */
  def issue(howlong: Duration) {
    require(howlong >= Tlease.MinLease)
    trans.write(encode(Tlease(howlong min Tlease.MaxLease)))
  }

  def npending(): Int = pending.size
}
