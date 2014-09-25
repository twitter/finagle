package com.twitter.finagle.mux

import com.twitter.conversions.time._
import com.twitter.finagle.mux.lease.exp.{Lessor, Lessee, nackOnExpiredLease}
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.tracing.{Annotation, DefaultTracer, Trace, Tracer}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util.{DefaultLogger, DefaultTimer}
import com.twitter.finagle.{CancelledRequestException, Context, Dtab, Service, WriteException}
import com.twitter.util._
import java.net.InetSocketAddress
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
  val Epsilon = 1.second // TODO decide whether this should be hard coded or not
}

/**
 * A ServerDispatcher for the mux protocol.
 */
private[finagle] class ServerDispatcher private[finagle](
    trans: Transport[ChannelBuffer, ChannelBuffer],
    service: Service[ChannelBuffer, ChannelBuffer],
    canDispatch: Boolean, // XXX: a hack to indicate whether a server understand {T,R}Dispatch
    lessor: Lessor, // the lessor that the dispatcher should register with in order to get leases
    tracer: Tracer
) extends Closable with Lessee {
  def this(
    trans: Transport[ChannelBuffer, ChannelBuffer],
    service: Service[ChannelBuffer, ChannelBuffer],
    canDispatch: Boolean
  ) = this(trans, service, canDispatch, Lessor.nil, DefaultTracer)

  def this(
    trans: Transport[ChannelBuffer, ChannelBuffer],
    service: Service[ChannelBuffer, ChannelBuffer],
    canDispatch: Boolean,
    lessor: Lessor
  ) = this(trans, service, canDispatch, lessor, DefaultTracer)

  def this(
    trans: Transport[ChannelBuffer, ChannelBuffer],
    service: Service[ChannelBuffer, ChannelBuffer],
    canDispatch: Boolean,
    tracer: Tracer
  ) = this(trans, service, canDispatch, Lessor.nil, tracer)

  import Message._

  lessor.register(this)

  @volatile private[this] var lease = Tlease.MaxLease
  @volatile private[this] var curElapsed = NilStopwatch.start()

  // Used to buffer requests with in-progress local service invocation.
  private[this] val pending = new ConcurrentHashMap[Int, Future[_]]

  private[this] val log = DefaultLogger

  // TODO: rewrite Treqs into Tdispatches?
  @volatile protected var receive: Message => Unit = {
    case Tdispatch(tag, contexts, /*ignore*/_dst, dtab, req) =>
      if (nackOnExpiredLease() && (lease <= Duration.Zero))
        trans.write(encode(RdispatchNack(tag, Seq.empty)))
      else {
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
          Trace.unwind {
            Trace.pushTracer(tracer)
            for ((k, v) <- contexts)
              Context.handle(ChannelBufferBuf(k), ChannelBufferBuf(v))
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
                  trans.write(encode(RdispatchOk(tag, Seq.empty, rep)))
                case Throw(exc) =>
                  trans.write(encode(RdispatchError(tag, Seq.empty, exc.toString)))
              }
            }
          }
        }
      }

    case Treq(tag, traceId, req) =>
      lessor.observeArrival()
      Trace.unwind {
        Trace.pushTracer(tracer)
        for (traceId <- traceId)
          Trace.setId(traceId)

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
              trans.write(encode(RreqOk(tag, rep)))
            case Throw(exc) =>
              trans.write(encode(RreqError(tag, exc.toString)))
          }
        }
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

  Local.letClear { loop() } onFailure { case cause =>
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
   * Emit a lease to the clients of this server.  If howlong is less than or
   * equal to 0, also nack all requests until a new lease is issued.
   */
  def issue(howlong: Duration) {
    require(howlong >= Tlease.MinLease)

    synchronized {
      val diff = (lease - curElapsed()).abs
      if (diff > ServerDispatcher.Epsilon) {
        curElapsed = Stopwatch.start()
        lease = howlong
        trans.write(encode(Tlease(howlong min Tlease.MaxLease)))
      } else if ((howlong < Duration.Zero) && (lease > Duration.Zero)) {
        curElapsed = Stopwatch.start()
        lease = howlong
      }
    }
  }

  def npending(): Int = pending.size
}
