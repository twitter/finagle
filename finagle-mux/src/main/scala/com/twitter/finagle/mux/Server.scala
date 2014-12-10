package com.twitter.finagle.mux

import com.twitter.app.GlobalFlag
import com.twitter.conversions.time._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.mux.lease.exp.{Lessor, Lessee, nackOnExpiredLease}
import com.twitter.finagle.netty3.{BufChannelBuffer, ChannelBufferBuf}
import com.twitter.finagle.tracing.{NullTracer, Trace, Tracer}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util.{DefaultLogger, DefaultTimer}
import com.twitter.finagle.{CancelledRequestException, Dtab, Service, Path}
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
  val Epsilon = 1.second // TODO decide whether this should be hard coded or not
}

object gracefulShutdownEnabled extends GlobalFlag(true, "Graceful shutdown enabled.  Temporary measure to allow servers to deploy without hurting clients.")

/**
 * A ServerDispatcher for the mux protocol.
 */
private[finagle] class ServerDispatcher private[finagle](
    trans: Transport[ChannelBuffer, ChannelBuffer],
    service: Service[Request, Response],
    canDispatch: Boolean, // XXX: a hack to indicate whether a server understands {T,R}Dispatch
    lessor: Lessor, // the lessor that the dispatcher should register with in order to get leases
    tracer: Tracer
) extends Closable with Lessee {

  import Message._

  lessor.register(this)

  @volatile private[this] var lease = Tlease.MaxLease
  @volatile private[this] var curElapsed = NilStopwatch.start()

  // Used to buffer requests with in-progress local service invocation.
  private[this] val pending = new ConcurrentHashMap[Int, Future[_]]

  private[this] val log = DefaultLogger

  //// TODO: rewrite Treqs into Tdispatches?
  private[this] def dispatch(tdispatch: Tdispatch): Unit = {
    val Tdispatch(tag, contexts, dst, dtab, bytes) = tdispatch
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
        val contextBufs = contexts map { case (k, v) =>
          ChannelBufferBuf(k) -> ChannelBufferBuf(v)
        }
        Contexts.broadcast.letUnmarshal(contextBufs) {
          if (dtab.length > 0)
            Dtab.local ++= dtab
          val elapsed = Stopwatch.start()
          val f = service(Request(dst, ChannelBufferBuf.Owned(bytes)))
          pending.put(tag, f)
          f respond { tr =>
            removeTag(tag)
            tr match {
              case Return(rep) =>
                lessor.observe(elapsed())
                trans.write(encode(RdispatchOk(tag, Seq.empty, BufChannelBuffer(rep.body))))
              case Throw(exc) =>
                trans.write(encode(RdispatchError(tag, Seq.empty, exc.toString)))
            }
          }
        }
      }
    }
  }

  private[this] def dispatchTreq(treq: Treq): Unit = {
    val Treq(tag, traceId, bytes) = treq
    lessor.observeArrival()
    Trace.letIdOption(traceId) {
      val elapsed = Stopwatch.start()
      val f = service(Request(Path.empty, ChannelBufferBuf.Owned(bytes)))
      pending.put(tag, f)
      f respond { tr =>
        removeTag(tag)
        tr match {
          case Return(rep) =>
            lessor.observe(elapsed())
            trans.write(encode(RreqOk(tag, BufChannelBuffer(rep.body))))
          case Throw(exc) =>
            trans.write(encode(RreqError(tag, exc.toString)))
        }
      }
    }
  }

  // TODO: rewrite Treqs into Tdispatches?
  private[this] val handle: PartialFunction[Message, Unit] = {
    case tdispatch: Tdispatch => dispatch(tdispatch)
    case treq: Treq => dispatchTreq(treq)
    case Tdiscarded(tag, why) =>
      pending.get(tag) match {
        case null => ()
        case f => f.raise(new ClientDiscardedRequestException(why))
      }
    case Tping(tag) =>
      trans.write(encode(Rping(tag)))
    case m@Tmessage(tag) =>
      val msg = Rerr(tag, f"Did not understand Tmessage ${m.typ}%d")
      trans.write(encode(msg))
  }

  private[this] val nackRequests: Message => Unit = {
    case tdispatch: Tdispatch =>
      trans.write(encode(RdispatchNack(tdispatch.tag, Nil)))
    case treq: Treq =>
      trans.write(encode(RreqNack(treq.tag)))
    case Rerr(tag, error) =>
      log.warning(f"Error received for tag=$tag%d after server close: $error%s")
    case Tping(tag) => // swallow pings
    case unexpected =>
      val name = unexpected.getClass.getName
      trans.write(encode(Rerr(
        unexpected.tag,
        s"Unexpected message of type $name received after server shutdown"
      )))
  }

  private[this] val readyToDrain: PartialFunction[Message, Unit] = {
    case Rdrain(1) =>
      draining = true
      receive = nackRequests
      if (pending.isEmpty) {
        log.info("Finished draining a connection")
        closep.setDone()
      } // OK because calls to receive are serial
  }

  // handle is a PartialFunction for the API, but is actually total
  @volatile protected var receive: Message => Unit = handle

  private[this] def removeTag(tag: Int): Unit = {
    pending.remove(tag)
    if (draining && pending.isEmpty) {
      log.info("Finished draining a connection")
      closep.setDone()
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

      receive(m)
      Local.restore(save)
      loop()
    }

  Local.letClear { 
    Trace.letTracer(tracer) { loop() }
  } onFailure { case cause =>
    // We know that if we have a failure, we cannot from this point forward
    // insert new entries in the pending map.
    val exc = new CancelledRequestException(cause)
    pending.asScala.foreach { case (tag, f) =>
      removeTag(tag)
      f.raise(exc)
    }

    lessor.unregister(this)
    trans.close()
  }

  trans.onClose ensure {
    service.close()
  }

  private[this] val closing = new AtomicBoolean(false)
  @volatile private[this] var draining = false
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
      receive = readyToDrain orElse handle

      if (gracefulShutdownEnabled()) {
        // Tdrain is the only T-typed message that servers ever send, so we don't
        // need to allocate a distinct tag to differentiate messages.
        log.info("Started draining a connection")
        trans.write(encode(Tdrain(1))) before {
          closep.within(DefaultTimer.twitter, deadline - Time.now) transform { _ =>
            lessor.unregister(this)
            trans.close()
          }
        }
      } else {
        closep.setDone
        lessor.unregister(this)
        trans.close()
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
