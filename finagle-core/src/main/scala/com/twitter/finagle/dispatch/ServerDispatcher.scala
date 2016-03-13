package com.twitter.finagle.dispatch

import com.twitter.finagle.context.{Contexts, RemoteInfo}
import com.twitter.finagle.tracing.{Annotation, Trace, Tracer, TraceId}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Service, NoStacktrace, CancelledRequestException}
import com.twitter.util._
import java.util.concurrent.atomic.AtomicReference

/**
  * A base Dispatcher type to allow for easy tracing upon `WireRecv` and
  * `WireSend` by the 
  * [[com.twitter.finagle.transport.Transport transporter]]. Clients should
  * provided custom behavior via the handle and dispatch methods
  * @tparam Req the type of Request
  * @tparam Rep the type of Response
  * @tparam Out the type of message coming "out" of a
  *  [[com.twitter.finagle.transport.Transport Transport]]
  * @param init a [[ServerDispatcherInitializer]]
  */
abstract class ServerDispatcher[Req, Rep, Out] extends Closable {

  protected val RecordWireSend: Unit => Unit = _ => Trace.record(Annotation.WireSend)

  /**
   * Dispatches a request. The first argument is the request. The second
   * argument `eos` (end-of-stream promise) must be fulfilled when the request
   * is complete.
   *
   * For non-streaming requests, `eos.setDone()` should be called immediately,
   * since the entire request is present. For streaming requests,
   * `eos.setDone()` must be called at the end of stream (in HTTP, this is on
   * receipt of last chunk). Refer to the implementation in
   * [[com.twitter.finagle.http.codec.HttpServerDispatcher]].
   */
  protected def dispatch(req: Out, eos: Promise[Unit]): Future[Rep]

  /**
    * Handles "business logic" of a protocol writing a `response` to the wire
    * via a [[com.twitter.finagle.transport.Transport Transport]]
    *
    * @param rep the Reponse to send out
    */
  protected def handle(rep: Rep): Future[Unit]
}

/**
  * Used by the [[ServerDispatcher]] to initialize tracing
  *
  * @tparam Req the Request type of the [[ServerDispatcher]]
  * @tparam Rep the Response type of the [[ServerDispatcher]]
  * @param tracer the Tracer to be used
  * @param fReq a function from `Request` type to 
  *  [[com.twitter.finagle.tracing.TraceId TraceId]]
  * @param fRep a function from `Response` type to
  *  [[com.twitter.finagle.tracing.TraceId TraceId]]
  */
case class ServerDispatcherInitializer(tracer: Tracer, fReq: Any => Option[TraceId], 
  fRep: Any => Option[TraceId])

object GenSerialServerDispatcher {
  private val Eof = Future.exception(new Exception("EOF") with NoStacktrace)
  // We don't use Future.never here, because object equality is important here
  private val Idle = new NoFuture
  private val Closed = new NoFuture
}

/**
 * A generic version of
 * [[com.twitter.finagle.dispatch.SerialServerDispatcher SerialServerDispatcher]],
 * allowing the implementor to furnish custom dispatchers & handlers.
 */
abstract class GenSerialServerDispatcher[Req, Rep, In, Out](trans: Transport[In, Out],
  init: ServerDispatcherInitializer) extends ServerDispatcher[Req, Rep, Out] {

  import GenSerialServerDispatcher._

  private[this] val state = new AtomicReference[Future[_]](Idle)
  private[this] val cancelled = new CancelledRequestException

  private[this] def loop(): Future[Unit] = {
    state.set(Idle)
    trans.read() flatMap { req =>
      val p = new Promise[Rep]
      if (state.compareAndSet(Idle, p)) {
        val eos = new Promise[Unit]
        val save = Local.save()
        try {
          Contexts.local.let(RemoteInfo.Upstream.AddressCtx, trans.remoteAddress) {
            trans.peerCertificate match {
              case None => p.become(dispatch(req, eos))
              case Some(cert) => Contexts.local.let(Transport.peerCertCtx, cert) {
                p.become(dispatch(req, eos))
              }
            }
          }
        } finally Local.restore(save)
        p map { res => (res, eos) }
      } else Eof
    } flatMap { case (rep, eos) =>
      Future.join(handle(rep), eos).unit
    } respond {
      case Return(()) if state.get ne Closed =>
        loop()

      case _ =>
        trans.close()
    }
  }

  // Clear all locals to start the loop; we want a clean slate.
  private[this] val looping = Local.letClear { loop() }

  trans.onClose ensure {
    looping.raise(cancelled)
    state.getAndSet(Closed).raise(cancelled)
  }

  /** Exposed for testing */
  protected[dispatch] def isClosing: Boolean = state.get() eq Closed

  // Note: this is racy, but that's inherent in draining (without
  // protocol support). Presumably, half-closing TCP connection is
  // also possible.
  def close(deadline: Time) = {
    if (state.getAndSet(Closed) eq Idle)
      trans.close(deadline)
    trans.onClose.unit
  }
}

/**
 * Dispatch requests from transport one at a time, queueing
 * concurrent requests. Transport errors are considered fatal; 
 * the service will be released after any error.
 *
 * @tparam Req the `Request` type for the [[ServerDispatcher]]
 * @tparam Rep the `Response` type for the [[ServerDispatcher]]
 * @param trans a [[com.twitter.finagle.transport.Transport Transport]]
 * @param service 
 * @param init a [[ServerDispatcherInitializer]] used to init tracing
 */
class SerialServerDispatcher[Req, Rep](
    trans: Transport[Rep, Req],
    service: Service[Req, Rep],
    init: ServerDispatcherInitializer)
    extends GenSerialServerDispatcher[Req, Rep, Rep, Req](trans, init) {

  trans.onClose ensure {
    service.close()
  }

  protected def dispatch(req: Req, eos: Promise[Unit]) = init.fReq(req) match {
    case Some(traceId) => Trace.letTracerAndId(init.tracer, traceId) {
      Trace.record(Annotation.WireRecv)
      service(req) ensure eos.setDone()
    }
    case None => service(req) ensure eos.setDone()
  }

  protected def handle(rep: Rep) = init.fRep(rep) match {
    case Some(traceId) => Trace.letTracerAndId(init.tracer, traceId) {
      trans.write(rep).onSuccess(RecordWireSend)
    }
    case None => trans.write(rep)
  }
}
