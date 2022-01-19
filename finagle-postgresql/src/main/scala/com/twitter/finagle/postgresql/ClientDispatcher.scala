package com.twitter.finagle.postgresql

import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.RemovalCause
import com.github.benmanes.caffeine.cache.RemovalListener
import com.twitter.cache.FutureCache
import com.twitter.cache.caffeine.CaffeineCache
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceProxy
import com.twitter.finagle.Stack
import com.twitter.finagle.param
import com.twitter.finagle.postgresql.FrontendMessage.DescriptionTarget
import com.twitter.finagle.postgresql.Params.CancelGracePeriod
import com.twitter.finagle.postgresql.Params.MaxConcurrentPrepareStatements
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.finagle.postgresql.machine._
import com.twitter.finagle.stats.LazyStatsReceiver
import com.twitter.finagle.postgresql.transport.ClientTransport
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.{StateMachine => _, _}

/**
 * Handles transforming the Postgres protocol to an RPC style.
 *
 * The Postgres protocol is not of the style `request => Future[Response]`.
 * Instead, it uses a stateful protocol where each connection is in a particular state and streams of requests / responses
 * take place to move the connection from one state to another.
 *
 * The dispatcher is responsible for managing this connection state and transforming the stream of request / response to
 * a single request / response style that conforms to Finagle's request / response style.
 *
 * The dispatcher uses state machines to handle the connection state management.
 *
 * When a connection is established, the [[HandshakeMachine]] is immediately executed and takes care of authentication.
 * Subsequent machines to execute are based on the client's query. For example, if the client submits a [[Request.Query]],
 * then the [[SimpleQueryMachine]] will be dispatched to manage the connection's state.
 *
 * Any unexpected error from the state machine will lead to tearing down the connection to make sure we don't
 * reuse a connection in an unknown / bad state.
 *
 * @see [[StateMachine]]
 */
class ClientDispatcher(
  transport: ClientTransport,
  params: Stack.Params)
    extends GenSerialClientDispatcher[Request, Response, FrontendMessage, BackendMessage](
      transport,
      params[param.Stats].statsReceiver,
      closeOnInterrupt = false
    ) {
  private[this] val param.Timer(timer) = params[com.twitter.finagle.param.Timer]
  private[this] val param.Stats(statsReceiver) = params[param.Stats]
  private[this] val CancelGracePeriod(cancelGracePeriod) = params[CancelGracePeriod]
  private[this] val connectionParameters = transport.context.connectionParameters

  private[this] val machineRunner: Runner = new Runner(transport)

  private[this] val statsScope = statsReceiver.scope("psql", "serial")
  private[this] val cancelLatencyMsStat = statsScope.stat("cancel_latency")
  private[this] val cancelCounter = statsScope.counter("cancel")
  private[this] val cancelTimeoutCounter = statsScope.counter("cancel_timeout")

  def machineDispatch[R <: Response](machine: StateMachine[R], promise: Promise[R]): Future[Unit] =
    machineRunner
      .dispatch(machine, promise)
      .transform {
        case Return(_) => Future.Done
        case Throw(_) => close()
      }

  override protected def dispatch(req: Request, p: Promise[Response]): Future[Unit] =
    dispatchRequest(req, p, connectionParameters)

  private def dispatchRequest(
    req: Request,
    p: Promise[Response],
    parameters: Response.ConnectionParameters
  ): Future[Unit] = {
    val dispatchPromise = new Promise[Unit]

    p.setInterruptHandler {
      case intr => cancel(intr, p, dispatchPromise)
    }

    dispatchPromise.become(
      req match {
        case Request.ConnectionParameters =>
          p.setValue(parameters)
          Future.Done
        case Request.Sync => machineDispatch(StateMachine.syncMachine, p)
        case Request.Query(q) => machineDispatch(new SimpleQueryMachine(q, parameters), p)
        case Request.Prepare(s, name) => machineDispatch(new PrepareMachine(name, s), p)
        case e: Request.Execute =>
          machineDispatch(
            new ExecuteMachine(e, parameters, reason => cancel(reason, p, dispatchPromise)),
            p)
        case Request.CloseStatement(name) =>
          machineDispatch(new CloseMachine(name, DescriptionTarget.PreparedStatement), p)
      }
    )

    dispatchPromise
  }

  private def cancel(
    reason: Throwable,
    p: Promise[Response],
    dispatchP: Promise[Unit]
  ): Unit = {
    cancelCounter.incr()
    val timeout = cancelGracePeriod().getOrElse(Duration.Zero)
    val cancelTime = Time.now

    within(dispatchP, timeout, new TimeoutException(timeout.toString)).respond { _ =>
      cancelLatencyMsStat.add((Time.now - cancelTime).inMillis)
      p.updateIfEmpty(Throw(reason))

      if (!dispatchP.isDefined) {
        cancelTimeoutCounter.incr()
        transport.close()
      }
    }
  }

  private def within[A](future: Future[A], timeout: Duration, exc: => Throwable): Future[A] = {
    if (timeout.isZero) {
      // Short-circuit timeout expiration to avoid scheduling on the actual timer.
      if (future.isDefined) {
        future
      } else {
        Future.exception(exc)
      }
    } else {
      future.within(timer, timeout, exc)
    }
  }
}

object ClientDispatcher {
  def cached(
    transport: ClientTransport,
    params: Stack.Params
  ): Service[Request, Response] =
    PrepareCache(
      new ClientDispatcher(transport, params),
      params[MaxConcurrentPrepareStatements].num,
      params[param.Stats].statsReceiver
    )
}

/**
 * Caches statements that have been successfully prepared over the connection
 * managed by the underlying service (a ClientDispatcher). This decreases
 * the chances of leaking prepared statements and can simplify the
 * implementation of prepared statements in the presence of a connection pool.
 */
final case class PrepareCache(
  svc: Service[Request, Response],
  maxSize: Int,
  statsReceiver: StatsReceiver,
) extends ServiceProxy[Request, Response](svc) {

  private[this] val scopedStatsReceiver = new LazyStatsReceiver(
    statsReceiver.scope("pstmt-cache")
  )
  private[this] val calls = scopedStatsReceiver.counter("calls")
  private[this] val misses = scopedStatsReceiver.counter("misses")
  private[this] val evictionCounters =
    RemovalCause.values().map { rc =>
      scopedStatsReceiver.counter(s"evicted_${rc.name().toLowerCase}")
    }

  private[this] val listener = new RemovalListener[Name.Named, Future[Response]] {
    override def onRemoval(
      key: Name.Named,
      response: Future[Response],
      cause: RemovalCause
    ): Unit = {
      evictionCounters(cause.ordinal()).incr()
      response.respond {
        case Return(Response.ParseComplete(_)) =>
          svc(Request.CloseStatement(key))
        case _ =>
      }
    }
  }

  private[this] val underlying = Caffeine
    .newBuilder()
    .maximumSize(maxSize.toLong)
    .removalListener(listener)
    .build[Name.Named, Future[Response]]()

  private[this] val cacheSize = scopedStatsReceiver.addGauge("num_items") {
    underlying.estimatedSize()
  }

  private[this] val maxSizeGauge = scopedStatsReceiver.addGauge("max_size") {
    maxSize
  }

  // we only cache Named portals, the unnamed portal is destroyed automatically upon reuse.
  private[this] val extractPortalName: Request.Prepare => Name.Named = {
    case Request.Prepare(_, n: Name.Named) => n
    case r =>
      throw new PgSqlClientError(
        s"Unexpected request type ${r.getClass.getName}. The cache can only accept Request.Prepared requests with Name.Named portals. Other requests should have been filtered out."
      )
  }

  private[this] val fn = FutureCache.default(
    fn = { req: Request =>
      misses.incr()
      svc(req)
    },
    cache = FutureCache.keyEncoded(extractPortalName, new CaffeineCache(underlying))
  )

  override def apply(request: Request): Future[Response] =
    request match {
      case r @ Request.Prepare(_, Name.Named(_)) =>
        calls.incr()
        fn(r)
      case _ => super.apply(request)
    }

  override def close(deadline: Time): Future[Unit] = {
    maxSizeGauge.remove()
    cacheSize.remove()

    super.close(deadline)
  }
}
