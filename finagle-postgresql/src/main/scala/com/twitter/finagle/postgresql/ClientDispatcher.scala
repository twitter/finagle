package com.twitter.finagle.postgresql

import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.RemovalCause
import com.github.benmanes.caffeine.cache.RemovalListener
import com.twitter.cache.FutureCache
import com.twitter.cache.caffeine.CaffeineCache
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceProxy
import com.twitter.finagle.Stack
import com.twitter.finagle.dispatch.ClientDispatcher.wrapWriteException
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.param.Stats
import com.twitter.finagle.postgresql.FrontendMessage.DescriptionTarget
import com.twitter.finagle.postgresql.Params.Credentials
import com.twitter.finagle.postgresql.Params.Database
import com.twitter.finagle.postgresql.Params.MaxConcurrentPrepareStatements
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.finagle.postgresql.machine._
import com.twitter.finagle.postgresql.transport.MessageDecoder
import com.twitter.finagle.postgresql.transport.Packet
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Return
import com.twitter.util.Throw

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
  transport: Transport[Packet, Packet],
  params: Stack.Params,
) extends GenSerialClientDispatcher[Request, Response, Packet, Packet](
      transport,
      params[Stats].statsReceiver
    ) {

  // implements Connection on Transport
  private[this] val transportConnection = new Connection {
    def send[M <: FrontendMessage](s: StateMachine.Send[M]): Future[Unit] =
      transport
        .write(s.encoder.toPacket(s.msg))
        .rescue {
          case exc => wrapWriteException(exc)
        }

    def receive(): Future[BackendMessage] =
      transport.read().map(rep => MessageDecoder.fromPacket(rep)).lowerFromTry

    def close(): Future[Unit] = transport.close()
  }

  private[this] val machineRunner: Runner = new Runner(transportConnection)

  def machineDispatch[R <: Response](machine: StateMachine[R], promise: Promise[R]): Future[Unit] =
    machineRunner
      .dispatch(machine, promise)
      .transform {
        case Return(_) => Future.Done
        case Throw(_) => close()
      }

  /**
   * This is used to keep the result of the startup sequence.
   *
   * The startup sequence is initiated when the connection is established, so there's no client response to fulfill.
   * Instead, we fulfill this promise to keep the data available subsequently.
   */
  private[this] val connectionParameters: Promise[Response.ConnectionParameters] = new Promise()

  /**
   * Immediately start the handshaking upon connection establishment, before any client requests.
   */
  private[this] val startup: Future[Unit] = machineRunner.dispatch(
    HandshakeMachine(params[Credentials], params[Database]),
    connectionParameters
  )

  override def apply(req: Request): Future[Response] =
    startup before super.apply(req)

  override protected def dispatch(req: Request, p: Promise[Response]): Future[Unit] =
    connectionParameters.poll match {
      case None =>
        Future.exception(
          new PgSqlClientError("Handshake result should be available at this point."))
      case Some(Throw(t)) =>
        // If handshaking failed, we cannot proceed with sending requests
        p.setException(t)
        close() // TODO: is it okay to close the connection here?
      case Some(Return(parameters)) =>
        req match {
          case Request.ConnectionParameters =>
            p.setValue(parameters)
            Future.Done
          case Request.Sync => machineDispatch(StateMachine.syncMachine, p)
          case Request.Query(q) => machineDispatch(new SimpleQueryMachine(q, parameters), p)
          case Request.Prepare(s, name) => machineDispatch(new PrepareMachine(name, s), p)
          case e: Request.Execute =>
            machineDispatch(new ExecuteMachine(e, parameters, () => transport.close()), p)
          case Request.CloseStatement(name) =>
            machineDispatch(new CloseMachine(name, DescriptionTarget.PreparedStatement), p)
        }
    }
}

object ClientDispatcher {
  def cached(
    transport: Transport[Packet, Packet],
    params: Stack.Params
  ): Service[Request, Response] =
    PrepareCache(
      new ClientDispatcher(transport, params),
      params[MaxConcurrentPrepareStatements].num,
      params[Stats].statsReceiver
    )
}

/**
 * Caches statements that have been successfully prepared over the connection
 * managed by the underlying service (a ClientDispatcher). This decreases
 * the chances of leaking prepared statements and can simplify the
 * implementation of prepared statements in the presence of a connection pool.
 */
case class PrepareCache(
  svc: Service[Request, Response],
  maxSize: Int,
  statsReceiver: StatsReceiver, // TODO: export same metrics as finagle-mysql
) extends ServiceProxy[Request, Response](svc) {

  private[this] val listener = new RemovalListener[Name.Named, Future[Response]] {
    override def onRemoval(
      key: Name.Named,
      response: Future[Response],
      cause: RemovalCause
    ): Unit = {
      response.respond {
        case Return(Response.ParseComplete(_)) =>
          svc(Request.CloseStatement(key))
        case _ =>
      }
    }
  }

  private[this] val underlying = Caffeine
    .newBuilder().maximumSize(maxSize.toLong)
    .removalListener(listener)
    .build[Name.Named, Future[Response]]()

  // we only cache Named portals, the unnamed portal is destroyed automatically upon reuse.
  private[this] val extractPortalName: Request.Prepare => Name.Named = {
    case Request.Prepare(_, n: Name.Named) => n
    case r =>
      throw new PgSqlClientError(
        s"Unexpected request type ${r.getClass.getName}. The cache can only accept Request.Prepared requests with Name.Named portals. Other requests should have been filtered out."
      )
  }

  private[this] val fn = FutureCache.default(
    fn = svc,
    cache = FutureCache.keyEncoded(extractPortalName, new CaffeineCache(underlying))
  )

  override def apply(request: Request): Future[Response] =
    request match {
      case r @ Request.Prepare(_, Name.Named(_)) => fn(r)
      case _ => super.apply(request)
    }
}
