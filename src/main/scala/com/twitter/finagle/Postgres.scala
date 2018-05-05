package com.twitter.finagle

import com.twitter.finagle.Stack.{Param, Params, Role}
import com.twitter.finagle.client.{StackClient, StdStackClient, Transporter}
import com.twitter.finagle.dispatch.SerialClientDispatcher
import com.twitter.finagle.naming.BindingFactory.Dest
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.finagle.param._
import com.twitter.finagle.postgres.codec._
import com.twitter.finagle.postgres.messages._
import com.twitter.finagle.postgres.values.ValueDecoder
import com.twitter.finagle.service.FailFastFactory.FailFast
import com.twitter.finagle.service._
import com.twitter.finagle.ssl.client.{ SslClientEngineFactory, SslClientSessionVerifier }
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.util.{Monitor => _, _}
import com.twitter.logging.Logger
import java.net.SocketAddress
import org.jboss.netty.channel.{ChannelPipelineFactory, Channels}

import scala.language.existentials

object Postgres {

  object PostgresDefaultMonitor extends com.twitter.util.Monitor {
    val log = Logger(getClass)
    def handle(exc: Throwable): Boolean = exc match {
      case ServerError(_, _, _, Some(sqlState), _, _, _) if !(sqlState startsWith "XX") =>
        // typically, a ServerError isn't critical and is often expected
        // it will be logged in the Debug level for visibility without too much noise in production
        log.debug(exc, "ServerError acknowledged by PostgresDefaultMonitor")
        true
      case _ => false
    }
  }

  abstract class RequiredParam[T](name: String) extends Param[T] {
    def default = throw new IllegalArgumentException(s"$name must be defined")
  }

  case class User(user: String) extends AnyVal
  object User { implicit object param extends RequiredParam[User]("User") }

  case class Password(password: Option[String]) extends AnyVal
  implicit object Password extends Stack.Param[Password] {
    override def default: Password = Password(None)
    override def show(p: Password): Seq[(String, () => String)] = Nil
  }

  case class Database(database: String) extends AnyVal
  object Database { implicit object param extends RequiredParam[Database]("Database") }

  case class CustomTypes(types: Option[Map[Int, postgres.PostgresClient.TypeSpecifier]]) extends AnyVal
  object CustomTypes { implicit val param = Param(CustomTypes(None)) }

  case class CustomReceiveFunctions(functions: PartialFunction[String, ValueDecoder[T] forSome {type T}]) extends AnyVal
  object CustomReceiveFunctions { implicit val param = Param(CustomReceiveFunctions(PartialFunction.empty)) }

  case class BinaryResults(binaryResults: Boolean) extends AnyVal
  object BinaryResults { implicit val param = Param(BinaryResults(false)) }

  case class BinaryParams(binaryParams: Boolean) extends AnyVal
  object BinaryParams { implicit val param = Param(BinaryParams(false)) }

  private def defaultParams = StackClient.defaultParams +
    ProtocolLibrary("postgresql") +
    Label("postgres") +
    FailFast(false) +
    param.ResponseClassifier(defaultResponseClassifier) +
    Retries.Policy(defaultRetryPolicy) +
    Monitor(PostgresDefaultMonitor)

  private def defaultStack = StackClient.newStack[PgRequest, PgResponse]
    .replace(StackClient.Role.prepConn, PrepConnection)
    .replace(Retries.Role, Retries.moduleWithRetryPolicy[PgRequest, PgResponse])

  private def pipelineFactory(params: Stack.Params) = {
    val SslClientEngineFactory.Param(sslFactory) = params[SslClientEngineFactory.Param]
    val SslClientSessionVerifier.Param(sessionVerifier) = params[SslClientSessionVerifier.Param]
    val Transport.ClientSsl(ssl) = params[Transport.ClientSsl]

    new ChannelPipelineFactory {
      def getPipeline = {
        val pipeline = Channels.pipeline()

        pipeline.addLast("binary_to_packet", new PacketDecoder(ssl.nonEmpty))
        pipeline.addLast("packet_to_backend_messages", new BackendMessageDecoder(new BackendMessageParser))
        pipeline.addLast("backend_messages_to_postgres_response", new PgClientChannelHandler(sslFactory, sessionVerifier, ssl, ssl.nonEmpty))
        pipeline
      }
    }
  }

  private def mkTransport(params: Stack.Params, addr: SocketAddress) =
    Netty3Transporter.apply[PgRequest, PgResponse](
      pipelineFactory(params),
      addr,
      params + Transport.ClientSsl(None)  // we want to give this param to Postgres but not directly to transport
                                          // because postgres doesn't start out in TLS
    )

  case class Client(
    stack: Stack[ServiceFactory[PgRequest, PgResponse]] = defaultStack,
    params: Stack.Params = defaultParams
  ) extends StdStackClient[PgRequest, PgResponse, Client]
    with WithSessionPool[Client] with WithDefaultLoadBalancer[Client] {
    type In = PgRequest
    type Out = PgResponse
    type Context = TransportContext

    def newRichClient(): postgres.PostgresClientImpl = {

      val Dest(name) = params[Dest]
      val CustomTypes(customTypes) = params[CustomTypes]
      val CustomReceiveFunctions(customReceiveFunctions) = params[CustomReceiveFunctions]
      val Label(id) = params[Label]
      val BinaryResults(binaryResults) = params[BinaryResults]
      val BinaryParams(binaryParams) = params[BinaryParams]

      val client = newClient(name, id)

      new postgres.PostgresClientImpl(client, id, customTypes, customReceiveFunctions, binaryResults, binaryParams)
    }

    def newRichClient(addr: String): postgres.PostgresClientImpl = dest(addr).newRichClient()
    def newRichClient(addr: Name): postgres.PostgresClientImpl = dest(addr).newRichClient()

    def dest(
      addr: String
    ): Client = {
      Resolver.evalLabeled(addr) match {
        case (n, "") => dest(n)
        case (n, l) =>
          val Label(label) = params[Label]
          val cb = conditionally(label.isEmpty || l != addr, _.withLabel(l))
          cb.dest(n)
      }
    }

    def dest(name: Name): Client = configured(Dest(name))

    def withBinaryParams(enable: Boolean = true) = configured(BinaryParams(enable))
    def withBinaryResults(enable: Boolean = true) = configured(BinaryResults(enable))
    def database(database: String) = configured(Database(database))
    def withCustomTypes(customTypes: Map[Int, postgres.PostgresClient.TypeSpecifier]) =
      configured(CustomTypes(Some(customTypes)))
    def withDefaultTypes() = configured(CustomTypes(Some(postgres.PostgresClient.defaultTypes)))
    def withCustomReceiveFunctions(receiveFunctions: PartialFunction[String, ValueDecoder[T] forSome { type T }]) =
      configured(CustomReceiveFunctions(receiveFunctions))

    def withCredentials(user: String, password: Option[String]): Client =
      configured(User(user)).configured(Password(password))
    def withCredentials(user: String, password: String): Client =
      withCredentials(user, Some(password))
    def withCredentials(user: String): Client = withCredentials(user, None)

    def withRetryPolicy(policy: RetryPolicy[Try[Nothing]]) = configured(Retries.Policy(policy))

    def conditionally(bool: Boolean, conf: Client => Client) = if(bool) conf(this) else this

    protected def newTransporter(addr: SocketAddress): Transporter[In, Out, Context] = mkTransport(params, addr)

    protected def newDispatcher(transport: Transport[In, Out] { type Context <: Client.this.Context }): Service[PgRequest, PgResponse] = {
      new Dispatcher(
        transport,
        params[Stats].statsReceiver
      )
    }

    protected def copy1(
      stack: Stack[ServiceFactory[PgRequest, PgResponse]],
      params: Params
    ): Client {type In = Client.this.In; type Out = Client.this.Out} = copy(stack, params)
  }

  private class Dispatcher(transport: Transport[PgRequest, PgResponse], statsReceiver: StatsReceiver)
    extends SerialClientDispatcher[PgRequest, PgResponse](transport, statsReceiver) {

    override def apply(
      req: PgRequest
    ): Future[PgResponse] = req match {
      // allow Terminate requests to go through no matter what
      case PgRequest(Terminate, true) => transport.write(req).flatMap(_ => transport.close().map(_ => Terminated))
      case _ => super.apply(req)
    }
  }

  private object PrepConnection extends Stack.ModuleParams[ServiceFactory[PgRequest, PgResponse]] {
    def parameters: Seq[Param[_]] = Nil
    def role: Role = StackClient.Role.prepConn
    val description = "Prepare the PostgreSQL connection by authenticating and managing connection failures"
    def make(params: Params, next: ServiceFactory[PgRequest, PgResponse]): ServiceFactory[PgRequest, PgResponse] = {
      val User(user) = params[User]
      val Password(password) = params[Password]
      val Database(db) = params[Database]
      val Transport.ClientSsl(ssl) = params[Transport.ClientSsl]
      new AuthenticationProxy(new HandleErrorsProxy(next), user, password, db, ssl.nonEmpty)
    }
  }

  private object SqlState {
    def unapply(err: Throwable): Option[String] = err match {
      case ServerError(_, _, _, sqlState, _, _, _) => sqlState
      case _ => None
    }
  }

  private object Retryable {
    def unapply(err: Throwable): Option[Throwable] = err match {
      // Class 53 - Insufficient resources (can be retried)
      case SqlState(sqlState) if sqlState startsWith "53" => Some(err)
      // Class 08 - Connection exception (can be retried)
      case SqlState(sqlState) if sqlState startsWith "08" => Some(err)
      case _ => None
    }
  }

  // The response classifier is used by FailureAccrual; any error that doesn't indicate bad health of the
  // endpoint must be classified as a success or it will cause the endpoint to be marked as dead.
  // Connection problems and resource errors (which are retryable) are considered failures, along with
  // errors indicating the server is in a bad state. Other ServerErrors are considered successful, even though they
  // are failures - this is to prevent them from improperly triggering FailureAccrual to mark the endpoint as dead.
  private val defaultResponseClassifier: service.ResponseClassifier = {
    case ReqRep(a, Return(_)) => ResponseClass.Success

    // Retryable ServerErrors - these should be retryable, but currently we can't actually retry anything
    // this is because an individual protocol message can't be retried - the entire message flow since the last
    // sync() would have to be captured and retried.
    // TODO: track this in the prepared statement
    case ReqRep(a, Throw(Retryable(_))) => ResponseClass.NonRetryableFailure
    // Critical errors in the server
    case ReqRep(a, Throw(SqlState(sqlState))) if sqlState startsWith "XX" => ResponseClass.NonRetryableFailure
    case ReqRep(a, Throw(SqlState(sqlState))) if sqlState startsWith "58" => ResponseClass.NonRetryableFailure
    // Operator intervention - these should be failures as well
    case ReqRep(a, Throw(SqlState(sqlState))) if sqlState startsWith "57" => ResponseClass.NonRetryableFailure
    case ReqRep(a, Throw(SqlState(_)))    => ResponseClass.Success
    case ReqRep(a, Throw(ClientError(_))) => ResponseClass.Success
  }

  private val defaultBackoff = Backoff.exponential(Duration.fromMilliseconds(50), 2, Duration.fromSeconds(5))

  private val defaultRetryPolicy = RetryPolicy.backoff(defaultBackoff) {
    RetryPolicy.TimeoutAndWriteExceptionsOnly orElse RetryPolicy.ChannelClosedExceptionsOnly
  }

}
