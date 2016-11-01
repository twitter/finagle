package com.twitter.finagle

import com.twitter.finagle.Stack.{Param, Params, Role}
import com.twitter.finagle.client.{StackClient, StdStackClient, Transporter}
import com.twitter.finagle.dispatch.SerialClientDispatcher
import com.twitter.finagle.factory.BindingFactory.Dest
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.finagle.param.{Label, ProtocolLibrary, Stats}
import com.twitter.finagle.postgres.codec._
import com.twitter.finagle.postgres.messages._
import com.twitter.finagle.postgres.values.ValueDecoder
import com.twitter.finagle.service.FailFastFactory.FailFast
import com.twitter.finagle.service._
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Duration, Return, Throw}
import org.jboss.netty.channel.{ChannelPipelineFactory, Channels}

object Postgres {

  abstract class RequiredParam[T](name: String) extends Param[T] {
    def default = throw new IllegalArgumentException(s"$name must be defined")
  }

  case class User(user: String) extends AnyVal
  object User { implicit object param extends RequiredParam[User]("User") }

  case class Password(password: Option[String]) extends AnyVal
  object Password { implicit val param = Param(Password(None)) }

  case class Database(database: String) extends AnyVal
  object Database { implicit object param extends RequiredParam[Database]("Database") }

  case class CustomTypes(types: Option[Map[Int, postgres.Client.TypeSpecifier]]) extends AnyVal
  object CustomTypes { implicit val param = Param(CustomTypes(None)) }

  case class CustomReceiveFunctions(functions: PartialFunction[String, ValueDecoder[T] forSome {type T}]) extends AnyVal
  object CustomReceiveFunctions { implicit val param = Param(CustomReceiveFunctions(PartialFunction.empty)) }

  case class BinaryResults(binaryResults: Boolean) extends AnyVal
  object BinaryResults { implicit val param = Param(BinaryResults(false)) }

  case class BinaryParams(binaryParams: Boolean) extends AnyVal
  object BinaryParams { implicit val param = Param(BinaryParams(false)) }

  private def defaultParams = StackClient.defaultParams +
    ProtocolLibrary("postgresql") + Label("postgres") + FailFast(false) + param.ResponseClassifier(responseClassifier) +
    Retries.Policy(retryPolicy)

  private def defaultStack = StackClient.newStack[PgRequest, PgResponse]
    .replace(StackClient.Role.prepConn, PrepConnection)

  private def pipelineFactory(params: Stack.Params) = {
    val Transport.TLSClientEngine(ssl) = params[Transport.TLSClientEngine]

    new ChannelPipelineFactory {
      def getPipeline = {
        val pipeline = Channels.pipeline()

        pipeline.addLast("binary_to_packet", new PacketDecoder(ssl.nonEmpty))
        pipeline.addLast("packet_to_backend_messages", new BackendMessageDecoder(new BackendMessageParser))
        pipeline.addLast("backend_messages_to_postgres_response", new PgClientChannelHandler(ssl, ssl.nonEmpty))
        pipeline
      }
    }
  }

  private def mkTransport(params: Stack.Params) =
    Netty3Transporter.apply[PgRequest, PgResponse](
      pipelineFactory(params),
      params + Transport.TLSClientEngine(None)  // we want to give this param to Postgres but not directly to transport
                                                // because postgres doesn't start out in TLS
    )

  case class Client(
    stack: Stack[ServiceFactory[PgRequest, PgResponse]] = defaultStack,
    params: Stack.Params = defaultParams
  ) extends StdStackClient[PgRequest, PgResponse, Client] {
    type In = PgRequest
    type Out = PgResponse

    def newRichClient(): postgres.Client = {

      val Dest(name) = params[Dest]
      val CustomTypes(customTypes) = params[CustomTypes]
      val CustomReceiveFunctions(customReceiveFunctions) = params[CustomReceiveFunctions]
      val Label(id) = params[Label]
      val BinaryResults(binaryResults) = params[BinaryResults]
      val BinaryParams(binaryParams) = params[BinaryParams]

      val client = newClient(name, id)

      new postgres.Client(client, id, customTypes, customReceiveFunctions, binaryResults, binaryParams)
    }

    def newRichClient(addr: String): postgres.Client = dest(addr).newRichClient()
    def newRichClient(addr: Name): postgres.Client = dest(addr).newRichClient()

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
    def withCustomTypes(customTypes: Map[Int, postgres.Client.TypeSpecifier]) =
      configured(CustomTypes(Some(customTypes)))
    def withDefaultTypes() = configured(CustomTypes(Some(postgres.Client.defaultTypes)))
    def withCustomReceiveFunctions(receiveFunctions: PartialFunction[String, ValueDecoder[T] forSome { type T }]) =
      configured(CustomReceiveFunctions(receiveFunctions))

    def withCredentials(user: String, password: Option[String]): Client =
      configured(User(user)).configured(Password(password))
    def withCredentials(user: String, password: String): Client =
      withCredentials(user, Some(password))
    def withCredentials(user: String): Client = withCredentials(user, None)

    def conditionally(bool: Boolean, conf: Client => Client) = if(bool) conf(this) else this

    protected def newTransporter(): Transporter[In, Out] = mkTransport(params)

    protected def newDispatcher(transport: Transport[In, Out]): Service[PgRequest, PgResponse] = {
      new SerialClientDispatcher(
        transport,
        params[Stats].statsReceiver
      )
    }

    protected def copy1(
      stack: Stack[ServiceFactory[PgRequest, PgResponse]],
      params: Params
    ): Client {type In = Client.this.In; type Out = Client.this.Out} = copy(stack, params)
  }

  private object PrepConnection extends Stack.ModuleParams[ServiceFactory[PgRequest, PgResponse]] {
    def parameters: Seq[Param[_]] = Nil
    def role: Role = StackClient.Role.prepConn
    val description = "Prepare the PostgreSQL connection by authenticating and managing connection failures"
    def make(params: Params, next: ServiceFactory[PgRequest, PgResponse]): ServiceFactory[PgRequest, PgResponse] = {
      val User(user) = params[User]
      val Password(password) = params[Password]
      val Database(db) = params[Database]
      val Transport.TLSClientEngine(ssl) = params[Transport.TLSClientEngine]
      new AuthenticationProxy(new HandleErrorsProxy(next), user, password, db, ssl.nonEmpty)
    }
  }

  private val responseClassifier: service.ResponseClassifier = {
    case ReqRep(a, Return(_)) => ResponseClass.Success
    case ReqRep(a, Throw(ServerError(_, _, _, Some(_), _, _, _))) => ResponseClass.Success
    case ReqRep(a, Throw(ClientError(_))) => ResponseClass.Success
  }

  private val retryPolicy = RetryPolicy.backoff(
    Backoff.exponential(Duration.fromMilliseconds(50), 2, Duration.fromSeconds(5)))(
    RetryPolicy.TimeoutAndWriteExceptionsOnly orElse RetryPolicy.ChannelClosedExceptionsOnly)

}
