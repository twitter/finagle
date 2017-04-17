package com.twitter.finagle

import com.twitter.finagle.client.{DefaultPool, StackClient, StdStackClient, Transporter}
import com.twitter.finagle.framer.LengthFieldFramer
import com.twitter.finagle.mysql._
import com.twitter.finagle.mysql.transport.Packet
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.param.{Monitor => _, ResponseClassifier => _, ExceptionStatsHandler => _, Tracer => _, _}
import com.twitter.finagle.service.{ResponseClassifier, RetryBudget}
import com.twitter.finagle.stats.{ExceptionStatsHandler, NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.Transport
import com.twitter.io.Buf
import com.twitter.util.{Duration, Monitor}
import java.net.SocketAddress

/**
 * Supplements a [[com.twitter.finagle.Client]] with convenient
 * builder methods for constructing a mysql client.
 */
trait MysqlRichClient { self: com.twitter.finagle.Client[Request, Result] =>

  def richClientStatsReceiver: StatsReceiver = NullStatsReceiver

  /**
   * Creates a new `RichClient` connected to the logical
   * destination described by `dest` with the assigned
   * `label`. The `label` is used to scope client stats.
   */
  def newRichClient(dest: Name, label: String): mysql.Client with mysql.Transactions with mysql.Cursors =
    mysql.Client(newClient(dest, label), richClientStatsReceiver)

  /**
   * Creates a new `RichClient` connected to the logical
   * destination described by `dest`.
   */
  def newRichClient(dest: String): mysql.Client with mysql.Transactions with mysql.Cursors =
    mysql.Client(newClient(dest), richClientStatsReceiver)
}

object MySqlClientTracingFilter {
  object Stackable extends Stack.Module1[param.Label, ServiceFactory[Request, Result]] {
    val role = ClientTracingFilter.role
    val description = "Add MySql client specific annotations to the trace"
    def make(_label: param.Label, next: ServiceFactory[Request, Result]) = {
      val param.Label(label) = _label
      // TODO(jeff): should be able to get this directly from ClientTracingFilter
      val annotations = new AnnotatingTracingFilter[Request, Result](
        label, Annotation.ClientSend(), Annotation.ClientRecv())
      annotations andThen TracingFilter andThen next
    }
  }

  object TracingFilter extends SimpleFilter[Request, Result] {
    def apply(request: Request, service: Service[Request, Result]) = {
      if (Trace.isActivelyTracing) {
        request match {
          case QueryRequest(sqlStatement) => Trace.recordBinary("mysql.query", sqlStatement)
          case PrepareRequest(sqlStatement) => Trace.recordBinary("mysql.prepare", sqlStatement)
          // TODO: save the prepared statement and put it in the executed request trace
          case ExecuteRequest(id, _, _, _) => Trace.recordBinary("mysql.execute", id)
          case _ => Trace.record("mysql." + request.getClass.getSimpleName.replace("$", ""))
        }
      }
      service(request)
    }
  }
}


/**
 * @example {{{
 * val client = Mysql.client
 *   .withCredentials("<username>", "<password>")
 *   .withDatabase("<db>")
 *   .newRichClient("inet!localhost:3306")
 * }}}
 */
object Mysql extends com.twitter.finagle.Client[Request, Result] with MysqlRichClient {

  object param {
    /**
     * A class eligible for configuring the maximum number of prepare
     * statements.  After creating `num` prepare statements, we'll start purging
     * old ones.
     */
    case class MaxConcurrentPrepareStatements(num: Int) {
      assert(num <= Int.MaxValue, s"$num is not <= Int.MaxValue bytes")
      assert(num > 0, s"$num must be positive")

      def mk(): (MaxConcurrentPrepareStatements, Stack.Param[MaxConcurrentPrepareStatements]) =
        (this, MaxConcurrentPrepareStatements.param)
    }

    object MaxConcurrentPrepareStatements {
      implicit val param = Stack.Param(MaxConcurrentPrepareStatements(20))
    }
  }

  /**
   * Implements a mysql client in terms of a
   * [[com.twitter.finagle.client.StackClient]]. The client inherits a wealth
   * of features from finagle including connection pooling and load
   * balancing.
   *
   * Additionally, this class provides methods for constructing a rich
   * client which exposes a rich mysql api.
   */
  case class Client(
      stack: Stack[ServiceFactory[Request, Result]] = StackClient.newStack
        .replace(ClientTracingFilter.role, MySqlClientTracingFilter.Stackable),
      params: Stack.Params = StackClient.defaultParams + DefaultPool.Param(
          low = 0, high = 1, bufferSize = 0,
          idleTime = Duration.Top,
          maxWaiters = Int.MaxValue) +
        ProtocolLibrary("mysql"))
    extends StdStackClient[Request, Result, Client]
    with WithSessionPool[Client]
    with WithDefaultLoadBalancer[Client]
    with MysqlRichClient {

    protected def copy1(
      stack: Stack[ServiceFactory[Request, Result]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    protected type In = Buf
    protected type Out = Buf

    protected def newTransporter(addr: SocketAddress): Transporter[In, Out] = {
      val framerFactory = () => {
        new LengthFieldFramer(
          lengthFieldBegin = 0,
          lengthFieldLength = 3,
          lengthAdjust = Packet.HeaderSize, // Packet size field doesn't include the header size.
          maxFrameLength = Packet.HeaderSize + Packet.MaxBodySize,
          bigEndian = false
        )
      }
      Netty4Transporter.framedBuf(Some(framerFactory), addr, params)
    }

    protected def newDispatcher(transport: Transport[Buf, Buf]):  Service[Request, Result] = {
      val param.MaxConcurrentPrepareStatements(num) = params[param.MaxConcurrentPrepareStatements]
      mysql.ClientDispatcher(
        transport.map(_.toBuf, Packet.fromBuf),
        Handshake(params),
        num
      )
    }

    /**
     * The maximum number of concurrent prepare statements.
     */
    def withMaxConcurrentPrepareStatements(num: Int): Client =
      configured(param.MaxConcurrentPrepareStatements(num))

    /**
     * The credentials to use when authenticating a new session.
     */
    def withCredentials(u: String, p: String): Client =
      configured(Handshake.Credentials(Option(u), Option(p)))

    /**
     * Database to use when this client establishes a new session.
     */
    def withDatabase(db: String): Client =
      configured(Handshake.Database(Option(db)))

    /**
     * The default character set used when establishing a new session.
     */
    def withCharset(charset: Short): Client =
      configured(Handshake.Charset(charset))

    // Java-friendly forwarders
    // See https://issues.scala-lang.org/browse/SI-8905
    override val withSessionPool: SessionPoolingParams[Client] =
      new SessionPoolingParams(this)
    override val withLoadBalancer: DefaultLoadBalancingParams[Client] =
      new DefaultLoadBalancingParams(this)
    override val withTransport: ClientTransportParams[Client] =
      new ClientTransportParams(this)
    override val withSession: ClientSessionParams[Client] =
      new ClientSessionParams(this)
    override val withSessionQualifier: SessionQualificationParams[Client] =
      new SessionQualificationParams(this)
    override val withAdmissionControl: ClientAdmissionControlParams[Client] =
      new ClientAdmissionControlParams(this)

    override def withLabel(label: String): Client = super.withLabel(label)
    override def withStatsReceiver(statsReceiver: StatsReceiver): Client =
      super.withStatsReceiver(statsReceiver)
    override def withMonitor(monitor: Monitor): Client = super.withMonitor(monitor)
    override def withTracer(tracer: Tracer): Client = super.withTracer(tracer)
    override def withExceptionStatsHandler(exceptionStatsHandler: ExceptionStatsHandler): Client =
      super.withExceptionStatsHandler(exceptionStatsHandler)
    override def withRequestTimeout(timeout: Duration): Client =
      super.withRequestTimeout(timeout)
    override def withResponseClassifier(responseClassifier: ResponseClassifier): Client =
      super.withResponseClassifier(responseClassifier)
    override def withRetryBudget(budget: RetryBudget): Client = super.withRetryBudget(budget)
    override def withRetryBackoff(backoff: Stream[Duration]): Client = super.withRetryBackoff(backoff)

    override def configured[P](psp: (P, Stack.Param[P])): Client = super.configured(psp)
    override def filtered(filter: Filter[Request, Result, Request, Result]): Client =
      super.filtered(filter)

    override def richClientStatsReceiver: StatsReceiver = params[Stats].statsReceiver
  }

  val client = Client()

  def newClient(dest: Name, label: String): ServiceFactory[Request, Result] =
    client.newClient(dest, label)

  def newService(dest: Name, label: String): Service[Request, Result] =
    client.newService(dest, label)

  /**
   * The credentials to use when authenticating a new session.
   */
  @deprecated("Use client.withCredentials", "6.22.0")
  def withCredentials(u: String, p: String): Client =
    client.configured(Handshake.Credentials(Option(u), Option(p)))

  /**
   * Database to use when this client establishes a new session.
   */
  @deprecated("Use client.withDatabase", "6.22.0")
  def withDatabase(db: String): Client =
    client.configured(Handshake.Database(Option(db)))

  /**
   * The default character set used when establishing
   * a new session.
   */
  @deprecated("Use client.withCharset", "6.22.0")
  def withCharset(charset: Short): Client =
    client.configured(Handshake.Charset(charset))

  /**
   * A client configured with parameter p.
   */
  @deprecated("Use client.configured", "6.22.0")
  def configured[P: Stack.Param](p: P): Client =
    client.configured(p)
}
