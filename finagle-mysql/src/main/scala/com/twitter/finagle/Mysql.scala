package com.twitter.finagle

import com.twitter.finagle.client.{DefaultPool, StackClient, StdStackClient, Transporter}
import com.twitter.finagle.decoder.LengthFieldFramer
import com.twitter.finagle.mysql._
import com.twitter.finagle.mysql.transport.Packet
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.param.{ExceptionStatsHandler => _, Monitor => _, ResponseClassifier => _, Tracer => _, _}
import com.twitter.finagle.service.{ResponseClassifier, RetryBudget}
import com.twitter.finagle.stats.{ExceptionStatsHandler, NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.io.Buf
import com.twitter.util.{Duration, Future, Monitor}
import java.net.SocketAddress

/**
 * Supplements a [[com.twitter.finagle.Client]] with convenient
 * builder methods for constructing a mysql client.
 */
trait MysqlRichClient { self: com.twitter.finagle.Client[Request, Result] =>

  /**
   * Whether the client supports unsigned integer fields
   */
  protected val supportUnsigned: Boolean

  def richClientStatsReceiver: StatsReceiver = NullStatsReceiver

  /**
   * Creates a new `RichClient` connected to the logical
   * destination described by `dest` with the assigned
   * `label`. The `label` is used to scope client stats.
   */
  def newRichClient(
    dest: Name,
    label: String
  ): mysql.Client with mysql.Transactions =
    mysql.Client(newClient(dest, label), richClientStatsReceiver, supportUnsigned)

  /**
   * Creates a new `RichClient` connected to the logical
   * destination described by `dest`.
   *
   * @param dest the location to connect to, e.g. "host:port". See the
   *             [[https://twitter.github.io/finagle/guide/Names.html user guide]]
   *             for details on destination names.
   */
  def newRichClient(dest: String): mysql.Client with mysql.Transactions =
    mysql.Client(newClient(dest), richClientStatsReceiver, supportUnsigned)
}

object MySqlClientTracingFilter {
  object Stackable extends Stack.Module1[param.Label, ServiceFactory[Request, Result]] {
    val role: Stack.Role = ClientTracingFilter.role
    val description: String = "Add MySql client specific annotations to the trace"
    def make(
      _label: param.Label,
      next: ServiceFactory[Request, Result]
    ): ServiceFactory[Request, Result] = {
      // TODO(jeff): should be able to get this directly from ClientTracingFilter
      val annotations = new AnnotatingTracingFilter[Request, Result](
        _label.label,
        Annotation.ClientSend(),
        Annotation.ClientRecv()
      )
      annotations.andThen(TracingFilter).andThen(next)
    }
  }

  object TracingFilter extends SimpleFilter[Request, Result] {
    def apply(request: Request, service: Service[Request, Result]): Future[Result] = {
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

  protected val supportUnsigned: Boolean = param.UnsignedColumns.param.default.supported

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
      implicit val param: Stack.Param[MaxConcurrentPrepareStatements] =
        Stack.Param(MaxConcurrentPrepareStatements(20))
    }

    /**
     * Configure whether to support unsigned integer fields should be considered when
     * returning elements of a [[Row]]. If not supported, unsigned fields will be decoded
     * as if they were signed, potentially resulting in corruption in the case of overflowing
     * the signed representation. Because Java doesn't support unsigned integer types
     * widening may be necessary to support the unsigned variants. For example, an unsigned
     * Int is represented as a Long.
     *
     * `Value` representations of unsigned columns which are widened when enabled:
     * `ByteValue` -> `ShortValue``
     * `ShortValue` -> IntValue`
     * `LongValue` -> `LongLongValue`
     * `LongLongValue` -> `BigIntValue`
     */
    case class UnsignedColumns(supported: Boolean)
    object UnsignedColumns {
      implicit val param: Stack.Param[UnsignedColumns] = Stack.Param(UnsignedColumns(false))
    }
  }

  object Client {

    private object PoisonConnection {
      val Role: Stack.Role = Stack.Role("PoisonConnection")

      def module: Stackable[ServiceFactory[Request, Result]] =
        new Stack.Module0[ServiceFactory[Request, Result]] {
          def role: Stack.Role = Role

          def description: String = "Allows the connection to be poisoned and recycled"

          def make(next: ServiceFactory[Request, Result]): ServiceFactory[Request, Result] =
            new PoisonConnection(next)
        }
    }

    /**
     * This is a workaround for connection pooling that allows us to close a connection.
     */
    private class PoisonConnection(underlying: ServiceFactory[Request, Result])
      extends ServiceFactoryProxy(underlying) {

      override def apply(conn: ClientConnection): Future[Service[Request, Result]] = {
        super.apply(conn).map { svc =>
          new ServiceProxy[Request, Result](svc) {
            override def apply(request: Request): Future[Result] = {
              if (request eq PoisonConnectionRequest) {
                underlying.close().before {
                  Future.value(PoisonedConnectionResult)
                }
              } else {
                super.apply(request)
              }
            }
          }
        }
      }
    }

    private val params: Stack.Params = StackClient.defaultParams +
      ProtocolLibrary("mysql") +
      DefaultPool.Param(
        low = 0,
        high = 1,
        bufferSize = 0,
        idleTime = Duration.Top,
        maxWaiters = Int.MaxValue
      )

    private val stack: Stack[ServiceFactory[Request, Result]] = StackClient.newStack
      .replace(ClientTracingFilter.role, MySqlClientTracingFilter.Stackable)
      // Note: there is a stack overflow in insertAfter using CanStackFrom, thus the module.
      .insertAfter(DefaultPool.Role, PoisonConnection.module)
  }

  /**
   * Implements a mysql client in terms of a
   * [[com.twitter.finagle.client.StackClient]]. The client inherits a wealth
   * of features from finagle including connection pooling and load
   * balancing.
   *
   * Additionally, this class provides methods via [[MysqlRichClient]] for constructing
   * a client which exposes an API that has use case specific methods, for example
   * [[mysql.Client.read]], [[mysql.Client.modify]], and [[mysql.Client.prepare]].
   * This is an easier experience for most users.
   *
   * @example
   * {{{
   * import com.twitter.finagle.Mysql
   * import com.twitter.finagle.mysql.Client
   * import com.twitter.util.Future
   *
   * val client: Client = Mysql.client
   *   .withCredentials("username", "password")
   *   .withDatabase("database")
   *   .newRichClient("host:port")
   * val names: Future[Seq[String]] =
   *   client.select("SELECT name FROM employee") { row =>
   *     row.stringOrNull("name")
   *   }
   * }}}
   */
  case class Client(
    stack: Stack[ServiceFactory[Request, Result]] = Client.stack,
    params: Stack.Params = Client.params
  ) extends StdStackClient[Request, Result, Client]
      with WithSessionPool[Client]
      with WithDefaultLoadBalancer[Client]
      with MysqlRichClient {

    protected val supportUnsigned: Boolean = params[param.UnsignedColumns].supported

    protected def copy1(
      stack: Stack[ServiceFactory[Request, Result]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    protected type In = Buf
    protected type Out = Buf
    protected type Context = TransportContext

    protected def newTransporter(addr: SocketAddress): Transporter[In, Out, Context] = {
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

    protected def newDispatcher(transport: Transport[Buf, Buf] {
      type Context <: Client.this.Context
    }): Service[Request, Result] = {
      val param.MaxConcurrentPrepareStatements(num) = params[param.MaxConcurrentPrepareStatements]
      mysql.ClientDispatcher(
        transport.map(_.toBuf, Packet.fromBuf),
        Handshake(params),
        num,
        supportUnsigned
      )
    }

    /**
     * The maximum number of concurrent prepare statements.
     */
    def withMaxConcurrentPrepareStatements(num: Int): Client =
      configured(param.MaxConcurrentPrepareStatements(num))

    /**
     * The credentials to use when authenticating a new session.
     *
     * @param p if `null`, no password is used.
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

    /**
      * Don't set the CLIENT_FOUND_ROWS flag when establishing a new
      * session. This will make "INSERT ... ON DUPLICATE KEY UPDATE"
      * statements return the "correct" update count.
      *
      * See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_row-count
      */
    def withAffectedRows(): Client =
      configured(Handshake.FoundRows(false))

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
    override def withRetryBackoff(backoff: Stream[Duration]): Client =
      super.withRetryBackoff(backoff)

    override def withStack(stack: Stack[ServiceFactory[Request, Result]]): Client =
      super.withStack(stack)
    override def configured[P](psp: (P, Stack.Param[P])): Client = super.configured(psp)
    override def filtered(filter: Filter[Request, Result, Request, Result]): Client =
      super.filtered(filter)

    override def richClientStatsReceiver: StatsReceiver = params[Stats].statsReceiver
  }

  def client: Mysql.Client = Client()

  def newClient(dest: Name, label: String): ServiceFactory[Request, Result] =
    client.newClient(dest, label)

  def newService(dest: Name, label: String): Service[Request, Result] =
    client.newService(dest, label)
}
