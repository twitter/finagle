package com.twitter.finagle.exp

import com.twitter.finagle._
import com.twitter.finagle.client.{StackClient, StackClientLike, DefaultPool, Transporter}
import com.twitter.finagle.exp.mysql._
import com.twitter.finagle.exp.mysql.transport.{MysqlTransporter, Packet}
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.transport.Transport
import com.twitter.util.Duration

/**
 * Supplements a [[com.twitter.finagle.Client]] with convenient
 * builder methods for constructing a mysql client.
 */
trait MysqlRichClient { self: com.twitter.finagle.Client[Request, Result] =>
  /**
   * Creates a new `RichClient` connected to the logical
   * destination described by `dest` with the assigned
   * `label`. The `label` is used to scope client stats.
   */
  def newRichClient(dest: Name, label: String): mysql.Client =
    mysql.Client(newClient(dest, label))

  /**
   * Creates a new `RichClient` connected to the logical
   * destination described by `dest`.
   */
  def newRichClient(dest: String): mysql.Client =
    mysql.Client(newClient(dest))
}

/**
 * Tracing filter for mysql client requests.
 */
private object MysqlTracing extends SimpleFilter[Request, Result] { self =>
  def apply(request: Request, service: Service[Request, Result]) = {
    request match {
      case QueryRequest(sqlStatement) => Trace.recordBinary("mysql.query", sqlStatement)
      case PrepareRequest(sqlStatement) => Trace.recordBinary("mysql.prepare", sqlStatement)
      // TODO: save the prepared statement and put it in the executed request trace
      case ExecuteRequest(id, _, _, _) => Trace.recordBinary("mysql.execute", id)
      case _ => Trace.record("mysql." + request.getClass.getSimpleName.replace("$", ""))
    }
    service(request)
  }

  // TODO: We should consider adding toStackable(elem) to CanStackFrom so this sort of
  // boiler-plate isn't necessary. For example, we should be able to do MysqlTracing +: stack
  // and the role should be inferred (maybe as the class name).
  object mysqlTracing extends Stack.Role
  val module = new Stack.Simple[ServiceFactory[Request, Result]](mysqlTracing) {
    val description = "trace mysql specific calls to the loaded tracer"
    def make(params: Stack.Params, next: ServiceFactory[Request, Result]) =
      self andThen next
  }
}

/**
 * Implements a mysql client in terms of a [[com.twitter.finagle.StackClient]].
 * The client inherits a wealth of features from finagle including connection
 * pooling and load balancing.
 */
object MysqlStackClient extends StackClient[Request, Result](
  MysqlTracing.module +: StackClient.newStack,
  Stack.Params.empty
) {
  protected type In = Packet
  protected type Out = Packet
  protected val newTransporter = MysqlTransporter(_)
  protected val newDispatcher: Stack.Params => Dispatcher = { prms =>
    trans => mysql.ClientDispatcher(trans, Handshake(prms))
  }
}

/**
 * Wraps a mysql client with builder semantics. Additionally, this class provides
 * methods for constructing a rich client which exposes a rich mysql api.
 */
class MysqlClient(client: StackClient[Request, Result])
  extends StackClientLike[Request, Result, MysqlClient](client)
  with MysqlRichClient {
  protected def newInstance(client: StackClient[Request, Result]) =
    new MysqlClient(client)

  /**
   * The credentials to use when authenticating a new session.
   */
  def withCredentials(u: String, p: String): MysqlClient =
    configured(Handshake.Credentials(Option(u), Option(p)))

  /**
   * Database to use when this client establishes a new session.
   */
  def withDatabase(db: String): MysqlClient =
    configured(Handshake.Database(Option(db)))

  /**
   * The default character set used when establishing
   * a new session.
   */
  def withCharset(charset: Short): MysqlClient =
    configured(Handshake.Charset(charset))
}

/**
 * @example {{{
 * val client = Mysql
 *   .withCredentials("username", "password")
 *   .withDatabase("db")
 *   .newRichClient("inet!localhost:3306")
 * }}}
 */
object Mysql extends MysqlClient(
  MysqlStackClient
    .configured(DefaultPool.Param(
      low = 0, high = 1, bufferSize = 0,
      idleTime = Duration.Top,
      maxWaiters = Int.MaxValue))
)
