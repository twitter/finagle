package com.twitter.finagle.exp

import com.twitter.finagle.{Client, Name, Service, ServiceFactory, SimpleFilter}
import com.twitter.finagle.client.{Bridge, DefaultClient, DefaultPool}
import com.twitter.finagle.exp.mysql._
import com.twitter.finagle.exp.mysql.transport.{MysqlTransporter, Packet}
import com.twitter.finagle.tracing.Trace
import com.twitter.util.Future
import java.net.SocketAddress

/**
 * Supplements a [[com.twitter.finagle.Client]]
 * with builder methods for constructing a mysql
 * client.
 */
trait MysqlRichClient extends Client[Request, Result] {
  /**
   * The credentials to use when authenticating a new session.
   */
  def withCredentials(u: String, p: String): MysqlRichClient

  /**
   * Database to use when this client
   * establishes a new session.
   */
  def withDatabase(db: String): MysqlRichClient

  /**
   * The default character set used when establishing
   * a new session.
   */
  def withCharset(charset: Short): MysqlRichClient

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
 * A builder for a finagle `Client[mysql.Request, mysql.Result]`.
 * The client inherits a wealth of features from
 * finagle including connection pooling and load balancing.
 * Additionally, this object provides methods for
 * constructing a `RichClient` that exposes a rich mysql api.
 *
 * @example {{{
 * val client = Mysql
 *   .withCredentials("<username>", "<password>")
 *   .withDatabase("db")
 *   .newRichClient("inet!localhost:3306")
 * }}}
 *
 */
object Mysql extends MysqlRichClient {
  /**
   * Tracing filter for mysql client requests.
   */
  private[this] object MysqlTracing extends SimpleFilter[Request, Result] {
    def apply(request: Request, service: Service[Request, Result]) = {
      request match {
        case QueryRequest(sqlStatement) => Trace.recordBinary("mysql.query", sqlStatement)
        case PrepareRequest(sqlStatement) => Trace.recordBinary("mysql.prepare", sqlStatement)
        // TODO: save the prepared statement and put it in the executed request trace
        case ExecuteRequest(id, _, _, _) => Trace.recordBinary("mysql.execute", id)
        case _ => Trace.record("mysql." + request.getClass.getName)
      }
      service(request)
    }
  }

  /**
   * An implementation of MysqlRichClient in terms of a finagle DefaultClient.
   */
  private[this] case class MysqlClient(
    handshake: Handshake
  ) extends MysqlRichClient {
    private[this] val defaultClient = DefaultClient[Request, Result](
      name = "mysql",
      pool = DefaultPool(high = 1),
      endpointer = {
        val bridge = Bridge[Packet, Packet, Request, Result](
          MysqlTransporter, mysql.ClientDispatcher(_, handshake)
        )
        (sa, sr) => bridge(sa, sr)
      }
    )

    def withCredentials(u: String, p: String) =
      copy(handshake = handshake.copy(
        username = Option(u),
        password = Option(p)
      ))

    def withDatabase(db: String) =
      copy(handshake = handshake.copy(
        database = Option(db)
      ))

    def withCharset(cs: Short) =
      copy(handshake = handshake.copy(
        charset = cs
      ))

    override def newClient(dest: Name, label: String): ServiceFactory[Request, Result] =
      MysqlTracing andThen defaultClient.newClient(dest, label)
  }

  private[this] val impl = MysqlClient(Handshake())
  def withCredentials(u: String, p: String): MysqlRichClient = impl.withCredentials(u, p)
  def withDatabase(db: String): MysqlRichClient = impl.withDatabase(db)
  def withCharset(charset: Short): MysqlRichClient = impl.withCharset(charset)
  override def newClient(dest: Name, label: String): ServiceFactory[Request, Result] =
    impl.newClient(dest, label)
}
