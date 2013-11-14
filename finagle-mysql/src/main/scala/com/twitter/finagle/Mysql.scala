package com.twitter.finagle.exp

import com.twitter.finagle.{Client, Name, Service, ServiceFactory, SimpleFilter}
import com.twitter.finagle.client.{Bridge, DefaultClient, DefaultPool}
import com.twitter.finagle.exp.mysql._
import com.twitter.finagle.exp.mysql.transport.{MysqlTransporter, Packet}
import com.twitter.finagle.tracing.Trace
import com.twitter.util.Future
import java.net.SocketAddress

/**
 * Tracing filter for mysql client requests.
 */
object MysqlTracing extends SimpleFilter[Request, Result] {
  def apply(request: Request, service: Service[Request, Result]) = {
    request match {
      case QueryRequest(sqlStatement) => Trace.recordBinary("mysql.query", sqlStatement)
      case PrepareRequest(sqlStatement) => Trace.recordBinary("mysql.prepare", sqlStatement)
      // TODO: save the prepared statement and put it in the executed request trace
      case ExecuteRequest(ps, flags, iterationCount) => Trace.recordBinary("mysql.execute", "?")
      case _ => Trace.record("mysql." + request.getClass.getName)
    }
    service(request)
  }
}

/**
 * A rich mysql client supplements a Client[Request, Result]
 * with convenient mysql specific builder methods.
 */
trait MysqlRichClient { self: Client[Request, Result] =>
  /**
   * Configure the client the given credentials.
   */
  def withCredentials(u: String, p: String): Client[Request, Result]

  /**
   * Initial database to use when this client
   * establishes a new session. This can be subsequently
   * changed with a select db query.
   */
  def withDatabase(db: String): Client[Request, Result]

  /**
   * The default character set used when establishing
   * a new session.
   */
  def withCharset(charset: Short): Client[Request, Result]

  /**
   * Creates a new rich mysql client connected to
   * the destination described by `dest` and `label`.
   */
  def newRichClient(dest: Name, label: String): mysql.Client =
    mysql.Client(newClient(dest, label))

  /**
   * Creates a new rich mysql client terminated to the endpoint
   * resolved by `dest`.
   */
  def newRichClient(dest: String): mysql.Client =
    mysql.Client(newClient(dest))
}

/**
 * Brings all the pieces together and implements
 * a mysql client in terms of a finagle DefaultClient.
 */
case class MysqlClient private[finagle](
  handshake: Handshake
) extends Client[Request, Result]
  with MysqlRichClient {
  val defaultClient = new DefaultClient[Request, Result](
    name = "mysql",
    // TODO: Remove limit when we can multiplex requests over
    // the DefaultPool with respect to prepared statements.
    pool = DefaultPool(high = 1),
    endpointer = {
      val bridge = Bridge[Packet, Packet, Request, Result](
        MysqlTransporter, new ClientDispatcher(_, handshake)
      )
      (sa, sr) => bridge(sa, sr)
    }
  )

  def withCredentials(u: String, p: String): MysqlClient =
    copy(handshake = handshake.copy(
      username = Option(u),
      password = Option(p)
    ))

  def withDatabase(db: String): MysqlClient =
    copy(handshake = handshake.copy(
      database = Option(db)
    ))

  def withCharset(cs: Short): MysqlClient =
    copy(handshake = handshake.copy(
      charset = cs
    ))

  override def newClient(dest: Name, label: String): ServiceFactory[Request, Result] =
    MysqlTracing andThen defaultClient.newClient(dest, label)
}

object Mysql extends MysqlClient(Handshake())
