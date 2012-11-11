package com.twitter.finagle.postgres

import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.postgres.protocol.Communication
import com.twitter.finagle.postgres.protocol.OK
import com.twitter.finagle.postgres.protocol.PgCodec
import com.twitter.finagle.postgres.protocol.PgRequest
import com.twitter.finagle.postgres.protocol.PgResponse
import com.twitter.finagle.postgres.protocol.Query
import com.twitter.finagle.postgres.protocol.QueryResponse
import com.twitter.finagle.postgres.protocol.ResultSet
import com.twitter.finagle.postgres.protocol.Row
import com.twitter.util.Future

class Client(factory: ServiceFactory[PgRequest, PgResponse]) {
  private[this] lazy val underlying = factory.apply()

  def query(sql: String): Future[QueryResponse] = sendQuery(sql) {
    case rs: ResultSet => Future(rs)
    case ok: OK => Future(ok)
  }

  def fetch(sql: String): Future[ResultSet] = sendQuery(sql) {
    case rs: ResultSet => Future(rs)
  }

  def execute(sql: String): Future[OK] = sendQuery(sql) {
    case ok: OK => Future(ok)
  }

  def select[T](sql: String)(f: Row => T): Future[Seq[T]] = for {
    rs <- fetch(sql)
    spool <- rs.map(f)
    rows <- spool.toSeq
  } yield (rows)

  def close() {
    factory.close()
  }

  private[this] def sendQuery[T](sql: String)(handler: PartialFunction[PgResponse, Future[T]]) = send(Communication.request(new Query(sql)))(handler)

  private[this] def send[T](r: PgRequest)(handler: PartialFunction[PgResponse, Future[T]]) =
    underlying flatMap { service =>
      service(r) flatMap (handler orElse {
        case some => throw new UnsupportedOperationException("TODO Support exceptions correctly " + some)
      })
    }

}

object Client {

  def apply(host: String, username: String, password: Option[String], database: String): Client = {
    val factory: ServiceFactory[PgRequest, PgResponse] = ClientBuilder()
      .codec(new PgCodec(username, password, database))
      .hosts(host)
      .hostConnectionLimit(1)
      .buildFactory()

    new Client(factory)
  }

}
