package com.twitter.finagle.postgres

import java.sql.{ Date => SQLDate }
import com.twitter.util.Future
import com.twitter.finagle.postgres.protocol.PgRequest
import com.twitter.finagle.postgres.protocol.PgResponse
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.postgres.protocol.FieldDescription
import org.jboss.netty.buffer.ChannelBuffer
import java.sql.Timestamp
import com.twitter.finagle.postgres.protocol.Query
import com.twitter.finagle.postgres.protocol.Communication
import com.twitter.finagle.postgres.protocol.Charsets
import com.twitter.finagle.postgres.protocol.RowDescription
import com.twitter.finagle.postgres.protocol.ReadyForQuery
import com.twitter.finagle.postgres.protocol.CommandComplete
import com.twitter.finagle.postgres.protocol.DataRow
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.postgres.protocol.PgCodec
import com.twitter.logging.Logger
import com.twitter.finagle.postgres.protocol.CommandComplete
import com.twitter.finagle.postgres.protocol.Deleted
import com.twitter.finagle.postgres.protocol.Inserted
import com.twitter.finagle.postgres.protocol.IntValue
import com.twitter.finagle.postgres.protocol.QueryResponse
import com.twitter.finagle.postgres.protocol.ResultSet
import com.twitter.finagle.postgres.protocol.Row
import com.twitter.finagle.postgres.protocol.Updated
import com.twitter.finagle.postgres.protocol.ValueParser
import com.twitter.finagle.postgres.protocol.Field

class Client(factory: ServiceFactory[PgRequest, PgResponse]) {
  private[this] lazy val underlying = factory.apply()

  def query(sql: String) = Communication.request(new Query(sql))

  def select[T](sql: String)(f: Row => T): Future[Seq[T]] = send(query(sql)) {
    case ResultSet(_, rows) => rows.map(f)
    case _ => throw new IllegalStateException("Select method is used for non-select operation")
  }

  def insert[T](sql: String): Future[Int] = send(query(sql)) {
    case Inserted(num) => num
    case _ => throw new IllegalStateException("Insert method is used for non-insert operation")
  }

  def delete[T](sql: String): Future[Int] = send(query(sql)) {
    case Deleted(num) => num
    case _ => throw new IllegalStateException("Delete method is used for non-delete operation")
  }

  def update[T](sql: String): Future[Int] = send(query(sql)) {
    case Updated(num) => num
    case _ => throw new IllegalStateException("Update method is used for non-update operation")
  }

  def close() {
    factory.close()
  }

  private[this] def send[T](r: PgRequest)(handler: PartialFunction[PgResponse, T]) =
    underlying flatMap { service =>
      service(r) map (handler orElse {
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
