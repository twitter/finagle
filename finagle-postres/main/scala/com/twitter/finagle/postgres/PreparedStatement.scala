package com.twitter.finagle.postgres

import com.twitter.concurrent.AsyncStream
import com.twitter.finagle.postgres.codec.Errors
import com.twitter.util.Future

/*
 * A query that supports parameter substitution. Can help prevent SQL injection attacks.
 */
trait PreparedStatement {
  def fire(params: Param[_]*): Future[QueryResponse]

  def exec(params: Param[_]*): Future[OK] = fire(params: _*) flatMap {
    case ok @ OK(_)   => Future.value(ok)
    case ResultSet(_) => Future.exception(Errors.client("Update query expected"))
  }

  def selectToStream[T](params: Param[_]*)(f: Row => T): Future[AsyncStream[T]] = fire(params: _*) map {
    case ResultSet(rows) => rows.map(f)
    case OK(_) => AsyncStream.empty
  }
  def select[T](params: Param[_]*)(f: Row => T): Future[Seq[T]] =
    selectToStream(params: _*)(f).flatMap(_.toSeq)

  def selectFirst[T](params: Param[_]*)(f: Row => T): Future[Option[T]] =
    select[T](params:_*)(f) flatMap { rows => Future.value(rows.headOption) }
}
