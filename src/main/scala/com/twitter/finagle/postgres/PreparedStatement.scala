package com.twitter.finagle.postgres

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

  def select[T](params: Param[_]*)(f: Row => T): Future[Seq[T]] = fire(params: _*) map {
    case ResultSet(rows) => rows.map(f)
    case OK(_) => Seq.empty[Row].map(f)
  }

  def selectFirst[T](params: Param[_]*)(f: Row => T): Future[Option[T]] =
    select[T](params:_*)(f) flatMap { rows => Future.value(rows.headOption) }
}
