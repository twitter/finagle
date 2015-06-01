package com.twitter.finagle.exp.mysql

import com.twitter.util.Future

/**
 * A PreparedStatement represents a parameterized
 * sql statement which may be applied concurrently
 * with varying parameters.
 */
trait PreparedStatement {
  /**
   * Executes the prepared statement with the
   * given `params`.
   */
  def apply(params: Parameter*): Future[Result]

  /**
   * Executes the prepared statement with the
   * given `params` and maps `f` to the rows
   * of the returned ResultSet. If no ResultSet
   * is returned, the function returns an empty Seq.
   */
  def select[T](params: Parameter*)(f: Row => T): Future[Seq[T]] =
    apply(params: _*) map {
      case rs: ResultSet => rs.rows.map(f)
      case _ => Nil
    }
}
