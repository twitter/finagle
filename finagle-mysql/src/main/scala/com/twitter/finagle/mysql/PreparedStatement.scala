package com.twitter.finagle.mysql

import com.twitter.function.JavaFunction
import com.twitter.util.Future
import java.{util => ju}
import scala.annotation.varargs
import scala.collection.JavaConverters._

/**
 * A `PreparedStatement` represents a parameterized SQL statement which may be
 * applied concurrently with varying parameters.
 *
 * These are SQL statements with `?`'s used for the parameters which are
 * "filled in" per usage by `read`, `select`, and `modify`.
 *
 * @see [[Client.prepare(String)]]
 * @see [[CursoredStatement]] for a lazy stream of [[Row]]s.
 */
trait PreparedStatement {

  /**
   * Executes the prepared statement with the given `params`.
   *
   * '''Note:''' this is a lower-level API. For SELECT queries, prefer using
   * [[select]] or [[read]], and use [[modify]] for DML (INSERT/UPDATE/DELETE)
   * and DDL.
   *
   * For Scala users, you can use the implicit conversions to [[Parameter]]
   * by importing `Parameter._`. For example:
   * {{{
   * import com.twitter.finagle.mysql.{Client, PreparedStatement, Result}
   * import com.twitter.finagle.mysql.Parameter._
   * import com.twitter.util.Future
   *
   * val client: Client = ???
   * val preparedStatement: PreparedStatement =
   *   client.prepare("INSERT INTO a_table (column1, column2) VALUES (?, ?)")
   *
   * // note the implicit conversions of the String and Int to Parameters
   * val result: Future[Result] = preparedStatement("value1", 1234)
   * }}}
   *
   * Java users, see [[asJava]] and use [[PreparedStatement.AsJava.execute]].
   *
   * @return a [[Result]] `Future`. A successful SELECT query will satisfy the
   *         `Future` with a [[ResultSet]] while DML and DDL will be an [[OK]].
   *         If there is a server error the `Future` will be failed with a [[ServerError]]
   *         exception, '''not''' an [[Error]] from the [[Result]] ADT.
   */
  def apply(params: Parameter*): Future[Result]

  /**
   * Executes the prepared statement SELECT query with the given `params`.
   *
   * For Scala users, you can use the implicit conversions to [[Parameter]]
   * by importing `Parameter._`. For example:
   * {{{
   * import com.twitter.finagle.mysql.{Client, PreparedStatement, ResultSet}
   * import com.twitter.finagle.mysql.Parameter._
   * import com.twitter.util.Future
   *
   * val client: Client = ???
   * val preparedStatement: PreparedStatement =
   *   client.prepare("SELECT column1 FROM a_table WHERE column2 = ? AND column3 = ?)")
   *
   * // note the implicit conversions of the String and Int to Parameters
   * val resultSet: Future[ResultSet] = preparedStatement.read("value1", 1234)
   * }}}
   *
   * Java users, see [[asJava]] and use [[PreparedStatement.AsJava.read]].
   *
   * @see [[select]]
   */
  def read(params: Parameter*): Future[ResultSet] =
    apply(params: _*).flatMap {
      case rs: ResultSet => Future.value(rs)
      case r => Future.exception(new IllegalStateException(s"Unsupported response to a read='$r'"))
    }

  /**
   * Executes the prepared statement DML (e.g. INSERT/UPDATE/DELETE) or DDL
   * (e.g. CREATE TABLE, DROP TABLE, COMMIT, START TRANSACTION, etc) with the
   * given `params`.
   *
   * For Scala users, you can use the implicit conversions to [[Parameter]]
   * by importing `Parameter._`. For example:
   * {{{
   * import com.twitter.finagle.mysql.{Client, OK, PreparedStatement}
   * import com.twitter.finagle.mysql.Parameter._
   * import com.twitter.util.Future
   *
   * val client: Client = ???
   * val preparedStatement: PreparedStatement =
   *   client.prepare("INSERT INTO a_table (column1, column2) VALUES (?, ?)")
   *
   * // note the implicit conversions of the String and Int to Parameters
   * val ok: Future[OK] = preparedStatement.modify("value1", 1234)
   * }}}
   *
   * Java users, see [[asJava]] and use [[PreparedStatement.AsJava.modify]].
   */
  def modify(params: Parameter*): Future[OK] =
    apply(params: _*).flatMap {
      case ok: OK => Future.value(ok)
      case r =>
        Future.exception(new IllegalStateException(s"Unsupported response to a modify='$r'"))
    }

  /**
   * Executes the prepared statement with the given `params` and maps `f` to the
   * rows of the returned [[ResultSet]]. If no [[ResultSet]] is returned, the function
   * returns an empty `Seq`.
   *
   * For Scala users, you can use the implicit conversions to [[Parameter]]
   * by importing `Parameter._`. For example:
   * {{{
   * import com.twitter.finagle.mysql.{Client, PreparedStatement, StringValue}
   * import com.twitter.finagle.mysql.Parameter._
   * import com.twitter.util.Future
   *
   * val client: Client = ???
   * val preparedStatement: PreparedStatement =
   *   client.prepare("SELECT column1 FROM a_table WHERE column2 = ?")
   *
   * // note the implicit conversion of the Int, 1234, into a Parameter
   * val result: Future[Seq[String] = preparedStatement.select(1234) { row =>
   *   row.stringOrNull("column1")
   * }
   * }}}
   *
   * Java users, see [[asJava]] and use [[PreparedStatement.AsJava.select]].
   *
   * @see [[read]]
   */
  def select[T](params: Parameter*)(f: Row => T): Future[Seq[T]] =
    apply(params: _*).map {
      case rs: ResultSet => rs.rows.map(f)
      case _ => Nil
    }

  /**
   * Provides a Java-friendly API for this [[PreparedStatement]].
   */
  final def asJava: PreparedStatement.AsJava =
    new PreparedStatement.AsJava(this)

}

object PreparedStatement {

  private[this] val ScalaSeqToFutureJavaList: Seq[Any] => Future[ju.List[Any]] =
    seq => Future.value(seq.asJava)

  private[this] def scalaSeqToFutureJavaList[T]: Seq[T] => Future[ju.List[T]] =
    ScalaSeqToFutureJavaList.asInstanceOf[Seq[T] => Future[ju.List[T]]]

  /**
   * A Java-friendly API for [[PreparedStatement]]s.
   *
   * These should be constructed via [[PreparedStatement.asJava]] but is package
   * exposed for testing.
   */
  final class AsJava private[mysql] (underlying: PreparedStatement) {

    /**
     * Executes the prepared statement with the given `params`.
     *
     * Use [[Parameters.of]] for converting the inputs into [[Parameter]]s.
     *
     * {{{
     * import com.twitter.finagle.mysql.Client;
     * import com.twitter.finagle.mysql.PreparedStatement.AsJava;
     * import com.twitter.finagle.mysql.Result;
     * import com.twitter.util.Future;
     * import static com.twitter.finagle.mysql.Parameters.of;
     *
     * Client client = ...
     * PreparedStatement.AsJava preparedStatement = client
     *   .prepare("SELECT column1 FROM a_table WHERE column2 = ?")
     *   .asJava();
     * Future<Result> result = preparedStatement.execute(of("value1"), of(1234));
     * }}}
     *
     * @see [[PreparedStatement.apply]]
     */
    @varargs
    def execute(params: Parameter*): Future[Result] =
      underlying(params: _*)

    /**
     * Executes the prepared statement SELECT query with the given `params`.
     *
     * Use [[Parameters.of]] for converting the inputs into [[Parameter]]s.
     *
     * {{{
     * import com.twitter.finagle.mysql.Client;
     * import com.twitter.finagle.mysql.PreparedStatement.AsJava;
     * import com.twitter.finagle.mysql.ResultSet;
     * import com.twitter.util.Future;
     * import static com.twitter.finagle.mysql.Parameters.of;
     *
     * Client client = ...
     * PreparedStatement.AsJava preparedStatement = client
     *   .prepare("SELECT column1 FROM a_table WHERE column2 = ?")
     *   .asJava();
     * Future<ResultSet> resultSet = preparedStatement.read(of("value1"), of(1234));
     * }}}
     *
     * @see [[select]]
     * @see [[PreparedStatement.read]]
     */
    @varargs
    def read(params: Parameter*): Future[ResultSet] =
      underlying.read(params: _*)

    /**
     * Executes the prepared statement DML (e.g. INSERT/UPDATE/DELETE) or DDL
     * (e.g. CREATE TABLE, DROP TABLE, COMMIT, START TRANSACTION, etc) with the
     * given `params`.
     *
     * Use [[Parameters.of]] for converting the inputs into [[Parameter]]s.
     *
     * {{{
     * import com.twitter.finagle.mysql.Client;
     * import com.twitter.finagle.mysql.OK;
     * import com.twitter.finagle.mysql.PreparedStatement.AsJava;
     * import com.twitter.util.Future;
     * import static com.twitter.finagle.mysql.Parameters.of;
     *
     * Client client = ...
     * PreparedStatement.AsJava preparedStatement = client
     *   .prepare("INSERT INTO a_table (column1, column2) VALUES (?, ?)")
     *   .asJava();
     * Future<OK> ok = preparedStatement.modify(of("value1"), of(1234));
     * }}}
     *
     * @see [[PreparedStatement.modify]]
     */
    @varargs
    def modify(params: Parameter*): Future[OK] =
      underlying.modify(params: _*)

    /**
     * Executes the prepared statement with the given `params` and maps `f` to the
     * rows of the returned [[ResultSet]]. If no [[ResultSet]] is returned, the function
     * returns an empty `List`.
     *
     * Use [[Parameters.of]] for converting the inputs into [[Parameter]]s.
     *
     * {{{
     * import com.twitter.finagle.mysql.Client;
     * import com.twitter.finagle.mysql.PreparedStatement.AsJava;
     * import com.twitter.finagle.mysql.Row;
     * import com.twitter.finagle.mysql.StringValue;
     * import com.twitter.util.Future;
     * import java.util.List
     * import static com.twitter.finagle.mysql.Parameters.of;
     *
     * Client client = ...
     * PreparedStatement.AsJava preparedStatement = client
     *   .prepare("SELECT column1 FROM a_table WHERE column2 = ?")
     *   .asJava();
     * Future<List<String>> result = preparedStatement.select((Row row) -> {
     *     return row.stringOrNull();
     *   },
     *   of(1234)
     * );
     * }}}
     *
     * @see [[read]]
     * @see [[PreparedStatement.select]]
     */
    @varargs
    def select[T](f: JavaFunction[Row, T], params: Parameter*): Future[ju.List[T]] = {
      val asSeq = underlying.select(params: _*)(f(_))
      asSeq.flatMap(scalaSeqToFutureJavaList[T])
    }
  }

}
