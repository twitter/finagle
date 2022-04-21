package com.twitter.finagle.mysql

import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.SimpleFilter
import com.twitter.finagle.Stack
import com.twitter.finagle.mysql.param.Credentials
import com.twitter.finagle.mysql.param.Database
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.tracing.Tracing
import com.twitter.util.Future
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.util.TablesNamesFinder
import scala.collection.mutable
import scala.util.control.NonFatal

private[finagle] object MysqlTracingFilter {
  val UserAnnotationKey = "clnt/mysql.user"
  val DatabaseAnnotationKey = "clnt/mysql.database"
  val QueryAnnotationKey = "clnt/mysql.query"
  val QueryTablesAnnotationKey = "clnt/mysql.query.tables"
  val PrepareAnnotationKey = "clnt/mysql.prepare"
  val PrepareTablesAnnotationKey = "clnt/mysql.prepare.tables"
  val ExecuteAnnotationKey = "clnt/mysql.execute"
  val UnknownReqAnnotationPrefix = "clnt/mysql."

  def module = new Stack.Module2[Credentials, Database, ServiceFactory[Request, Result]] {
    val role: Stack.Role = Stack.Role("MysqlTracing")
    val description: String = "mysql client annotations"

    def make(
      _credentials: Credentials,
      _database: Database,
      next: ServiceFactory[Request, Result]
    ): ServiceFactory[Request, Result] = {
      new MysqlTracingFilter(_credentials.username, _database.db).andThen(next)
    }
  }

  // Is this query a control query:
  //  - sent by a client itself, not a user
  //  - doesn't reference any data
  private def isControlQuery(query: String): Boolean = {
    query.startsWith("START") ||
    query.startsWith("SET") ||
    query == "COMMIT" ||
    query == "ROLLBACK" ||
    query == "LOCK" ||
    query == "UNLOCK"
  }

  // We're using https://github.com/JSQLParser/JSqlParser SQL parsing library to extract a few
  // details out of a raw SQL query:
  //  - Query verb (Select, Drop, Alter, etc)
  //  - List of tables involved in the query
  private def traceQueryDetails(
    trace: Tracing,
    verbAnnotationKey: String,
    tablesAnnotationKey: String,
    query: String
  ): Unit = {
    // The parser we're using doesn't support START TRANSACTION (and perhaps other) commands
    // so we need to wrap parsing with a few heuristics to ensure safe and quick parsing,
    // with minimal number of exceptions thrown.
    if (!isControlQuery(query)) {
      try {
        val statement = CCJSqlParserUtil.parse(query)
        val finder = new TablesNamesFinder
        val tables = finder.getTableList(statement)
        val verb = statement.getClass.getSimpleName.toUpperCase()

        trace.recordBinary(verbAnnotationKey, verb)
        if (!tables.isEmpty) {
          trace.recordBinary(tablesAnnotationKey, String.join(", ", tables))
        }
      } catch {
        case NonFatal(_) => () // ignore that query
      }
    }
  }
}

private class MysqlTracingFilter(username: Option[String], database: Option[String])
    extends SimpleFilter[Request, Result] {
  import MysqlTracingFilter._

  // we cache based on the class name of the request to avoid string concat
  private[this] val unknownReqAnnoCache = mutable.Map.empty[String, String]

  private[this] def getClassName(request: Request): String = {
    val className = request.getClass.getSimpleName
    // We need to synchronize or else we risk map corruption and that can result
    // in infinite loops.
    unknownReqAnnoCache.synchronized {
      unknownReqAnnoCache.getOrElseUpdate(
        className,
        UnknownReqAnnotationPrefix + className.replace("$", ""))
    }
  }

  def apply(request: Request, service: Service[Request, Result]): Future[Result] = {
    val trace = Trace()

    if (trace.isActivelyTracing) {
      username match {
        case Some(user) => trace.recordBinary(UserAnnotationKey, user)
        case _ => // no-op
      }
      database match {
        case Some(db) => trace.recordBinary(DatabaseAnnotationKey, db)
        case _ => // no-op
      }

      request match {
        case QueryRequest(sqlStatement) =>
          traceQueryDetails(trace, QueryAnnotationKey, QueryTablesAnnotationKey, sqlStatement)
        case PrepareRequest(sqlStatement) =>
          traceQueryDetails(trace, PrepareAnnotationKey, PrepareTablesAnnotationKey, sqlStatement)
        case ExecuteRequest(id, _, _, _) =>
          trace.recordBinary(ExecuteAnnotationKey, id)
        case _ =>
          trace.record(getClassName(request))
      }
    }

    service(request)
  }
}
