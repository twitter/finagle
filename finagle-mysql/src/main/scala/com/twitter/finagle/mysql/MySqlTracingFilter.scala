package com.twitter.finagle.mysql

import com.twitter.finagle.{SimpleFilter, Service, ServiceFactory, Stack}
import com.twitter.finagle.mysql.param.{Credentials, Database}
import com.twitter.finagle.tracing.Trace
import com.twitter.util.Future
import scala.collection.mutable

private[finagle] object MysqlTracingFilter {
  val UserAnnotationKey = "clnt/mysql.user"
  val DatabaseAnnotationKey = "clnt/mysql.database"
  val QueryAnnotationKey = "clnt/mysql.query"
  val PrepareAnnotationKey = "clnt/mysql.prepare"
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

  // For security purposes we only emit the first token which will always be a SQL
  // command (like SELECT) or a fragment (DROP [DATABASE|TABLE]). We don't want to
  // implement the full parser here so we won't deal with the two token commands.
  private def firstToken(str: String): String =
    str.substring(0, math.max(0, str.indexOf(' ')))
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
          trace.recordBinary(QueryAnnotationKey, firstToken(sqlStatement))
        case PrepareRequest(sqlStatement) =>
          trace.recordBinary(PrepareAnnotationKey, firstToken(sqlStatement))
        case ExecuteRequest(id, _, _, _) =>
          trace.recordBinary(ExecuteAnnotationKey, id)
        case _ =>
          trace.record(getClassName(request))
      }
    }

    service(request)
  }
}
