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
}

private class MysqlTracingFilter(username: Option[String], database: Option[String])
    extends SimpleFilter[Request, Result] {
  import MysqlTracingFilter._

  // we cache based on the class name of the request to avoid string concat
  val unknownReqAnnoCache = mutable.Map.empty[String, String]

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
          trace.recordBinary(QueryAnnotationKey, sqlStatement)
        case PrepareRequest(sqlStatement) =>
          trace.recordBinary(PrepareAnnotationKey, sqlStatement)
        case ExecuteRequest(id, _, _, _) =>
          trace.recordBinary(ExecuteAnnotationKey, id)
        case _ =>
          val className = request.getClass.getSimpleName

          // not thread safe but also not worth the synchronization
          val anno = unknownReqAnnoCache.getOrElseUpdate(
            className,
            UnknownReqAnnotationPrefix + className.replace("$", ""))

          trace.record(anno)
      }
    }

    service(request)
  }
}
