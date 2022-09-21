package com.twitter.finagle.client

import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.SimpleFilter
import com.twitter.finagle.Stack
import com.twitter.finagle.Stackable
import com.twitter.finagle.context.Contexts
import com.twitter.util.Future

private[finagle] object DynamicBackupRequestFilter {
  val role = Stack.Role("DynamicBackupRequestFilter")
  val description = "Send a backup request at a configurable latency, initialized by MethodBuilder"

  private val localFilter = new Contexts.local.Key[BackupRequestFilter[_, _]]()

  def let[T](filter: BackupRequestFilter[_, _])(f: => T): T =
    Contexts.local.let(localFilter, filter) { f }

  private def current: Option[BackupRequestFilter[_, _]] =
    Contexts.local.get(localFilter)

  def placeholder[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.NoOpModule[ServiceFactory[Req, Rep]](role, description)

  def perRequestModule[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module0[ServiceFactory[Req, Rep]] {
      val role = DynamicBackupRequestFilter.role
      val description = DynamicBackupRequestFilter.description

      def make(next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] =
        new DynamicBackupRequestFilter[Req, Rep].andThen(next)
    }
}

/**
 * A dynamically configurable BackupRequestFilter utilized by MethodBuilder.
 *
 * See [[BackupRequestFilter]] for more information on each parameter.
 */
private[finagle] final class DynamicBackupRequestFilter[Req, Rep] extends SimpleFilter[Req, Rep] {

  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
    DynamicBackupRequestFilter.current match {
      case Some(filter) => filter.asInstanceOf[BackupRequestFilter[Req, Rep]].apply(req, service)
      case None => service(req)
    }
  }
}
