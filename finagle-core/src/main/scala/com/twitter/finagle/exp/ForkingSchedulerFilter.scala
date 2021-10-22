package com.twitter.finagle.exp

import com.twitter.concurrent.ForkingScheduler
import com.twitter.finagle.Failure
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.SimpleFilter
import com.twitter.finagle.Stack
import com.twitter.finagle.Stackable
import com.twitter.util._

/**
 * Filter placed in finagle's default server stack to offload the
 * execution of `Future` computations using a `ForkingScheduler`. It's
 * disabled automatically in case the current scheduler doesn't
 * support forking.
 *
 * This implementation doesn't have a client filter because it's
 * expected that the forking scheduler will handle the thread shift
 * back to its workers after a client returns.
 */
object ForkingSchedulerFilter {

  private[this] val Role = Stack.Role("UseForkingScheduler")
  private[this] val Description = "Forks the execution if the scheduler has forking capability"

  private[finagle] sealed abstract class Param
  private[finagle] object Param {

    final case class Enabled(scheduler: ForkingScheduler) extends Param
    final case object Disabled extends Param

    implicit val param: Stack.Param[Param] = new Stack.Param[Param] {
      lazy val default: Param =
        ForkingScheduler() match {
          case Some(scheduler) =>
            Enabled(scheduler)
          case None =>
            Disabled
        }

      override def show(p: Param): Seq[(String, () => String)] = {
        val enabledStr = p match {
          case Enabled(scheduler) => scheduler.toString
          case Disabled => "Disabled"
        }
        Seq(("ForkingSchedulerFilter", () => enabledStr))
      }
    }
  }

  def server[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[Param, ServiceFactory[Req, Rep]] {

      def make(p: Param, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = p match {
        case Param.Enabled(scheduler) => (new Server(scheduler)).andThen(next)
        case Param.Disabled => next
      }

      def role: Stack.Role = Role
      def description: String = Description
    }

  final class Server[Req, Rep](scheduler: ForkingScheduler) extends SimpleFilter[Req, Rep] {

    private[this] final val overloadedFailure =
      Future.exception(Failure.rejected("Forking scheduler overloaded"))

    def apply(request: Req, service: Service[Req, Rep]): Future[Rep] =
      scheduler.tryFork(service(request)).flatMap {
        case Some(v) => Future.value(v)
        case None => overloadedFailure
      }
  }
}
