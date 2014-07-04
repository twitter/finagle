package com.twitter.finagle.factory

import com.twitter.finagle._
import com.twitter.util.{Future, Duration, Timer}

private[finagle] object TimeoutFactory {
  object ServiceTimeout extends Stack.Role

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.factory.TimeoutFactory]].
   */
  case class Param(timeout: Duration)
  implicit object Param extends Stack.Param[Param] {
    val default = Param(Duration.Top)
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.factory.TimeoutFactory]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Simple[ServiceFactory[Req, Rep]](ServiceTimeout) {
      val description = "Time out service acquisition after a given period"
      def make(params: Params, next: ServiceFactory[Req, Rep]) = {
        val TimeoutFactory.Param(timeout) = params[TimeoutFactory.Param]
        val param.Label(label) = params[param.Label]
        val param.Timer(timer) = params[param.Timer]

        val exc = new ServiceTimeoutException(timeout)
        exc.serviceName = label
        new TimeoutFactory(next, timeout, exc, timer)
      }
    }
}

/**
 * A factory wrapper that times out the service acquisition after the
 * given time.
 */
class TimeoutFactory[Req, Rep](
    self: ServiceFactory[Req, Rep],
    timeout: Duration,
    exception: ServiceTimeoutException,
    timer: Timer)
  extends ServiceFactoryProxy[Req, Rep](self)
{
  override def apply(conn: ClientConnection) = {
    val res = super.apply(conn)
    res.within(timer, timeout) rescue {
      case exc: java.util.concurrent.TimeoutException =>
        res.raise(exc)
        res onSuccess { _.close() }
        Future.exception(exception)
    }
  }
}
