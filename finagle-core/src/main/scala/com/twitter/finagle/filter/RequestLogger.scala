package com.twitter.finagle.filter

import com.twitter.finagle.{
  ClientConnection,
  Service,
  ServiceFactory,
  ServiceFactoryProxy,
  ServiceProxy,
  Stack
}
import com.twitter.finagle.tracing.Trace
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Future, Stopwatch}
import java.util.concurrent.TimeUnit

object RequestLogger {

  sealed trait Param {
    def mk(): (Param, Stack.Param[Param]) = (this, Param.param)
  }

  private[finagle] object Param {
    case object Disabled extends Param
    case object Enabled extends Param

    implicit val param: Stack.Param[Param] = Stack.Param(Param.Disabled)
  }

  /**
   * Enables the [[RequestLogger]].
   */
  val Enabled: Param = Param.Enabled

  /**
   * Disables the [[RequestLogger]] (disabled by default).
   */
  val Disabled: Param = Param.Disabled

  /**
   * The name of the logger used.
   */
  val loggerName = "com.twitter.finagle.request.Logger"

  private val log = Logger.get(loggerName)

  private[this] def withLogging[Req, Rep](
    logger: Logger,
    label: String,
    nowNanos: () => Long,
    role: Stack.Role,
    svcFac: ServiceFactory[Req, Rep]
  ): ServiceFactory[Req, Rep] =
    new ServiceFactoryProxy[Req, Rep](svcFac) {
      private[this] val requestLogger = new RequestLogger(logger, label, role.name, nowNanos)
      override def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
        super.apply(conn).map { svc =>
          new ServiceProxy[Req, Rep](svc) {
            override def apply(request: Req): Future[Rep] = {
              if (!requestLogger.shouldTrace) {
                super.apply(request)
              } else {
                val startNanos = requestLogger.start()
                try requestLogger.endAsync(startNanos, super.apply(request))
                finally requestLogger.endSync(startNanos)
              }
            }
          }
        }
      }
    }

  // This version is only exposed for testing of `RequestLogger`.
  private[filter] def newStackTransformer(
    logger: Logger,
    label: String,
    nowNanos: () => Long
  ): Stack.Transformer =
    new Stack.Transformer {
      def apply[Req, Rep](stack: Stack[ServiceFactory[Req, Rep]]): Stack[ServiceFactory[Req, Rep]] =
        stack.map((hd, sf) => withLogging(logger, label, nowNanos, hd.role, sf))
    }

  /*
   * Used to [[Stack.transform transform]] a [[Stack]] to include request tracing.
   *
   * @param label the label of the client or server
   *
   * @param nowNanos the current time in nanoseconds
   */
  private[finagle] def newStackTransformer(
    label: String,
    nowNanos: () => Long = Stopwatch.systemNanos
  ): Stack.Transformer = newStackTransformer(log, label, nowNanos)

}

/**
 * Produces fine-grained logging of requests and responses as they flow through
 * the Finagle stack, typically [[com.twitter.finagle.Filter Filters]].
 *
 * Instances are thread-safe and safe to be used by multiple threads.
 *
 * @param logger the logger to write request logs to
 *
 * @param label the label of the client or server
 *
 * @param name used in the logs to indicate what is starting and ending.
 *
 * @note logs are done at `TRACE` level in the "com.twitter.finagle.request.Logger"
 *       `Logger`.
 */
private class RequestLogger(logger: Logger, label: String, name: String, nowNanos: () => Long) {

  def shouldTrace: Boolean =
    logger.isLoggable(Level.TRACE)

  def start(): Long = {
    val start = nowNanos()
    val traceId = Trace.id
    logger.trace(s"traceId=$traceId $label $name begin")
    start
  }

  def endAsync[T](startNanos: Long, future: Future[T]): Future[T] = {
    // We use `future.transform` to make sure that we have the right order of execution
    // for the logging. If we instead used `future.ensure` we attach a continuation but
    // have no guarantees as to whether it will execute before or after any other composition
    // operations, potentially bundling expensive synchronous work from higher in the call
    // chain into the recorded time.
    future.transform { _ =>
      val traceId = Trace.id
      val elapsedUs = elapsedMicros(startNanos)
      logger.trace(s"traceId=$traceId $label $name end cumulative async elapsed $elapsedUs us")
      future
    }
  }

  def endSync(startNanos: Long): Unit = {
    val traceId = Trace.id
    val elapsedUs = elapsedMicros(startNanos)
    logger.trace(s"traceId=$traceId $label $name end cumulative sync elapsed $elapsedUs us")
  }

  private[this] def elapsedMicros(startNanos: Long): Long = {
    val elapsedNanos = nowNanos() - startNanos
    TimeUnit.MICROSECONDS.convert(elapsedNanos, TimeUnit.NANOSECONDS)
  }

}
