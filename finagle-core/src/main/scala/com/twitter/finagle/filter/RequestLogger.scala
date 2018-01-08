package com.twitter.finagle.filter

import com.twitter.finagle.{ClientConnection, Service, ServiceFactory, ServiceFactoryProxy, ServiceProxy, Stack}
import com.twitter.finagle.tracing.Trace
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Future, Stopwatch}
import java.util.concurrent.TimeUnit

object RequestLogger {

  /**
   * The name of the logger used.
   */
  val loggerName = "com.twitter.finagle.request.Logger"

  private val log = Logger.get(loggerName)

  private[this] def withLogging[Req, Rep](
    label: String,
    nowNanos: () => Long,
    role: Stack.Role,
    svcFac: ServiceFactory[Req, Rep]
  ): ServiceFactory[Req, Rep] =
    new ServiceFactoryProxy[Req, Rep](svcFac) {
      private[this] val requestLogger = new RequestLogger(label, role.name, nowNanos)
      override def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
        super.apply(conn).map { svc =>
          new ServiceProxy[Req, Rep](svc) {
            override def apply(request: Req): Future[Rep] = {
              if (!requestLogger.shouldTrace) {
                super.apply(request)
              } else {
                val startNanos = requestLogger.start()
                try requestLogger.endAsync(startNanos, super.apply(request))
                finally
                  requestLogger.endSync(startNanos)
              }
            }
          }
        }
      }
    }

  /**
   * Used to [[Stack.transform transform]] a [[Stack]] to include request tracing.
   *
   * @param label the label of the client or server
   */
  private[finagle] def newStackTransformer(
    label: String,
    nowNanos: () => Long = Stopwatch.systemNanos
  ): Stack.Transformer =
    new Stack.Transformer {
      def apply[Req, Rep](
        stack: Stack[ServiceFactory[Req, Rep]]
      ): Stack[ServiceFactory[Req, Rep]] = {
        stack.transform {
          case Stack.Leaf(hd, svcFac) =>
            Stack.Leaf(hd, withLogging(label, nowNanos, hd.role, svcFac))
          case Stack.Node(hd, mk, next) =>
            val mkWithLogging =
              (ps: Stack.Params, stack: Stack[ServiceFactory[Req, Rep]]) => {
                val mkStack = mk(ps, stack)
                val mkSvcFac = mkStack.make(ps)
                val svcFac = withLogging(label, nowNanos, hd.role, mkSvcFac)
                Stack.Leaf(stack.head, svcFac)
              }
            Stack.Node(hd, mkWithLogging, next)
        }
      }
    }

}

/**
 * Produces fine-grained logging of requests and responses as they flow through
 * the Finagle stack, typically [[com.twitter.finagle.Filter Filters]].
 *
 * Instances are thread-safe and safe to be used by multiple threads.
 *
 * @param label the label of the client or server
 *
 * @param name used in the logs to indicate what is starting and ending.
 *
 * @note logs are done at `TRACE` level in the "com.twitter.finagle.request.Logger"
 *       `Logger`.
 */
private class RequestLogger(
  label: String,
  name: String,
  nowNanos: () => Long
) {
  import RequestLogger._

  def shouldTrace: Boolean =
    log.isLoggable(Level.TRACE)

  def start(): Long = {
    val start = nowNanos()
    val traceId = Trace.id
    log.trace(s"traceId=$traceId $label $name begin")
    start
  }

  def endAsync[T](
    startNanos: Long,
    future: Future[T]
  ): Future[T] = {
    future.ensure {
      val traceId = Trace.id
      val elapsedUs = elapsedMicros(startNanos)
      log.trace(s"traceId=$traceId $label $name end cumulative async elapsed $elapsedUs us")
    }
  }

  def endSync(startNanos: Long): Unit = {
    val traceId = Trace.id
    val elapsedUs = elapsedMicros(startNanos)
    log.trace(s"traceId=$traceId $label $name end cumulative sync elapsed $elapsedUs us")
  }

  private[this] def elapsedMicros(startNanos: Long): Long = {
    val elapsedNanos = nowNanos() - startNanos
    TimeUnit.MICROSECONDS.convert(elapsedNanos, TimeUnit.NANOSECONDS)
  }

}
