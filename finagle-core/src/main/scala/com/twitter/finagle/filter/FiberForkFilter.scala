package com.twitter.finagle.filter

import com.twitter.app.GlobalFlag
import com.twitter.concurrent.Scheduler
import com.twitter.finagle.CoreToggles
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.SimpleFilter
import com.twitter.finagle.Stack
import com.twitter.finagle.Stackable
import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.stats.FinagleStatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.ExecutorWorkQueueFiber
import com.twitter.util.ExecutorServiceFuturePool
import com.twitter.util.Fiber
import com.twitter.util.Future
import com.twitter.util.FuturePool
import com.twitter.util.SchedulerWorkQueueFiber
import com.twitter.util.WorkQueueFiber.FiberMetrics

object restrictWorkQueueFiberZone
    extends GlobalFlag[Boolean](
      true,
      "When enabled, restricts work queue fiber usage to ATLA. Enabled by default."
    )

private class FiberForkFilter[Req, Rep](fiberFactory: () => Fiber) extends SimpleFilter[Req, Rep] {

  def apply(
    request: Req,
    service: Service[Req, Rep]
  ): Future[Rep] = {
    Fiber.let(fiberFactory()) {
      service(request)
    }
  }
}

private[finagle] object FiberForkFilter {
  private[this] val log = Logger.get

  val Description: String = "Fork requests into a new fiber"
  val Role: Stack.Role = Stack.Role("FiberForkFilter")

  private val toggle = CoreToggles("com.twitter.finagle.filter.UseWorkQueueFiber")

  private[filter] def generateFiberFactory(pool: Option[FuturePool]): () => Fiber = {
    val workQueueFiberEnabled: Boolean = {
      val serverInfo = ServerInfo()
      val zoneEnabled = serverInfo.zone.contains("atla") || !restrictWorkQueueFiberZone()
      zoneEnabled && toggle(serverInfo.id.hashCode)
    }
    if (workQueueFiberEnabled) {
      pool match {
        case Some(exec: ExecutorServiceFuturePool) =>
          log.info("Server has Offload Pool backed work-queueing fibers enabled")
          () => new ExecutorWorkQueueFiber(exec.executor, FiberForkFilter.Metrics)
        case _ =>
          log.info("Server has Scheduler backed work-queueing fibers enabled")
          () => new SchedulerWorkQueueFiber(Scheduler(), FiberForkFilter.Metrics)
      }
    } else {
      log.info("Server has work-queueing fibers disabled")
      () => Fiber.newCachedSchedulerFiber()
    }
  }

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[OffloadFilter.Param, ServiceFactory[Req, Rep]] {
      def role: Stack.Role = Role
      def description: String = Description

      def make(p: OffloadFilter.Param, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = {
        new FiberForkFilter(generateFiberFactory(p.pool)).andThen(next)
      }
    }

  def Metrics = new FiberMetrics {
    private[this] val statsReceiver = FinagleStatsReceiver.scope("fiber")
    private[this] val createdFibers = statsReceiver.counter("created")
    private[this] val schedulerSubmissions = statsReceiver.counter("submissions")
    private[this] val taskSubmissions = statsReceiver.counter("tasks")
    private[this] val threadLocalSubmissions = statsReceiver.counter("threadlocaltask")
    private[this] val flushes = statsReceiver.counter("flushes")

    override def fiberCreated(): Unit = createdFibers.incr()
    override def threadLocalSubmitIncrement(): Unit = threadLocalSubmissions.incr()
    override def taskSubmissionIncrement(): Unit = taskSubmissions.incr()
    override def schedulerSubmissionIncrement(): Unit = schedulerSubmissions.incr()
    override def flushIncrement(): Unit = flushes.incr()
  }
}
