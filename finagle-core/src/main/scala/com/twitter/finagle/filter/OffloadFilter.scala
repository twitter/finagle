package com.twitter.finagle.filter

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle.offload.numWorkers
import com.twitter.finagle.stats.FinagleStatsReceiver
import com.twitter.finagle.{Service, ServiceFactory, SimpleFilter, Stack, Stackable}
import com.twitter.util.{Future, FuturePool, Promise}
import java.util.concurrent.{ExecutorService, Executors}

/**
 * These modules introduce async-boundary into the future chain, effectively shifting continuations
 * off of IO threads (into a given [[FuturePool]]).
 *
 * This filter can be enabled by default through the flag `com.twitter.finagle.offload.numWorkers`.
 */
private[finagle] object OffloadFilter {

  val Role = Stack.Role("OffloadWorkFromIO")
  val Description = "Offloading computations from IO threads"

  private[this] lazy val (defautPool, defautPoolStats) = {
    numWorkers.get match {
      case None =>
        (None, Seq.empty)
      case Some(threads) =>
        val factory = new NamedPoolThreadFactory("finagle/offload", makeDaemons = true)
        val pool = FuturePool.interruptible(Executors.newFixedThreadPool(threads, factory))
        val stats = FinagleStatsReceiver.scope("offload_pool")
        val gauges = Seq(
          stats.addGauge("pool_size") { pool.poolSize },
          stats.addGauge("active_tasks") { pool.numActiveTasks },
          stats.addGauge("completed_tasks") { pool.numCompletedTasks }
        )
        (Some(pool), gauges)
    }
  }

  sealed abstract class Param
  object Param {

    def apply(pool: FuturePool): Param = Enabled(pool)
    def apply(executor: ExecutorService): Param = Enabled(FuturePool(executor))

    final case class Enabled(pool: FuturePool) extends Param
    final case object Disabled extends Param

    implicit val param: Stack.Param[Param] =
      Stack.Param(defautPool.map(Enabled(_)).getOrElse(Disabled))
  }

  def client[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] = new Module[Req, Rep](new Client(_))

  def server[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] = new Module[Req, Rep](new Server(_))

  final class Client[Req, Rep](pool: FuturePool) extends SimpleFilter[Req, Rep] {
    def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
      // What we're trying to achieve is to ensure all continuations spawn out of a future returned
      // from this method are run outside of the IO thread (in the given FuturePool).
      //
      // Simply doing:
      //
      //   service(request).flatMap(pool.apply(_))
      //
      // is not enough as the successful offloading is contingent on winning the race between
      // pool.apply and promise linking (via flatMap). In our high load simulations we observed that
      // offloading won't happen (because the race is lost) in about 6% of the cases.
      //
      // One way of increasing our chances is making the race unfair by also including the dispatch
      // time in the equation: the offloading won't happen if both making an RPC dispatch and
      // bouncing through the executor (FuturePool) somehow happened to outrun a synchronous code in
      // this method.
      //
      // You would be surprised but this can happen. Same simulations report that we lose a race in
      // about 1 in 1 000 000 of cases this way (0.0001%).
      //
      // There is no better explanation to this than in Bryce's own words:
      //
      // > Thread pauses are nuts.
      // > Kernels are crazy.
      val response = service(request)
      val shifted = Promise.interrupts[Rep](response)
      response.respond(t => pool(shifted.update(t)))

      shifted
    }
  }

  final class Server[Req, Rep](pool: FuturePool) extends SimpleFilter[Req, Rep] {
    def apply(request: Req, service: Service[Req, Rep]): Future[Rep] =
      // Offloading on the server-side is fairly straightforward: it comes down to running
      // service.apply (users' work) outside of the IO thread.
      //
      // Unfortunately, there is no (easy) way to bounce back to the IO thread as we return into
      // the stack. It's more or less fine as we will switch back to IO as we enter the pipeline
      // (Netty land).
      pool(service(request)).flatten
  }

  private final class Module[Req, Rep](makeFilter: FuturePool => SimpleFilter[Req, Rep])
      extends Stack.Module1[Param, ServiceFactory[Req, Rep]] {

    def make(p: Param, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = p match {
      case Param.Enabled(pool) => makeFilter(pool).andThen(next)
      case Param.Disabled => next
    }

    def role: Stack.Role = Role
    def description: String = Description
  }
}
