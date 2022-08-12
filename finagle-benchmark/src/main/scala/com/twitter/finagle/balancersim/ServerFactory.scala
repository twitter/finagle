package com.twitter.finagle.balancersim

import com.twitter.finagle.loadbalancer.EndpointFactory
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.Address
import com.twitter.finagle.ClientConnection
import com.twitter.finagle.Failure
import com.twitter.finagle.Service
import com.twitter.finagle.Status
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Time
import java.util.concurrent.atomic.AtomicInteger

private trait Server extends EndpointFactory[Unit, Unit] {
  val address = Address.Failed(new Exception)
  def remake() = {}

  /**
   * The maximum amount of concurrent load observed.
   */
  def maxLoad: Long

  /**
   * The total number of load that this server received.
   */
  def count: Long

  /**
   * The success rate of this server
   */
  def successRate: Double
}

/**
 * Creates a ServiceFactory that applies a latency and failure profile to Services
 * it creates.
 */
private object ServerFactory {

  /**
   * Creates a [[Server]] with the given `id`. Per request, `nextLatency`
   * and `nextIsFailure` is applied to determine the type of response and it's
   * latency
   */
  def apply(
    id: String,
    nextLatency: () => Duration,
    nextIsFailure: () => Boolean,
    sr: StatsReceiver
  ): Server = new Server {
    private val _load = new AtomicInteger(0)
    private val _maxLoad = new AtomicInteger(0)
    private val _numRequests = new AtomicInteger(0)
    private val _successes = new AtomicInteger(0)

    private val failedResponse = Future.exception(Failure.rejected("boom"))

    private val service = new Service[Unit, Unit] {
      val numRequests = sr.counter("count")
      val gauges = Seq(
        sr.addGauge("load") { _load.get() },
        sr.addGauge("maxload") { _maxLoad.get() }
      )

      def apply(req: Unit): Future[Unit] = {
        synchronized {
          val l = _load.incrementAndGet()
          if (l > _maxLoad.get()) _maxLoad.set(l)
        }
        numRequests.incr()
        _numRequests.incrementAndGet()
        Future.sleep(nextLatency())(DefaultTimer).before {
          _load.decrementAndGet()

          if (nextIsFailure()) failedResponse
          else {
            _successes.incrementAndGet()
            Future.Done
          }
        }
      }
    }

    def successRate: Double = _successes.get().toLong / count.toDouble
    def maxLoad: Long = _maxLoad.get().toLong
    def count: Long = _numRequests.get().toLong
    def apply(conn: ClientConnection): Future[Service[Unit, Unit]] = Future.value(service)
    def close(deadline: Time): Future[Unit] = Future.Done
    def status: Status = service.status
    override def toString: String = id
  }
}
