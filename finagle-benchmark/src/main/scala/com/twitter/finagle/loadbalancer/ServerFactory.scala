package com.twitter.finagle.loadbalancer

import com.twitter.finagle.{Address, ClientConnection, Service}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Duration, Future, Time}
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
}

/**
 * Creates a ServiceFactory that applies a latency profile to Services
 * it creates.
 */
private object ServerFactory {

  /**
   * Creates a [[Server]] with the given `id` and applies `nextLatency`
   * latency for each request.
   */
  def apply(
    id: String,
    nextLatency: () => Duration,
    sr: StatsReceiver
  ) = new Server {
    private val _load = new AtomicInteger(0)
    private val _maxLoad = new AtomicInteger(0)
    private val _numRequests = new AtomicInteger(0)

    private val service = new Service[Unit, Unit] {
      val numRequests = sr.counter("count")
      val gauges = Seq(
        sr.addGauge("load") { _load.get() },
        sr.addGauge("maxload") { _maxLoad.get() }
      )

      def apply(req: Unit) = {
        synchronized {
          val l = _load.incrementAndGet()
          if (l > _maxLoad.get()) _maxLoad.set(l)
        }
        numRequests.incr()
        _numRequests.incrementAndGet()
        Future.sleep(nextLatency())(DefaultTimer).ensure {
          _load.decrementAndGet()
        }
      }
    }

    def maxLoad = _maxLoad.get().toLong
    def count = _numRequests.get().toLong
    def apply(conn: ClientConnection) = Future.value(service)
    def close(deadline: Time) = Future.Done
    override def toString = id
  }
}
