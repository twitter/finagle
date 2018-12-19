package com.twitter.finagle.zipkin.core

import collection.JavaConverters._
import com.twitter.conversions.DurationOps._
import com.twitter.util.{TimeoutException, Future, Await}
import java.util.concurrent.ConcurrentHashMap

/**
 * Cache to make sure we only have one instance of a tracer.
 * It takes care of flushing tracers on shutdown.
 * @tparam T Any subtype of [[RawZipkinTracer]] to be stored
 */
private[zipkin] class TracerCache[T <: RawZipkinTracer] {
  // to make sure we only create one instance of the tracer per host, port & category
  private[this] val map = new ConcurrentHashMap[String, T].asScala

  def getOrElseUpdate(key: String, mk: => T): T =
    synchronized(map.getOrElseUpdate(key, mk))

  // Try to flush the tracers when we shut
  // down. We give it 100ms.
  Runtime.getRuntime.addShutdownHook(new Thread {
    setName("RawZipkinTracer-ShutdownHook")
    override def run(): Unit = {
      val tracers = synchronized(map.values.toSeq)
      val joined = Future.join(tracers map (_.flush()))
      try {
        Await.result(joined, 100.milliseconds)
      } catch {
        case _: TimeoutException =>
          System.err.println("Failed to flush all traces before quitting")
      }
    }
  })
}
