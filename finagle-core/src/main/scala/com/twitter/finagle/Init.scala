package com.twitter.finagle

import com.twitter.concurrent.Once
import com.twitter.finagle.exp.FinagleScheduler
import com.twitter.finagle.loadbalancer.aperture.DeterministicOrdering
import com.twitter.finagle.stats.FinagleStatsReceiver
import com.twitter.finagle.util.DefaultLogger
import com.twitter.util.FuturePool
import java.util.concurrent.atomic.AtomicReference
import java.util.logging.Level
import java.util.Properties
import scala.util.control.NonFatal

/**
 * Global initialization of Finagle.
 */
private[twitter] object Init {
  private val log = DefaultLogger

  // Used to record Finagle versioning in trace info.
  private val unknownVersion = "?"
  private val _finagleVersion = new AtomicReference[String](unknownVersion)
  private val _finagleBuildRevision = new AtomicReference[String](unknownVersion)

  private[this] val gauges = {
    // because unboundedPool and interruptibleUnboundedPool share a common
    // `ExecutorService`, these metrics apply to both of the FuturePools.
    val pool = FuturePool.unboundedPool
    val fpoolStats = FinagleStatsReceiver.scope("future_pool")
    Seq(
      fpoolStats.addGauge("pool_size") { pool.poolSize },
      fpoolStats.addGauge("active_tasks") { pool.numActiveTasks },
      fpoolStats.addGauge("completed_tasks") { pool.numCompletedTasks },
      FinagleStatsReceiver.addGauge("aperture_coordinate") {
        DeterministicOrdering() match {
          case Some(coord) => coord.value.toFloat
          // We know the coordinate's range is [-1.0, 1.0], so anything outside
          // of this can be used to signify empty.
          case None => -2f
        }
      }
    )
  }

  def finagleVersion: String = _finagleVersion.get

  def finagleBuildRevision: String = _finagleBuildRevision.get

  private def tryProps(path: String): Option[Properties] = {
    try {
      val resourceOpt = Option(getClass.getResourceAsStream(path))
      resourceOpt match {
        case None =>
          log.log(Level.FINER, s"Finagle's build.properties not found: $path")
          None
        case Some(resource) =>
          try {
            val p = new Properties
            p.load(resource)
            Some(p)
          } finally {
            resource.close()
          }
      }
    } catch {
      case NonFatal(exc) =>
        log.log(
          Level.WARNING, s"Exception while loading Finagle's build.properties: $path", exc)
        None
    }
  }

  // package protected for testing
  private[finagle] def loadBuildProperties: Option[Properties] = {
    val candidates = Seq(
      "finagle-core",
      "finagle-core_2.11",
      "finagle-core_2.12"
    )
    candidates.flatMap { c => tryProps(s"/com/twitter/$c/build.properties") }
      .headOption
  }

  private[this] val once = Once {
    FinagleScheduler.init()

    val p = loadBuildProperties.getOrElse { new Properties() }

    _finagleVersion.set(p.getProperty("version", unknownVersion))
    _finagleBuildRevision.set(p.getProperty("build_revision", unknownVersion))

    log.info("Finagle version %s (rev=%s) built at %s".format(
      finagleVersion,
      finagleBuildRevision,
      p.getProperty("build_name", "?")
    ))
  }

  /**
   * Runs the initialization if it has not yet run.
   */
  def apply(): Unit = once()
}
