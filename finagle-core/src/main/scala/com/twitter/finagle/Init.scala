package com.twitter.finagle

import com.twitter.concurrent.Once
import com.twitter.finagle.exp.FinagleScheduler
import com.twitter.finagle.loadbalancer.aperture
import com.twitter.finagle.loadbalancer.aperture.ProcessCoordinate.FromInstanceId
import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.finagle.stats.FinagleStatsReceiver
import com.twitter.finagle.client.StackClient
import com.twitter.finagle.server.StackServer
import com.twitter.finagle.util.DefaultLogger
import com.twitter.finagle.util.LoadService
import com.twitter.jvm.JvmStats
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
    val apertureStats = FinagleStatsReceiver.scope("aperture")
    Seq(
      fpoolStats.addGauge("pool_size") { pool.poolSize },
      fpoolStats.addGauge("active_tasks") { pool.numActiveTasks },
      fpoolStats.addGauge("completed_tasks") { pool.numCompletedTasks },
      apertureStats.addGauge("coordinate") {
        aperture.ProcessCoordinate() match {
          case Some(coord) => coord.offset.toFloat
          // We know the coordinate's range is [0, 1.0), so anything outside
          // of this can be used to signify empty.
          case None => -1f
        }
      },
      apertureStats.addGauge("peerset_size") {
        aperture.ProcessCoordinate() match {
          case Some(FromInstanceId(_, size)) => size.toFloat
          case _ => -1f
        }
      }
    )
  }

  JvmStats.register(DefaultStatsReceiver)

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
        log.log(Level.WARNING, s"Exception while loading Finagle's build.properties: $path", exc)
        None
    }
  }

  // package protected for testing
  private[finagle] def loadBuildProperties: Option[Properties] = {
    val candidates = Seq(
      "finagle-core",
      "finagle-core_2.12",
      "finagle-core_2.13"
    )
    candidates.flatMap { c => tryProps(s"/com/twitter/$c/build.properties") }.headOption
  }

  private[this] val once = Once {
    LoadService[FinagleInit]().foreach { init =>
      try {
        init()
      } catch {
        case NonFatal(nf) =>
          log.log(Level.WARNING, s"error running ${init.label}", nf)
      }
    }

    FinagleScheduler.init()

    val p = loadBuildProperties.getOrElse { new Properties() }

    _finagleVersion.set(p.getProperty("version", unknownVersion))
    _finagleBuildRevision.set(p.getProperty("build_revision", unknownVersion))

    // Load up the client and server `Stack.Transformer`s.
    LoadService[ServerStackTransformer]().foreach { nt =>
      StackServer.DefaultTransformer.append(nt)
    }
    LoadService[ClientStackTransformer]().foreach { nt =>
      StackClient.DefaultTransformer.append(nt)
    }
    // we split out params injection from stack transformation because params are typically
    // injected at the top of the stack, and it can create confusing deps to inject them via
    // StackTransformers.  at the same time, we occasionally want to inject parameters
    // before we ever call Stack#make.  this gives us an opportunity to do it.
    LoadService[ClientParamsInjector]().foreach { nt => StackClient.DefaultInjectors.append(nt) }
    LoadService[ServerParamsInjector]().foreach { nt => StackServer.DefaultInjectors.append(nt) }

    log.info(
      "Finagle version %s (rev=%s) built at %s".format(
        finagleVersion,
        finagleBuildRevision,
        p.getProperty("build_name", "?")
      )
    )
  }

  /**
   * Runs the initialization if it has not yet run.
   */
  def apply(): Unit = once()
}
