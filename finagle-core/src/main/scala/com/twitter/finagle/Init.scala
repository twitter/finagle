package com.twitter.finagle

import com.twitter.concurrent.Once
import com.twitter.finagle.exp.FinagleScheduler
import com.twitter.finagle.util.DefaultLogger
import com.twitter.util.NonFatal
import java.util.Properties
import java.util.concurrent.atomic.AtomicReference
import java.util.logging.Level

/**
 * Global initialization of Finagle.
 */
private[twitter] object Init {
  private val log = DefaultLogger

  // Used to record Finagle versioning in trace info.
  private val unknownVersion = "?"
  private val _finagleVersion = new AtomicReference[String](unknownVersion)
  private val _finagleBuildRevision = new AtomicReference[String](unknownVersion)

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
      "finagle-core_2.10",
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
