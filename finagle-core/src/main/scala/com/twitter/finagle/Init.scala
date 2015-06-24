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

  def finagleVersion: String = _finagleVersion.get

  private def tryProps(path: String): Option[Properties] = {
    try {
      val resource = getClass.getResource(path)
      if (resource == null) {
        log.log(Level.FINER, s"Finagle's build.properties not found: $path")
        None
      } else {
        val p = new Properties
        p.load(resource.openStream())
        Some(p)
      }
    } catch {
      case NonFatal(exc) =>
        log.log(
          Level.WARNING, s"Exception while loading finagle's build.properties: $path", exc)
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

    log.info("Finagle version %s (rev=%s) built at %s".format(
      finagleVersion,
      p.getProperty("build_revision", "?"),
      p.getProperty("build_name", "?")
    ))
  }

  /**
   * Runs the initialization if it has not yet run.
   */
  def apply(): Unit = once()
}
