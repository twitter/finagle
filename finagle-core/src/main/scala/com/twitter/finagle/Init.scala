package com.twitter.finagle

import com.twitter.finagle.exp.FinagleScheduler
import com.twitter.util.NonFatal
import java.util.Properties
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.logging.{Level, Logger}


/**
 * Global initialization of Finagle.
 */
private object Init {
  private val inited = new AtomicBoolean(false)
  private val log = Logger.getLogger("finagle")

  // Used to record Finagle versioning in trace info.
  private val unknownVersion = "?"
  private val _finagleVersion = new AtomicReference[String](unknownVersion)
  def finagleVersion = _finagleVersion.get

  def apply() {
    if (!inited.compareAndSet(false, true))
      return

    FinagleScheduler.init()

    val p = new Properties
    try {
      val resource = getClass.getResource("/com/twitter/finagle-core/build.properties")
      if (resource == null)
        log.log(Level.WARNING, "Finagle's build.properties not found")
      else
        p.load(resource.openStream())
    } catch {
      case NonFatal(exc) =>
        log.log(Level.WARNING, "Exception while loading finagle's build.properties", exc)
    }

    _finagleVersion.set(p.getProperty("version", unknownVersion))

    log.info("Finagle version %s (rev=%s) built at %s".format(
      finagleVersion,
      p.getProperty("build_revision", "?"),
      p.getProperty("build_name", "?")
    ))
  }
}
