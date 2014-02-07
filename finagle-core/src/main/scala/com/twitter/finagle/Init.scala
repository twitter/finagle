package com.twitter.finagle

import com.twitter.util.NonFatal
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.{Level, Logger}

/**
 * Global initialization of Finagle.
 */
private object Init {
  private val inited = new AtomicBoolean(false)
  private val log = Logger.getLogger("finagle")

  def apply() {
    if (!inited.compareAndSet(false, true))
      return

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

    log.info("Finagle version %s (rev=%s) built at %s".format(
      p.getProperty("version", "?"), p.getProperty("build_revision", "?"),
      p.getProperty("build_name", "?")))
  }
}
