package com.twitter.finagle.builder

import java.util.{Collections, IdentityHashMap}
import scala.collection.JavaConverters._

object Registry {
  private[this] val active = Collections.synchronizedMap(new IdentityHashMap[Object, String])

  Runtime.getRuntime().addShutdownHook(new Thread {
    override def run() {
      for (desc <- active.asScala.values)
        System.err.printf("[Finagle/not closed] %s\n", desc)
    }
  })

  def add(x: Object, desc: String) = {
    active.put(x, desc)
  }

  def del(x: Object) = {
    active.remove(x)
  }
}