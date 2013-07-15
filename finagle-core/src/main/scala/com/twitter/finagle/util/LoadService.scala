package com.twitter.finagle.util

import java.util.ServiceLoader
import scala.collection.JavaConverters._

/**
 * Load a singleton class using [[java.util.ServiceLoader]].
 */
object LoadService {
  def apply[T: ClassManifest](): Seq[T] = {
    val clazz = implicitly[ClassManifest[T]].erasure.asInstanceOf[Class[T]]
    ServiceLoader.load(clazz, clazz.getClassLoader).iterator().asScala.toList
  }
}
