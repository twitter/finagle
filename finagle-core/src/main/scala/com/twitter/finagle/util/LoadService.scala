package com.twitter.finagle.util

import scala.reflect.ClassTag

/**
 * Load a singleton class in the manner of [[java.util.ServiceLoader]]. It is
 * more resilient to varying Java packaging configurations than ServiceLoader.
 *
 * @see `com.twitter.app.LoadService` in util-app
 */
object LoadService {

  def apply[T: ClassTag](): Seq[T] =
    com.twitter.app.LoadService()

}
