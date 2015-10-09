package com.twitter.finagle.memcached.exp

import com.twitter.app.{Flaggable, GlobalFlag}

/**
 * Note: This is a temporary workaround to use local memcache and will be replaced with Dtab override soon.
 *
 * Restriction: If a service uses more than one memcache cluster, it cannot use local memcache by this way.
 *
 * Todo: Depracate it once memcache has wily support (TRFC-434).
 */

object localMemcachedPort extends GlobalFlag[Option[Int]] (
  None,
  "port to use for local memcached; " +
    "this is a temporary workaround and will be deprecated once memcache has wily support."
)(new Flaggable[Option[Int]] {
    def parse(s: String) = s match {
      case "" => None
      case value => Some(value.toInt)
    }
    override def show(intOpt: Option[Int]) = intOpt.map(_.toString).getOrElse("Not defined")
})

object LocalMemcached {
  def enabled: Boolean = localMemcachedPort().isDefined
  def port: Int = localMemcachedPort().getOrElse(
    throw new IllegalArgumentException("localMemcached port is not defined.")
  )
}