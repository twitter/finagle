package com.twitter.finagle.serverset2

import com.twitter.finagle.util.LoadService

/**
 * An Identity provides identifying metadata for a process.
 * Identifiers provide a uniform method of accessing identifying
 * data for processes running in different environments.
 *
 * They are identified by a `scheme` and `id`. The `scheme` provides
 * the name of the Identifier, and provides a context for `id`. The
 * `id` will be None if the Identifier cannot resolve information
 * for the process, or Some[String] if information is available.
 *
 * Identities have an associated priority, to allow for ordering
 * when enumerating multiple Identities.
 *
 * These are loaded by Finagle through the
 * [[com.twitter.finagle.util.LoadService service loading mechanism]]. Thus, in
 * order to implement a new Identity, a class implementing `Identity` with a
 * 0-arg constructor must be registered in a file named
 * `META-INF/services/com.twitter.finagle.serverset2.Identity` included in the
 * classpath; see Oracle's
 * [[http://docs.oracle.com/javase/6/docs/api/java/util/ServiceLoader.html ServiceLoader]]
 * documentation for further details.
 */
trait Identity {
  val scheme: String
  val id: Option[String]
  val priority: Int
}

/**
 * Identity that represents the user a process is running as.
 *
 * `scheme`: "user"
 * `id`: Some(username) if available
 * `priority`: 99
 */
class UserIdentity extends Identity {
  val scheme = "user"
  val id = Some(System.getProperty("user.name"))
  val priority = 99
}

/**
 * Access all available Identities
 *
 * Identities are formatted as "/scheme/id".
 */
private[finagle] object Identities {
  private def filter(c: Identity): Option[String] = c.id match {
    case Some(id) => Some("/%s/%s".format(c.scheme, id))
    case _ => None
  }

  /**
   * Return all available Identities
   *
   * @return a Sequence of Identities presented as "/scheme/id"
   */
  def get(): Seq[String] = {
    LoadService[Identity]()
      .sortBy(_.priority)
      .flatMap(filter)
  }
}
