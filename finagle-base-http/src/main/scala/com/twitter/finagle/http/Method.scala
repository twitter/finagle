package com.twitter.finagle.http

/**
 * Represents the HTTP method.
 *
 * For Java-friendly enums, see [[com.twitter.finagle.http.Methods]].
 */
sealed abstract class Method(name: String) {
  override def toString: String = name
}

object Method {
  case object Get extends Method("GET")
  case object Post extends Method("POST")
  case object Put extends Method("PUT")
  case object Head extends Method("HEAD")
  case object Patch extends Method("PATCH")
  case object Delete extends Method("DELETE")
  case object Trace extends Method("TRACE")
  case object Connect extends Method("CONNECT")
  case object Options extends Method("OPTIONS")

  private case class Custom(name: String) extends Method(name)

  /**
   * Construct a Method.
   *
   * Note: We are conservative here and ignore the case of `name` for the
   * common method types. This makes it impossible to construct a GET method,
   * for example, accidentally with the wrong case. For other names, not part
   * of the common methods, we observe the case.
   */
  def apply(name: String): Method = name.toUpperCase match {
    case "GET" => Get
    case "POST" => Post
    case "PUT" => Put
    case "HEAD" => Head
    case "PATCH" => Patch
    case "DELETE" => Delete
    case "TRACE" => Trace
    case "CONNECT" => Connect
    case "OPTIONS" => Options
    case method => Custom(name)
  }
}
