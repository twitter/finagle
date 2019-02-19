package com.twitter.finagle.http

/**
 * Represents the HTTP method.
 *
 * The method is a case-sensitive string defined as part of the request
 * line of the HTTP protocol.
 *
 * @param name case sensitive `String` representation of the HTTP method.
 *
 * @see https://tools.ietf.org/html/rfc7230#section-3.1.1
 */
final class Method private (val name: String) {
  override def toString: String = name

  override def equals(other: Any): Boolean = other match {
    case other: Method => (this eq other) || this.name == other.name
    case _ => false
  }

  override def hashCode(): Int = name.hashCode
}

object Method {

  val Get: Method = new Method("GET")
  val Post: Method = new Method("POST")
  val Put: Method = new Method("PUT")
  val Head: Method = new Method("HEAD")
  val Patch: Method = new Method("PATCH")
  val Delete: Method = new Method("DELETE")
  val Trace: Method = new Method("TRACE")
  val Connect: Method = new Method("CONNECT")
  val Options: Method = new Method("OPTIONS")

  /**
   * Construct a Method.
   *
   * Note: We are conservative here and ignore the case of `name` for the
   * common method types. This makes it impossible to construct a GET method,
   * for example, accidentally with the wrong case. For other names, not part
   * of the common methods, we preserve the case.
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
    case _ => new Method(name)
  }
}
