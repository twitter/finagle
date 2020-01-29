package com.twitter.finagle.http.cookie

/**
 * SameSite cookie attribute. The server may set this in the Set-Cookie to
 * ensure that the cookie is not sent with cross-site requests.
 *
 * As of April 2018, support for this attribute is not implemented by
 * all browsers. See [0] for more details.
 *
 * [0] https://tools.ietf.org/html/draft-west-first-party-cookies-07
 */
sealed abstract class SameSite private ()

object SameSite {

  /**
   * See [1] for the distinction between the Lax and Strict types.
   * [1] https://tools.ietf.org/html/draft-west-first-party-cookies-07#section-4.1.1
   */
  case object Lax extends SameSite

  case object Strict extends SameSite

  case object None extends SameSite

  /**
   * Represents the attribute not being set on the Cookie.
   */
  case object Unset extends SameSite

  /**
   * Converts the given SameSite value (string) to the corresponding
   * SameSite object. Anything not Lax or Strict is considered Unset.
   * Note that this is case-sensitive.
   */
  def fromString(s: String): SameSite = s match {
    case "Lax" => Lax
    case "Strict" => Strict
    case "None" => None
    case _ => Unset
  }

}
