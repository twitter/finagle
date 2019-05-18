package com.twitter.finagle.http

import com.twitter.finagle.http.cookie.SameSite
import com.twitter.util.Duration
import java.util.{BitSet => JBitSet}

object Cookie {

  private[finagle] val DefaultMaxAge: Duration = Duration.Bottom

  private[this] val IllegalNameChars: Set[Char] =
    Set('\t', '\n', '\u000b', '\f', '\r', ' ', ',', ';', '=')
  private[this] val IllegalValueChars: Set[Char] = Set('\n', '\u000b', '\f', '\r', ';')

  private[this] val IllegalNameCharsBitSet: JBitSet = {
    val bs = new JBitSet
    IllegalNameChars.foreach(bs.set(_))
    bs
  }

  private[this] val IllegalValueCharsBitSet: JBitSet = {
    val bs = new JBitSet
    IllegalValueChars.foreach(bs.set(_))
    bs
  }

  private[http] def stringContains(string: String, chars: JBitSet): Boolean = {
    var i = 0
    val until = string.length
    while (i < until && !chars.get(string.charAt(i))) {
      i += 1
    }
    i != until
  }

  // These are the same checks made by Netty's DefaultCookie
  private def validateName(name: String): String = {
    val trimmed = name.trim
    if (trimmed.isEmpty) throw new IllegalArgumentException("Cookie name cannot be empty")
    else {
      if (stringContains(trimmed, IllegalNameCharsBitSet))
        throw new IllegalArgumentException(
          s"Cookie name contains one of the following prohibited characters: ${IllegalNameChars
            .mkString(",")}: $trimmed"
        )
      else
        trimmed
    }
  }

  // These are the same checks made by Netty's DefaultCookie
  private def validateField(field: String): String = {
    if (field == null) field
    else {
      val trimmed = field.trim
      if (trimmed.isEmpty) {
        null
      } else if (stringContains(trimmed, IllegalValueCharsBitSet))
        throw new IllegalArgumentException(
          s"Cookie field contains one of the following prohibited characters: ${IllegalValueChars
            .mkString(",")}: $trimmed"
        )
      else trimmed
    }
  }
}

/**
 * @note `domain` and `path` may be null.
 */
final class Cookie private (
  val name: String,
  val value: String,
  val domain: String,
  val path: String,
  private[this] val _maxAge: Option[Duration],
  val secure: Boolean,
  val httpOnly: Boolean,
  val sameSite: SameSite) { self =>

  /**
   * Create a cookie.
   */
  def this(
    name: String,
    value: String,
    domain: Option[String] = None,
    path: Option[String] = None,
    maxAge: Option[Duration] = None,
    secure: Boolean = false,
    httpOnly: Boolean = false,
    sameSite: SameSite = SameSite.Unset
  ) = this(
    name = Cookie.validateName(name),
    value = value,
    domain = Cookie.validateField(domain.orNull),
    path = Cookie.validateField(path.orNull),
    _maxAge = maxAge,
    secure = secure,
    httpOnly = httpOnly,
    sameSite = sameSite
  )

  def this(name: String, value: String) = this(
    name,
    value,
    None,
    None,
    None,
    false,
    false,
    SameSite.Unset
  )

  def this(
    name: String,
    value: String,
    domain: Option[String],
    path: Option[String],
    maxAge: Option[Duration],
    secure: Boolean,
    httpOnly: Boolean
  ) = this(
    name,
    value,
    domain,
    path,
    maxAge,
    secure,
    httpOnly,
    SameSite.Unset
  )

  def maxAge: Duration = _maxAge match {
    case Some(maxAge) => maxAge
    case None => Cookie.DefaultMaxAge
  }

  // Helper method for `equals` that returns true if two strings are both null, or have the
  // same value (ignoring case)
  private[this] def stringsEqual(s0: String, s1: String): Boolean = {
    if (s0 == null) {
      if (s1 == null) true
      else false
    } else {
      if (s1 == null) false
      else s0.equalsIgnoreCase(s1)
    }
  }

  private[this] def copy(
    name: String = self.name,
    value: String = self.value,
    domain: Option[String] = Option(self.domain),
    path: Option[String] = Option(self.path),
    maxAge: Option[Duration] = self._maxAge,
    secure: Boolean = self.secure,
    httpOnly: Boolean = self.httpOnly,
    sameSite: SameSite = self.sameSite
  ): Cookie =
    new Cookie(
      name,
      value,
      domain,
      path,
      maxAge,
      secure,
      httpOnly,
      sameSite
    )

  /**
   * Create a new [[Cookie]] with the same set fields, and domain `domain`.
   */
  def domain(domain: Option[String]): Cookie =
    copy(domain = domain)

  /**
   * Create a new [[Cookie]] with the same set fields, and value `value`.
   */
  def value(value: String): Cookie =
    copy(value = value)

  /**
   * Create a new [[Cookie]] with the same set fields, and maxAge `maxAge`
   */
  def maxAge(maxAge: Option[Duration]): Cookie =
    copy(maxAge = maxAge)

  /**
   * Create a new [[Cookie]] with the same set fields, and path `path`.
   */
  def path(path: Option[String]): Cookie =
    copy(path = path)

  /**
   * Create a new [[Cookie]] with the same set fields, and httpOnly `httpOnly`
   */
  def httpOnly(httpOnly: Boolean): Cookie =
    copy(httpOnly = httpOnly)

  /**
   * Create a new [[Cookie]] with the same set fields, and secure `secure`
   */
  def secure(secure: Boolean): Cookie =
    copy(secure = secure)

  /**
   * Create a new [[Cookie]] with the same set fields, and sameSite `sameSite`
   */
  def sameSite(sameSite: SameSite): Cookie =
    copy(sameSite = sameSite)

  /**
   * Returns true if `obj` equals `this`. Two cookies are considered equal if their names,
   * paths, and domains are the same (ignoring case). This mimics the `equals` method on Netty's
   * DefaultCookie.
   */
  override def equals(obj: Any): Boolean = obj match {
    case c: Cookie =>
      name.equalsIgnoreCase(c.name) && stringsEqual(path, c.path) && stringsEqual(domain, c.domain)
    case _ => false
  }

  /**
   * From Netty 3's DefaultCookie
   */
  override def hashCode(): Int =
    name.hashCode
}
