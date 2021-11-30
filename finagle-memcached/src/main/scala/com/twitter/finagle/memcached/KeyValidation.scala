package com.twitter.finagle.memcached

import com.twitter.io.Buf
import com.twitter.finagle.memcached.util.Bufs.nonEmptyStringToBuf

/**
 * Cache command key validation logic. Valid keys are:
 * - not null,
 * - not empty,
 * - not longer than 250 bytes, and
 * - do not contain any of the invalid characters `['\n', '\r', ' ', '\u0000']`.
 *
 * All cache commands accepting key/keys should test those keys with these tools.
 */
object KeyValidation {

  /** Enumerates possible outcomes of a key validation */
  private[this] sealed trait KeyValidationResult

  /** The key is valid */
  private[this] case object ValidKey extends KeyValidationResult

  /** The key is null or empty */
  private[this] case object NullKey extends KeyValidationResult

  /** The key is too long */
  private[this] case object KeyTooLong extends KeyValidationResult

  /**
   * The key has at least one invalid character in it
   * @param ch value of the first invalid character
   */
  private[this] case class InvalidCharacter(ch: Byte) extends KeyValidationResult

  private[this] val MaxKeyLength = 250

  private[this] val KeyCheck: Buf => Unit = checkKey(_)

  private[this] object processor extends Buf.Processor {
    def apply(byte: Byte): Boolean = !invalidChar(byte)
  }

  /**
   * Checks a collection of keys ensuring that all keys are valid as defined by
   * the `checkKey` function.
   */
  private[memcached] def checkKeys(keys: Iterable[Buf]): Unit = {
    // Validating keys
    val ks = keys
    if (ks == null)
      throw new IllegalArgumentException("Invalid keys: cannot have null for keys")

    ks.foreach(KeyCheck)
  }

  /**
   * Check a key to ensure validity. See the object description for validity criteria.
   */
  private[memcached] def checkKey(key: Buf): Unit =
    validateKeyBuf(key) match {
      case ValidKey => ()
      case NullKey =>
        throw new IllegalArgumentException("Invalid keys: key cannot be null")
      case KeyTooLong =>
        throw new IllegalArgumentException(
          s"Invalid keys: key cannot be longer than $MaxKeyLength bytes (${key.length})"
        )
      case InvalidCharacter(ch) =>
        throw new IllegalArgumentException(
          f"Invalid keys: key cannot have whitespace or control characters: '0x${ch}%x'"
        )
    }

  /**
   * Determines whether the key is valid. Invalid keys will cause an
   * exception to be thrown when passed into various methods on the client.
   * This method is suitable for gracefully prevalidating keys.
   *
   * @param key the key to check
   *
   * @return true if the key is valid, false otherwise
   */
  def validateKey(key: String): Boolean = validateKeyBuf(key) match {
    case ValidKey => true
    case InvalidCharacter(_) | KeyTooLong | NullKey => false
  }

  private[this] def validateKeyBuf(key: Buf): KeyValidationResult =
    if (key == null) {
      NullKey
    } else if (tooLong(key)) {
      KeyTooLong
    } else {
      val index = invalidByteIndex(key)
      if (index != -1) {
        val ch = key.get(index)
        InvalidCharacter(ch)
      } else {
        ValidKey
      }
    }

  /** Returns -1 if no invalid bytes */
  private def invalidByteIndex(key: Buf): Int = key.process(processor)

  private def tooLong(key: Buf): Boolean = key.length > MaxKeyLength

  private def invalidChar(b: Byte): Boolean =
    b <= ' ' && (b == '\n' || b == '\u0000' || b == '\r' || b == ' ')
}
