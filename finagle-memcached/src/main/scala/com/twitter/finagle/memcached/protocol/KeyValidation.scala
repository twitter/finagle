package com.twitter.finagle.memcached.protocol

import com.twitter.io.Buf

/**
 * Cache command key validation logic.
 * The validation is done by searching each buffer for invalid character defined as
 * any of the set ['\n', '\r', ' ', '\u0000'].
 *
 * All cache commands accepting key/keys should test those keys with these tools.
 */
private[memcached] object KeyValidation {
  private[this] val MaxKeyLength = 250

  private[this] val KeyCheck: Buf => Unit = checkKey(_)

  private[this] object processor extends Buf.Processor {
    def apply(byte: Byte): Boolean = !invalidChar(byte)
  }

  /**
   * Checks a collection of keys ensuring that all keys are valid as defined by
   * the `checkKey` function.
   */
  def checkKeys(keys: Iterable[Buf]): Unit = {
    // Validating keys
    val ks = keys
    if (ks == null)
      throw new IllegalArgumentException("Invalid keys: cannot have null for keys")

    ks.foreach(KeyCheck)
  }

  /**
   * Check a key to ensure validity. For a key to be considered valid it must not
   * - be `null`
   * - be longer than 250 bytes (ascii chars) in length
   * - contain any of the invalid characters ['\n', '\r', ' ', '\u0000']
   */
  def checkKey(key: Buf): Unit = {
    if (key == null)
      throw new IllegalArgumentException("Invalid keys: key cannot be null")

    if (tooLong(key))
      throw new IllegalArgumentException(
        s"Invalid keys: key cannot be longer than $MaxKeyLength bytes (${key.length})"
      )

    val index = invalidByteIndex(key)
    if (index != -1) {
      val ch = key.get(index)
      throw new IllegalArgumentException(
        f"Invalid keys: key cannot have whitespace or control characters: '0x${ch}%x'"
      )
    }
  }

  /** Returns -1 if no invalid bytes */
  private def invalidByteIndex(key: Buf): Int = key.process(processor)

  private def tooLong(key: Buf): Boolean = key.length > MaxKeyLength

  private def invalidChar(b: Byte): Boolean =
    b <= ' ' && (b == '\n' || b == '\u0000' || b == '\r' || b == ' ')
}
